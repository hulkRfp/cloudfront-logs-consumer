# CloudFront Kinesis Log Consumer

从 AWS Kinesis Data Stream 实时消费 CloudFront 日志并写入 Apache Doris。

## 功能特性

- 实时消费 Kinesis 按需模式 stream，自动感知 shard 扩缩容（分裂/合并）
- 日志字段支持重命名、删除、新增，非重点字段自动合并到 `extras` 列（JSON 格式）
- 特殊字符（SQL 注入、XSS、Unicode 等）安全保留，经 JSON 序列化后正确入库
- 单条日志异常自动跳过，不影响整批处理
- Kinesis 拉取和 Doris 写入均有重试机制（指数退避，最多 5 次）
- 支持 K8S 水平扩展，基于 Redis 分布式锁动态分配 shard，Pod 数少于 shard 数时单个 Pod 自动持有多个 shard，随时扩缩容无需手动配置
- 支持历史数据补跑（单次运行，完成后自动退出，支持断点续跑，不影响正常消费位点）
- 支持调试模式，输出处理后的日志格式到控制台，不写 Doris
- 配置支持 YAML 文件和环境变量两种方式，环境变量优先级更高

## 文件结构

```
cloudfront-log-consumer/
├── main.py              # 入口：参数解析、组件组装、启动
├── consumer.py          # StreamConsumer：Kinesis 消费主循环；run_debug_format：调试模式
├── lock.py              # ShardLockManager：Redis 分布式锁，shard 动态分配与 rebalance
├── checkpoint.py        # Checkpoint：Redis 消费位点管理（降级时内存存储）
├── transformer.py       # Transformer：CloudFront 日志字段解析与转换
├── writer.py            # DorisWriter：Doris Stream Load 写入，含重试
├── config.py            # 配置加载（YAML + 环境变量覆盖）、JSON 日志初始化
├── config.yaml          # 配置文件
├── requirements.txt     # Python 依赖
├── Dockerfile           # 镜像构建
└── k8s-deployment.yaml  # K8S 部署清单（Deployment + Job + ServiceAccount）
```

## 快速开始

**安装依赖**

```bash
pip install -r requirements.txt
```

**配置 AWS 凭证**（任选其一）

```bash
# 方式一：环境变量
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
export AWS_DEFAULT_REGION=us-east-1

# 方式二：IAM Role（EKS/EC2 推荐）
# 直接绑定 Role，无需配置凭证
```

**启动消费**

```bash
python main.py
python main.py --config /path/to/config.yaml  # 指定配置文件
```

## 配置说明

### config.yaml

```yaml
kinesis:
  region: "us-east-1"
  stream_name: "cloudfront-realtime-logs"
  batch_size: 5000                # 每次 GetRecords 拉取条数上限
  poll_interval_active: 1.0       # 有数据时的拉取间隔（秒）
  poll_interval_idle: 30.0        # 无数据时的拉取间隔（秒）
  shard_refresh_interval: 60      # shard 列表刷新间隔（秒）
  initial_position: "LATEST"      # 首次消费位置：LATEST / TRIM_HORIZON / AT_TIMESTAMP
  # initial_timestamp: "2026-03-15T10:00:00+00:00"  # AT_TIMESTAMP 时生效

doris:
  fe_host: "doris-fe"
  fe_port: 8030
  database: "your_db"
  table: "cloudfront_logs"
  username: "root"
  password: "your_password"
  batch_size: 5000                # 攒够多少条触发一次写入（与 flush_interval 双重触发，满足其一即 flush）
  flush_interval: 30.0            # 超时强制 flush（秒）；日志量小时数据最多延迟此时间落盘

transform:
  source_fields:                  # CloudFront 日志原始字段，按 tab 顺序排列
    - timestamp
    - c-ip
    - sc-status
    # ...

  target_fields:                  # 写入 Doris 的目标字段列表，含 extras 则启用扩展字段
    - log_time
    - client_ip
    - extras                      # 不在此列表中的字段自动归入 extras（JSON 字符串）

  rename:                         # 字段重命名，原名 -> 新名
    timestamp: log_time
    "c-ip": client_ip

  drop_fields:                    # 直接丢弃的字段
    - x-edge-response-result-type

  add_fields:                     # 新增字段
    source: "literal:cloudfront"  # 固定值（需同时加入 target_fields，否则会归入 extras）
    env_name: "env:APP_ENV"       # 从环境变量读取

  url_decode_fields:              # 需要 URL 解码的字段（保留原始特殊字符）
    - uri_query
    - referer
    - user_agent
```

### 环境变量覆盖

格式：`CF_<SECTION>__<KEY>=value`，双下划线表示层级，自动按原始类型转换。

```bash
CF_KINESIS__STREAM_NAME=cloudfront-realtime-logs
CF_KINESIS__REGION=ap-east-1
CF_DORIS__FE_HOST=10.0.0.1
CF_DORIS__DATABASE=prod_db
CF_DORIS__PASSWORD=secret
```

## 运行模式

### 正常消费（持续运行）

```bash
python main.py
```

### 查看消费位点

```bash
python main.py --list-checkpoints
```

输出示例：
```
=== cloudfront-realtime-logs ===
{
  "shardId-000000000000": "49651234567890123456789012345678901234567890123456789",
  "shardId-000000000001": "49651234567890123456789012345678901234567890123456790"
}
```

### 补跑历史数据

从指定时间段消费一次后自动退出，不影响正常消费的 checkpoint。

补跑保证：
- 不漏：用 `FROM_TIMESTAMP` 拿到所有相关 shard（含 split/merge 产生的 CLOSED parent shard），每个 shard 从 `AT_TIMESTAMP` 消费到结束或超过 `end_time`
- 不重：每批写入后保存独立的 backfill checkpoint（Redis key 前缀 `cf:backfill-checkpoint:`），中断后重启从断点续跑；无 Redis 时从头重跑（Doris label 幂等保证不重复入库）
- 不错：`ApproximateArrivalTimestamp` 超过 `end_time` 的记录自动跳过，该 shard 停止消费

```bash
# 补跑指定时间段
python main.py --backfill \
  --start-time 2026-03-01T00:00:00+00:00 \
  --end-time   2026-03-02T00:00:00+00:00

# 只指定起始时间，消费到当前最新
python main.py --backfill --start-time 2026-03-01T00:00:00+00:00
```

### 调试日志格式

拉取指定数量的日志，输出经 transformer 处理后的 JSON 结构到控制台，**不写 Doris，不更新 checkpoint**。

```bash
# 从最早的数据拉 10 条（默认）
python main.py --debug-format

# 从指定时间点拉 20 条
python main.py --debug-format \
  --start-time 2026-03-17T08:00:00+00:00 \
  --limit 20
```

输出示例：
```json
[
  {
    "log_time": "2026-03-17 08:00:01",
    "client_ip": "1.2.3.4",
    "status_code": 200,
    "method": "GET",
    "host": "example.com",
    "uri_path": "/index.html",
    "uri_query": "foo=bar&baz=1",
    "time_taken": 0.023,
    "bytes_sent": 1024,
    "bytes_received": 512,
    "edge_result": "Hit",
    "edge_location": "NRT12-C1",
    "referer": null,
    "user_agent": "Mozilla/5.0 ...",
    "ssl_protocol": "TLSv1.3",
    "asn": 4134,
    "source": "cloudfront",
    "extras": "{\"origin-fbl\": null, \"time-to-first-byte\": 0.01}"
  }
]
```

## K8S 部署

### 持续消费（Deployment）

基于 Redis 分布式锁实现 shard 动态分配，每个 Pod 启动后自动抢占空闲 shard，无需手动配置分片序号。

**shard 分配机制：**
- 每次 shard 列表刷新时（默认 60 秒），每个 Pod 对所有未在本地消费的 shard 尝试抢锁
- 一个 Pod 可持有多个 shard 的锁，shard 数 > Pod 数时自动均摊，确保所有 shard 都有消费者
- Pod 下线后，其持有的锁在 TTL 到期（默认 30 秒）后自动释放，其他 Pod 在下次 rebalance 时接管，最大空窗期 = `lock_ttl + shard_refresh_interval`（默认 90 秒）
- shard split/merge 后，parent shard 进入 CLOSED 状态，`GetRecords` 返回 `NextShardIterator=null`，Pod 自动结束该 shard 消费并释放锁，转而消费 child shard

需在 config.yaml 或通过环境变量配置 Redis 连接（checkpoint 和分布式锁共用同一 Redis 实例）：

```bash
CF_REDIS__HOST=redis-host
CF_REDIS__PASSWORD=secret
```

直接调整 `replicas` 即可扩缩容，无需其他操作：

```bash
kubectl scale deployment cloudfront-consumer --replicas=4
```

> 注意：未配置 Redis 时应用以单实例模式启动（打印 WARNING），checkpoint 降级为内存存储（Pod 重启后丢失）。已配置 Redis 但连接失败时应用直接退出，不会在不确定状态下运行。backfill/debug 模式不连接 Redis，可独立运行。

### 补跑历史数据（Job）

修改 `k8s-deployment.yaml` 中 Job 的 `--start-time` / `--end-time` 参数后执行：

```bash
kubectl apply -f k8s-deployment.yaml
kubectl logs -f job/cloudfront-consumer-backfill
```

Job 完成后自动退出，1 小时后自动清理（`ttlSecondsAfterFinished: 3600`）。

### 构建镜像

```bash
docker build -t your-registry/cloudfront-consumer:latest .
docker push your-registry/cloudfront-consumer:latest
```

## IAM 权限

消费 Kinesis 所需的最小权限：

```json
{
  "Effect": "Allow",
  "Action": [
    "kinesis:GetRecords",
    "kinesis:GetShardIterator",
    "kinesis:ListShards",
    "kinesis:DescribeStream"
  ],
  "Resource": "arn:aws:kinesis:*:*:stream/cloudfront-*"
}
```

EKS 环境推荐使用 IRSA（IAM Roles for Service Accounts），在 `k8s-deployment.yaml` 的 ServiceAccount 上配置 `eks.amazonaws.com/role-arn` 注解。
