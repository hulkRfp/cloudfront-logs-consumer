FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

# 默认 config 由 K8S ConfigMap 挂载到 /etc/consumer/config.yaml
CMD ["python", "main.py", "--config", "/etc/consumer/config.yaml"]
