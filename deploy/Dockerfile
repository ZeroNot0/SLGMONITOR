# SLG Monitor 3.0 - 腾讯云托管 / 任意 Docker 环境
# 构建：在项目根目录执行 docker build -f deploy/Dockerfile -t slg-monitor:latest .
# 运行：docker run -d -p 8000:8000 --name slg-monitor slg-monitor:latest

FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python", "server/start_server.py", "--port", "8000"]
