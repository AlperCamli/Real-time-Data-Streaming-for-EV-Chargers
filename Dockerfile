FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python \
    redis \
    clickhouse-driver \
    pyyaml \
    prometheus-client

COPY src ./src
COPY config ./config

CMD ["python", "-m", "src.processor.main", "--config", "config/processor.default.yaml"]
