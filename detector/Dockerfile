# syntax=docker/dockerfile:1.7
FROM python:3.14-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip build && \
    pip wheel --no-deps -w /wheels . && \
    pip wheel -w /wheels '.[ha,sinks]'

FROM python:3.14-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PROMANOMALY_LISTEN=":9092"

RUN groupadd --system --gid 1000 promanomaly && \
    useradd  --system --uid 1000 --gid promanomaly --home-dir /app promanomaly && \
    mkdir -p /app /etc/promanomaly && \
    chown -R promanomaly:promanomaly /app /etc/promanomaly

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels 'promanomaly[ha,sinks]' && \
    rm -rf /wheels

USER promanomaly:promanomaly

EXPOSE 9092

ENTRYPOINT ["promanomaly"]
CMD ["--help"]
