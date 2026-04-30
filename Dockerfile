# ── Base stage ────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS base

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY protocol.py cache.py ./
COPY server.py server_async.py server_hybrid.py ./
COPY client.py launcher.py ./

# ── Server target ─────────────────────────────────────────────────────────────
FROM base AS server
EXPOSE 9000
CMD ["python", "server_hybrid.py"]

# ── Client target ─────────────────────────────────────────────────────────────
FROM base AS client
VOLUME ["/data", "/received"]
ENTRYPOINT ["python", "client.py"]
