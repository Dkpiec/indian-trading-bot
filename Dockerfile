# Indian Trading Bot — Isolated Multi-Tenant Trading Platform
# Python 3.12-slim, uses wheelhouse for fast offline installs
FROM python:3.12-slim

SHELL ["/bin/bash", "-c"]

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Kolkata

WORKDIR /app

# Install system deps (curl, wget, procps for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies from wheelhouse (fast, offline)
COPY wheelhouse ./wheelhouse
COPY requirements.txt .
RUN pip install --no-cache-dir --find-links ./wheelhouse -r requirements.txt

# Copy application code
COPY . .

# Copy strategy catalog (immutable snapshots from Hermes engine)
COPY catalog/ /app/catalog/

# Entrypoint supervises uvicorn with restart-on-crash backoff (like trading engine)
RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=60s \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["/app/entrypoint.sh"]