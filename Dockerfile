# Indian Trading Bot — Isolated Multi-Tenant Trading Platform
# Python 3.12-slim, no shared volumes with existing containers
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install wheels from local wheelhouse.
COPY wheelhouse/ /tmp/wheelhouse/
RUN pip install --no-deps /tmp/wheelhouse/*.whl 2>/dev/null || true && \
    rm -rf /tmp/wheelhouse

# upstox_client ships as a raw package dir (no setup.py/pyproject.toml), so
# pip install of a directory fails. Copy it straight into site-packages.
COPY wheelhouse/upstox_client /tmp/upstox_client_pkg/
RUN SITE=$(python3 -c "import site; print(site.getsitepackages()[0])") && \
    if [ -d /tmp/upstox_client_pkg ]; then \
        cp -r /tmp/upstox_client_pkg "$SITE"/upstox_client && \
        echo "upstox_client installed into $SITE"; \
    else \
        echo "WARN: upstox_client package not found in wheelhouse"; \
    fi && \
    rm -rf /tmp/upstox_client_pkg

# Copy application code
COPY . .

# Copy strategy catalog (immutable snapshots from Hermes engine)
COPY catalog/ /app/catalog/

# Create non-root user
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "core.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
