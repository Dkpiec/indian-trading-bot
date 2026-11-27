#!/bin/sh
# Indian Trading Bot entrypoint — phased dependency install + supervised uvicorn

set -u

WHEELHOUSE=/app/wheelhouse

mkdir -p /app/logs 2>/dev/null || true
echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] entrypoint start (pid $$)" >> /app/logs/entrypoint_boot.log 2>/dev/null || true

install() {
    if [ -d "$WHEELHOUSE" ]; then
        # Try wheelhouse first, fall back to PyPI if package not found
        pip install --no-cache-dir --no-index --find-links="$WHEELHOUSE" "$@" 2>/dev/null || \
        pip install --no-cache-dir "$@"
    else
        pip install --no-cache-dir "$@"
    fi
}

echo "[entrypoint] phase 1: control-plane deps" >&2
# Minimal deps for core.main to import and uvicorn to start
# Use set -o pipefail to catch install failures in pipeline
set -o pipefail
install fastapi "uvicorn[standard]" pydantic pydantic-settings "sqlalchemy[asyncio]" asyncpg redis structlog cryptography pyjwt httpx python-multipart aiofiles tenacity 2>&1 | tail -2
set +o pipefail

# Healthcheck tools
if ! command -v curl >/dev/null 2>&1 || ! command -v pgrep >/dev/null 2>&1; then
    echo "[entrypoint] installing healthcheck tools (curl, procps)" >&2
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -qq >/dev/null 2>&1 && \
        apt-get install -y --no-install-recommends curl procps >/dev/null 2>&1 || true
    fi
fi

run_supervised() {
    name="$1"; shift
    backoff=1
    while true; do
        started=$(date +%s)
        "$@" >> "/app/logs/${name}.log" 2>&1
        rc=$?
        ran=$(( $(date +%s) - started ))
        [ "$ran" -ge 120 ] && backoff=1
        echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] ${name} exited rc=${rc} after ${ran}s; restarting in ${backoff}s" >&2
        sleep "$backoff"
        backoff=$((backoff * 2))
        [ "$backoff" -gt 300 ] && backoff=300
    done
}

mkdir -p /app/logs
echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] entrypoint booting (uvicorn supervised)" >&2

# Control plane first: minimal deps installed, port 8000 answering for healthcheck
run_supervised uvicorn python3 -m uvicorn core.main:app --host 0.0.0.0 --port 8000 --workers 1 &
UVICORN_SUP_PID=$!

# Phase 2: full requirements from wheelhouse
echo "[entrypoint] phase 2: full requirements" >&2
if [ -f /app/requirements.txt ]; then
    if ! install -r /app/requirements.txt; then
        echo "[entrypoint] phase 2 offline install failed — retrying with network" >&2
        python3 -m pip install --no-cache-dir -r /app/requirements.txt 2>&1 | tail -3
    fi
fi

# Phase 2b: ensure upstox-python-sdk + protobuf + six are available
if python3 -c "import upstox_client, google.protobuf, six" 2>/dev/null; then
    echo "[entrypoint] upstox-python-sdk + protobuf + six already available" >&2
elif [ -d "$WHEELHOUSE" ]; then
    if ! python3 -c "import google.protobuf" 2>/dev/null; then
        echo "[entrypoint] installing protobuf from wheelhouse/network" >&2
        for _whl in "$WHEELHOUSE"/protobuf-*.whl "$WHEELHOUSE"/six-*.whl; do
            [ -f "$_whl" ] && python3 -m pip install --no-cache-dir "$_whl" 2>/dev/null
        done
        if ! python3 -c "import google.protobuf" 2>/dev/null; then
            echo "[entrypoint] wheelhouse protobuf failed — trying network install" >&2
            if python3 -m pip install --no-cache-dir protobuf 2>&1 | tail -1 >&2; then
                echo "[entrypoint] protobuf installed from PyPI" >&2
            else
                echo "[entrypoint] CRITICAL: protobuf install FAILED" >&2
            fi
        fi
    fi
    if ! python3 -c "import six" 2>/dev/null; then
        echo "[entrypoint] installing six from wheelhouse/network" >&2
        for _whl in "$WHEELHOUSE"/six-*.whl; do
            [ -f "$_whl" ] && python3 -m pip install --no-cache-dir "$_whl" 2>/dev/null
        done
        if ! python3 -c "import six" 2>/dev/null; then
            if python3 -m pip install --no-cache-dir six 2>&1 | tail -1 >&2; then
                echo "[entrypoint] six installed from PyPI" >&2
            else
                echo "[entrypoint] CRITICAL: six install FAILED" >&2
            fi
        fi
    fi
    if ls "$WHEELHOUSE"/upstox_python_sdk-*.whl >/dev/null 2>&1; then
        python3 -m pip install --no-cache-dir "$WHEELHOUSE"/upstox_python_sdk-*.whl 2>/dev/null \
            && echo "[entrypoint] upstox-python-sdk installed from wheelhouse" >&2 \
            || echo "[entrypoint] WARNING: upstox-python-sdk wheel install failed" >&2
    fi
fi
python3 -c "import upstox_client" 2>/dev/null \
    && echo "[entrypoint] upstox-python-sdk present — WS LTP streamer will start" >&2 \
    || echo "[entrypoint] WARNING: upstox-python-sdk missing — option LTPs will rely on Dhan/Fyers REST fallback" >&2

# Block on the control plane
wait "$UVICORN_SUP_PID"
RC=$?
echo "uvicorn supervisor exited rc=${RC}; container will restart" >&2
exit "$RC"