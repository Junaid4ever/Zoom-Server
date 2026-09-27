#!/bin/sh
set -e
PORT="${PORT:-8080}"
echo "Starting on 0.0.0.0:${PORT}"
if [ -x .venv/bin/uvicorn ]; then
  exec .venv/bin/uvicorn main:asgi_app --host 0.0.0.0 --port "$PORT"
fi
if command -v uvicorn >/dev/null 2>&1; then
  exec uvicorn main:asgi_app --host 0.0.0.0 --port "$PORT"
fi
exec python -m uvicorn main:asgi_app --host 0.0.0.0 --port "$PORT"
