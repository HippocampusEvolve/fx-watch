#!/usr/bin/env sh
set -eu

cmd="${1:-api}"
shift 2>/dev/null || true

case "$cmd" in
  migrate)
    echo "[entrypoint] alembic upgrade head"
    alembic upgrade head
    ;;
  api)
    exec uvicorn fxwatch.api:app --host 0.0.0.0 --port 8000 --log-level info
    ;;
  worker)
    exec python -m fxwatch.scheduler
    ;;
  cli)
    exec fxwatch "$@"
    ;;
  test)
    exec pytest -q "$@"
    ;;
  *)
    exec "$cmd" "$@"
    ;;
esac
