#!/bin/sh
# start.sh — container entrypoint for Dynamic Hypergraph Explorer
# Prints diagnostic info to stdout BEFORE handing off to uvicorn so that
# "no runtime logs" incidents can be triaged without Zeabur dashboard access.
set -eu

echo "[start] ==========================="
echo "[start] Dynamic Hypergraph Explorer"
echo "[start] ==========================="
echo "[start] date        : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "[start] whoami      : $(whoami 2>/dev/null || echo unknown)"
echo "[start] cwd         : $(pwd)"
echo "[start] PORT env    : ${PORT:-<unset, will use 8080>}"
echo "[start] python      : $(python --version 2>&1)"
echo "[start] uvicorn     : $(python -m uvicorn --version 2>&1 | head -1)"
echo "[start] client dir  : $(ls /app/client/ 2>/dev/null | tr '\n' ' ' || echo MISSING)"
echo "[start] DH_CACHE_DIR: ${DH_CACHE_DIR:-<unset, default ./data/cache>}"
echo "[start] data dir    : $(ls /data/ 2>/dev/null | tr '\n' ' ' || echo not-yet-created)"
echo "[start] ---"
echo "[start] Pre-warming v14 cache (server/warmup.py) ..."
python -m server.warmup
WARMUP_EXIT=$?
if [ "$WARMUP_EXIT" -ne 0 ]; then
    echo "[start] WARNING: warmup exited with code ${WARMUP_EXIT} — some entries will be computed on first request"
fi
echo "[start] ---"
echo "[start] Starting uvicorn on 0.0.0.0:${PORT:-8080} ..."

exec python -m uvicorn server.main:app \
    --host 0.0.0.0 \
    --port "${PORT:-8080}" \
    --log-level info
