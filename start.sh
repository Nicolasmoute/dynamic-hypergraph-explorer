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
echo "[start] data dir    : $(ls /app/data/ 2>/dev/null | tr '\n' ' ' || echo not-yet-created)"
echo "[start] ---"
echo "[start] Starting uvicorn on 0.0.0.0:${PORT:-8080} ..."

exec python -m uvicorn server.main:app \
    --host 0.0.0.0 \
    --port "${PORT:-8080}" \
    --log-level info
# trigger redeploy: port web:8080 configured 2026-04-30T08:05:28Z
