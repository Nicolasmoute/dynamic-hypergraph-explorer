FROM python:3.11-slim

WORKDIR /app

COPY server/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/
COPY client/ ./client/
COPY start.sh ./
RUN chmod +x start.sh

# Pre-create the cache directory so a volume mount at /app/data works cleanly
# even on first run.  The server also calls CACHE_DIR.mkdir(parents=True) at
# startup, but creating it here means the mount-point exists in the image layer.
RUN mkdir -p /app/data/cache

EXPOSE 8080

# start.sh prints diagnostic info before handing off to uvicorn — this makes
# "no runtime logs" incidents triageable without platform dashboard access.
# Uses $PORT if injected by the host (Zeabur), otherwise defaults to 8080.
CMD ["./start.sh"]
