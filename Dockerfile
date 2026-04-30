FROM python:3.11-slim

WORKDIR /app

COPY server/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/
COPY client/ ./client/
COPY start.sh ./
RUN chmod +x start.sh

# Do NOT pre-create /data here — the volume is mounted at /data so any image
# layer content there would be shadowed by the mount anyway.  The server code
# calls CACHE_DIR.mkdir(parents=True, exist_ok=True) on first write.

EXPOSE 8080

# start.sh prints diagnostic info before handing off to uvicorn — this makes
# "no runtime logs" incidents triageable without platform dashboard access.
# Uses $PORT if injected by the host (Zeabur), otherwise defaults to 8080.
CMD ["./start.sh"]
