FROM python:3.11-slim

WORKDIR /app

# Zeabur exposes the deployment commit SHA during build. Bake it into the
# image so /health can report the live revision even when git metadata is not
# present at runtime.
ARG ZEABUR_GIT_COMMIT_SHA
ENV DH_GIT_SHA=${ZEABUR_GIT_COMMIT_SHA}

COPY server/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/
COPY client/ ./client/
COPY start.sh ./
RUN chmod +x start.sh

# Cache directory configuration.
# The persistent volume should be mounted at /data in the container.
# Setting DH_CACHE_DIR here ensures the default is correct even if the
# orchestrator env var is not explicitly configured — the server code
# calls CACHE_DIR.mkdir(parents=True, exist_ok=True) on first write.
ENV DH_CACHE_DIR=/data/cache

EXPOSE 8080

# start.sh prints diagnostic info before handing off to uvicorn — this makes
# "no runtime logs" incidents triageable without platform dashboard access.
# Uses $PORT if injected by the host (Zeabur), otherwise defaults to 8080.
CMD ["./start.sh"]
