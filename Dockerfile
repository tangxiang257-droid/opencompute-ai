FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -r requirements.txt
COPY backend/ backend/
COPY compute/ compute/
COPY cluster_v03/ cluster_v03/
COPY cluster_v031/ cluster_v031/
COPY cluster_v04/ cluster_v04/
COPY cluster_v05/ cluster_v05/
COPY cluster_v07/ cluster_v07/
COPY public_pool/ public_pool/
COPY web/ web/
# The optional trusted TP Compose stack starts the coordinator as UID 10001.
# Keep default image UID unchanged for backward compatibility with existing volumes.
RUN mkdir -p /data && chown 10001:10001 /data
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1
EXPOSE 8000
CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
