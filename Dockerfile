FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -r requirements.txt
COPY backend/ backend/
COPY compute/ compute/
COPY cluster_v03/ cluster_v03/
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1
EXPOSE 8000
CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
