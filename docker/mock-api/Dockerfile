FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.34.0
COPY docker/mock-api/app.py /app/app.py
COPY data/raw/chicago_taxi.jsonl /data/raw/chicago_taxi.jsonl

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]