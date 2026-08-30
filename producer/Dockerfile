FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.yaml .
COPY schemas/ schemas/
COPY gbfs_producer.py .

CMD ["python", "-u", "gbfs_producer.py"]
