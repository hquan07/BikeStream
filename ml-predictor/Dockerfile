FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY train.py .
COPY predict.py .

# Default: run prediction loop (training is triggered separately)
CMD ["python", "-u", "predict.py"]
