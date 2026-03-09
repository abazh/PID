FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

ENV BOOTSTRAP=localhost:9092     TOPIC=iot.sensors

CMD ["python", "producer.py"]
