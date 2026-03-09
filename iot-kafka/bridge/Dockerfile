FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mqtt_to_kafka.py .
COPY wait_for.py .

ENV BOOTSTRAP=localhost:9092     TOPIC_KAFKA=iot.sensors     BROKER_MQTT=mosquitto     TOPIC_MQTT=sensors/+/metrics

# Wait for Kafka (29092) and Mosquitto (1883) before starting the bridge
CMD ["python", "wait_for.py", "kafka:29092", "mosquitto:1883", "--", "python", "mqtt_to_kafka.py"]
