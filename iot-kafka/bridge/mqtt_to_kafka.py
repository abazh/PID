import os, json
from kafka import KafkaProducer
import paho.mqtt.client as mqtt

BOOTSTRAP   = os.getenv("BOOTSTRAP", "kafka:29092")
TOPIC_KAFKA = os.getenv("TOPIC_KAFKA", "iot.sensors")
BROKER_MQTT = os.getenv("BROKER_MQTT", "mosquitto")
TOPIC_MQTT  = os.getenv("TOPIC_MQTT", "sensors/+/metrics")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

def on_connect(client, userdata, flags, rc):
    print("MQTT connected", rc, flush=True)
    client.subscribe(TOPIC_MQTT)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        key = data.get("device_id", "unknown")
        producer.send(TOPIC_KAFKA, key=key, value=data)
        print(f"MQTT→Kafka: {key} {data}", flush=True)
    except Exception as e:
        print("Bad message:", e, flush=True)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_MQTT, 1883, 60)
client.loop_forever()
