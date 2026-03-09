import os, json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("TOPIC", "iot.sensors")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

device_ids = [f"sensor-{i:03d}" for i in range(1, 6)]

def gen_event(device_id):
    now = datetime.now(timezone.utc).isoformat()
    base_temp = 24 + device_ids.index(device_id)
    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id,
        "timestamp": now,
        "temperature": round(random.gauss(base_temp, 1.2), 2),
        "humidity": round(random.uniform(40, 70), 2),
        "battery": round(random.uniform(20, 100), 1)
    }

print(f"Producing to {TOPIC} @ {BOOTSTRAP} (Ctrl+C to stop)", flush=True)
try:
    while True:
        for dev in device_ids:
            ev = gen_event(dev)
            producer.send(TOPIC, key=dev, value=ev).get(timeout=5)
            print("→", ev, flush=True)
        time.sleep(1.0)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    producer.close()
