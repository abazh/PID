import json, random, time, uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

BROKER = "localhost"
client = mqtt.Client()
client.connect(BROKER, 1883, 60)
client.loop_start()

device_ids = [f"sensor-{i:03d}" for i in range(1, 6)]

def gen(device_id):
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

print("Publishing to MQTT...", flush=True)
try:
    while True:
        for dev in device_ids:
            payload = gen(dev)
            topic = f"sensors/{dev}/metrics"
            client.publish(topic, json.dumps(payload), qos=0, retain=False)
            print(f"→ MQTT {topic}", payload, flush=True)
        time.sleep(1.0)
except KeyboardInterrupt:
    pass
finally:
    client.loop_stop()
    client.disconnect()
