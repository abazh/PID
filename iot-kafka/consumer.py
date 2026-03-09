import json
from collections import deque, defaultdict
from kafka import KafkaConsumer

BOOTSTRAP = "localhost:9092"
TOPIC = "iot.sensors"
TEMP_ALERT = 27.5
WINDOW_N = 10

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    group_id="iot-analytics"
)

window = defaultdict(lambda: deque(maxlen=WINDOW_N))
print(f"Consuming from {TOPIC}...")
for msg in consumer:
    device_id = msg.key or msg.value.get("device_id", "unknown")
    data = msg.value
    t = float(data.get("temperature", 0.0))
    ts = data.get("timestamp", "?")
    if t >= TEMP_ALERT:
        print(f"[ALERT] {device_id} @ {ts} | temp={t}°C ≥ {TEMP_ALERT}°C")

    win = window[device_id]; win.append(t)
    avg = sum(win)/len(win)
    print(f"[{device_id}] t={t}°C avg_last_{len(win)}={avg:.2f}°C")
