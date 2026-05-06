import json
import os
import time
import uuid

import paho.mqtt.client as mqtt


BROKER_HOST = os.getenv("MQTT_HOST", "mosquitto")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/sensors")
MQTT_USER = os.getenv("MQTT_USER", "iot")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "iot123")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data/mqtt_raw")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(MQTT_TOPIC)
    else:
        raise RuntimeError(f"MQTT connect failed with code {rc}")


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return

    ensure_dir(OUTPUT_DIR)
    filename = f"{int(time.time())}_{uuid.uuid4().hex}.json"
    file_path = os.path.join(OUTPUT_DIR, filename)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(data, handle)
        handle.write("\n")


client = mqtt.Client(client_id=f"mqtt-bridge-{uuid.uuid4().hex}")
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_forever()
