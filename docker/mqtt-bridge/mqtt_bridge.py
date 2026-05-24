import json
import os
import time
import uuid
import threading
import paho.mqtt.client as mqtt

BROKER_HOST = os.getenv("MQTT_HOST", "mosquitto")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/sensors")
MQTT_USER = os.getenv("MQTT_USER", "iot")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "iot123")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data/mqtt_raw")
LOG_EVERY = int(os.getenv("LOG_EVERY", "500")) # Increased log interval to avoid spam
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL", "5"))

_message_count = 0
_buffer = []
_buffer_lock = threading.Lock()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    try:
        os.chmod(path, 0o777)
    except Exception:
        pass

def cleanup_old_files():
    """Background thread to delete files older than 10 minutes."""
    while True:
        try:
            now = time.time()
            if os.path.exists(OUTPUT_DIR):
                for filename in os.listdir(OUTPUT_DIR):
                    if not filename.endswith(".json"):
                        continue
                    file_path = os.path.join(OUTPUT_DIR, filename)
                    if os.path.isfile(file_path) and os.stat(file_path).st_mtime < now - 600:
                        try:
                            os.remove(file_path)
                        except Exception:
                            pass
        except Exception as e:
            print(f"Cleanup error: {e}")
        time.sleep(60)

def flush_buffer():
    """Write the current buffer to a JSON file (JSON lines format)."""
    global _buffer
    with _buffer_lock:
        if not _buffer:
            return
        data_to_write = _buffer
        _buffer = []

    ensure_dir(OUTPUT_DIR)
    filename = f"{int(time.time())}_{uuid.uuid4().hex}.json"
    file_path = os.path.join(OUTPUT_DIR, filename)
    
    try:
        with open(file_path, "w", encoding="utf-8") as handle:
            for item in data_to_write:
                json.dump(item, handle)
                handle.write("\n")
        try:
            os.chmod(file_path, 0o666)
        except Exception:
            pass
    except Exception as e:
        print(f"Write error: {e}")

def background_flusher():
    """Background thread to flush the buffer periodically."""
    while True:
        time.sleep(FLUSH_INTERVAL_SEC)
        flush_buffer()

# Start background threads
threading.Thread(target=cleanup_old_files, daemon=True).start()
threading.Thread(target=background_flusher, daemon=True).start()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker {BROKER_HOST}:{BROKER_PORT} and subscribed to {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        raise RuntimeError(f"MQTT connect failed with code {rc}")

def on_message(client, userdata, msg):
    global _message_count
    payload = msg.payload.decode("utf-8", errors="replace")
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        print("Skipping non-JSON payload")
        return

    # Add to buffer safely
    with _buffer_lock:
        _buffer.append(data)
        should_flush = len(_buffer) >= BATCH_SIZE

    # Flush if batch size is reached
    if should_flush:
        flush_buffer()

    _message_count += 1
    if LOG_EVERY > 0 and _message_count % LOG_EVERY == 0:
        print(f"Received {_message_count} messages and buffered to files.")

if __name__ == "__main__":
    ensure_dir(OUTPUT_DIR)
    client = mqtt.Client(client_id=f"mqtt-bridge-{uuid.uuid4().hex}")
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    
    print(f"Starting MQTT bridge (batching {BATCH_SIZE} msgs or {FLUSH_INTERVAL_SEC}s)...")
    while True:
        try:
            client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            print(f"Connection lost: {e}, retrying in 5 seconds...")
            time.sleep(5)
