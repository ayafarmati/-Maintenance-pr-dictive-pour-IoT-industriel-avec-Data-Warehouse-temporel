import csv
import json
import time
import paho.mqtt.client as mqtt
import os
from datetime import datetime

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_HOST", "127.0.0.1") # "mosquitto" when in Docker, "127.0.0.1" locally
MQTT_PORT = 1883
MQTT_TOPIC = "iot/sensors"
MQTT_USER = "iot"
MQTT_PASSWORD = "iot123"

# Path to the dataset
CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "test_FD001.csv")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully to MQTT broker")
    else:
        print(f"Connection failed with code {rc}")

def stream_data_to_mqtt():
    # Initialize MQTT Client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="python_stream_producer")
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    
    try:
        print(f"Connecting to MQTT Broker at {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")
        return

    if not os.path.exists(CSV_FILE_PATH):
        print(f"File not found: {CSV_FILE_PATH}")
        return

    print(f"Starting to stream data from {CSV_FILE_PATH} to topic '{MQTT_TOPIC}'...")
    
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            
            for row in csv_reader:
                unit_number = row.pop("unit_number")
                time_in_cycles = row.pop("time_in_cycles")
                sensor_id = f"unit_{unit_number}"
                event_time = datetime.utcnow().isoformat()
                
                # Pour chaque autre colonne (les senseurs/métriques), on envoie un message
                for metric, value in row.items():
                    try:
                        val_float = float(value)
                    except ValueError:
                        continue # on ignore si ce n'est pas un nombre
                    
                    payload = {
                        "sensor_id": sensor_id,
                        "metric": metric,
                        "value": val_float,
                        "event_time": event_time
                    }
                    
                    json_payload = json.dumps(payload)
                    client.publish(MQTT_TOPIC, json_payload)
                    print(f"Published: {json_payload}")
                
                # Attente après chaque cycle (ligne entière) pour simuler le stream
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected from MQTT Broker.")

if __name__ == "__main__":
    stream_data_to_mqtt()
