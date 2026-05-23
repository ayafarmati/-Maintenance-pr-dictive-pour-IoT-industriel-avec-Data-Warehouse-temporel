import csv
import json
import time
import paho.mqtt.client as mqtt
import os
from datetime import datetime
from collections import defaultdict

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_HOST", "127.0.0.1") # "mosquitto" when in Docker, "127.0.0.1" locally
MQTT_PORT = 1884
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

    print(f"Reading and grouping data from {CSV_FILE_PATH}...")
    cycles_data = defaultdict(list)
    
    try:
        # 1. Lire le CSV et regrouper par time_in_cycles
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                cycle = int(row["time_in_cycles"])
                cycles_data[cycle].append(row)
                
        if not cycles_data:
            print("No data found in CSV.")
            return
            
        max_cycle = max(cycles_data.keys())
        print(f"Starting to stream data to topic '{MQTT_TOPIC}'. Found {max_cycle} cycles max.")
        
        # 2. Envoyer les messages cycle par cycle
        for cycle in range(1, max_cycle + 1):
            rows_for_cycle = cycles_data.get(cycle, [])
            if not rows_for_cycle:
                continue
            
            # Même timestamp pour tous les capteurs de tous les moteurs pour ce cycle
            event_time = datetime.utcnow().isoformat()
            print(f"--- Envoi du Cycle {cycle} pour {len(rows_for_cycle)} unités ---")
            
            for row in rows_for_cycle:
                row_data = dict(row) # Copie pour pouvoir pop sans risque
                unit_number = row_data.pop("unit_number")
                time_in_cycles = int(row_data.pop("time_in_cycles")) # Retiré pour le mettre en attribut fixe
                engine_model = row_data.pop("engine_model", "Turbofan-CFM56") # Au cas où
                sensor_id = f"unit_{unit_number}"
                
                # Pour chaque autre colonne (les senseurs/métriques), on envoie un message
                for metric, value in row_data.items():
                    try:
                        val_float = float(value)
                    except ValueError:
                        continue # on ignore si ce n'est pas un nombre
                    
                    payload = {
                        "sensor_id": sensor_id,
                        "engine_model": engine_model,
                        "time_in_cycles": time_in_cycles,
                        "metric": metric,
                        "value": val_float,
                        "event_time": event_time
                    }
                    
                    json_payload = json.dumps(payload)
                    client.publish(MQTT_TOPIC, json_payload)
            
            # Attente de 60 secondes après chaque cycle pour simuler le stream (1 cycle = 1 minute)
            time.sleep(60)
            
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
