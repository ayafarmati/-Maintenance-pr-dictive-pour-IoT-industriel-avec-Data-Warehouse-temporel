import csv
import os

file_path = os.path.join("data", "train_FD001_avec_RUL.csv")

# Liste de modèles réalistes pour varier les données
models = [
    "Turbofan-CFM56",
    "Turbofan-GE90",
    "Turbofan-PW4000",
    "Turbofan-Trent800",
    "Turbofan-V2500"
]

# Read existing data
with open(file_path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=";")
    fieldnames = reader.fieldnames
    rows = list(reader)

# Add engine_model to fieldnames if not present
if "engine_model" not in fieldnames:
    fieldnames.append("engine_model")

# Update rows
for row in rows:
    unit_num = int(row["unit_number"])
    # Assigner le modèle de façon déterministe (toujours le même pour un unit_number donné)
    assigned_model = models[unit_num % len(models)]
    row["engine_model"] = assigned_model

# Write back
with open(file_path, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
    writer.writeheader()
    writer.writerows(rows)

print(f"Successfully updated {file_path} with diverse engine models")
