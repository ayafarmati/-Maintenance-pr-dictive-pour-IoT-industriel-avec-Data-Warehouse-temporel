import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from datetime import datetime
import mlflow
from scipy.stats import ks_2samp

# 1. Initialisation de Spark
spark = (
    SparkSession.builder
    .appName("iot-detect-drift")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# 2. Configuration MLflow
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("rul_xgboost")

silver_path = "s3a://iot-lake/hudi/slv_sensor_features"
reference_path = "s3://iot-lake/models/reference_data.parquet"

# 3. Lecture des données courantes (Silver)
print(f"Lecture des données courantes depuis {silver_path}...")
silver_df = spark.read.format("hudi").load(silver_path)

ml_df = silver_df.withColumn("unit_number", regexp_replace(col("sensor_id"), "unit_", "").cast("int"))

feature_cols = [
    "unit_number", "time_in_cycles", "altitude_vol", "vitesse_mach", "angle_manette_gaz", 
    "temp_entree_ventilateur", "temp_sortie_compresseur_BP", "temp_sortie_compresseur_HP", 
    "temp_sortie_turbine_BP", "pression_entree_ventilateur", "pression_conduit_contournement", 
    "pression_totale_compresseur_HP", "vitesse_physique_ventilateur", "vitesse_physique_coeur", 
    "ratio_pression_globale", "pression_statique_compresseur_HP", "ratio_carburant_pression", 
    "vitesse_corrigee_ventilateur", "vitesse_corrigee_coeur", "taux_dilution_BPR", 
    "ratio_carburant_air", "enthalpie_air_purge", "vitesse_ventilateur_demandee", 
    "vitesse_corrigee_ventilateur_demandee", "debit_refroidissement_turbine_HP", 
    "debit_refroidissement_turbine_BP"
]

pdf_current = ml_df.select(*feature_cols).toPandas()

if pdf_current.empty:
    print("Aucune donnée courante trouvée. Fin du job.")
    spark.stop()
    exit(0)

# 4. Lecture des données de référence
print(f"Lecture des données de référence depuis {reference_path}...")
try:
    pdf_reference = pd.read_parquet(
        reference_path, 
        storage_options={
            "key": "minio",
            "secret": "minio123",
            "client_kwargs": {"endpoint_url": "http://minio:9000"},
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }
    )
except Exception as e:
    print(f"Impossible de lire les données de référence : {e}")
    spark.stop()
    exit(1)

# 5. Détection de Concept Drift avec Scipy (Test de Kolmogorov-Smirnov)
print("Calcul du Concept Drift (Test KS)...")

drifted_columns = 0
total_columns = len(feature_cols)
drift_details = {}

# P-value threshold for drift (typically 0.05)
p_value_threshold = 0.05

for col_name in feature_cols:
    if col_name in pdf_current.columns and col_name in pdf_reference.columns:
        # Drop NaNs
        ref_data = pdf_reference[col_name].dropna()
        curr_data = pdf_current[col_name].dropna()
        
        if len(ref_data) > 0 and len(curr_data) > 0:
            # KS Test: Compare two distributions
            statistic, p_value = ks_2samp(ref_data, curr_data)
            is_drifted = p_value < p_value_threshold
            drift_details[col_name] = {"p_value": p_value, "drifted": is_drifted}
            if is_drifted:
                drifted_columns += 1

share_of_drifted_columns = drifted_columns / total_columns if total_columns > 0 else 0
dataset_drift = share_of_drifted_columns >= 0.20  # On alerte si plus de 20% des features ont dérivé

print(f"Dérive du jeu de données détectée : {dataset_drift}")
print(f"Part de colonnes en dérive : {share_of_drifted_columns:.2f} ({drifted_columns}/{total_columns})")

# 6. Logging dans MLflow
with mlflow.start_run(run_name="concept_drift_monitoring"):
    mlflow.log_metric("dataset_drift", int(dataset_drift))
    mlflow.log_metric("share_of_drifted_columns", share_of_drifted_columns)
    mlflow.log_metric("number_of_drifted_columns", drifted_columns)
    
    # Log detailed drift metrics
    for col_name, details in drift_details.items():
        mlflow.log_metric(f"drift_pvalue_{col_name}", details["p_value"])

# 7. Écriture des métriques vers ClickHouse
print("Écriture des métriques de drift vers ClickHouse...")
now = datetime.now()
date_id = int(now.strftime("%Y%m%d%H%M"))

drift_df = spark.createDataFrame(
    [(date_id, bool(dataset_drift), float(share_of_drifted_columns), int(drifted_columns))],
    ["date_id", "dataset_drift", "share_of_drifted_columns", "number_of_drifted_columns"]
)

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/iot_metrics_DW"
clickhouse_properties = {
    "user": "iot",
    "password": "iot123",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

drift_df.write \
    .option("createTableOptions", "ENGINE=MergeTree() ORDER BY date_id") \
    .jdbc(url=clickhouse_url, table="fact_model_drift", mode="append", properties=clickhouse_properties)

print("Job de détection de drift terminé avec succès !")
spark.stop()
