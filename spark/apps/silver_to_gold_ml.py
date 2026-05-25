import os
import mlflow
import pandas as pd
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import (
    col, regexp_replace, hash, abs, date_format, 
    year, month, dayofmonth, hour, minute, to_timestamp, concat, when, lit, avg, stddev, coalesce
)
from pyspark.sql.window import Window

# 1. Initialisation de Spark
spark = (
    SparkSession.builder
    .appName("iot-silver-to-gold-ml")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# 2. Configuration MLflow et récupération du modèle
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"

mlflow.set_tracking_uri("http://mlflow:5000")
client = mlflow.tracking.MlflowClient()

try:
    experiment = client.get_experiment_by_name("rul_xgboost")
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id], 
        filter_string="tags.mlflow.runName != 'concept_drift_monitoring'",
        order_by=["start_time DESC"], 
        max_results=1
    )
    latest_run_id = runs[0].info.run_id
    model_uri = f"runs:/{latest_run_id}/model"
    print(f"Chargement du modèle MLflow depuis l'URI: {model_uri}")
except Exception as e:
    print(f"Erreur lors de la récupération du modèle MLflow : {e}")
    spark.stop()
    exit(1)

# Chargement direct en modèle natif XGBoost (évite les problèmes de vérification de signature PyFunc)
loaded_model = mlflow.xgboost.load_model(model_uri)

# 3. Lecture de la couche Silver
silver_path = "s3a://iot-lake/hudi/slv_sensor_features"
print(f"Lecture des données Silver depuis {silver_path}...")
try:
    silver_df = spark.read.format("hudi").load(silver_path)
except Exception as e:
    print(f"Erreur lors de la lecture de la table Silver (elle n'existe peut-être pas encore) : {e}")
    spark.stop()
    exit(0)

# 4. Préparation des données pour l'inférence
# Le modèle attend une colonne `unit_number` de type entier, mais la couche Silver a `sensor_id` (ex: "unit_1")
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
    "debit_refroidissement_turbine_BP", "engine_model"
]

sensor_cols = [c for c in feature_cols if c not in ("unit_number", "time_in_cycles", "engine_model")]

windows = [5, 10, 15]
for w in windows:
    window_spec = Window.partitionBy("unit_number").orderBy("time_in_cycles").rowsBetween(-(w-1), Window.currentRow)
    
    mean_cols = []
    std_cols = []
    for c in sensor_cols:
        ml_df = ml_df.withColumn(f"{c}_mean_{w}", avg(col(c)).over(window_spec))
        ml_df = ml_df.withColumn(f"{c}_std_{w}", coalesce(stddev(col(c)).over(window_spec), lit(0.0)))
        mean_cols.append(f"{c}_mean_{w}")
        std_cols.append(f"{c}_std_{w}")
        
    feature_cols.extend(mean_cols)
    feature_cols.extend(std_cols)

print("Application du modèle ML pour prédire le RUL (via Pandas Driver)...")
# 5. Inférence via Pandas sur le Driver (évite d'installer XGBoost sur 8 workers)
pdf = ml_df.toPandas()

if not pdf.empty:
    X = pdf[feature_cols].copy()
    # MLflow signature (inférée du CSV de train) attend ces colonnes en 'long' (int64)
    # L'agrégation Spark Silver 'avg' a produit des 'double' (float64), d'où l'erreur
    cols_to_int = ["enthalpie_air_purge", "vitesse_ventilateur_demandee", "time_in_cycles", "unit_number"]
    for c in cols_to_int:
        if c in X.columns:
            X[c] = X[c].fillna(0).astype("int64")
            
    if "engine_model" in X.columns:
        X["engine_model"] = X["engine_model"].astype("category")
            
    pdf["predicted_rul"] = loaded_model.predict(X)
else:
    pdf["predicted_rul"] = pd.Series(dtype=float)

# Reconversion en Spark DataFrame
predictions_df = spark.createDataFrame(pdf)

# Ajout du status_id basé sur le RUL prédit pour la dimension d'état
predictions_df = predictions_df.withColumn(
    "status_id",
    when(col("predicted_rul") <= 15, lit(1))
    .when((col("predicted_rul") > 15) & (col("predicted_rul") <= 45), lit(2))
    .otherwise(lit(3))
)

# 6. Modélisation en Schéma en Étoile (Star Schema)
print("Construction du schéma en étoile...")

# A. Dimension Engine
dim_engine = predictions_df.select(
    "sensor_id", "unit_number", "engine_model"
).distinct().withColumn(
    "engine_id", abs(hash(col("sensor_id")))
)

# B. Dimension Date
# Utilisation de window_start (timestamp string) pour créer une dimension temporelle riche
dim_date = predictions_df.select(
    col("window_start").alias("timestamp_str")
).distinct().withColumn(
    "timestamp", to_timestamp(col("timestamp_str"))
).withColumn(
    "date_id", date_format(col("timestamp"), "yyyyMMddHHmm").cast("long")
).withColumn(
    "year", year(col("timestamp"))
).withColumn(
    "month", month(col("timestamp"))
).withColumn(
    "day", dayofmonth(col("timestamp"))
).withColumn(
    "hour", hour(col("timestamp"))
).withColumn(
    "minute", minute(col("timestamp"))
)

# C. Dimension Status (RUL Bands)
status_data = [
    (1, "Critique", "RUL <= 15 cycles (Nécessite une maintenance immédiate)"),
    (2, "Avertissement", "15 < RUL <= 45 cycles (Planifier une maintenance)"),
    (3, "Sain", "RUL > 45 cycles (Fonctionnement nominal)")
]
dim_status = spark.createDataFrame(status_data, ["status_id", "status_label", "description"])

# D. Table des Faits (Fact Engine Health)
# On joint les IDs des dimensions
fact_cols = [
    col("engine_id"), col("date_id"), col("status_id"), col("time_in_cycles"), col("predicted_rul")
] + [col(c) for c in feature_cols if c not in ("unit_number", "engine_model", "time_in_cycles")]

fact_engine_health = predictions_df.join(
    dim_engine, on=["sensor_id", "unit_number", "engine_model"], how="inner"
).join(
    dim_date.select("timestamp_str", "date_id"), 
    predictions_df.window_start == dim_date.timestamp_str, 
    how="inner"
).select(*fact_cols).withColumn(
    "fact_id", abs(hash(concat(col("engine_id"), col("date_id"), col("time_in_cycles"))))
)

# 7. Écriture vers ClickHouse
clickhouse_url = "jdbc:clickhouse://clickhouse:8123/iot_metrics_DW"
clickhouse_properties = {
    "user": "iot",
    "password": "iot123",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

def write_to_clickhouse(df, table_name, order_by_col):
    print(f"Écriture de la table {table_name} vers ClickHouse...")
    df.write \
      .option("createTableOptions", f"ENGINE=MergeTree() ORDER BY {order_by_col}") \
      .jdbc(url=clickhouse_url, table=table_name, mode="overwrite", properties=clickhouse_properties)

write_to_clickhouse(dim_engine, "dim_engine", "engine_id")
write_to_clickhouse(dim_date, "dim_date", "date_id")
write_to_clickhouse(dim_status, "dim_status", "status_id")
write_to_clickhouse(fact_engine_health, "fact_engine_health", "fact_id")

print("Pipeline Silver to Gold terminé avec succès !")
spark.stop()
