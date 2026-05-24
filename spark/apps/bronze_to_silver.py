import os
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, window, avg, current_timestamp, to_timestamp, coalesce

# Initialisation de Spark
spark = (
    SparkSession.builder
    .appName("iot-bronze-to-silver")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

bronze_path = "s3a://iot-lake/hudi/brz_sensor_metrics_mqtt"
silver_path = "s3a://iot-lake/hudi/slv_sensor_features"

# 1. Lecture de la couche Bronze
try:
    bronze_df = spark.read.format("hudi").load(bronze_path)
except Exception as e:
    print(f"Erreur lors de la lecture de la table Bronze (elle n'existe peut-être pas encore) : {e}")
    spark.stop()
    exit(0)

# 2. Transformation
# Convertir event_time en timestamp pour pouvoir utiliser les fenêtres temporelles
df_with_timestamp = bronze_df.withColumn("event_timestamp", to_timestamp(col("event_time")))

# Agréger par sensor_id et fenêtre de temps (1 minute), puis pivoter les métriques
silver_df = (
    df_with_timestamp.groupBy(
        col("sensor_id"),
        col("engine_model"),
        col("time_in_cycles"),
        window(col("event_timestamp"), "1 minute").alias("time_window")
    )
    .pivot("metric")
    .agg(avg("value"))
)

# Chargement du référentiel (Baselines) pour l'imputation des données manquantes
try:
    baselines_df = spark.read.parquet("s3a://iot-lake/reference/sensor_baselines")
    
    # Identifier les colonnes pivotées (qui sont les métriques)
    pivoted_cols = [c for c in silver_df.columns if c not in ["sensor_id", "engine_model", "time_in_cycles", "time_window"]]
    
    # Jointure avec le référentiel
    silver_df = silver_df.join(baselines_df, on="sensor_id", how="left")
    
    # Remplacer les valeurs nulles par la moyenne de référence
    for c in pivoted_cols:
        if f"ref_{c}" in baselines_df.columns:
            silver_df = silver_df.withColumn(c, coalesce(col(c), col(f"ref_{c}")))
            
    # Supprimer les colonnes de référence qui ne sont plus nécessaires
    ref_cols = [c for c in baselines_df.columns if c.startswith("ref_")]
    silver_df = silver_df.drop(*ref_cols)
    print("Imputation avec les moyennes de référence appliquée avec succès.")
    
except Exception as e:
    print(f"Attention: Impossible de charger le référentiel pour l'imputation. Le traitement continue sans nettoyage. Erreur: {e}")

# Extraire le début de la fenêtre pour avoir un timestamp lisible et l'utiliser comme clé de partitionnement/enregistrement
silver_df = (
    silver_df
    .withColumn("window_start", col("time_window.start").cast("string"))
    .withColumn("date", col("time_window.start").cast("date").cast("string")) # Pour le partitionnement
    .withColumn("silver_processed_at", current_timestamp())
    .drop("time_window")
)

# 3. Écriture dans la couche Silver (Upsert)
hudi_silver_options = {
    "hoodie.table.name": "slv_sensor_features",
    "hoodie.datasource.write.recordkey.field": "sensor_id,window_start",
    "hoodie.datasource.write.precombine.field": "silver_processed_at",
    "hoodie.datasource.write.partitionpath.field": "date",
    # COW est meilleur pour les tables analytiques Silver/Gold qui sont lues très souvent et mises à jour en batch
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE", 
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.enable": "false",
}

(
    silver_df.write
    .format("hudi")
    .options(**hudi_silver_options)
    .mode("append")
    .save(silver_path)
)

print("Traitement Bronze -> Silver terminé avec succès.")
spark.stop()
