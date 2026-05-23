import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, concat, lit

# Initialisation de Spark
spark = (
    SparkSession.builder
    .appName("iot-compute-baselines")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

input_path = "s3a://iot-lake/datasets/train_FD001_avec_RUL.csv"
output_path = "s3a://iot-lake/reference/sensor_baselines"

print(f"Lecture des données d'entraînement depuis {input_path}...")
# Lecture du CSV (attention au délimiteur ;)
train_df = spark.read.csv(input_path, header=True, inferSchema=True, sep=";")

# Colonnes à exclure du calcul de la moyenne
excluded_cols = ["unit_number", "time_in_cycles", "RUL", "engine_model"]

# Colonnes de métriques
metric_cols = [c for c in train_df.columns if c not in excluded_cols]

print(f"Calcul des moyennes pour {len(metric_cols)} métriques...")

# Liste des expressions d'agrégation
agg_exprs = [avg(col(c)).alias(f"ref_{c}") for c in metric_cols]

# Calcul des moyennes groupées par unit_number et engine_model
baselines_df = train_df.groupBy("unit_number", "engine_model").agg(*agg_exprs)

# Transformer le unit_number (int) en sensor_id (string) pour pouvoir faire la jointure plus tard
baselines_df = baselines_df.withColumn("sensor_id", concat(lit("unit_"), col("unit_number").cast("string")))

# Ne conserver que le sensor_id et les métriques de référence
final_cols = ["sensor_id"] + [f"ref_{c}" for c in metric_cols]
final_baselines_df = baselines_df.select(*final_cols)

print(f"Sauvegarde du référentiel dans {output_path}...")
# Sauvegarde en Parquet (idéal pour le Reference Data)
(
    final_baselines_df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Référentiel généré avec succès !")
spark.stop()
