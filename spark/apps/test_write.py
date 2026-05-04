from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


spark = (
    SparkSession.builder
    .appName("iot-spark-hudi-minio-test")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

rows = [
    ("sensor-001", "temperature", 24.8),
    ("sensor-002", "vibration", 0.17),
    ("sensor-003", "pressure", 1.42),
]

df = spark.createDataFrame(rows, ["sensor_id", "metric", "value"])
df = df.withColumn("processed_at", current_timestamp())

df.show(truncate=False)

output_path = "s3a://iot-lake/hudi/sensor_metrics"

hudi_options = {
    "hoodie.table.name": "sensor_metrics",
    "hoodie.datasource.write.recordkey.field": "sensor_id",
    "hoodie.datasource.write.precombine.field": "processed_at",
    "hoodie.datasource.write.partitionpath.field": "metric",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.enable": "false",
}

(
    df.write
    .format("hudi")
    .options(**hudi_options)
    .mode("append")
    .save(output_path)
)

print(f"Spark Hudi write completed. Output path: {output_path}")

spark.stop()
