import os

# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import current_timestamp, to_date, col
# pyrefly: ignore [missing-import]
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, IntegerType


spark = (
    SparkSession.builder
    .appName("iot-mqtt-to-hudi")
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "1g")
    .config("spark.cores.max", "12") # Utiliser 12 cœurs (3 workers)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    # --- NOUVELLES CONFIGURATIONS D'OPTIMISATION SPARK ---
    .config("spark.sql.shuffle.partitions", "20") # Réduit les partitions lors des mélanges (shuffles)
    .config("spark.default.parallelism", "20")    # Réduit le découpage par défaut
    # -----------------------------------------------------
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("engine_model", StringType(), True),
        StructField("time_in_cycles", IntegerType(), True),
        StructField("metric", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("event_time", StringType(), True),
    ]
)

input_path = "/opt/spark-data/mqtt_raw"
os.makedirs(input_path, exist_ok=True)

parsed = (
    spark.readStream.schema(schema)
    .option("maxFilesPerBatch", 100)
    .json(input_path)
    .filter("sensor_id IS NOT NULL AND sensor_id <> ''")
    .withColumn("processed_at", current_timestamp())
    .withColumn("date", to_date(col("event_time")).cast("string"))
)

hudi_options = {
    "hoodie.table.name": "sensor_metrics_mqtt",
    "hoodie.datasource.write.recordkey.field": "sensor_id,metric,event_time",
    "hoodie.datasource.write.precombine.field": "event_time",
    "hoodie.datasource.write.partitionpath.field": "date",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "insert",
    "hoodie.datasource.hive_sync.enable": "false",
    "hoodie.datasource.write.ignore.failed": "true",
    # --- OPTIMISATIONS HUDI POUR STREAMING (MOR & SMALL FILES) ---
    "hoodie.upsert.shuffle.parallelism": "20",
    "hoodie.insert.shuffle.parallelism": "20",
    "hoodie.compact.inline": "true",
    "hoodie.compact.inline.max.delta.commits": "5",
    "hoodie.parquet.small.file.limit": "104857600", # 100MB
    "hoodie.parquet.max.file.size": "125829120", # 120MB
    "hoodie.clustering.async.enabled": "true",
    "hoodie.clustering.inline": "false",
    "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824", # 1GB
    # -------------------------------------------------------------
}

output_path = "s3a://iot-lake/hudi/brz_sensor_metrics_mqtt"
checkpoint_path = "/opt/spark-checkpoints/mqtt_to_hudi"

query = (
    parsed.writeStream
    .foreachBatch(
        lambda batch_df, batch_id: (
            batch_df.write
            .format("hudi")
            .options(**hudi_options)
            .mode("append")
            .save(output_path)
        )
    )
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="1 minute")
    .start()
)

query.awaitTermination()
