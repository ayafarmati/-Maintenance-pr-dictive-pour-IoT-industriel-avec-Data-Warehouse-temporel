import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


spark = (
    SparkSession.builder
    .appName("iot-mqtt-to-hudi")
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

schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("metric", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("event_time", StringType(), True),
    ]
)

input_path = "/opt/spark-data/mqtt_raw"
os.makedirs(input_path, exist_ok=True)

parsed = (
    spark.readStream.schema(schema)
    .json(input_path)
    .filter("sensor_id IS NOT NULL AND sensor_id <> ''")
    .withColumn("processed_at", current_timestamp())
)

hudi_options = {
    "hoodie.table.name": "sensor_metrics_mqtt",
    "hoodie.datasource.write.recordkey.field": "sensor_id",
    "hoodie.datasource.write.precombine.field": "processed_at",
    "hoodie.datasource.write.partitionpath.field": "metric",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.enable": "false",
    "hoodie.datasource.write.ignore.failed": "true",
}

output_path = "s3a://iot-lake/hudi/sensor_metrics_mqtt"
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
    .start()
)

query.awaitTermination()
