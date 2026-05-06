from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("hudi-to-clickhouse")
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

hudi_path = "s3a://iot-lake/hudi/sensor_metrics_mqtt"

source_df = (
    spark.read.format("hudi")
    .load(hudi_path)
    .filter("metric = 'student'")
    .filter("sensor_id IS NOT NULL AND sensor_id <> ''")
)

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/iot"
clickhouse_table = "sensor_metrics_mqtt_student"

(
    source_df.select("sensor_id", "metric", "value", "event_time", "processed_at")
    .write
    .format("jdbc")
    .option("url", clickhouse_url)
    .option("dbtable", clickhouse_table)
    .option("user", "iot")
    .option("password", "iot123")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("createTableOptions", "ENGINE=MergeTree() ORDER BY (sensor_id)")
    .mode("append")
    .save()
)

spark.stop()
