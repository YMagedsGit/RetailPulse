from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .appName("Bronze Events Stream") \
    .enableHiveSupport() \
    .getOrCreate()

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "retailpulse") \
    .option("startingOffsets", "earliest") \
    .load()

bronze = raw.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("raw_payload"),
    F.col("topic").alias("kafka_topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.current_timestamp().alias("ingested_at")
).withColumn(
    "ingest_date",
    F.date_format(F.col("ingested_at"), "yyyy-MM-dd")
)

query = bronze.writeStream \
    .format("parquet") \
    .option(
        "path",
        "hdfs://namenode:8020/retailpulse/bronze/app_events_raw"
    ) \
    .option(
        "checkpointLocation",
        "hdfs://namenode:8020/retailpulse/bronze/checkpoint"
    ) \
    .partitionBy("ingest_date") \
    .trigger(once=True) \
    .start()

query.awaitTermination()