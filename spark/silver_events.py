from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("retailpulse_silver_events")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()
)

fields = [
    "event_id",
    "event_type",
    "event_timestamp",
    "customer_id",
    "session_id",
    "order_id",
    "product_id",
    "store_id",
    "channel",
    "sequence"
]

schema = StructType(
    [StructField(f, StringType(), True) for f in fields] +
    [StructField("_corrupt", StringType(), True)]
)

TYPES = [
    "product_viewed",
    "cart_updated",
    "checkout_started",
    "order_confirmed",
    "fulfillment_update"
]


bronze = spark.read.parquet(
    "hdfs://namenode:8020/retailpulse/bronze/app_events_raw"
)


parsed = (bronze
    .withColumn("j", F.from_json("raw_payload", schema,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt"}))
    .select("raw_payload", "kafka_partition", "kafka_offset", "kafka_timestamp",
            "ingested_at", "ingest_date",
            F.col("j").isNull().alias("_j_null"), "j.*"))


# Standardize types and values 

std = (
    parsed
    .withColumn("event_id", F.trim(F.col("event_id")))
    .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    .withColumn("channel", F.lower(F.trim(F.col("channel"))))
    .withColumn(
        "event_ts",
        F.col("event_timestamp").cast("timestamp")
    )
    .withColumn(
        "customer_id",
        F.col("customer_id").cast("int")
    )
    .withColumn(
        "order_id",
        F.col("order_id").cast("bigint")
    )
    .withColumn(
        "product_id",
        F.col("product_id").cast("int")
    )
    .withColumn(
        "store_id",
        F.col("store_id").cast("int")
    )
    .withColumn(
        "sequence",
        F.col("sequence").cast("bigint")
    )
)

# Data quality rules

reason = (
    F.when(
        F.col("_corrupt").isNotNull() | 
        F.col("_j_null"),
        "UNPARSEABLE_JSON"
    )
    .when(
        F.col("event_id").isNull() | 
        (F.col("event_id") == ""), 
        "MISSING_EVENT_ID"
    )
    .when(
        F.col("event_type").isNull() |
        ~F.col("event_type").isin(TYPES),
        "INVALID_EVENT_TYPE"
    )
    .when(
        F.col("event_ts").isNull(),
        "INVALID_EVENT_TIMESTAMP"
    )
    .when(
        F.col("event_ts") >
        F.current_timestamp() +
        F.expr("INTERVAL 5 MINUTES"),
        "FUTURE_EVENT_TIMESTAMP"
    )
    .when(
        F.col("channel").isNull() |
        ~F.col("channel").isin("web", "mobile"),
        "INVALID_CHANNEL"
    )
    .when(
        F.col("event_type").isin(
            "order_confirmed",
            "fulfillment_update"
        ) &
        F.col("order_id").isNull(),
        "MISSING_ORDER_ID"
    )
)

tagged = std.withColumn(
    "reject_reason",
    reason
).cache()

# Rejects which are row-level errors that cannot be fixed by deduplication or other means

rejects = (
    tagged
    .where(F.col("reject_reason").isNotNull())
    .select(
        "raw_payload",
        "reject_reason",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
        "ingest_date"
    )
)

# Deduplication which is based on event_id and sequence, with the earliest ingested_at and sequence being kept

w = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("ingested_at").asc(),
        F.col("sequence").asc_nulls_last(),
        F.col("kafka_partition").asc(),
        F.col("kafka_offset").asc()
    )
)

ranked = (
    tagged
    .where(F.col("reject_reason").isNull())
    .withColumn(
        "rn",
        F.row_number().over(w)
    )
)

# Clean Silver records

silver = (
    ranked
    .where(F.col("rn") == 1)
    .withColumn(
        "event_date",
        F.date_format(
            F.col("event_ts"),
            "yyyy-MM-dd"
        )
    )
    .select(
        "event_id",
        "event_type",
        "event_ts",
        "customer_id",
        "session_id",
        "order_id",
        "product_id",
        "store_id",
        "channel",
        "sequence",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
        "event_date"
    )
)

# Duplicate records

dups = (
    ranked
    .where(F.col("rn") > 1)
    .select(
        "event_id",
        "event_type",
        "event_ts",
        "sequence",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
        "ingest_date"
    )
)

# Write Silver tables

silver.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet(
        "hdfs://namenode:8020/retailpulse/silver/app_events"
    )
    
rejects.write \
    .mode("overwrite") \
    .partitionBy("ingest_date") \
    .parquet(
        "hdfs://namenode:8020/retailpulse/silver/app_events_rejects"
    )
dups.write \
    .mode("overwrite") \
    .partitionBy("ingest_date") \
    .parquet(
        "hdfs://namenode:8020/retailpulse/silver/app_events_duplicates"
    )

# Validation

tables = [
    "retailpulse_bronze.app_events_raw",
    "retailpulse_silver.app_events",
    "retailpulse_silver.app_events_rejects",
    "retailpulse_silver.app_events_duplicates"
]

print("Silver:", spark.read.parquet(
    "hdfs://namenode:8020/retailpulse/silver/app_events"
).count())

print("Rejects:", spark.read.parquet(
    "hdfs://namenode:8020/retailpulse/silver/app_events_rejects"
).count())

print("Duplicates:", spark.read.parquet(
    "hdfs://namenode:8020/retailpulse/silver/app_events_duplicates"
).count())
tagged.unpersist()

spark.stop()