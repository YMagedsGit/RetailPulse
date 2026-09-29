from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

spark = (
    SparkSession.builder
    .appName("retailpulse_gold_daily_store_revenue")
    .config("spark.sql.session.timeZone", "UTC")
    
    # Critical Spark 2.4.1 Parquet + Hive compatibility settings
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("spark.sql.parquet.outputTimestampType", "INT96")
    .config("spark.sql.hive.convertMetastoreParquet", "true")
    
    # Dynamic partitioning configs
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()
)

# Suppress non-critical logs
spark.sparkContext.setLogLevel("WARN")

orders_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/orders")
stores_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/stores")

daily_store_revenue = orders_df.filter(F.col("order_date").isNotNull()) \
    .join(stores_df, "store_id", "inner") \
    .groupBy(
        F.col("store_id"),
        F.col("store_name"),
        F.col("country_code"),
        F.col("order_date")
    ) \
    .agg(
        F.count(F.when(F.col("order_status") != "cancelled", F.col("order_id"))).alias("order_count"),
        F.count(F.when(F.col("order_status") == "cancelled", F.col("order_id"))).alias("cancelled_count"),
        F.sum(F.when(F.col("order_status") != "cancelled", F.col("order_total")).otherwise(0)).cast(DecimalType(14,2)).alias("gross_revenue"),
        F.sum(F.when(F.col("order_status") != "cancelled", F.col("discount_amount")).otherwise(0)).cast(DecimalType(14,2)).alias("total_discount"),
        F.sum(F.when(F.col("order_status") != "cancelled", F.col("order_total") - F.col("discount_amount")).otherwise(0)).cast(DecimalType(14,2)).alias("net_revenue"),
        (
            F.sum(F.when(F.col("order_status") != "cancelled", F.col("order_total")).otherwise(0)) /
            F.expr("nullif(count(case when order_status != 'cancelled' then order_id end), 0)")
        ).cast(DecimalType(12,2)).alias("avg_order_value")
    )

daily_store_revenue.write \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .parquet("hdfs://namenode:8020/retailpulse/gold/daily_store_revenue")

print("Daily Store Revenue Count:", spark.read.parquet("hdfs://namenode:8020/retailpulse/gold/daily_store_revenue").count())

#### additional Gold Layer Job: Payment Health Aggregation
payments_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/payments")


payment_health = payments_df \
    .groupBy(
        F.col("payment_method"),
        F.col("payment_status"),
        F.col("payment_date")
    ) \
    .agg(
        F.count("*").alias("payment_count"),
        F.sum(F.col("amount")).cast(DecimalType(14,2)).alias("total_amount")
    )

payment_health.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("payment_date") \
    .parquet("hdfs://namenode:8020/retailpulse/gold/payment_health")

print("Payment Health Count:", spark.read.parquet("hdfs://namenode:8020/retailpulse/gold/payment_health").count())


#### Gold Layer Job: Inventory Health Aggregation 
inventory_snapshots = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/inventory_snapshots")

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("snapshot_at").desc())

ranked_snapshots = inventory_snapshots.withColumn("rn", F.row_number().over(w))

inventory_health = ranked_snapshots \
    .filter(F.col("rn") == 1) \
    .select(
        F.col("product_id"),
        F.col("store_id"),
        F.col("stock_on_hand"),
        F.col("snapshot_at").alias("as_of"),
        (F.col("stock_on_hand") == 0).alias("is_out_of_stock"),
        ((F.col("stock_on_hand") > 0) & (F.col("stock_on_hand") <= 10)).alias("is_low_stock")
    )

inventory_health.sort("product_id", "store_id") \
    .write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("hdfs://namenode:8020/retailpulse/gold/inventory_health")

print("Inventory Health Count:", spark.read.parquet("hdfs://namenode:8020/retailpulse/gold/inventory_health").count())



######### Data Quality Summary Job #########

spark.sparkContext.setLogLevel("WARN")

orders_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/orders")
customers_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/customers")
stores_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/stores")

order_items_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/order_items")
products_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/products")

payments_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/payments")
fulfillment_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/fulfillment_events")
inventory_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/inventory_snapshots")

app_events_raw_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/bronze/app_events_raw")
app_events_rejects_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/app_events_rejects")
app_events_dups_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/app_events_duplicates")
app_events_df = spark.read.parquet("hdfs://namenode:8020/retailpulse/silver/app_events")

orders_total = orders_df.count()
order_items_total = order_items_df.count()
payments_total = payments_df.count()
fulfillment_total = fulfillment_df.count()
inventory_total = inventory_df.count()
events_bronze_total = app_events_raw_df.count()

batch_checks_list = []

def run_check(df_bad, layer, source_table, check_name, check_type, total_rows):
    bad_count = df_bad.count()
    return (layer, source_table, check_name, check_type, bad_count, total_rows)

# Orders missing customer
orders_no_cust = orders_df.join(customers_df, "customer_id", "left_anti")
batch_checks_list.append(run_check(orders_no_cust, "silver_batch", "orders", "orders_missing_customer", "referential_integrity", orders_total))

# Orders missing store
orders_no_store = orders_df.join(stores_df, "store_id", "left_anti")
batch_checks_list.append(run_check(orders_no_store, "silver_batch", "orders", "orders_missing_store", "referential_integrity", orders_total))

# Order items missing product
items_no_prod = order_items_df.join(products_df, "product_id", "left_anti")
batch_checks_list.append(run_check(items_no_prod, "silver_batch", "order_items", "order_items_missing_product", "referential_integrity", order_items_total))

# Payments missing order
payments_no_order = payments_df.join(orders_df, "order_id", "left_anti")
batch_checks_list.append(run_check(payments_no_order, "silver_batch", "payments", "payments_missing_order", "referential_integrity", payments_total))

# Fulfillment events missing order
fulfillment_no_order = fulfillment_df.join(orders_df, "order_id", "left_anti")
batch_checks_list.append(run_check(fulfillment_no_order, "silver_batch", "fulfillment_events", "fulfillment_events_missing_order", "referential_integrity", fulfillment_total))

# Inventory snapshots missing product
inv_no_prod = inventory_df.join(products_df, "product_id", "left_anti")
batch_checks_list.append(run_check(inv_no_prod, "silver_batch", "inventory_snapshots", "inventory_snapshots_missing_product", "referential_integrity", inventory_total))

# Inventory snapshots missing store
inv_no_store = inventory_df.join(stores_df, "store_id", "left_anti")
batch_checks_list.append(run_check(inv_no_store, "silver_batch", "inventory_snapshots", "inventory_snapshots_missing_store", "referential_integrity", inventory_total))

# Payments unmapped status
payments_unmapped = payments_df.filter(~F.col("payment_status").isin('paid', 'failed', 'pending', 'refunded'))
batch_checks_list.append(run_check(payments_unmapped, "silver_batch", "payments", "payments_unmapped_status", "unmapped_value", payments_total))

batch_checks_df = spark.createDataFrame(
    batch_checks_list,
    ["layer", "source_table", "check_name", "check_type", "bad_rows", "total_rows"]
)

rejects_grouped = app_events_rejects_df.groupBy("reject_reason") \
    .agg(F.count("*").alias("bad_rows")) \
    .select(
        F.lit("silver_streaming").alias("layer"),
        F.lit("app_events").alias("source_table"),
        F.col("reject_reason").alias("check_name"),
        F.lit("validation_rule").alias("check_type"),
        F.col("bad_rows"),
        F.lit(events_bronze_total).alias("total_rows")
    )

dups_df = app_events_dups_df.select(
    F.lit("silver_streaming").alias("layer"),
    F.lit("app_events").alias("source_table"),
    F.lit("duplicate_event_id").alias("check_name"),
    F.lit("duplicate").alias("check_type"),
    F.lit(app_events_dups_df.count()).alias("bad_rows"),
    F.lit(events_bronze_total).alias("total_rows")
).limit(1)

valid_df = app_events_df.select(
    F.lit("silver_streaming").alias("layer"),
    F.lit("app_events").alias("source_table"),
    F.lit("valid_events").alias("check_name"),
    F.lit("volume").alias("check_type"),
    F.lit(app_events_df.count()).alias("bad_rows"),
    F.lit(events_bronze_total).alias("total_rows")
).limit(1)

streaming_checks_df = rejects_grouped.union(dups_df).union(valid_df)

dq_summary = batch_checks_df.union(streaming_checks_df) \
    .withColumn("checked_at", F.current_timestamp()) \
    .withColumn("run_date", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))

dq_summary.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("run_date") \
    .parquet("hdfs://namenode:8020/retailpulse/gold/data_quality_summary")

summary_result = spark.read.parquet("hdfs://namenode:8020/retailpulse/gold/data_quality_summary")

print("\n--- Data Quality Summary Records ---")
summary_result.orderBy("layer", "source_table", "check_name").show(100, False)

batch_failing_count = summary_result.filter((F.col("layer") == "silver_batch") & (F.col("bad_rows") > 0)).count()
print("batch_checks_failing count: ", batch_failing_count)

spark.stop()