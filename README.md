# RetailPulse - Streaming Part (Flume -> Kafka -> Spark -> Bronze -> Silver)

This is the first half of the RetailPulse capstone. It covers the application-event side of the pipeline: generating events, moving them through Flume into Kafka, landing them raw in Bronze with Spark Structured Streaming, then cleaning them up into Silver. The relational side (Sqoop from PostgreSQL) is a separate track and will get its own section once it's done.

## Streaming part

```
flume/
  retailpulse_flume.conf
spark/
  bronze_events_stream.py
  silver_events.py
generator/
  generate_retailpulse_events.py
images/
  flume-completed-count.png
  kafka-sample-output.png
```


## 1. Kafka: create the topic first

Started with Kafka to make a topic and a consumer so I could see the output before touching Flume.

```bash
docker exec -it 07-01-kafka bash
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic retailpulse --partitions 6 --replication-factor 1

kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic retailpulse --from-beginning
```

6 partitions because I want Spark to be able to parallelize reading later, and because the message key (`session_id`) will spread across a decent number of sessions anyway. Now Kafka just sits there waiting for Flume to send it something.

## 2. Flume: spooldir -> Kafka

Config is in `flume/flume.conf`:

The `regex_extractor` interceptor pulls `session_id` out of the JSON line and uses it as the Kafka message key, so all events from one session land in the same partition (ordering within a session). File channel instead of memory so a crash mid-batch doesn't just lose events sitting in RAM.

Spooldir picks files up, renames them to `.COMPLETED` once read, and never deletes them (`deletePolicy = never`), so I still have the raw files on disk as evidence even after Kafka has them.

## 3. Generating the events

Used the generator script (`generator/generate_retailpulse_events.py`), ran it 3 times, so I should end up with 30,000 events from the generator plus whatever came before -> more on the final count below.

Checked the file count landed correctly:

```bash
docker exec -it 06-02-flume bash
cd /var/log/flume_lab/retailpulse
cat *.COMPLETED | wc -l
```

![Flume completed file line count](images/flume-completed-count.png)

30,000 - matches.

Sample of what showed up on the Kafka consumer side:

![Kafka console consumer sample](images/kafka-sample-output.png)

## 4. Checking Kafka partition offsets

```bash
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic retailpulse
retailpulse:0:4952
retailpulse:1:4997
retailpulse:2:5049
retailpulse:3:4974
retailpulse:4:4815
retailpulse:5:5213
```

Sum of these = 30,000, which matches the file line count. Nothing lost between Flume and Kafka.

---

## 5. Bronze layer

Now that streaming into Kafka is proven, moved to Hive + Spark to actually land the data and finish Bronze.

Connect to Hive:

```bash
docker exec -it 02-03-hive-server beeline -u 'jdbc:hive2://localhost:10000/'
```

```sql
CREATE DATABASE IF NOT EXISTS retailpulse_bronze;

CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_bronze.app_events_raw (
    kafka_key        STRING,
    raw_payload      STRING,
    kafka_topic      STRING,
    kafka_partition  INT,
    kafka_offset     BIGINT,
    kafka_timestamp  TIMESTAMP,
    ingested_at      TIMESTAMP
)
PARTITIONED BY (ingest_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/retailpulse/bronze/app_events_raw';
```

External table since Hive isn't the one writing to it, Spark streaming is. Bronze keeps the raw payload untouched plus enough Kafka metadata (topic/partition/offset/timestamp) to trace any row back to where it came from in Kafka.

Spark job: `spark/bronze_events_stream.py`. What it does:

- reads the `retailpulse` topic from `earliest`
- keeps `key`, `value` (raw JSON string, untouched), `topic`, `partition`, `offset`, `timestamp`
- stamps `ingested_at` = when Spark actually wrote it
- partitions the Parquet output by `ingest_date`
- runs with `trigger(once=True)` so it processes what's there and stops, rather than running forever - I re-run it whenever new files show up instead of keeping a long-running stream up

Run with:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.1 \
  /mnt/notebooks/jobs/bronze_events_stream.py
```

### Verifying Bronze

```sql
SELECT COUNT(*) FROM retailpulse_bronze.app_events_raw;
-- 40000

SELECT kafka_partition, COUNT(*), MIN(kafka_offset), MAX(kafka_offset)
FROM retailpulse_bronze.app_events_raw GROUP BY kafka_partition;
```

```
+------------------+-------+------+-------+
| kafka_partition  |  _c1  | _c2  |  _c3  |
+------------------+-------+------+-------+
| 0                | 6603  | 0    | 6602  |
| 1                | 6663  | 0    | 6662  |
| 2                | 6731  | 0    | 6730  |
| 3                | 6632  | 0    | 6631  |
| 4                | 6420  | 0    | 6419  |
| 5                | 6951  | 0    | 6950  |
+------------------+-------+------+-------+
```

```sql
SELECT COUNT(*) FROM (
  SELECT kafka_partition, kafka_offset FROM retailpulse_bronze.app_events_raw
  GROUP BY kafka_partition, kafka_offset HAVING COUNT(*) > 1) d;
-- 0
```

40,000 because I ended up running the generator 4 times total (not 3) ran one more time after the running spark to make sure it will read the new data while it's being pushed and it did, no duplicate (partition, offset) pairs, which is the real proof nothing got double-written. Bronze is done.

---

## 6. Silver layer

Now that Bronze is solid, moved to cleaning it up into Silver: parse the JSON, tag anything broken as a reject, and de-duplicate on `event_id`.

### Tables

```sql
CREATE DATABASE IF NOT EXISTS retailpulse_silver;

CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_silver.app_events (
  event_id STRING, event_type STRING, event_ts TIMESTAMP,
  customer_id INT, session_id STRING, order_id BIGINT,
  product_id INT, store_id INT, channel STRING, `sequence` BIGINT,
  kafka_partition INT, kafka_offset BIGINT, ingested_at TIMESTAMP
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/retailpulse/silver/app_events';

CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_silver.app_events_rejects (
  raw_payload STRING, reject_reason STRING,
  kafka_partition INT, kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP, ingested_at TIMESTAMP
)
PARTITIONED BY (ingest_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/retailpulse/silver/app_events_rejects';

CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_silver.app_events_duplicates (
  event_id STRING, event_type STRING, event_ts TIMESTAMP, `sequence` BIGINT,
  kafka_partition INT, kafka_offset BIGINT, ingested_at TIMESTAMP
)
PARTITIONED BY (ingest_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/retailpulse/silver/app_events_duplicates';
```

Three tables: the clean events, the ones that got rejected (with a reason so I can audit them), and the ones that were duplicates (kept, not deleted, for the same reason).

### The script

`spark/silver_events.py` does the following, in order:

1. reads all of Bronze with `spark.read.parquet(...)` (never through `spark.sql`/`spark.read.table` - Spark and Hive on this stack disagree on how they write/read Parquet metadata classes, so anything that goes through the Hive metastore from Spark risks a `NoClassDefFoundError`. Reading/writing straight off the HDFS path sidesteps that completely)
2. parses `raw_payload` as JSON with `PERMISSIVE` mode
3. standardizes types (`event_id` trimmed, `event_type`/`channel` lowercased + trimmed, timestamps and numeric fields cast properly)
4. runs the data-quality rules below, first match wins, and tags each row with a `reject_reason`
5. de-duplicates whatever's left on `event_id`, using a window ordered by `ingested_at` -> `sequence` -> `kafka_partition` -> `kafka_offset`, keeping only `rn = 1`
6. writes the three outputs, overwriting the existing partitions each run

### Data-quality rules (first one that matches wins)

| # | Rule | Reason |
| --- | --- | --- |
| 1 | JSON didn't parse at all | `UNPARSEABLE_JSON` |
| 2 | `event_id` missing/empty | `MISSING_EVENT_ID` |
| 3 | `event_type` not one of the 5 known types | `INVALID_EVENT_TYPE` |
| 4 | `event_timestamp` didn't parse | `INVALID_EVENT_TIMESTAMP` |
| 5 | `event_timestamp` more than 5 min in the future | `FUTURE_EVENT_TIMESTAMP` |
| 6 | `channel` not `web`/`mobile` | `INVALID_CHANNEL` |
| 7 | `order_confirmed`/`fulfillment_update` with no `order_id` | `MISSING_ORDER_ID` |

### Running it and the output

```bash
>>> print("Silver:", spark.read.parquet(
...     "hdfs://namenode:8020/retailpulse/silver/app_events"
... ).count())
Silver: 37636
>>> print("Rejects:", spark.read.parquet(
...     "hdfs://namenode:8020/retailpulse/silver/app_events_rejects"
... ).count())
Rejects: 380
>>> print("Duplicates:", spark.read.parquet(
...     "hdfs://namenode:8020/retailpulse/silver/app_events_duplicates"
... ).count())
Duplicates: 1984
```

$$37{,}636 + 380 + 1{,}984 = 40{,}000$$

Everything accounted for, nothing silently dropped.

### Verifying Silver is actually right

```sql
SELECT reject_reason, COUNT(*) FROM retailpulse_silver.app_events_rejects GROUP BY reject_reason;
```

```
+-------------------+------+
|   reject_reason   | _c1  |
+-------------------+------+
| UNPARSEABLE_JSON  | 380  |
+-------------------+------+
```

(This took a fix - originally these 380 rows were falling through to `MISSING_EVENT_ID` because `from_json` returns a null struct instead of populating `_corrupt` for these truncated lines. Added an explicit `isNull()` check on the parsed struct so rule 1 actually catches them.)

```sql
SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM retailpulse_silver.app_events;
```

```
+------+
| _c0  |
+------+
| 0    |
+------+
```

No duplicate `event_id`s left in Silver. All good.

### Note on the late events / duplicates overlap

Silver's `event_date` only shows 2 dates (today and yesterday), even though the generator shifts about 3% of events back 1-3 days. Checked this against Bronze directly and it's not a bug - in the generator, the "late" check and the "duplicate" check use the same random roll and the duplicate range fully contains the late range, so almost every late-shifted event also gets its `event_id` overwritten into a duplicate. That means the late-timestamped copy correctly gets caught by dedup and routed to `app_events_duplicates` (where its true timestamp is still visible), not into `app_events`. So this is expected generator behavior, not a Silver defect - just noting it here so it doesn't look like an unexplained gap later.

---

## Where this stands

Streaming part (Flume -> Kafka -> Spark -> Bronze -> Silver) is done and verified: counts reconcile, rejects are labeled correctly, no duplicate `event_id`s in Silver, and reruns don't change the numbers.

Next up: Sqoop ingestion of the `capstone_retail` PostgreSQL tables into their own Bronze/Silver tables (separate track, not related to the events data), then Gold on top of both, then the Metabase dashboard.
