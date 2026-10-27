"""Spark Job 1 - Order Enrichment (runs on VM4).

Usage:  spark-submit spark_job1_enrich.py [orders_landed.jsonl] [order_metrics.jsonl]
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

TS_FMT = "yyyy-MM-dd HH:mm:ss"
STR_COLS = ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city"]
TS_COLS = ["order_timestamp", "accepted_timestamp", "picked_up_timestamp",
           "expected_delivery_timestamp", "delivered_timestamp"]
NUM_COLS = ["distance_km", "order_amount", "delivery_fee"]

# Explicit schema: a value of the wrong JSON type (or a malformed line) becomes null
SCHEMA = T.StructType(
    [T.StructField(c, T.StringType()) for c in STR_COLS + TS_COLS]
    + [T.StructField(c, T.DoubleType()) for c in NUM_COLS]
    + [T.StructField("status", T.StringType())]
)

OUT_COLS = ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
            "order_amount", "delivery_fee", "distance_km", "preparation_time_min",
            "delivery_time_min", "fulfillment_time_min", "delivery_speed_kmph", "delay_min",
            "delivery_status", "delay_category"]


def session(app):
    return (SparkSession.builder.appName(app)
            .config("spark.sql.session.timeZone", "UTC")             # no DST jumps in minute maths
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # bad timestamp -> null, not error
            .config("spark.ui.showConsoleProgress", "false")           # keep demo output readable
            .getOrCreate())


def local_uri(path):
    # file:// forces the local disk; a bare path would go to HDFS if fs.defaultFS points there
    return Path(path).resolve().as_uri()


def minutes(start, end):
    return (F.col(end + "_ts").cast("long") - F.col(start + "_ts").cast("long")) / 60


def enrich(spark, in_path):
    raw = spark.read.schema(SCHEMA).json(local_uri(in_path))  # blank lines are skipped
    df = raw
    for c in TS_COLS:
        df = df.withColumn(c + "_ts", F.to_timestamp(c, TS_FMT))

    valid = F.col("status") == "DELIVERED"
    for c in STR_COLS:
        valid &= F.col(c).isNotNull() & (F.trim(c) != "")
    for c in NUM_COLS:
        valid &= F.col(c).isNotNull()
    for c in TS_COLS:
        valid &= F.col(c + "_ts").isNotNull()
    valid &= (F.col("distance_km") > 0) & (F.col("order_amount") > 0)

    clean = df.filter(valid).dropDuplicates(SCHEMA.fieldNames())  # exact duplicates counted once

    delay = F.greatest(F.lit(0.0), minutes("expected_delivery_timestamp", "delivered_timestamp"))
    out = (clean
           .withColumn("preparation_time_min", F.round(minutes("accepted_timestamp", "picked_up_timestamp"), 2))
           .withColumn("delivery_time_min", F.round(minutes("picked_up_timestamp", "delivered_timestamp"), 2))
           .withColumn("fulfillment_time_min", F.round(minutes("order_timestamp", "delivered_timestamp"), 2))
           .withColumn("delivery_speed_kmph",
                       F.round(F.col("distance_km") / (minutes("picked_up_timestamp", "delivered_timestamp") / 60), 2))
           .withColumn("delay_min", F.round(delay, 2))
           .withColumn("delivery_status", F.when(F.col("delay_min") == 0, "ON_TIME").otherwise("DELAYED"))
           .withColumn("delay_category",
                       F.when(F.col("delay_min") == 0, "ON_TIME")
                        .when(F.col("delay_min") <= 15, "SLIGHT_DELAY")
                        .when(F.col("delay_min") <= 30, "MODERATE_DELAY")
                        .otherwise("SEVERE_DELAY"))
           .select(OUT_COLS)
           .orderBy("order_id"))
    return raw, out


def write_jsonl(df, out_path):
    # ponytail: collect() to the driver gives one plain file the evaluator accepts;
    # fine for thousands of rows, use coalesce(1).write.json + rename for millions
    with open(out_path, "w") as f:
        for line in df.toJSON().collect():
            f.write(line + "\n")


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "orders_landed.jsonl"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "order_metrics.jsonl"
    spark = session("A2-Job1-OrderEnrichment")
    spark.sparkContext.setLogLevel("WARN")
    raw, out = enrich(spark, in_path)
    out.cache()
    write_jsonl(out, out_path)
    print(f"Job 1: {raw.count()} records read -> {out.count()} valid, de-duplicated orders written to {out_path}")
    out.groupBy("delay_category").count().orderBy("delay_category").show()
    spark.stop()
