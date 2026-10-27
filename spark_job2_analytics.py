"""Spark Job 2 - Operations Analytics (runs on VM1).

Usage:  spark-submit spark_job2_analytics.py [order_metrics_landed.jsonl] [restaurant_summary.csv] [city_summary.csv]
"""
import csv
import sys

from pyspark.sql import functions as F, types as T

from spark_job1_enrich import local_uri, session

SCHEMA = T.StructType(
    [T.StructField(c, T.StringType()) for c in
     ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city"]]
    + [T.StructField(c, T.DoubleType()) for c in
       ["order_amount", "delivery_fee", "distance_km", "preparation_time_min", "delivery_time_min",
        "fulfillment_time_min", "delivery_speed_kmph", "delay_min"]]
    + [T.StructField(c, T.StringType()) for c in ["delivery_status", "delay_category"]]
)


def avg2(c):
    return F.round(F.avg(c), 2).alias("avg_" + c)


def summaries(spark, in_path):
    df = (spark.read.schema(SCHEMA).json(local_uri(in_path))
          .dropna(subset=["order_id"])
          .dropDuplicates())  # Kafka may redeliver a record; exact duplicates count once
    common = [F.count("*").alias("order_count"), avg2("order_amount")]
    delayed = F.sum(F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0)).alias("delayed_order_count")
    rate = F.round(100 * F.col("delayed_order_count") / F.col("order_count"), 2)

    restaurant = (df.groupBy("restaurant_id")
                  .agg(*common, avg2("preparation_time_min"), avg2("delivery_time_min"), delayed)
                  .withColumn("delay_rate_pct", rate)
                  .orderBy("restaurant_id"))
    city = (df.groupBy("city")
            .agg(*common, avg2("delivery_time_min"), delayed)
            .withColumn("delay_rate_pct", rate)
            .orderBy("city"))
    return restaurant, city


def write_csv(df, out_path):
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        w.writerows(df.collect())


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "order_metrics_landed.jsonl"
    restaurant_path = sys.argv[2] if len(sys.argv) > 2 else "restaurant_summary.csv"
    city_path = sys.argv[3] if len(sys.argv) > 3 else "city_summary.csv"
    spark = session("A2-Job2-OperationsAnalytics")
    spark.sparkContext.setLogLevel("WARN")
    restaurant, city = summaries(spark, in_path)
    write_csv(restaurant, restaurant_path)
    write_csv(city, city_path)
    restaurant.show(truncate=False)
    city.show(truncate=False)
    print(f"Job 2: wrote {restaurant_path} and {city_path}")
    spark.stop()
