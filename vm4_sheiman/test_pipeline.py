"""Self-check for both Spark jobs on a hand-made fixture (expected values worked out by hand).

Run on a VM:  spark-submit test_pipeline.py      Prints "ALL CHECKS PASSED" or fails an assert.
"""
import csv
import json
import os
import tempfile

import spark_job1_enrich as job1
import spark_job2_analytics as job2


def order(oid, rest, city, delivered, amount, **over):
    # accepted 10:02, picked up 10:12 (prep 10), expected 10:30, distance 6 km
    rec = {"order_id": oid, "customer_id": "C" + oid, "restaurant_id": rest, "delivery_partner_id": "D1",
           "city": city, "order_timestamp": "2026-09-01 10:00:00", "accepted_timestamp": "2026-09-01 10:02:00",
           "picked_up_timestamp": "2026-09-01 10:12:00", "expected_delivery_timestamp": "2026-09-01 10:30:00",
           "delivered_timestamp": "2026-09-01 " + delivered, "distance_km": 6.0, "order_amount": amount,
           "delivery_fee": 25, "status": "DELIVERED"}
    rec.update(over)
    return json.dumps(rec)


VALID = [
    order("A1", "R1", "Bengaluru", "10:30:00", 100),  # delay 0  -> ON_TIME, delivery 18, speed 20.0
    order("A2", "R1", "Bengaluru", "10:40:00", 200),  # delay 10 -> SLIGHT,  delivery 28, speed 12.86
    order("A3", "R2", "Pune", "10:50:00", 300),       # delay 20 -> MODERATE, delivery 38
    order("A4", "R2", "Pune", "11:15:00", 400),       # delay 45 -> SEVERE,  delivery 63
    order("A5", "R2", "Pune", "10:45:00", 500),       # delay 15 -> SLIGHT (boundary), delivery 33
    order("A6", "R1", "Mumbai", "11:00:00", 600),     # delay 30 -> MODERATE (boundary), delivery 48
]
INVALID = [
    VALID[0],                                                            # exact duplicate of A1
    '{"order_id": "X1", "customer_id": ',                                # malformed JSON
    "",                                                                  # blank line
    '{"record_type":"invalid_order_record","note":"missing fields"}',  # no order fields
    order("X2", "R1", None, "10:30:00", 100),                            # null city
    order("X3", "R1", "Pune", "10:30:00", 100, customer_id=""),          # empty id
    order("X4", "R1", "Pune", "10:30:00", 100, accepted_timestamp="bad"),
    order("X5", "R1", "Pune", "10:30:00", 100, distance_km=0),
    order("X6", "R1", "Pune", "10:30:00", -5),
    order("X7", "R1", "Pune", "10:30:00", 100, status="CANCELLED"),
    order("A2", "R1", "Bengaluru", "10:40:00", 0),                       # same id as A2 but amount 0
]


def read_csv(path, key):
    with open(path) as f:
        return {r[key]: r for r in csv.DictReader(f)}


def main():
    spark = job1.session("A2-tests")
    spark.sparkContext.setLogLevel("ERROR")
    d = tempfile.mkdtemp()
    p = lambda name: os.path.join(d, name)

    with open(p("orders_landed.jsonl"), "w") as f:
        f.write("\n".join(VALID + INVALID) + "\n")

    _, out = job1.enrich(spark, p("orders_landed.jsonl"))
    job1.write_jsonl(out, p("order_metrics.jsonl"))
    with open(p("order_metrics.jsonl")) as f:
        rows = {r["order_id"]: r for r in map(json.loads, f)}
    assert sorted(rows) == ["A1", "A2", "A3", "A4", "A5", "A6"], sorted(rows)
    assert list(rows["A1"]) == job1.OUT_COLS
    a1, a2 = rows["A1"], rows["A2"]
    assert (a1["preparation_time_min"], a1["delivery_time_min"], a1["fulfillment_time_min"]) == (10, 18, 30)
    assert (a1["delivery_speed_kmph"], a1["delay_min"], a1["delivery_status"], a1["delay_category"]) == (20, 0, "ON_TIME", "ON_TIME")
    assert (a2["delivery_speed_kmph"], a2["delay_min"], a2["delivery_status"]) == (12.86, 10, "DELAYED")
    cats = {k: r["delay_category"] for k, r in rows.items()}
    assert cats == {"A1": "ON_TIME", "A2": "SLIGHT_DELAY", "A3": "MODERATE_DELAY", "A4": "SEVERE_DELAY",
                    "A5": "SLIGHT_DELAY", "A6": "MODERATE_DELAY"}, cats

    # Job 2 input carries every record twice, as a Kafka redelivery would
    with open(p("order_metrics.jsonl")) as f:
        metrics = f.read()
    with open(p("order_metrics_landed.jsonl"), "w") as f:
        f.write(metrics + metrics)
    restaurant, city = job2.summaries(spark, p("order_metrics_landed.jsonl"))
    job2.write_csv(restaurant, p("restaurant_summary.csv"))
    job2.write_csv(city, p("city_summary.csv"))

    r = read_csv(p("restaurant_summary.csv"), "restaurant_id")
    assert list(r["R1"]) == ["restaurant_id", "order_count", "avg_order_amount", "avg_preparation_time_min",
                             "avg_delivery_time_min", "delayed_order_count", "delay_rate_pct"]
    assert [float(r["R1"][k]) for k in list(r["R1"])[1:]] == [3, 300, 10, 31.33, 2, 66.67], r["R1"]
    assert [float(r["R2"][k]) for k in list(r["R2"])[1:]] == [3, 400, 10, 44.67, 3, 100], r["R2"]

    c = read_csv(p("city_summary.csv"), "city")
    assert list(c["Pune"]) == ["city", "order_count", "avg_order_amount", "avg_delivery_time_min",
                               "delayed_order_count", "delay_rate_pct"]
    expect = {"Bengaluru": [2, 150, 23, 1, 50], "Mumbai": [1, 600, 48, 1, 100], "Pune": [3, 400, 44.67, 3, 100]}
    assert {k: [float(v[f]) for f in list(v)[1:]] for k, v in c.items()} == expect, c

    spark.stop()
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
