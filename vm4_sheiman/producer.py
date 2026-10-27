"""Publish every non-blank line of a JSONL file to a Kafka topic, byte-for-byte.

Usage:  python3 producer.py <file> <topic>        (needs: export BS=<VM2_IP>:9092)
  VM1:  python3 producer.py orders.jsonl orders
  VM4:  python3 producer.py order_metrics.jsonl order_metrics

Lines are sent raw (no parsing), so malformed records reach Spark Job 1,
which is where the assignment says cleaning must happen.
"""
import os
import sys
from collections import Counter

from kafka import KafkaProducer

path, topic = sys.argv[1], sys.argv[2]

# kafka-python >= 3.0 defaults: idempotent producer, retries until delivery_timeout_ms (120 s).
# So a leader change during the broker-failure test is retried, not lost and not duplicated.
producer = KafkaProducer(
    bootstrap_servers=os.environ["BS"].split(","),
    acks="all",   # leader acks only after every in-sync replica has the record
)

with open(path, "rb") as f:
    futures = [producer.send(topic, value=line.rstrip(b"\r\n")) for line in f if line.strip()]
producer.flush()

# .get() raises if any record was not acknowledged, so a failed send is never silent
per_partition = Counter(fut.get(timeout=30).partition for fut in futures)
producer.close()
print(f"sent {len(futures)} records to '{topic}' | per partition: {dict(sorted(per_partition.items()))}")
