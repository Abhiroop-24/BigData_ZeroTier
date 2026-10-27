"""Read a whole Kafka topic from the beginning and land it in a local file.

Usage:  python3 consumer.py <topic> <out_file> [idle_seconds=15]   (needs: export BS=<VM2_IP>:9092)
  VM4:  python3 consumer.py orders orders_landed.jsonl
  VM1:  python3 consumer.py order_metrics order_metrics_landed.jsonl

No consumer group: nothing is committed, so every run replays the full topic
and overwrites <out_file>. It exits after idle_seconds with no new message.
"""
import os
import sys
from collections import Counter

from kafka import KafkaConsumer

topic, out_path = sys.argv[1], sys.argv[2]
idle = float(sys.argv[3]) if len(sys.argv) > 3 else 15

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=os.environ["BS"].split(","),
    group_id=None,                 # no group -> no committed offsets -> full replay every run
    auto_offset_reset="earliest",  # start from the oldest record still in the topic
    enable_auto_commit=False,
    consumer_timeout_ms=int(idle * 1000),
)

per_partition = Counter()
with open(out_path, "wb") as out:
    for msg in consumer:
        out.write(msg.value + b"\n")
        out.flush()
        per_partition[msg.partition] += 1
consumer.close()
print(f"landed {sum(per_partition.values())} records from '{topic}' into {out_path} "
      f"| per partition: {dict(sorted(per_partition.items()))}")
