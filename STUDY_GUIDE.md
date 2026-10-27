# 🎓 Big Data A2: Complete Viva & Architecture Study Guide
## Distributed Food Ordering Pipeline across 4 VMs on ZeroTier (Team 10)

This comprehensive guide covers the system architecture, design decisions, code walkthrough, failure mechanics, and every anticipated TA viva question.

---

## 📑 Table of Contents
1. [The 60-Second Pipeline Summary](#1-the-60-second-pipeline-summary)
2. [End-to-End Architecture & VM Mapping](#2-end-to-end-architecture--vm-mapping)
3. [Kafka Core Concepts & KRaft Architecture](#3-kafka-core-concepts--kraft-architecture)
4. [Broker Failure & Fault Tolerance Explained](#4-broker-failure--fault-tolerance-explained)
5. [Code & Business Rules Deep Dive](#5-code--business-rules-deep-dive)
6. [Spark Processing & Analytics Walkthrough](#6-spark-processing--analytics-walkthrough)
7. [Networking: ZeroTier Peer-to-Peer Fabric](#7-networking-zerotier-peer-to-peer-fabric)
8. [Comprehensive Viva Questions & Answers](#8-comprehensive-viva-questions--answers)
9. [Key Numbers & Dataset Benchmarks to Memorize](#9-key-numbers--dataset-benchmarks-to-memorize)

---

## 1. The 60-Second Pipeline Summary
*(Memorize this for the intro in front of the TA)*

> "Our system processes food ordering data through a **two-stage distributed batch pipeline** spanning four virtual machines connected over a **ZeroTier peer-to-peer network**.
> 
> **Kafka** runs in **KRaft mode** (without ZooKeeper) across two brokers: **VM 2** serves as Broker 1 and the sole KRaft metadata controller, while **VM 3** serves as Broker 2. Both topics (`orders` and `order_metrics`) are configured with **2 partitions** and a **replication factor of 2**.
> 
> In Stage 1, **VM 1** publishes 62 raw orders to Kafka topic `orders`. **VM 4** consumes the topic to local disk, and runs **Spark Job 1** to validate, clean, exact de-duplicate, and compute operational delivery metrics (leaving 44 valid orders). VM 4 then publishes these 44 enriched records to Kafka topic `order_metrics`.
> 
> In Stage 2, **VM 1** consumes `order_metrics` to local disk, and runs **Spark Job 2** to compute aggregations across restaurants and cities. 
> 
> Because every partition is replicated across both brokers with `min.insync.replicas=1` and `acks=all`, the entire pipeline continues operating uninterrupted when Broker 2 (VM 3) is killed."

---

## 2. End-to-End Architecture & VM Mapping

```text
                            ZeroTier Private LAN (10.35.23.0/24)
───────────────────────────────────────────────────────────────────────────────────────────
   VM 1 (Aadish)                                                   VM 4 (Sheiman)
┌──────────────────────┐                                       ┌──────────────────────┐
│ orders.jsonl (62)    │                                       │ consumer.py          │
│   │                  │                                       │   │ (lands 62 rows)  │
│   ▼                  │           KAFKA CLUSTER               │   ▼                  │
│ producer.py          │       ┌───────────────────┐           │ orders_landed.jsonl  │
│   │                  │       │  VM 2: Broker 1   │           │   │                  │
│   │  topic "orders"  │       │  + Controller     │           │   ▼                  │
│   └──────────────────┼──────►│  (10.35.23.108)   │──────────►│ spark_job1_enrich.py │
│                      │       ├───────────────────┤           │   │ (cleans -> 44)   │
│ consumer.py          │       │  VM 3: Broker 2   │           │   ▼                  │
│   ▲                  │       │  (10.35.23.132)   │◄──────────┼─producer.py          │
│   │ (lands 44 rows)  │       │  (Failover node)  │           │  order_metrics.jsonl │
│   │                  │       └───────────────────┘           └──────────────────────┘
│ order_metrics_landed │                 ▲
│   │                  │                 │ topic "order_metrics"
│   ▼                  │                 │
│ spark_job2_analytics │─────────────────┘
│   │
│   ├── restaurant_summary.csv (6 rows)
│   └── city_summary.csv (4 rows)
└──────────────────────┘
```

### Why 4 VMs?
- **VM 1 (Client 1):** Ingestion source and final analytics consumer.
- **VM 2 (Kafka Leader/Controller):** KRaft metadata management and primary partition leader.
- **VM 3 (Kafka Follower):** Replicated partition follower, used for testing broker failover.
- **VM 4 (Client 2):** Middle-stage consumer, cleaning engine (Spark), and enriched publisher.

---

## 3. Kafka Core Concepts & KRaft Architecture

### 3.1 What is KRaft mode?
Historically, Apache Kafka relied on **Apache ZooKeeper** for cluster metadata, broker registration, topic definitions, and partition leader elections.  
**KRaft (Kafka Raft)** replaces ZooKeeper completely:
- Kafka stores its cluster metadata in an internal, Raft-replicated topic called **`@metadata`** (or `__cluster_metadata`).
- Controller nodes form a Raft quorum to elect a leader and commit metadata events sequentially.
- **Benefits:** Faster leader failover (milliseconds vs. tens of seconds), eliminates external ZooKeeper dependency, scales to millions of partitions, and operates as a unified distributed system.

### 3.2 Key Configuration Parameters in `a2-kraft.properties`

| Configuration | Setting | Purpose / Justification |
| :--- | :--- | :--- |
| `process.roles` | `broker,controller` on VM2; `broker` on VM3 | Defines node duties. VM2 runs both broker and Raft controller; VM3 is a pure data broker. |
| `node.id` | `1` on VM2; `2` on VM3 | Unique cluster-wide integer identifier for each node. |
| `controller.quorum.voters` | `1@10.35.23.108:9093` | Points all nodes to node 1 as the single voting KRaft controller. |
| `listeners` | `PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093` | The network interfaces and ports that Kafka binds to locally. |
| `advertised.listeners` | `PLAINTEXT://<VM_ZT_IP>:9092` | The actual IP address Kafka returns to clients during metadata discovery. **Must be the ZeroTier IP, never localhost!** |
| `num.partitions` | `2` | Guarantees parallelism and distributes load across both brokers. |
| `default.replication.factor` | `2` | Ensures every partition has a backup replica on the second broker. |
| `min.insync.replicas` | `1` | Allows write operations with `acks=all` to succeed even when one broker goes down. If set to 2, killing VM3 would freeze producer writes with `NotEnoughReplicasException`. |
| `offsets.topic.replication.factor` | `2` | Ensures the internal `__consumer_offsets` topic is replicated across both brokers. |

---

## 4. Broker Failure & Fault Tolerance Explained

### 4.1 How High Availability (HA) Works
Both topics (`orders` and `order_metrics`) have:
- `Partition 0`: Leader = Broker 1, Replicas = [1, 2], ISR = [1, 2]
- `Partition 1`: Leader = Broker 2, Replicas = [2, 1], ISR = [1, 2]

1. **Leader Replica:** Handles all read and write requests from producers and consumers.
2. **Follower Replica:** Replicates records by sending continuous `Fetch` requests to the leader.
3. **ISR (In-Sync Replicas):** The set of replicas that are fully caught up with the leader's log within `replica.lag.time.max.ms` (30 seconds).

### 4.2 What happens when VM 3 is killed?
1. **Heartbeat loss:** The KRaft controller on VM 2 notices Broker 2 has stopped sending heartbeats.
2. **ISR Shrinks:** The controller updates metadata and shrinks the ISR for both partitions from `[1, 2]` to `[1]`.
3. **Partition 1 Failover:** Partition 1's leader was Broker 2. The controller immediately promotes Broker 1 to be the new leader for Partition 1.
4. **Partition 0 Unchanged:** Partition 0 was already led by Broker 1, so it continues serving requests uninterrupted.
5. **No Unavailable Partitions:** Both partitions remain 100% available on Broker 1. Writes with `acks=all` succeed because `min.insync.replicas=1`.

### 4.3 What happens when VM 3 restarts?
1. Broker 2 starts and connects to the KRaft controller on VM 2.
2. Broker 2 requests the missing offsets from Broker 1 and writes them to its local disk log.
3. Once caught up, the controller adds Broker 2 back to the ISR: `Isr: 1, 2`.
4. Preferred leader rebalance will eventually restore Broker 2 as leader of Partition 1.

---

## 5. Code & Business Rules Deep Dive

### 5.1 Why Producers and Consumers are Built the Way They Are

#### `producer.py`
- **Reads raw lines, no business parsing:** The spec forbids filtering in the producer. Cleaning is strictly Spark's responsibility.
- **`acks="all"`:** Guarantees that the leader will not acknowledge a message until all current members of the ISR have written it to their commit log. Prevents data loss during leader election.
- **Idempotence & Retries:** `kafka-python>=3.0` enables idempotence by default, preventing duplicate records during network retransmissions.
- **`flush()`:** Flushes all memory buffers to ensure all records reach the cluster before the script exits.

#### `consumer.py`
- **`group_id=None` (No Consumer Group):** We assign partitions directly or use non-group subscriptions. If a consumer group were used, committed offsets would cause re-runs to read 0 records. By skipping consumer groups and reading from offset 0, the script provides repeatable **batch semantics**.
- **`auto_offset_reset="earliest"`:** Always reads the complete topic history from the beginning.
- **Idle Timeout (default 60s / 15s):** Because Kafka is an infinite log and has no "End of Stream" concept, the consumer waits for `N` seconds of silence before closing the file and exiting cleanly.

---

## 6. Spark Processing & Analytics Walkthrough

### 6.1 Spark Job 1 (`spark_job1_enrich.py`)
- **Explicit Schema Enforcement:** Instead of expensive runtime schema inference (`inferSchema=true`), an explicit `StructType` is used. Any malformed JSON lines parse as nulls and are safely discarded.
- **Cleaning Filters Applied:**
  1. `order_id` must not be null or blank.
  2. `customer_id` and `restaurant_id` must be valid.
  3. `city` must be present.
  4. `order_amount` > 0 and `distance_km` > 0.
  5. `status == 'DELIVERED'` (all CANCELLED, PENDING, REFUNDED, FAILED orders dropped).
  6. All three timestamps (`order_time`, `pickup_time`, `delivery_time`) must parse as valid ISO timestamps (`yyyy-MM-dd HH:mm:ss`).
- **Exact Deduplication:**
  ```python
  df = df.dropDuplicates(all_14_schema_fields)
  ```
  *Crucial:* Deduplication is performed on all fields, NOT just `order_id`. If an `order_id` appears twice (once corrupt and once valid), deduplicating on all fields allows the cleaning filters to drop the corrupt row while retaining the valid row.
- **Computed Metrics:**
  - `preparation_time_min` = `(pickup_time - order_time) / 60`
  - `delivery_time_min` = `(delivery_time - pickup_time) / 60`
  - `fulfillment_time_min` = `preparation_time_min + delivery_time_min`
  - `speed_kmph` = `distance_km / (delivery_time_min / 60)`
  - `delay_min` = `max(0, delivery_time_min - estimated_delivery_time_min)`
  - `delivery_status` = `ON_TIME` (if delay == 0) else `DELAYED`
  - `delay_category`:
    - `0 min` ➔ `ON_TIME`
    - `1–15 min` ➔ `SLIGHT_DELAY`
    - `16–30 min` ➔ `MODERATE_DELAY`
    - `>30 min` ➔ `SEVERE_DELAY`

### 6.2 Spark Job 2 (`spark_job2_analytics.py`)
- Reads landed `order_metrics_landed.jsonl`.
- Applies exact deduplication again to ensure safety against replay during broker failure.
- Computes Restaurant Summaries (`restaurant_summary.csv`):
  - `order_count`, `avg_order_amount`, `avg_preparation_time_min`, `avg_delivery_time_min`, `delayed_order_count`, `delay_rate_pct`.
- Computes City Summaries (`city_summary.csv`):
  - `order_count`, `avg_order_amount`, `avg_delivery_time_min`, `delayed_order_count`, `delay_rate_pct`.

---

## 7. Networking: ZeroTier Peer-to-Peer Fabric

### Why ZeroTier?
- Student laptops and university VMs are located behind different Wi-Fi networks and NAT routers with private RFC1918 IPs (e.g. `10.0.2.15` or `192.168.1.x`).
- Kafka brokers and Spark workers require direct, bidirectional TCP connectivity on ports 9092 and 9093.
- ZeroTier creates a **virtual Software-Defined WAN (SD-WAN)** using UDP hole-punching and peer-to-peer 256-bit encryption. Every VM receives a static, reachable IPv4 address on the `10.35.23.0/24` subnet.

---

## 8. Comprehensive Viva Questions & Answers

### Q1: Why use KRaft instead of ZooKeeper?
> **Answer:** KRaft integrates metadata consensus directly into Kafka using the Raft consensus algorithm. It removes the operational overhead of running a separate ZooKeeper cluster, supports significantly faster controller failover (milliseconds), enables metadata snapshots, and allows clusters to scale to millions of partitions.

### Q2: Why 2 partitions and Replication Factor 2?
> **Answer:** 
> - **2 Partitions:** Allows parallel consumption by up to 2 consumers in a consumer group and spreads throughput evenly across both brokers.
> - **Replication Factor 2:** Ensures every partition has a primary copy on one broker and a synchronized backup replica on the other, providing zero data loss if one broker fails.

### Q3: What is the In-Sync Replicas (ISR) list?
> **Answer:** The ISR is the subset of partition replicas that are fully caught up with the partition leader within the configured window (`replica.lag.time.max.ms`, default 30s). Only ISR members are eligible to be elected as new leaders during a failover without data loss.

### Q4: Why can't VM 2 be stopped during the failure test?
> **Answer:** VM 2 is configured as the sole KRaft Controller (`controller.quorum.voters=1@10.35.23.108:9093`). If VM 2 stops, the cluster loses its metadata quorum: partition leader elections cannot occur, broker heartbeats fail, and all partitions go offline. True High Availability for the controller would require at least 3 controller voters so a quorum majority (2 out of 3) survives 1 node failure.

### Q5: What does `acks=all` mean? Why is `min.insync.replicas=1` critical here?
> **Answer:** `acks=all` means the producer waits until every broker currently in the ISR acknowledges writing the record before considering the write successful.  
> `min.insync.replicas=1` sets the minimum number of acknowledged replicas required for the write to succeed. When Broker 2 is killed, the ISR drops to 1. If `min.insync.replicas` were set to 2, every write would fail with `NotEnoughReplicasException`. With 1, writes continue successfully.

### Q6: Why does our consumer use `group_id=None` instead of a consumer group?
> **Answer:** Consumer groups commit offsets into the internal `__consumer_offsets` topic. If we used a consumer group, a second run or a replay would find committed offsets and read 0 records. By setting `group_id=None` and `auto_offset_reset="earliest"`, our consumers implement **batch semantics**: every execution reads all records from offset 0 and overwrites the local snapshot.

### Q7: Why not read Kafka directly from Spark using Spark Streaming?
> **Answer:** The course specification explicitly forbids streaming and requires batch processing. Landing the data to local JSONL files cleanly decouples the storage layer (Kafka) from the compute layer (Spark). Each stage can be re-run, debugged, and inspected independently without coupling Spark jobs to Kafka broker state.

### Q8: What happens when the pipeline is re-run during the failure test? Why don't counts double?
> **Answer:** Kafka is an append-only commit log, not a queue. Replaying the topic reads both the original 62 records and the 62 newly produced records (total 124). However, Spark Job 1 executes `dropDuplicates(all_14_fields)`, which collapses exact duplicate records back to the original 44 unique records. The downstream analytics remain 100% byte-identical.

### Q9: What is the difference between Narrow and Wide transformations in Spark?
> **Answer:**
> - **Narrow Transformations (e.g. `filter`, `withColumn`):** Data in each partition can be computed independently without shuffling across other partitions.
> - **Wide Transformations (e.g. `groupBy`, `dropDuplicates`, `orderBy`):** Require shuffling data across network partitions, introducing a stage boundary in the Spark DAG.

### Q10: Why use explicit Spark schemas instead of schema inference?
> **Answer:** Schema inference scans the entire dataset before planning, is slow, and parses malformed records into unpredictable string types. An explicit schema parses corrupt data or incorrect types into `null`, allowing our validity filter to reliably identify and drop invalid records.

---

## 9. Key Numbers & Dataset Benchmarks to Memorize

| Metric | Hand-Checked Value | Explanation |
| :--- | :--- | :--- |
| **Raw orders in input file** | **62** | Team 10 dataset (`orders.jsonl`) |
| **Invalid/dropped orders** | **18** | 4 exact duplicates + 6 non-DELIVERED + 3 missing fields + 1 missing city + 2 bad timestamps + 1 zero amount + 1 zero distance |
| **Valid enriched orders** | **44** | Records written to `order_metrics.jsonl` |
| **ON_TIME category count** | **11** | Delay == 0 min |
| **SLIGHT_DELAY category count** | **11** | 1 <= Delay <= 15 min |
| **MODERATE_DELAY category count** | **15** | 16 <= Delay <= 30 min |
| **SEVERE_DELAY category count** | **7** | Delay > 30 min |
| **Total restaurant rows** | **6** | R01 through R06 in `restaurant_summary.csv` |
| **Total city rows** | **4** | Bengaluru (16), Hyderabad (7), Mumbai (14), Pune (7) |
| **TA Checker Accuracy** | **100% / 100% / 100%** | Zero mismatches, zero missing, zero extra |
