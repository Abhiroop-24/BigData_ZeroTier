# Team 10 - Big Data Assignment 2
## Complete Step-by-Step Distributed Food Ordering Pipeline Guide

This guide is the complete, sequential walkthrough of everything each person runs, why they run it, and what output to expect.

---

## 👥 Team & Architecture Overview

```text
ZeroTier Private Network (a581878f7d78cd79)
┌────────────────────────────────────────────────────────────────┐
│ VM 1 (Aadish): Producer 1, Consumer 2, Spark Job 2 (Analytics) │
│ VM 2 (Abhiroop): Kafka Broker 1 + KRaft Controller (Master)    │
│ VM 3 (Adarsh): Kafka Broker 2 (Replica)                        │
│ VM 4 (Sheiman): Consumer 1, Spark Job 1 (Enrichment), Prod 2   │
└────────────────────────────────────────────────────────────────┘
```

| VM | Person | Role | ZeroTier IP | Port |
| :--- | :--- | :--- | :--- | :--- |
| **VM 1** | **Aadish** | Client (Producer 1 · Consumer 2 · Spark Job 2) | `10.35.23.124` | — |
| **VM 2** | **Abhiroop** | **Kafka Broker 1 + Sole KRaft Controller** | `10.35.23.108` | `9092` (Broker), `9093` (Controller) |
| **VM 3** | **Adarsh** | **Kafka Broker 2** (Stopped in failure demo) | `10.35.23.132` | `9092` (Broker) |
| **VM 4** | **Sheiman** | Client (Consumer 1 · Spark Job 1 · Producer 2) | `10.35.23.55` | — |

- **Bootstrap Server (`BS`):** `10.35.23.108:9092`
- **KRaft Cluster ID:** `haM9CpNJS2SxnRNHB_stXg`

---

## ⚡ PHASE 1: Initial Setup (Run Once per VM)

Each member clones only their specific VM folder into `~/a2`.

### 👤 Aadish (VM 1)
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm1_aadish ~/a2
cd ~/a2
python3 -m venv venv && source venv/bin/activate
pip install "kafka-python>=3.0"
```
> **What this does:** Clones the repository, installs the Python Kafka client inside a virtual environment, and prepares `orders.jsonl` (62 raw records).

### 👤 Abhiroop (VM 2)
*(Already fully configured and running as a systemd background service at `~/a2`).*

### 👤 Adarsh (VM 3)
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm3_adarsh ~/a2
cd ~/a2
export KAFKA_HOME=/opt/kafka
bash kafka_node_setup.sh vm3 10.35.23.108 10.35.23.132 haM9CpNJS2SxnRNHB_stXg
```
> **What this does:** Generates `a2-kraft.properties` for Broker 2, formats storage using the shared cluster ID (`haM9CpNJS2SxnRNHB_stXg`), opens the firewall on ZeroTier, and starts Kafka.

### 👤 Sheiman (VM 4)
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm4_sheiman ~/a2
cd ~/a2
python3 -m venv venv && source venv/bin/activate
pip install "kafka-python>=3.0"
```
> **What this does:** Prepares VM 4's environment and installs the Kafka Python client.

---

## 📌 PHASE 2: Terminal Environment Exports

Every person must run these lines whenever they open a new terminal:

```bash
# Aadish (VM 1):
cd ~/a2 && source venv/bin/activate
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"

# Abhiroop (VM 2):
cd ~/a2
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"

# Adarsh (VM 3):
cd ~/a2
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"

# Sheiman (VM 4):
cd ~/a2 && source venv/bin/activate
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
```

---

## 🚦 PHASE 3: Pre-Flight Verification (Run in Parallel)

Before generating any live pipeline outputs, everyone runs their preflight script:

```bash
# VM 1 (Aadish):
bash preflight.sh vm1

# VM 2 (Abhiroop):
bash preflight.sh vm2

# VM 3 (Adarsh):
bash preflight.sh vm3

# VM 4 (Sheiman):
bash preflight.sh vm4
```

> **Target:** All 4 machines must end with **`== READY`**.  
> If any check fails, fix the red `FAIL` line before proceeding.

---

## 🚀 PHASE 4: Live Pipeline Execution (Run in STRICT Sequential Order)

Follow this order step-by-step. Do not skip or change the sequence!

```text
[Step 1: VM4 Consumer] ──waits──> [Step 2: VM1 Producer sends 62 records]
           │
           ▼
[Step 3: VM4 Spark Job 1 (Cleans & Enriches -> 44 records)]
           │
           ▼
[Step 4: VM4 Producer sends 44 records] ──> [Step 5: VM1 Consumer lands 44 records]
                                                           │
                                                           ▼
                                                [Step 6: VM1 Spark Job 2 (CSVs)]
```

### 🔹 STEP 1: VM 4 starts Consumer 1
- **Machine:** VM 4 (Sheiman)
- **Command:**
  ```bash
  python3 consumer.py orders orders_landed.jsonl 60
  ```
- **What it does:** Subscribes to topic `orders` and waits for incoming messages. It will remain running until 60 seconds of idle time pass.
- **Action:** Once running, Sheiman tells Aadish: *"Consumer 1 is listening, publish orders now!"*

---

### 🔹 STEP 2: VM 1 publishes raw orders
- **Machine:** VM 1 (Aadish)
- **Command:**
  ```bash
  python3 producer.py orders.jsonl orders
  ```
- **What it does:** Reads 62 raw JSON orders line-by-line and pushes them to Kafka topic `orders` using `acks=all`. Kafka partitions the messages evenly across Partition 0 and Partition 1.
- **Expected Output:**
  ```text
  sent 62 records to 'orders' | per partition: {0: 31, 1: 31}
  ```

---

### 🔹 STEP 3: VM 4 runs Spark Job 1 (Order Enrichment)
- **Machine:** VM 4 (Sheiman)
- **Wait:** Wait ~10–15 seconds until Step 1's consumer stops and outputs: `landed 62 records from 'orders' into orders_landed.jsonl`.
- **Command:**
  ```bash
  spark-submit spark_job1_enrich.py
  ```
- **What it does:**
  1. Reads `orders_landed.jsonl` (62 records).
  2. Applies cleaning rules: drops malformed JSON, negative distances/amounts, non-`DELIVERED` statuses, and removes exact duplicates (18 bad records dropped).
  3. Computes: preparation time, delivery time, fulfillment time, speed, delay, and delay categories.
  4. Saves exactly **44 valid unique records** to `order_metrics.jsonl`.
- **Expected Output:**
  ```text
  Job 1: 62 records read -> 44 valid, de-duplicated orders written to order_metrics.jsonl
  ON_TIME: 11 | SLIGHT: 11 | MODERATE: 15 | SEVERE: 7
  ```

---

### 🔹 STEP 4: VM 4 publishes enriched metrics
- **Machine:** VM 4 (Sheiman)
- **Command:**
  ```bash
  python3 producer.py order_metrics.jsonl order_metrics
  ```
- **What it does:** Sends the 44 cleaned and enriched records to Kafka topic `order_metrics`.
- **Expected Output:**
  ```text
  sent 44 records to 'order_metrics'
  ```
- **Action:** Sheiman tells Aadish: *"Enriched metrics published to Kafka!"*

---

### 🔹 STEP 5: VM 1 receives enriched records
- **Machine:** VM 1 (Aadish)
- **Command:**
  ```bash
  python3 consumer.py order_metrics order_metrics_landed.jsonl 15
  ```
- **What it does:** Replays Kafka topic `order_metrics` and lands them into `order_metrics_landed.jsonl`.
- **Expected Output:**
  ```text
  landed 44 records from 'order_metrics' into order_metrics_landed.jsonl
  ```

---

### 🔹 STEP 6: VM 1 runs Spark Job 2 (Operations Analytics)
- **Machine:** VM 1 (Aadish)
- **Command:**
  ```bash
  spark-submit spark_job2_analytics.py
  ```
- **What it does:**
  1. Reads `order_metrics_landed.jsonl` (44 records).
  2. De-duplicates records.
  3. Aggregates data by restaurant and by city.
  4. Writes out `restaurant_summary.csv` and `city_summary.csv`.
- **Expected Output:**
  Two summary tables printed in the terminal:
  - `restaurant_summary.csv`: 6 restaurant rows (R01 to R06).
  - `city_summary.csv`: 4 city rows (Bengaluru, Hyderabad, Mumbai, Pune).

---

### 🏆 STEP 7: Check Accuracy on TA Evaluation Site
1. Collect the 3 generated files:
   - `order_metrics.jsonl` (from VM 4)
   - `restaurant_summary.csv` (from VM 1)
   - `city_summary.csv` (from VM 1)
2. Open: **https://assignment2eval2026.vercel.app**
3. Select Team Size: **4 / 5**, Team: **10**.
4. Upload all 3 files.
5. **Expected result:** **100% / 100% / 100%** with 0 mismatched, 0 missing, and 0 extra rows.

---

## 💥 PHASE 5: Broker-Failure & Recovery Test (Graded Demo)

This is the exact sequence to demonstrate to the TA during your evaluation slot:

### 1. Show Cluster Health Before Failure
On **VM 1** or **VM 2**, run:
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
$KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server $BS | grep "id:"
```
> **Say to TA:** *"Both brokers (node 1 and node 2) are active. Both partitions of topic orders have Replication Factor 2 and an In-Sync Replica (ISR) of [1,2]. Partition 0 is led by Broker 1, and Partition 1 is led by Broker 2."*

---

### 2. Kill Broker 2
On **VM 3 (Adarsh)**:
```bash
$KAFKA_HOME/bin/kafka-server-stop.sh
sleep 10 && jps
```
> *(The `Kafka` process terminates on VM 3. **VM 2 / Abhiroop is NEVER stopped** because VM 2 is the KRaft metadata controller).*

---

### 3. Prove Failover to the TA
On **VM 1** or **VM 2**, run:
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
```
> **Say to TA:** *"Notice that `Replicas` still lists [1,2] because that is the static assignment, but the `ISR` shrank to [1]. Leadership of Partition 1 automatically moved from Broker 2 to Broker 1. Zero partitions are unavailable because Broker 1 holds a complete replica."*

---

### 4. Prove Pipeline Continuity While Broker 2 is Dead
Re-run the entire pipeline (Steps 1 through 6 from Phase 4) while VM 3 is offline:
1. **VM 4:** `python3 consumer.py orders orders_landed.jsonl 60`
2. **VM 1:** `python3 producer.py orders.jsonl orders`
3. **VM 4:** `spark-submit spark_job1_enrich.py`
4. **VM 4:** `python3 producer.py order_metrics.jsonl order_metrics`
5. **VM 1:** `python3 consumer.py order_metrics order_metrics_landed.jsonl 15`
6. **VM 1:** `spark-submit spark_job2_analytics.py`

> **Say to TA:** *"Kafka continued accepting and delivering orders without error. Because Kafka is an immutable log and our Spark job performs exact deduplication, repeating the pipeline during broker failure produces 100% identical accuracy."*

---

### 5. Restart Broker 2 & Show Recovery
On **VM 3 (Adarsh)**:
```bash
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/a2-kraft.properties
```

Wait 10 seconds, then on **VM 1** or **VM 2**, run:
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
```
> **Say to TA:** *"Broker 2 fetched missed logs from the leader and automatically caught up. The In-Sync Replicas list has restored to `Isr: 1,2`."*

---

## 🧹 PHASE 6: Demo-Day Clean Reset (Before Evaluation Slot)

To reset topics and remove previous test files before your live evaluated run:

1. **Delete old outputs:**
   - On **VM 1:**
     ```bash
     rm -f ~/a2/order_metrics_landed.jsonl ~/a2/restaurant_summary.csv ~/a2/city_summary.csv
     ```
   - On **VM 4:**
     ```bash
     rm -f ~/a2/orders_landed.jsonl ~/a2/order_metrics.jsonl
     ```
   *(Never delete `orders.jsonl`!)*

2. **Re-create empty topics (Run on VM 2 / Abhiroop):**
   ```bash
   for t in orders order_metrics; do
     $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --delete --topic $t 2>/dev/null || true
   done
   sleep 5
   for t in orders order_metrics; do
     $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --create --topic $t --partitions 2 --replication-factor 2 --config min.insync.replicas=1
   done
   ```
3. Run `bash preflight.sh <your_vm>` on all 4 machines to confirm `== READY`.
