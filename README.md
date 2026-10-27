# Team 10 - Big Data Assignment 2
## Distributed Food Ordering Pipeline across 4 VMs on ZeroTier

This repository contains all code, scripts, configuration, and step-by-step instructions for **Team 10**.

---

## 👥 Team & VM Assignments

| VM | Role | Owner | ZeroTier Node ID | ZeroTier IP | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **VM 1** | Producer 1 · Consumer 2 · Spark Job 2 | **Aadish** | `3220b05fad` | `10.35.23.124` | 🟢 Online |
| **VM 2** | Kafka Broker 1 **+ KRaft Controller** (never stopped) | **Abhiroop** | `159995b1b0` | `10.35.23.108` | 🟢 **Active & Ready** |
| **VM 3** | Kafka Broker 2 (stopped in failure test) | **Adarsh** | `16525a0f62` | `10.35.23.132` | 🟢 **Registered in Kafka** |
| **VM 4** | Consumer 1 · Spark Job 1 · Producer 2 | **Sheiman** | `b50563d3ae` | `10.35.23.63` | 🟢 Online |

### 🌐 Network Details
- **ZeroTier Network ID:** `a581878f7d78cd79`
- **Kafka Bootstrap Server (`BS`):** `10.35.23.108:9092`
- **KRaft Cluster ID:** `haM9CpNJS2SxnRNHB_stXg`

---

## ⚡ STEP 1: How Each Person Clones Their Folder (Run ONCE)

### 👤 Aadish (VM 1):
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm1_aadish ~/a2
cd ~/a2
python3 -m venv venv && source venv/bin/activate
pip install "kafka-python>=3.0"
```

### 👤 Abhiroop (VM 2):
*(Already running Kafka as Broker 1 + Controller at `~/a2`)*

### 👤 Adarsh (VM 3):
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm3_adarsh ~/a2
cd ~/a2
export KAFKA_HOME=/opt/kafka
bash kafka_node_setup.sh vm3 10.35.23.108 10.35.23.132 haM9CpNJS2SxnRNHB_stXg
```

### 👤 Sheiman (VM 4):
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm4_sheiman ~/a2
cd ~/a2
python3 -m venv venv && source venv/bin/activate
pip install "kafka-python>=3.0"
```

---

## 📌 STEP 2: Terminal Setup (Run in EVERY terminal before running commands)

- **Aadish (VM 1):**
  ```bash
  cd ~/a2 && source venv/bin/activate
  export KAFKA_HOME=/opt/kafka
  export BS="10.35.23.108:9092"
  ```
- **Abhiroop (VM 2):**
  ```bash
  cd ~/a2
  export KAFKA_HOME=/opt/kafka
  export BS="10.35.23.108:9092"
  ```
- **Adarsh (VM 3):**
  ```bash
  cd ~/a2
  export KAFKA_HOME=/opt/kafka
  export BS="10.35.23.108:9092"
  ```
- **Sheiman (VM 4):**
  ```bash
  cd ~/a2 && source venv/bin/activate
  export KAFKA_HOME=/opt/kafka
  export BS="10.35.23.108:9092"
  ```

---

## 🚦 STEP 3: Pre-Flight Check (Run simultaneously on all VMs)

Everyone runs their check in their terminal:

- **Aadish (VM 1):**
  ```bash
  bash preflight.sh vm1
  ```
- **Abhiroop (VM 2):**
  ```bash
  bash preflight.sh vm2
  ```
- **Adarsh (VM 3):**
  ```bash
  bash preflight.sh vm3
  ```
- **Sheiman (VM 4):**
  ```bash
  bash preflight.sh vm4
  ```

> **Target:** Every VM must output `== READY`.

---

## 🎬 STEP 4: Live Pipeline Execution (Follow STRICT order 1 to 6)

| Sequence | Who | VM | Exact Command | What it does / What to look for |
| :---: | :---: | :---: | :--- | :--- |
| **1** | **Sheiman** | **VM 4** | `python3 consumer.py orders orders_landed.jsonl 60` | **Starts FIRST.** Listens on topic `orders`. Tell Aadish to start. |
| **2** | **Aadish** | **VM 1** | `python3 producer.py orders.jsonl orders` | Publishes 62 raw records to Kafka topic `orders`. |
| **3** | **Sheiman** | **VM 4** | *(Wait ~10s for step 1 to finish)*<br>`spark-submit spark_job1_enrich.py` | Reads 62 records ➔ cleans & de-duplicates ➔ writes **44 valid records** to `order_metrics.jsonl`. |
| **4** | **Sheiman** | **VM 4** | `python3 producer.py order_metrics.jsonl order_metrics` | Publishes 44 records to Kafka topic `order_metrics`. Tell Aadish when sent. |
| **5** | **Aadish** | **VM 1** | `python3 consumer.py order_metrics order_metrics_landed.jsonl 15` | Lands 44 records from Kafka into `order_metrics_landed.jsonl`. |
| **6** | **Aadish** | **VM 1** | `spark-submit spark_job2_analytics.py` | Computes summaries ➔ writes `restaurant_summary.csv` and `city_summary.csv`. |

### 🏆 Checking Accuracy:
Upload the 3 output files to **https://assignment2eval2026.vercel.app**:
- `order_metrics.jsonl` (from VM 4)
- `restaurant_summary.csv` (from VM 1)
- `city_summary.csv` (from VM 1)
- Select Team size: `4 / 5`, Team: `10`. Expected: **100% / 100% / 100%**.

---

## 💥 STEP 5: Broker-Failure Test (Graded Demo with TA)

Follow this sequence in front of the TA:

### Sub-step 5.1: Show 2 Healthy Brokers
**Aadish (VM 1)** or **Abhiroop (VM 2)** runs:
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
```
> **Point out to TA:** `PartitionCount: 2`, `ReplicationFactor: 2`, `Isr: 1,2` on both partitions.

---

### Sub-step 5.2: Stop Broker 2 (Adarsh / VM 3)
**Adarsh (VM 3)** runs:
```bash
$KAFKA_HOME/bin/kafka-server-stop.sh
sleep 10 && jps
```
> *(Kafka line disappears on VM 3. **Abhiroop / VM 2 NEVER STOPS**).*

---

### Sub-step 5.3: Show Failover to TA
**Aadish (VM 1)** or **Abhiroop (VM 2)** runs:
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
```
> **Point out to TA:**
> - `Isr: 1` (Broker 2 dropped from ISR).
> - All partitions now led by `Leader: 1` (Abhiroop).
> - `0` unavailable partitions.

---

### Sub-step 5.4: Replay Pipeline During Outage
Repeat the exact pipeline (Steps 1 to 6 in STEP 4) while VM 3 is offline:
1. **VM 4 (Sheiman):** `python3 consumer.py orders orders_landed.jsonl 60`
2. **VM 1 (Aadish):** `python3 producer.py orders.jsonl orders`
3. **VM 4 (Sheiman):** `spark-submit spark_job1_enrich.py`
4. **VM 4 (Sheiman):** `python3 producer.py order_metrics.jsonl order_metrics`
5. **VM 1 (Aadish):** `python3 consumer.py order_metrics order_metrics_landed.jsonl 15`
6. **VM 1 (Aadish):** `spark-submit spark_job2_analytics.py`

> **Point out to TA:** The pipeline succeeds without errors and produces identical results even with Broker 2 down.

---

### Sub-step 5.5: Restart Broker 2 & Verify Recovery
**Adarsh (VM 3)** restarts his broker:
```bash
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/a2-kraft.properties
```

After 10 seconds, **Aadish (VM 1)** or **Abhiroop (VM 2)** verifies recovery:
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
```
> **Point out to TA:** Broker 2 has synchronized and rejoined the ISR: `Isr: 1,2`.
