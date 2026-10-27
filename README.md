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

## ⚡ How Each Teammate Installs Their Folder in 1 Step

### 👤 Aadish (VM 1):
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm1_aadish ~/a2
cd ~/a2
```

### 👤 Adarsh (VM 3):
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm3_adarsh ~/a2
cd ~/a2
```

### 👤 Sheiman (VM 4):
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git repo
cp -r repo/vm4_sheiman ~/a2
cd ~/a2
```

### 👤 Abhiroop (VM 2):
Already installed and active at `~/a2`.

---

## 📌 Terminal Setup (EVERYONE runs this in EVERY terminal)

### 👤 Aadish (VM 1)
```bash
cd ~/a2 && source venv/bin/activate
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
```

### 👤 Abhiroop (VM 2)
```bash
cd ~/a2
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
```

### 👤 Adarsh (VM 3)
```bash
cd ~/a2
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
```

### 👤 Sheiman (VM 4)
```bash
cd ~/a2 && source venv/bin/activate
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
```

---

## 🏁 STEP 0: Pre-Flight Check (Run in parallel to verify readiness)

| Person | VM | Command to run | Expected Result |
| :--- | :--- | :--- | :--- |
| **Aadish** | VM 1 | `bash preflight.sh vm1` | `== READY` |
| **Abhiroop** | VM 2 | `bash preflight.sh vm2` | `== READY` *(Already passed)* |
| **Adarsh** | VM 3 | `bash preflight.sh vm3` | `== READY` |
| **Sheiman** | VM 4 | `bash preflight.sh vm4` | `== READY` |

---

## 🚀 STEP 1: Live Pipeline Run (Follow the strict sequence)

### 🔹 Stage 1: Order Ingestion & Cleaning
1. **Sheiman (VM 4)** runs **FIRST**:
   ```bash
   python3 consumer.py orders orders_landed.jsonl 60
   ```
   *(Starts listening on Kafka topic `orders`. Tell Aadish when running).*

2. **Aadish (VM 1)** runs immediately after Sheiman starts:
   ```bash
   python3 producer.py orders.jsonl orders
   ```
   *(Sends 62 raw records to Kafka topic `orders`).*

3. **Sheiman (VM 4)**:
   - Waits for consumer to finish (~10–15s).
   - Run Spark Job 1 to clean & enrich:
   ```bash
   spark-submit spark_job1_enrich.py
   ```
   *(Cleans and de-duplicates: 62 read ➔ 44 valid records written to `order_metrics.jsonl`).*

---

### 🔹 Stage 2: Enriched Metrics & Final Analytics
4. **Sheiman (VM 4)** publishes enriched metrics:
   ```bash
   python3 producer.py order_metrics.jsonl order_metrics
   ```
   *(Sends 44 enriched records to topic `order_metrics`).*

5. **Aadish (VM 1)** consumes enriched metrics:
   ```bash
   python3 consumer.py order_metrics order_metrics_landed.jsonl 15
   ```
   *(Lands 44 records into `order_metrics_landed.jsonl`).*

6. **Aadish (VM 1)** runs Spark Job 2:
   ```bash
   spark-submit spark_job2_analytics.py
   ```
   *(Computes `restaurant_summary.csv` and `city_summary.csv`).*

7. **Verify outputs on accuracy checker:**
   - Upload `order_metrics.jsonl` (from VM 4), `restaurant_summary.csv` (from VM 1), and `city_summary.csv` (from VM 1) to:  
     👉 **https://assignment2eval2026.vercel.app** (Team size: 4 / 5, Team: 10).  
   - Expected accuracy: **100% / 100% / 100%**.

---

## 💥 STEP 2: Broker Failure Test (Graded in front of TA)

1. **Before failure (Aadish or Abhiroop shows TA):**
   ```bash
   $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
   ```
   *(Point out: `Replicas: 1,2` and `Isr: 1,2` on both partitions).*

2. **Adarsh (VM 3) stops Broker 2:**
   ```bash
   $KAFKA_HOME/bin/kafka-server-stop.sh
   sleep 10 && jps
   ```
   *(Abhiroop / VM 2 NEVER STOPS).*

3. **Check ISR after failure (Aadish or Abhiroop):**
   ```bash
   $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
   ```
   *(Point out to TA: `Isr: 1`. Leadership moved completely to Broker 1. Zero unavailable partitions).*

4. **Replay the pipeline while VM 3 is down:**
   - Sheiman runs: `python3 consumer.py orders orders_landed.jsonl 60`
   - Aadish runs: `python3 producer.py orders.jsonl orders`
   - Sheiman runs: `spark-submit spark_job1_enrich.py`
   - Sheiman runs: `python3 producer.py order_metrics.jsonl order_metrics`
   - Aadish runs: `python3 consumer.py order_metrics order_metrics_landed.jsonl 15`
   - Aadish runs: `spark-submit spark_job2_analytics.py`
   *(Data flow continues without error; Spark drops the replayed duplicates so outputs stay identical).*

5. **Adarsh (VM 3) restarts Broker 2:**
   ```bash
   $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/a2-kraft.properties
   ```

6. **Verify ISR restored (Aadish or Abhiroop):**
   ```bash
   sleep 10
   $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
   ```
   *(Point out to TA: Broker 2 caught up and rejoined the ISR: `Isr: 1,2`).*
