# Team 10 - Big Data Assignment 2
## Distributed Food Ordering Pipeline across 4 VMs on ZeroTier

This repository contains all code, scripts, configuration, and step-by-step instructions for **Team 10**.

---

## 👥 Team & VM Assignments

| VM | Role | Owner | ZeroTier Node ID | ZeroTier IP |
| :--- | :--- | :--- | :--- | :--- |
| **VM 1** | Producer 1 · Consumer 2 · Spark Job 2 | **Aadish** | `3220b05fad` | `10.35.23.124` |
| **VM 2** | Kafka Broker 1 **+ KRaft Controller** (never stopped) | **Abhiroop** | `159995b1b0` | `10.35.23.108` |
| **VM 3** | Kafka Broker 2 (stopped in failure test) | **Adarsh** | `16525a0f62` | `10.35.23.132` |
| **VM 4** | Consumer 1 · Spark Job 1 · Producer 2 | **Sheiman** | `b50563d3ae` | `10.35.23.63` |

### 🌐 Network Details
- **ZeroTier Network ID:** `a581878f7d78cd79`
- **Kafka Bootstrap Server (`BS`):** `10.35.23.108:9092`
- **KRaft Cluster ID:** `haM9CpNJS2SxnRNHB_stXg`

---

## ⚡ Quick Setup for Each Person

### 1. Common for EVERYONE: Clone this repo
```bash
cd ~
git clone https://github.com/Abhiroop-24/BigData_ZeroTier.git a2
cd ~/a2
```

---

### 2. VM 2 Setup (Abhiroop - Already Running)
- Runs Kafka Broker 1 + KRaft Controller.
- Listens on `10.35.23.108:9092` and Controller quorum on `10.35.23.108:9093`.

---

### 3. VM 3 Setup (Adarsh - Kafka Broker 2)

On **VM 3**, run:
```bash
cd ~/a2
export KAFKA_HOME=/opt/kafka    # or path to your kafka directory
bash kafka_node_setup.sh vm3 10.35.23.108 10.35.23.132 haM9CpNJS2SxnRNHB_stXg
```
Expected output:
```text
Kafka started as node 2 (broker)
```

#### In every new terminal on VM 3, export:
```bash
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
```

---

### 4. VM 1 Setup (Aadish - Producer 1, Consumer 2, Spark Job 2)

#### Step 4.1: Setup Python virtualenv
```bash
cd ~/a2
python3 -m venv venv
source venv/bin/activate
pip install "kafka-python>=3.0"
```

#### Step 4.2: Export Environment Variables (in every terminal)
```bash
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
cd ~/a2 && source venv/bin/activate
```

#### Step 4.3: Ensure Dataset is Ready
Make sure `order.jsonl` is present in `~/a2/orders.jsonl`:
```bash
cp ~/a2/order.jsonl ~/a2/orders.jsonl
sha256sum ~/a2/orders.jsonl
# Expected sha256: d0d430a0c89b84f374ac5f6d30f899827ba6eebb9346e877f5088c86a4e6c939
```

---

### 5. VM 4 Setup (Sheiman - Consumer 1, Spark Job 1, Producer 2)

*(If Sheiman is unavailable, VM 4 commands can be executed on VM 1 or another machine).*

#### Step 5.1: Setup Python virtualenv
```bash
cd ~/a2
python3 -m venv venv
source venv/bin/activate
pip install "kafka-python>=3.0"
```

#### Step 5.2: Export Environment Variables (in every terminal)
```bash
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
cd ~/a2 && source venv/bin/activate
```

#### Step 5.3: Run Self-Test
```bash
spark-submit test_pipeline.py
# Must output: ALL CHECKS PASSED
```

---

## 🚀 Running the Full Pipeline (Phase 5)

Once both brokers (VM 2 and VM 3) are active:

### Step 0: Create Topics (Run once from VM 2)
```bash
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"

for t in orders order_metrics; do
  $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --create --topic $t --partitions 2 --replication-factor 2
done

# Initialize consumer offsets
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server $BS --topic orders --from-beginning --max-messages 0 --timeout-ms 5000 2>/dev/null || true
```

### Execution Order:

| Step | VM | Command | What happens |
| :---: | :---: | :--- | :--- |
| **1** | **VM 4** | `python3 consumer.py orders orders_landed.jsonl 60` | Starts consumer, waits for orders |
| **2** | **VM 1** | `python3 producer.py orders.jsonl orders` | Publishes 62 records across 2 partitions |
| **3** | **VM 4** | *(Wait for Step 1 consumer to finish)* | Lands 62 records into `orders_landed.jsonl` |
| **4** | **VM 4** | `spark-submit spark_job1_enrich.py` | Enriches data → outputs `order_metrics.jsonl` (44 valid records) |
| **5** | **VM 4** | `python3 producer.py order_metrics.jsonl order_metrics` | Publishes 44 records to topic `order_metrics` |
| **6** | **VM 1** | `python3 consumer.py order_metrics order_metrics_landed.jsonl 15` | Lands 44 records into `order_metrics_landed.jsonl` |
| **7** | **VM 1** | `spark-submit spark_job2_analytics.py` | Generates `restaurant_summary.csv` and `city_summary.csv` |

---

## 🛠️ Broker-Failure Test (Phase 6 - Graded)

1. **Before failure (verify 2 brokers & ISR):**
   ```bash
   $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
   # Shows: Replicas: 1,2 / 2,1 and Isr: 1,2
   ```

2. **Stop VM 3 (only VM 3, NEVER stop VM 2):**
   On VM 3:
   ```bash
   $KAFKA_HOME/bin/kafka-server-stop.sh
   ```

3. **Check ISR after failure:**
   ```bash
   $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
   # ISR shrinks to: Isr: 1
   ```

4. **Run pipeline continuity:**
   Re-run pipeline steps 1–7. Producer succeeds without loss. Output files remain 100% identical.

5. **Restart VM 3:**
   On VM 3:
   ```bash
   $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/a2-kraft.properties
   ```
   After 10 seconds:
   ```bash
   $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
   # Shows ISR restored: Isr: 1,2
   ```
