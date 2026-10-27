# VM 1 — Aadish (Producer 1 · Consumer 2 · Spark Job 2)

- **ZeroTier IP:** `10.35.23.124`
- **Kafka Bootstrap Server (`BS`):** `10.35.23.108:9092`

---

### 1. Setup in Terminal (run every time)
```bash
cd ~/a2
python3 -m venv venv && source venv/bin/activate
pip install "kafka-python>=3.0"

export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
```

### 2. Pre-Flight Readiness Check
```bash
bash preflight.sh vm1
# Expected output: == READY
```

---

### 3. Live Pipeline Steps

#### Step 2: Publish raw orders
*(Run immediately after Sheiman starts Consumer 1)*
```bash
python3 producer.py orders.jsonl orders
```
*Expected: Sent 62 records to 'orders'.*

#### Step 5: Receive enriched metrics
*(Run after Sheiman publishes enriched metrics)*
```bash
python3 consumer.py order_metrics order_metrics_landed.jsonl 15
```
*Expected: Landed 44 records.*

#### Step 6: Run Spark Analytics Job 2
```bash
spark-submit spark_job2_analytics.py
```
*Expected: Generates `restaurant_summary.csv` and `city_summary.csv`.*
