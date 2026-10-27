# VM 4 — Sheiman (Consumer 1 · Spark Job 1 · Producer 2)

- **ZeroTier IP:** `10.35.23.63`
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

### 2. Readiness Self-Test
```bash
spark-submit test_pipeline.py
# Must show: ALL CHECKS PASSED

bash preflight.sh vm4
# Must show: == READY
```

---

### 3. Live Pipeline Steps

#### Step 1: Start Consumer 1 (Run FIRST)
```bash
python3 consumer.py orders orders_landed.jsonl 60
```
*(Starts listening. Tell Aadish to publish orders immediately).*  
*Consumer will land 62 records and exit automatically.*

#### Step 3: Run Spark Job 1
*(Run after Consumer 1 finishes)*
```bash
spark-submit spark_job1_enrich.py
```
*Expected: 62 records read -> 44 valid, de-duplicated orders written to `order_metrics.jsonl`.*

#### Step 4: Publish Enriched Metrics
```bash
python3 producer.py order_metrics.jsonl order_metrics
```
*Expected: Sent 44 records to 'order_metrics'. (Tell Aadish to consume).*
