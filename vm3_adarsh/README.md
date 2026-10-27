# VM 3 — Adarsh (Kafka Broker 2)

- **ZeroTier IP:** `10.35.23.132`
- **Controller (VM 2 IP):** `10.35.23.108`
- **Cluster ID:** `haM9CpNJS2SxnRNHB_stXg`

---

### 1. Setup Command (Run once)
```bash
cd ~/a2
export KAFKA_HOME=/opt/kafka
bash kafka_node_setup.sh vm3 10.35.23.108 10.35.23.132 haM9CpNJS2SxnRNHB_stXg
```
*Expected output: Kafka started as node 2 (broker).*

### 2. Environment Variables (run in all terminals)
```bash
export KAFKA_HOME=/opt/kafka
export BS="10.35.23.108:9092"
```

### 3. Readiness Check
```bash
bash preflight.sh vm3
# Expected output: == READY
```

---

### 4. Broker Failure Test Role (Phase 6)
During the failure demonstration:
1. **Stop Broker 2:**
   ```bash
   $KAFKA_HOME/bin/kafka-server-stop.sh
   sleep 10 && jps
   ```
2. **Restart Broker 2 (after team finishes failure test run):**
   ```bash
   $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/a2-kraft.properties
   ```
   *(NEVER run kafka_node_setup.sh during recovery; it wipes storage!).*
