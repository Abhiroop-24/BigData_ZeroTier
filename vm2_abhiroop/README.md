# VM 2 — Abhiroop (Kafka Broker 1 + KRaft Controller)

- **ZeroTier IP:** `10.35.23.108`
- **Role:** Broker 1 + Controller (`node.id=1`)
- **Quorum Voter:** `1@10.35.23.108:9093`
- **Cluster ID:** `haM9CpNJS2SxnRNHB_stXg`

---

### Status:
**Active and Running via systemd service.**

### ⚠️ Golden Rule:
**NEVER STOP KAFKA ON VM 2.** VM 2 is the sole controller.

### Useful Commands:
```bash
# Check service status
systemctl --user status kafka

# Describe topics
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 10.35.23.108:9092 --describe --topic orders

# Check registered brokers
$KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server 10.35.23.108:9092 | grep "id:"
```
