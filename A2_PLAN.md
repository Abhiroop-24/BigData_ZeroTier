# A2 – Distributed Food Ordering System · Team 10 Master Plan

> Kafka (KRaft, 2 brokers) + Spark batch · 4 VMs on ZeroTier · Course: Big Data UE24CS343AB2
> Spec: https://big-data-2026-supersite.super.site/distributed-food-ordering-system
> Accuracy checker: https://assignment2eval2026.vercel.app (team size "4 / 5", team **10**)

---

## 0. Where we stand

| Item | Status |
|---|---|
| Pipeline code (producer, consumer, Spark Job 1, Spark Job 2) | ✅ Written. Tested locally on team 10's real dataset |
| Accuracy on the TA checker (`order_metrics.jsonl`, `city_summary.csv`, `restaurant_summary.csv`) | ✅ **100 % / 100 % / 100 %**, with 0 mismatched, 0 missing and 0 extra rows |
| Replay safety: Kafka delivers every record twice, shuffled | ✅ Outputs are byte-identical, so a re-run during the failure test can't hurt accuracy |
| Self-test (`test_pipeline.py`), hand-checked values incl. 15/30-min boundaries | ✅ Passes |
| 2-node KRaft config + setup script (`kafka_node_setup.sh`) | ✅ Written, syntax-checked |
| Pre-demo checker for the TA's checklist (`preflight.sh`) | ✅ Written, ISR parsing tested |
| **VM work: Phases 1–7 below** | ⏳ **Team does this on the VMs** |

The code can't be tested end-to-end over real Kafka from a laptop, so **Phase 5 (first full run) and Phase 6 (failure rehearsal) are where that gets proven**. Do them at least once before demo day.

---

## 1. The 60-second picture

```
                    ZeroTier = one private virtual LAN across all 4 laptops
                                                                                    
 VM1  (Producer 1, Consumer 2, Spark Job 2)          VM4  (Consumer 1, Spark Job 1, Producer 2)
 ┌──────────────────────────────┐                    ┌──────────────────────────────────┐
 │ orders.jsonl (62 raw records)│                    │                                  │
 │   │ producer.py              │                    │ consumer.py → orders_landed.jsonl│
 │   ▼                          │   ┌────────────┐   │   │                              │
 │ ── topic "orders" ───────────┼──►│  KAFKA     │──►┼───┘ spark_job1_enrich.py         │
 │                              │   │  VM2: broker 1 + KRaft controller (never stop)   │
 │                              │   │  VM3: broker 2 (stopped in failure test)         │
 │ consumer.py ◄────────────────┼───│  2 topics × 2 partitions × RF 2                  │
 │   ▼                          │   └────────────┘◄──┼── producer.py ◄─ order_metrics.jsonl (44)
 │ order_metrics_landed.jsonl   │      topic "order_metrics"                            │
 │   │ spark_job2_analytics.py  │                    └──────────────────────────────────┘
 │   ▼                          │
 │ restaurant_summary.csv       │
 │ city_summary.csv             │
 └──────────────────────────────┘
```

**Stage 1 (Order Enrichment):** VM1 publishes raw orders to Kafka. VM4 lands them in a local file, then Spark Job 1 cleans, de-duplicates and enriches them. VM4 then publishes the result to topic `order_metrics`.
**Stage 2 (Operations Analytics):** VM1 lands `order_metrics` in a local file. Spark Job 2 then computes the restaurant and city summaries.

**Rules from the spec we must not break:**
- Spark never reads Kafka directly. Data is always landed in a local file first.
- No HDFS.
- `order_metrics.jsonl` reaches VM1 **only through Kafka**, never by copying.
- Both topics have 2 partitions and replication factor 2, on the same cluster.
- Only VM3 is stopped in the failure test. **Never stop VM2.**

---

## 2. Files in this folder and where each one goes

**Copy the entire folder to all 4 VMs** as `~/a2`. That's the simplest way to make sure every VM has every file it needs.

| File | Runs on | What it does |
|---|---|---|
| `producer.py` | VM1, VM4 | Sends every non-blank line of a file to a topic, **byte-for-byte**, with `acks=all`. Prints the per-partition counts. |
| `consumer.py` | VM4, VM1 | Reads the whole topic from offset 0 and **overwrites** the output file. Exits after N idle seconds (default 15). |
| `spark_job1_enrich.py` | VM4 | Spark Job 1: cleaning rules → exact de-dup → metrics → `order_metrics.jsonl` |
| `spark_job2_analytics.py` | VM1 | Spark Job 2: exact de-dup → restaurant + city summaries (CSV). Imports 2 helpers from Job 1's file, so both files must be in the folder. |
| `test_pipeline.py` | VM1, VM4 | `spark-submit test_pipeline.py` → `ALL CHECKS PASSED` (hand-computed fixture) |
| `kafka_node_setup.sh` | VM2, VM3 | One-time: writes the KRaft config, wipes and formats storage, opens the firewall, starts Kafka |
| `preflight.sh` | all | Checks the TA's checklist: env vars, broker reachable, topics, ISR, outputs cleared, input file unmodified |
| `order.jsonl` | VM1 | Team 10's dataset as downloaded (62 records, sha256 `d0d430a0…c939`). On VM1 name it `orders.jsonl` (the TA's wording). |
| `A2_PLAN.md` | — | This document |

**Getting the folder onto the VMs:** use any method: a private GitHub repo with `git clone`, Google Drive/USB, or `scp -r A2_Food_Ordering_Pipeline <user>@<VM_ZT_IP>:~/a2`. If a file passes through Windows Notepad and a `.sh` script then fails with `$'\r': command not found`, run `sed -i 's/\r$//' ~/a2/*.sh`.

---

## 3. Who owns what, and when

Fill in the names. **Everyone must understand every part.** The viva is individual and the TA can ask anyone anything.

| VM | Role | Owner | ZeroTier IP |
|---|---|---|---|
| VM1 | Producer 1 · Consumer 2 · Spark Job 2 | ________ | __________ |
| VM2 | Kafka broker 1 **+ KRaft controller** (never stopped) | ________ | __________ |
| VM3 | Kafka broker 2 (stopped in failure test) | ________ | __________ |
| VM4 | Consumer 1 · Spark Job 1 · Producer 2 | ________ | __________ |

**Timeline.** Evaluation starts next week, so finish the rehearsal before then.

| When | What | Who | Time |
|---|---|---|---|
| Session 1 (ASAP) | Phases 1–3: network, Kafka cluster, topics | all 4 online together (VM2 + VM3 owners drive) | ~1.5 h |
| Session 2 | Phases 4–5: client VMs, first full pipeline run, checker = 100 % | VM1 + VM4 drive | ~1 h |
| Session 3 | Phase 6: broker-failure rehearsal, **twice** | VM2 + VM3 drive, everyone watches | ~45 min |
| Before demo day | Viva prep (§12). Each person explains one phase to the others. | all | ongoing |
| Demo day, 30–60 min before the slot | Phase 7: reset topics, clear outputs, `preflight.sh` = READY on all 4 VMs | all | 20 min |

Why this order: Kafka has to work before anything else, and the failure test only means something once the normal pipeline works. The reset goes last because the TA only accepts outputs from the **live** run.

---

## 4. Phase 1 – ZeroTier network (all 4 VMs)

**1.1 Find each VM's ZeroTier IP**
```bash
ip -4 -o addr show | grep zt
```
Each VM should show a `zt…` interface with an IP such as `10.147.x.y/24`. Write it in the table above. It must match the IP shown on my.zerotier.com next to that member.

**1.2 Check that every VM can reach every other VM**
```bash
ping -c 3 <OTHER_VM_ZT_IP>
```
**1.3 On VM1 and VM4, stop any leftover single-node Kafka from the course demos**
```bash
$KAFKA_HOME/bin/kafka-server-stop.sh; sleep 3; jps     # must NOT list "Kafka"
```

| If… | then… |
|---|---|
| you use the VM's NAT/LAN IP (e.g. `10.0.2.15`) instead of the ZeroTier IP | VMs on different laptops can't reach each other. Only the ZeroTier IPs work across laptops. |
| a member shows "Not authorized" or has no IP | the network admin must tick **Auth** for that member on my.zerotier.com |
| ping fails but the IPs are right | `sudo systemctl restart zerotier-one`, then check `sudo zerotier-cli listnetworks` shows `OK` |
| a stray single-node Kafka keeps running on VM1/VM4 | a typo like `BS=localhost:9092` would silently talk to that wrong broker. Stopping it removes the trap. |

---

## 5. Phase 2 – Build the 2-node Kafka cluster (VM2 first, then VM3)

**2.1 VM2: stop the old Kafka and generate ONE cluster ID**
```bash
$KAFKA_HOME/bin/kafka-server-stop.sh; sleep 3
$KAFKA_HOME/bin/kafka-storage.sh random-uuid      # e.g. q1Sh-9_ISia_zwGINzRvyQ, paste it in the team chat
```
**2.2 VM2: set up and start broker 1 + controller**
```bash
cd ~/a2
bash kafka_node_setup.sh vm2 <VM2_IP> <VM2_IP> <CLUSTER_ID>
```
Expected last line: `Kafka started as node 1 (broker,controller)`.

**2.3 VM3: same idea, as broker 2** (same VM2_IP, its own IP, the **same** cluster ID)
```bash
$KAFKA_HOME/bin/kafka-server-stop.sh; sleep 3
cd ~/a2
bash kafka_node_setup.sh vm3 <VM2_IP> <VM3_IP> <CLUSTER_ID>
```
Expected: `Kafka started as node 2 (broker)`.

**2.4 Verify the cluster from any VM**
```bash
export KAFKA_HOME=/opt/kafka
export BS="<VM2_IP>:9092"
$KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server $BS | grep "id:"
#   <VM2_IP>:9092 (id: 1 rack: null) -> (     ← two lines = both brokers registered
#   <VM3_IP>:9092 (id: 2 rack: null) -> (
$KAFKA_HOME/bin/kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
#   LeaderId: 1, CurrentVoters: only node 1, CurrentObservers: includes node 2
```

### What the script does, and why each config line exists

The course's Kafka page only shows a **single-node** setup with `localhost`. A cluster across VMs needs the settings below. The script writes them to `$KAFKA_HOME/config/a2-kraft.properties` and leaves the original `server.properties` untouched.

| Setting | VM2 | VM3 | Why |
|---|---|---|---|
| `process.roles` | `broker,controller` | `broker` | VM2 runs both roles in one JVM (combined mode). VM3 only stores data and serves clients. |
| `node.id` | `1` | `2` | Unique ID per node. It's the "1"/"2" you see in `Leader:`, `Replicas:` and `Isr:`. |
| `controller.quorum.voters` | `1@VM2_IP:9093` | same | Who the controller is. One voter, so VM2 is the only node that can manage metadata. |
| `listeners` | `PLAINTEXT://:9092,CONTROLLER://:9093` | `PLAINTEXT://:9092` | Ports to bind on all interfaces: 9092 for clients and replication, 9093 for controller traffic |
| `advertised.listeners` | `PLAINTEXT://VM2_IP:9092,…` | `PLAINTEXT://VM3_IP:9092` | **The address brokers tell clients to connect to.** It must be the ZeroTier IP. |
| `controller.listener.names` / `inter.broker.listener.name` / `listener.security.protocol.map` | | | Name the listeners and say both are plaintext |
| `log.dirs` | `/opt/kafka/data/kraft` | same | Where partitions live. Not in `/tmp`, which is wiped on reboot. |
| `num.partitions=2`, `default.replication.factor=2` | | | Safe defaults if a topic is ever auto-created |
| `min.insync.replicas=1` | | | With `acks=all`, a write needs at least 1 in-sync copy. **This keeps producing alive while VM3 is down** (see §9). |
| `offsets.topic.replication.factor=2` | | | `__consumer_offsets` gets 2 copies. Otherwise the TA's console consumer can hang when VM3 is down. |
| `transaction.state.log.*` | | | Same idea for the transactions topic |
| `auto.create.topics.enable=false` | | | A topic-name typo fails loudly instead of silently creating a wrong topic |

It also runs `sudo ufw allow in on <zt-interface> to any port 9092` (and 9093 on VM2). The course prerequisites enabled ufw with only SSH allowed, which would block Kafka between VMs.

| If you do it differently… | what happens |
|---|---|
| `advertised.listeners` left as `localhost` | Clients on other VMs connect to VM2, get told "the leader is at localhost:9092", connect to **themselves**, and time out. This is the #1 multi-VM Kafka bug. |
| Each VM formats with a **different** cluster ID | VM3 refuses to join (`InconsistentClusterIdException` / invalid cluster id in `server.log`) |
| Both VMs use `node.id=1` | VM3's registration is rejected as a duplicate broker |
| Old single-node data is kept in `log.dirs` | Start fails on a `meta.properties` mismatch (old node.id or cluster.id). The script wipes it for that reason. |
| Firewall ports not opened | Clients see connection timeouts, and `kafka-broker-api-versions` lists only one or zero brokers |
| VM3 set up before VM2 | Harmless. VM3 keeps retrying until the controller on VM2 is up. |
| `kafka_node_setup.sh` re-run later | It wipes that node's data again. Only use it for first-time setup. For restarts, use `kafka-server-start.sh -daemon $KAFKA_HOME/config/a2-kraft.properties`. |

**After any reboot of VM2/VM3**, Kafka does not auto-start. Start it with:
```bash
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/a2-kraft.properties
```
Start VM2 first.

---

## 6. Phase 3 – Topics (from any VM, with both brokers up)

**3.1 Terminal environment (the TA's format, required on every VM)**
```bash
export KAFKA_HOME=/opt/kafka
export BS="<VM2_ZEROTIER_IP>:9092"
```
(The TA's message is missing the closing `"` after `:9092`. Include it.)

**3.2 Create both topics**
```bash
for t in orders order_metrics; do
  $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --create --topic $t --partitions 2 --replication-factor 2
done
```
**3.3 Describe them** (this is what the TA checks before the failure test)
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic order_metrics
```
Expected for each topic:
```
Topic: orders  TopicId: …  PartitionCount: 2  ReplicationFactor: 2  Configs: …
    Topic: orders  Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1,2
    Topic: orders  Partition: 1  Leader: 2  Replicas: 2,1  Isr: 2,1
```
(Leader numbers may be swapped, and `Elr`/`LastKnownElr` columns may appear. Both are fine.)

**3.4 Create `__consumer_offsets` now, while both brokers are up**
```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server $BS --topic orders --from-beginning --timeout-ms 5000
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic __consumer_offsets | head -1   # ReplicationFactor: 2
```

| If… | then… |
|---|---|
| RF = 1 | Each partition lives on one broker only. When VM3 stops, the partition it held goes **offline**, the failure test fails, and data could be lost. |
| 1 partition | Works, but violates the spec and loses parallelism. The TA checks `PartitionCount: 2`. |
| Topics created while VM3 is down | Fails: replication factor 2 is larger than the 1 available broker |
| Step 3.4 skipped and the TA runs `kafka-console-consumer.sh` during the outage | If `__consumer_offsets` gets created then, with only 1 broker alive, RF 2 is impossible and the console consumer hangs on "coordinator not available". Our own `consumer.py` doesn't use a group, so it is unaffected. |

---

## 7. Phase 4 – Client VMs (VM1 and VM4)

**4.1 Python Kafka client in a venv** (the course's way: plain `python3`, which is 3.12)
```bash
cd ~/a2
python3 -m venv venv && source venv/bin/activate
pip install "kafka-python>=3.0"
```
**4.2 Spark check**
```bash
spark-submit --version        # version 3.3.4
echo $PYSPARK_PYTHON          # /usr/bin/python3.10
```
**4.3 Self-test** (~30 s)
```bash
spark-submit test_pipeline.py     # → ALL CHECKS PASSED
```
**4.4 VM1 only: the dataset**
Download it from https://kunjalpatwari.github.io/A2_Dataset_Download/ (enter **10**), or copy `order.jsonl` from this folder. Then:
```bash
mv order.jsonl ~/a2/orders.jsonl            # renaming doesn't change the content
sha256sum ~/a2/orders.jsonl                 # must be d0d430a0c89b84f374ac5f6d30f899827ba6eebb9346e877f5088c86a4e6c939
```
**Never edit or delete this file** (the TA's rule). `preflight.sh` verifies the hash.

| If… | then… |
|---|---|
| You run a job with `python3 spark_job1_enrich.py` instead of `spark-submit` | `ModuleNotFoundError: pyspark`. On the VMs, PySpark ships inside `/opt/spark`, and `spark-submit` wires it up. |
| Spark runs on Python 3.12 | PySpark 3.3.4 breaks (cloudpickle). `spark-submit` uses `PYSPARK_PYTHON=python3.10`, so the venv doesn't interfere. |
| An old `kafka-python` (2.0.x) is installed | Its default `retries=0` means a send during the leader change can fail. 3.x retries for up to 120 s and is idempotent. |
| The dataset is opened and saved in an editor | Line endings or whitespace may change, the hash won't match, and that counts as "modified". |

---

## 8. Phase 5 – Full pipeline run (rehearse it exactly like the demo)

Every terminal starts with:
```bash
export KAFKA_HOME=/opt/kafka
export BS="<VM2_IP>:9092"
cd ~/a2 && source venv/bin/activate        # (venv only on VM1/VM4)
```

| # | VM | Command | Expected output (team 10) |
|---|---|---|---|
| 1 | VM4 | `python3 consumer.py orders orders_landed.jsonl 60` | Waits. Start it **first**: it listens while VM1 produces. |
| 2 | VM1 | `python3 producer.py orders.jsonl orders` | `sent 62 records to 'orders' \| per partition: {0: a, 1: b}`, with a + b = 62 |
| 3 | VM4 | (step 1 finishes ~60 s after the last record) | `landed 62 records from 'orders' into orders_landed.jsonl …` |
| 4 | VM4 | `spark-submit spark_job1_enrich.py` | `Job 1: 62 records read -> 44 valid, de-duplicated orders written to order_metrics.jsonl`, then the table ON_TIME 11 · SLIGHT 11 · MODERATE 15 · SEVERE 7 |
| 5 | VM4 | `python3 producer.py order_metrics.jsonl order_metrics` | `sent 44 records to 'order_metrics' …` |
| 6 | VM1 | `python3 consumer.py order_metrics order_metrics_landed.jsonl` | `landed 44 records …` (after 15 s idle) |
| 7 | VM1 | `spark-submit spark_job2_analytics.py` | The two tables below, and `Job 2: wrote restaurant_summary.csv and city_summary.csv` |
| 8 | any | Get the 3 output files onto one machine and upload them to the checker (team 10) | 100 / 100 / 100 |

**Expected Job 2 output (team 10).** Check the live run against this:
```
restaurant_id order_count avg_order_amount avg_preparation_time_min avg_delivery_time_min delayed_order_count delay_rate_pct
R01           8           639.0            12.5                     20.0                  4                   50.0
R02           8           629.75           12.75                    25.0                  8                   100.0
R03           7           542.0            13.43                    30.0                  4                   57.14
R04           7           639.0            13.29                    35.0                  7                   100.0
R05           7           614.57           13.14                    40.0                  3                   42.86
R06           7           590.14           13.0                     45.0                  7                   100.0

city      order_count avg_order_amount avg_delivery_time_min delayed_order_count delay_rate_pct
Bengaluru 16          634.38           22.5                  12                  75.0
Hyderabad 7           590.14           45.0                  7                   100.0
Mumbai    14          590.5            32.5                  11                  78.57
Pune      7           614.57           40.0                  3                   42.86
```

**Why 62 → 44 (know this for the viva).** 18 records are dropped:

| Reason | Count |
|---|---|
| Exact duplicates (a second identical copy) | 4 |
| `status` not DELIVERED (3 CANCELLED, 1 PENDING, 1 REFUNDED, 1 FAILED) | 6 |
| Missing almost all fields (`{"order_id":"BROKEN",…}` and 2 × `invalid_order_record`) | 3 |
| Missing `city` | 1 |
| Invalid timestamp (`"bad"`, `"not-a-timestamp"`) | 2 |
| `order_amount` = 0 | 1 |
| `distance_km` ≤ 0 | 1 |

Some order IDs (e.g. `T020002`, `T020005`) appear twice: once broken and once valid. Only the valid copy survives. That is correct: they are **not** exact duplicates, and the cleaning rules remove the broken one.

### Design choices in the code, and what the alternatives would do

| Choice | Why | If done the other way |
|---|---|---|
| Producer sends **raw lines**, no parsing | Cleaning is Spark Job 1's job, per the spec | Parsing in the producer would drop malformed lines before Kafka, so Spark would never see them and you'd be "adding business rules" outside Spark |
| Producer `acks="all"` + `flush()` + checks each send | Every record is confirmed by all in-sync replicas. A failed send crashes loudly. | `acks=0`/`1` is faster but can lose acknowledged data if the leader dies. Without `flush()`, the script can exit before its batches are sent, which loses records. |
| Consumer `group_id=None` + `auto_offset_reset="earliest"` | Each run replays the **whole topic** and overwrites the landed file, so a repeat run is always complete | With a consumer group, offsets get committed, so the **second run reads nothing**: an empty landed file and 0 output rows. With `"latest"`, a consumer started after the producer reads nothing. |
| Consumer stops after N idle seconds | Batch semantics: land what's there, then stop | With no timeout the consumer runs forever and the next step never starts. Too short a timeout (e.g. 2 s during a failover) exits early with partial data, so use 60 during the failure test. |
| Explicit Spark schema | Malformed JSON and wrong types become `null` and are filtered out. The types are fixed. | With schema inference, malformed lines go into a `_corrupt_record` column and types are guessed |
| Exact de-dup = `dropDuplicates(all 14 fields)` | The spec says "exact duplicate orders counted once" | De-duping on `order_id` only is a **different rule**: it can keep a broken copy and drop the valid one, which gives wrong counts |
| De-dup again in Job 2 | Kafka is at-least-once, so a replayed topic must not double-count | Without it, re-running Stage 1 during the failure test would double every count in the summaries |
| `file://` absolute paths | The Spark install configured Hadoop, so a bare path may resolve to **HDFS** | `Path does not exist: hdfs://…`, and HDFS is forbidden anyway |
| Session timezone = UTC | Minute differences never jump at a DST change | Usually the same result, but wrong by 60 min across a DST change |
| Built-in Spark functions, no Python UDFs | Runs in the JVM and is optimised by Catalyst | UDFs serialise every row to Python workers: slower, and more ways to break |
| Results written with `collect()` to **one** file | The checker needs exactly `order_metrics.jsonl` / `*.csv` files | `df.write.json("order_metrics.jsonl")` makes a **folder** of `part-*.json` files, which the checker can't take. (`collect()` only suits small data; for millions of rows use `coalesce(1).write` and rename.) |
| `delay_category` uses `≤15` / `≤30` | Matches the spec's 1–15 / 16–30 / >30. All team 10 times are whole minutes. | — |
| Field name `order_amount` | The spec's table shows `order amount` (a typo). The checker accepted `order_amount` at 100 %. | — |

---

## 9. Phase 6 – Broker-failure test (graded; rehearse it twice)

The TA will check 4 things. Here's how to show each one.

**6.1 Before the failure: topic configuration and ISR**
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic order_metrics
$KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server $BS | grep "id:"     # 2 brokers
```
Point out: **PartitionCount 2, ReplicationFactor 2, Replicas 1,2 / 2,1, Isr has both brokers**, and each broker leads one partition.

**6.2 Stop VM3 (only VM3, never VM2)**
```bash
# on VM3
$KAFKA_HOME/bin/kafka-server-stop.sh
sleep 10; jps            # no "Kafka" line
```
Graceful stop: VM3 first hands over leadership, then shuts down. If the TA wants a **crash**, use `kill -9 $(jps | awk '/Kafka/{print $1}')` instead. The controller then notices after about 9 s (`broker.session.timeout.ms`), so wait 10–15 s before describing.

**6.3 After the failure: ISR and leadership**
```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders
#   Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1
#   Partition: 1  Leader: 1  Replicas: 2,1  Isr: 1      ← leadership moved from 2 to 1
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions   # all 4 partitions listed
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions        # EMPTY = nothing offline
```
Say this: "Replicas still lists 2 because that's the *assignment*. ISR shrank to [1] because broker 2 is not in sync. Every partition still has a leader, so nothing is unavailable and no data was lost: broker 1 already had a full copy because RF = 2."

**6.4 Pipeline continuity while VM3 is down.** Re-run Phase 5 steps 1–7, using `60` as the idle time on both consumers.
- `orders` now holds the earlier 62 records plus 62 new ones, so VM4 lands **124**. Job 1 prints `124 records read -> 44 valid…`, because exact duplicates are removed. All outputs are **identical** to the first run, which we verified locally with a doubled, shuffled topic.
- If the TA asks "why 124?": "Kafka keeps data after it's read (it's a log, not a queue). Our consumer replays the full topic, and Spark's exact-duplicate removal makes the replay harmless."
- The producer's per-partition counts still show both partitions. Both are now led by broker 1.

**6.5 Restart VM3 and show it rejoining the ISR**
```bash
# on VM3 (NOT kafka_node_setup.sh, which would wipe the data)
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/a2-kraft.properties
# from any VM, after 10–20 s
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders      # Isr: 1,2 again
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions   # empty
```
VM3 copied the records it missed from the leader, then rejoined the ISR. Leadership may stay on broker 1. Kafka restores the "preferred" leader on its own within about 5 min (`auto.leader.rebalance.enable`). To show it immediately:
```bash
$KAFKA_HOME/bin/kafka-leader-election.sh --bootstrap-server $BS --election-type preferred --all-topic-partitions
```
**6.6 Run the pipeline once more** (Phase 5) to show it keeps working with both brokers back.

| If you do it differently… | what happens |
|---|---|
| **Stop VM2** | It's the **only controller** (the only voter in the quorum). With no controller there are no leader elections and no metadata changes, broker 2 can't stay registered, and broker 1's partitions go offline too. The cluster stops working, which is why the TA forbids it. Fixing that needs 3 controllers (a quorum of 3), which is out of scope. |
| `min.insync.replicas=2` | While VM3 is down the ISR is 1 < 2, so every `acks=all` write fails with `NotEnoughReplicas` and **continuity fails**. We use 1. The trade-off is that for that window the data has a single copy. |
| `acks=1` | Continuity still works, but if the leader died right after acking, before the follower copied the record, that record would be lost |
| RF = 1 | The partition that lived only on VM3 goes offline, `--unavailable-partitions` lists it, and producers/consumers fail for it |
| `BS` points only at VM3 | Clients can't even bootstrap once VM3 stops. Our `BS` is VM2, which never stops. (You may also list both: `BS=VM2:9092,VM3:9092`.) |
| Restart VM3 with `kafka_node_setup.sh` | Its data is wiped and fully re-copied from VM2. It still works, but it's slower and needless. |

---

## 10. Phase 7 – Demo-day reset (30–60 min before the slot)

**7.1** Both brokers up (`jps` on VM2/VM3). If needed, start VM2 first, then VM3.

**7.2 Recreate both topics** so the live run starts clean (62 → 62 → 44 → 44, no replay counts):
```bash
for t in orders order_metrics; do $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --delete --topic $t; done
sleep 10
for t in orders order_metrics; do
  $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --create --topic $t --partitions 2 --replication-factor 2
done
```
(If `create` says "marked for deletion", wait 10 s and run it again.) The topic **`orders`** is not the file **`orders.jsonl`**. Never touch the file.

**7.3 Clear old outputs** (the TA's rule). Delete exactly these files and **never run `rm *.jsonl` on VM1**.
```bash
# VM4
rm -f ~/a2/orders_landed.jsonl ~/a2/order_metrics.jsonl
# VM1
rm -f ~/a2/order_metrics_landed.jsonl ~/a2/restaurant_summary.csv ~/a2/city_summary.csv
```
**7.4 Every VM: open the demo terminal and run preflight**
```bash
export KAFKA_HOME=/opt/kafka
export BS="<VM2_IP>:9092"
cd ~/a2 && source venv/bin/activate      # VM1/VM4
bash preflight.sh vm1                    # vm2 / vm3 / vm4 on the others → must end with "== READY"
```
**7.5** Laptops on charger, sleep disabled, ZeroTier online, and **leave that terminal open**. The exported variables only live in that terminal.

---

## 11. Demo-day runbook (likely order)

1. **Intro (30 s):** the architecture (§1), who owns which VM, and that ZeroTier connects them.
2. **Show the setup:** run `cat $KAFKA_HOME/config/a2-kraft.properties` on VM2 and VM3. Then the topic describe plus `kafka-broker-api-versions` (6.1).
3. **Live pipeline:** Phase 5 steps 1–7, narrating each step. The TA uploads or records the outputs.
4. **Failure test:** 6.2 → 6.3 → 6.4 → 6.5 → 6.6.
5. **Viva:** individual questions (§12).

Say out loud what each step proves. For example: "VM4 got all 62 records split across both partitions, so Kafka moved the data between VMs."

---

## 12. Viva prep (individual marks, so everyone learns all of this)

### 12.1 The pipeline in 60 seconds (everyone memorise)
"A food platform's orders go through a two-stage batch pipeline across four VMs on a ZeroTier network. VM1 publishes the raw orders to the Kafka topic `orders`. Kafka runs in KRaft mode on VM2, which is broker plus controller, and VM3, which is a broker. Both topics have 2 partitions and replication factor 2. VM4 consumes the topic into a local file. Spark Job 1 drops invalid records and exact duplicates, then computes preparation, delivery and fulfilment times, speed, delay and delay category. VM4 publishes that to `order_metrics`. VM1 consumes it, and Spark Job 2 computes restaurant- and city-level averages and delay rates. Kafka gives reliable, replicated data movement; Spark does the computation. Because every partition has a replica on both brokers, the pipeline keeps running when VM3 fails."

### 12.2 Kafka concepts
| Q | A |
|---|---|
| What is a topic, partition and offset? | A topic is a named stream. It's split into partitions, each an append-only, ordered log. An offset is a record's position in its partition. Ordering is guaranteed **only within a partition**. |
| Why 2 partitions? | Parallelism: up to 2 consumers in a group could share the work, and partition leadership spreads load across the 2 brokers. The spec also requires it. |
| Replication factor 2? | Each partition has 2 copies, one per broker: a leader that handles reads/writes, and a follower that copies from the leader. |
| What is the ISR? | The In-Sync Replicas: replicas that have caught up with the leader within `replica.lag.time.max.ms` (30 s). Only ISR members can become leader without losing data. |
| What happened to the ISR when VM3 stopped? | It shrank from `1,2` to `1`. Partitions led by 2 elected 1 as leader. Replicas stayed `1,2` because that's the assignment, not the live state. |
| How does VM3 rejoin? | On restart it fetches the records it missed from the leader. Once caught up, the leader adds it back to the ISR. |
| What does `acks=all` mean? | The leader acknowledges only after every ISR member has the record. That's the strongest durability. |
| What is `min.insync.replicas`? | The minimum ISR size for an `acks=all` write to succeed. Ours is 1, so writes continue with one broker. 2 would block writes during the failure. |
| High watermark? | The highest offset copied to all ISR members. Consumers only read up to it, so they never see data that could be lost. |
| Is Kafka a queue? | No, it's a log. Reading doesn't delete anything. Data stays until retention (7 days by default), so we can **replay** it. |
| Consumer group? | Consumers sharing a `group.id` split the partitions between them and commit offsets in `__consumer_offsets`. We deliberately use **no group**, so every run re-reads the whole topic (batch semantics). |
| Delivery semantics? | Kafka gives at-least-once by default. Our producer is idempotent (kafka-python 3.x), which avoids duplicates from its own retries. Spark's exact-duplicate removal makes any replay harmless, so the result is effectively exactly-once. |
| What is KRaft? | Kafka Raft. Kafka stores its own metadata (topics, partitions, leaders, ISR) in an internal Raft-replicated log, `__cluster_metadata`, managed by controller nodes. It **replaces ZooKeeper**: one system instead of two, faster failover, more partitions. |
| What does the controller do? | It's the cluster brain: registers brokers, tracks heartbeats, elects partition leaders, updates the ISR in the metadata log, and pushes metadata to brokers. |
| Why can't VM2 be stopped? | It's the **only** voter in the controller quorum. With no controller there are no leader elections or metadata updates, and it's also broker 1, which leads partitions. HA for the controller needs 3 voters (a majority of 3 survives 1 failure). |
| `listeners` vs `advertised.listeners`? | `listeners` is what the broker binds to. `advertised.listeners` is the address it tells clients to use, so it must be the ZeroTier IP. |
| Bootstrap server? | Just the first contact point. The client fetches cluster metadata from it (all brokers and partition leaders), then talks to the leaders directly. |
| Why `node.id` 1 and 2? | Unique broker IDs. They're the numbers in Leader/Replicas/Isr. |

### 12.3 Spark concepts
| Q | A |
|---|---|
| Why Spark here? | Distributed, in-memory DataFrame processing. The same code would scale from 62 records to billions across a cluster. |
| Driver vs executor? `local[*]`? | The driver plans the job and the executors run tasks. `local[*]` runs both in one JVM, using every CPU core of the VM. |
| Lazy evaluation? | Transformations (`filter`, `withColumn`, `groupBy`) only build a plan (a DAG). Nothing runs until an **action** (`count`, `collect`, `show`). Catalyst then optimises the whole plan. |
| Narrow vs wide transformations? | Narrow transformations (`filter`, `withColumn`) work within a partition. Wide ones (`groupBy`, `dropDuplicates`, `orderBy`) **shuffle** data across partitions, which creates a stage boundary. |
| How do you handle malformed JSON? | We read with an explicit schema. A malformed line or wrong-typed value becomes null, and the validity filter drops any record with a null/blank required field, a bad timestamp, `distance_km ≤ 0`, `order_amount ≤ 0`, or status ≠ DELIVERED. |
| How are minutes computed? | Parse with `to_timestamp(…, 'yyyy-MM-dd HH:mm:ss')`, cast to epoch seconds, subtract, and divide by 60. |
| Delay category boundaries? | 0 → ON_TIME, ≤15 → SLIGHT, ≤30 → MODERATE, >30 → SEVERE. delivery_status is ON_TIME if delay = 0, otherwise DELAYED. |
| How is `delay_rate_pct` computed? | 100 × delayed_order_count / order_count, rounded to 2 decimal places |
| Why not read Kafka from Spark directly? | The spec forbids it. Landing to a file decouples the stages: each Spark job is a repeatable batch over a fixed snapshot, and Kafka and Spark can fail independently. |
| Why batch, not Structured Streaming? | The spec asks for batch, and the data is a finite set of completed orders. Streaming would add windows, watermarks and checkpoints we don't need. |
| Why `cache()` in Job 1? | The output DataFrame is used three times (write, count, category table). Caching computes it once. |

### 12.4 ZeroTier
| Q | A |
|---|---|
| What is it? | A software-defined virtual network. Every VM gets a private IP on one virtual LAN, even when the laptops are behind different Wi-Fi/NAT. Traffic is peer-to-peer and encrypted. |
| Why do we need it? | Kafka brokers and clients need direct IP connectivity on ports 9092/9093. VMs on different laptops can't reach each other otherwise. |
| What if a laptop's Wi-Fi changes? | The ZeroTier IP stays the same, so the Kafka config keeps working |

### 12.5 "What if…" questions the TA likes
Use the "If you do it differently" tables in §§4–9. Every row there is a viva answer.

### 12.6 Where each rule lives in the code (show the TA)
- Cleaning rules: [spark_job1_enrich.py:52-59](spark_job1_enrich.py:52)
- Exact de-dup, Job 1: [spark_job1_enrich.py:61](spark_job1_enrich.py:61). Job 2: [spark_job2_analytics.py:29](spark_job2_analytics.py:29)
- Time formulas, delay, status, category: [spark_job1_enrich.py:63-78](spark_job1_enrich.py:63)
- Aggregations: [spark_job2_analytics.py:31-41](spark_job2_analytics.py:31)
- `acks=all`: [producer.py:22](producer.py:22). No-group full replay: [consumer.py:22-25](consumer.py:22)

---

## 13. Troubleshooting

| Symptom | Cause → fix |
|---|---|
| `NoBrokersAvailable` / `TimeoutException` from clients | `BS` is wrong or unset in this terminal, the firewall is closed, or Kafka isn't running on VM2 → `echo $BS`, `bash preflight.sh` |
| Client connects but then hangs or times out | `advertised.listeners` isn't the ZeroTier IP → `cat $KAFKA_HOME/config/a2-kraft.properties` |
| VM3 won't start; `server.log` mentions the cluster id | A different cluster ID was used on VM3 → re-run `kafka_node_setup.sh vm3 …` with VM2's ID |
| `Replication factor: 2 larger than available brokers: 1` | VM3 is down. Start it, then create the topics. |
| Consumer lands 0 records | The producer hasn't run yet, or the topic was just recreated, or the idle timeout was too short → run the producer, then the consumer again |
| `Path does not exist: hdfs://…` | You're running an old copy of the code. These jobs use `file://`. |
| `ModuleNotFoundError: kafka` | The venv isn't active → `source ~/a2/venv/bin/activate` |
| Kafka gone after a VM reboot | It doesn't auto-start → start command at the end of §5 |
| Spark prints lots of `WARN` lines | Normal. The job result lines start with `Job 1:` / `Job 2:`. |
| Checker shows "mismatched rows" | Old files were uploaded, or `orders.jsonl` was modified → check the hash and redo Phase 7 |

Logs: `tail -50 $KAFKA_HOME/logs/server.log` on VM2/VM3.

---

## 14. TA checklist → how we meet it

| TA requirement | How | Check |
|---|---|---|
| `KAFKA_HOME` and `BS` exported in the Kafka terminal on each VM | §6 step 3.1, §10 step 7.4 | `preflight.sh` lines 1–3 |
| All previous output files cleared, `orders.jsonl` untouched | §10 step 7.3 | `preflight.sh`: "old output … cleared" plus the sha256 check of the input |
| Accuracy from the **live** run only | Topics recreated, live run during the demo | Expected numbers in §8 |
| Topic config and ISR before the failure | §9 step 6.1 | `preflight.sh` topic lines |
| ISR and leadership after stopping a broker | §9 steps 6.2–6.3 (stop **VM3** only) | `Isr: 1`, `Leader: 1`, `--unavailable-partitions` empty |
| Kafka reachable, data flow continues | §9 step 6.4 | 124 → 44 → identical outputs |
| Restarted broker rejoins ISR, pipeline continues | §9 steps 6.5–6.6 | `Isr: 1,2`, under-replicated list empty |
| VM2 never stopped | §9 | — |
| Viva, individual | §12 | Each member explains §1 and answers §12 cold |

## 15. Assumptions and open points

- The spec's VM table says VM3 is "broker and controller", but its text and the TA both say VM2 is the controller. We follow the text: **VM2 = broker + only controller, VM3 = broker only**.
- The spec writes `order amount` in Job 1's schema. We output `order_amount`, which the checker scored at 100 %.
- The download page names the file `order.jsonl`; the TA calls it `orders.jsonl`. Same content; we rename it on VM1.
- Kafka 3.9.2, Spark 3.3.4, Java 11 and Python 3.10 (for Spark) per the course "Versioning" page. `kafka-python>=3.0` runs on the system Python 3.12 venv.
