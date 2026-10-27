#!/usr/bin/env bash
# Pre-demo readiness check (the TA's checklist). Run it in the SAME terminal where you
# exported KAFKA_HOME and BS (and, on VM1/VM4, activated the venv):
#   bash preflight.sh vm1|vm2|vm3|vm4
ROLE=${1:?usage: bash preflight.sh vm1|vm2|vm3|vm4}
cd "$(dirname "$0")"
INPUT_SHA=d0d430a0c89b84f374ac5f6d30f899827ba6eebb9346e877f5088c86a4e6c939  # team 10 order.jsonl as downloaded
FAIL=0
ok()  { echo "  OK    $*"; }
bad() { echo "  FAIL  $*"; FAIL=1; }
chk() { local msg=$1; shift; if "$@" >/dev/null 2>&1; then ok "$msg"; else bad "$msg"; fi; }
absent() { for f in "$@"; do chk "old output $f cleared" test ! -e "$f"; done; }
topics() { "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" "$@" 2>/dev/null; }

echo "== environment ($ROLE)"
chk "KAFKA_HOME=$KAFKA_HOME has bin/kafka-topics.sh" test -x "$KAFKA_HOME/bin/kafka-topics.sh"
chk "BS is set ($BS)" test -n "$BS"
B=${BS%%,*}
chk "broker $B reachable" timeout 5 bash -c "exec 3<>/dev/tcp/${B%:*}/${B##*:}"

case $ROLE in
  vm2|vm3)
    chk "Kafka process running (jps)" bash -c "jps | grep -qw Kafka"
    chk "port 9092 listening" bash -c "ss -tln | grep -q ':9092 '"
    if [ "$ROLE" = vm2 ]; then chk "controller port 9093 listening" bash -c "ss -tln | grep -q ':9093 '"; fi ;;
  vm1|vm4)
    chk "python3 imports kafka (venv active?)" python3 -c "import kafka"
    chk "spark-submit on PATH" command -v spark-submit
    chk "pipeline code present" test -f producer.py -a -f consumer.py -a -f spark_job1_enrich.py
    chk "Spark self-check passes (spark-submit test_pipeline.py)" bash -c "spark-submit test_pipeline.py 2>&1 | grep -q 'ALL CHECKS PASSED'"
    if [ "$ROLE" = vm1 ]; then
      IN=$(ls orders.jsonl order.jsonl 2>/dev/null | head -1)
      chk "input dataset present and unmodified (${IN:-missing})" test "$(sha256sum "${IN:-/dev/null}" | cut -d' ' -f1)" = "$INPUT_SHA"
      chk "spark_job2_analytics.py present" test -f spark_job2_analytics.py
      absent order_metrics_landed.jsonl restaurant_summary.csv city_summary.csv
    else
      absent orders_landed.jsonl order_metrics.jsonl
    fi ;;
  *) echo "role must be vm1, vm2, vm3 or vm4"; exit 1 ;;
esac

echo "== topics"
for t in orders order_metrics; do
  d=$(topics --describe --topic "$t")
  chk "$t: 2 partitions, replication factor 2" grep -q "PartitionCount: 2.*ReplicationFactor: 2" <<<"$d"
  chk "$t: both partitions have 2 replicas in ISR" test "$(grep -cE 'Isr: [0-9]+,[0-9]+' <<<"$d")" -eq 2
done
chk "__consumer_offsets exists with RF 2 (else console consumers hang when VM3 is down)" \
  grep -q "ReplicationFactor: 2" <<<"$(topics --describe --topic __consumer_offsets | head -1)"

echo "== $([ $FAIL = 0 ] && echo 'READY' || echo 'NOT READY - fix the FAIL lines above')"
exit $FAIL
