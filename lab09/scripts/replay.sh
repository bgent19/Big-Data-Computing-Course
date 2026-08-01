#!/bin/sh
# =============================================================================
# SD411 Lab 09 - paced replay producer.
#
# Runs INSIDE the kafka container, which is Alpine-based. Its shell is sh, not
# bash, so nothing in this file may use bash-only syntax ($'\t', arrays,
# [[ ]] ). If you edit it, keep it POSIX.
#
# Usage (normally called for you by scripts/lab09_kafka.py):
#   sh /opt/lab09/scripts/replay.sh TOPIC KEYFIELD TOTAL RATE
#
#   TOPIC     destination topic, must already exist (auto-create is off)
#   KEYFIELD  entity | type | none
#   TOTAL     number of events to send
#   RATE      events per second, sent as one burst per second
#
# The pacing is deliberately coarse: RATE records, then sleep 1. A smooth
# per-record pace would need sub-second sleeps, which busybox does not
# reliably provide. Bursty delivery is also closer to what a real feed does,
# and it makes consumer lag visible in Part 4 instead of theoretical.
# =============================================================================
set -e

TOPIC="$1"
KEYFIELD="${2:-entity}"
TOTAL="${3:-60000}"
RATE="${4:-1000}"

SRC="/seed/stream/gh_events.tsv"
BIN="/opt/kafka/bin"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TAB=$(printf '\t')

if [ -z "$TOPIC" ]; then
  echo "FATAL: no topic given" >&2
  exit 64
fi
if [ ! -f "$SRC" ]; then
  echo "FATAL: replay file missing: $SRC" >&2
  echo "       Run scripts/make_stream.py on the VM (instructor, pre-term)." >&2
  exit 64
fi

# awk emits either "value" or "key<TAB>value" and pauses one second every RATE
# records. fflush() matters: without it awk buffers and the console producer
# sees the whole file at once, which destroys the pacing.
emit() {
  awk -F"$TAB" -v total="$TOTAL" -v rate="$RATE" -v kf="$KEYFIELD" -v tab="$TAB" '
    NR > total { exit }
    {
      if (kf == "entity")    { key = $1 }
      else if (kf == "type") { key = $2 }
      else                   { key = "" }

      if (kf == "none") { print $3 }
      else              { print key tab $3 }

      if (NR % rate == 0) { fflush(); system("sleep 1") }
    }
  ' "$SRC"
}

START=$(date +%s)
echo "replay: topic=$TOPIC key=$KEYFIELD total=$TOTAL rate=$RATE/s"

if [ "$KEYFIELD" = "none" ]; then
  emit | "$BIN/kafka-console-producer.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --producer-property acks=1
else
  emit | "$BIN/kafka-console-producer.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --producer-property acks=1 \
    --property parse.key=true \
    --property "key.separator=$TAB"
fi

END=$(date +%s)
echo "replay: done in $((END - START))s"
