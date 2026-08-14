#!/bin/sh
# produce.sh - pace the lab11 event-time stream into the Kafka topic.
#
# POSIX sh, not bash: this shells into the broker container, whose image is
# Alpine (lab09). Values are read from labN/.env with a grep helper, never by
# sourcing, because KAFKA_HEAP_OPTS contains spaces and `source`-ing it would
# execute the tail of the line as a command (lab09 finding).
#
# Pacing matches lab09's replay.sh: RATE records, then sleep 1, via an awk
# emitter feeding a single long-lived console producer. The producer sees a
# steady trickle rather than a firehose, so the streaming query gets several
# micro-batches to work with instead of one giant first batch.
#
# Run it ONCE. Producing a second time APPENDS a second copy of the stream to
# the topic (40000 records), which silently invalidates the oracle and every
# comparison downstream of it, so this script refuses a topic that already has
# records. FORCE=1 overrides; reset_lab11.sh --all drops the topic properly.
set -eu

# Run from the LAB ROOT: `docker compose` needs the compose file, and the .env
# next to it is where the topic name and rate come from. This script lives in
# scripts/, so that is one level up (same discipline as reset_lab11.sh).
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here/.." || { echo "produce.sh: cannot reach the lab root" >&2; exit 64; }
env_file="$here/../.env"

env() {
  # env KEY [default]
  v="$(grep -E "^$1=" "$env_file" 2>/dev/null | head -n1 | cut -d= -f2-)"
  if [ -z "${v:-}" ] && [ $# -ge 2 ]; then v="$2"; fi
  printf '%s' "$v"
}

TOPIC="$(env LAB11_TOPIC pitches)"
RATE="$(env LAB11_PRODUCE_RATE 2000)"
DATA="$(env SD411_DATA /opt/sd411/data)"
STREAM="${1:-$DATA/lab11/pitch_stream.jsonl}"

if [ ! -f "$STREAM" ]; then
  echo "produce.sh: stream file not found: $STREAM" >&2
  echo "produce.sh: run 'sudo ./provision/stage_pitch_stream.sh' first (instructor step)." >&2
  exit 64
fi

DC="docker compose"
BIN=/opt/kafka/bin

echo "produce.sh: ensuring topic '$TOPIC' (1 partition, auto-create is off)"
$DC exec -T broker "$BIN/kafka-topics.sh" \
  --bootstrap-server broker:9092 \
  --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null

# Already produced? kafka-get-offsets prints "topic:partition:endOffset". A
# non-zero end offset means the stream is already in the log and it is still
# replayable, so there is nothing to do; producing again would DOUBLE it.
end="$($DC exec -T broker "$BIN/kafka-get-offsets.sh" \
        --bootstrap-server broker:9092 --topic "$TOPIC" 2>/dev/null \
        | cut -d: -f3 | head -n1)"
end="${end:-0}"
if [ "$end" -gt 0 ] && [ -z "${FORCE:-}" ]; then
  echo "produce.sh: topic '$TOPIC' already holds $end records; NOT producing again." >&2
  echo "produce.sh: a second produce would append a duplicate copy of the stream and" >&2
  echo "            invalidate your oracle. The topic is replayable as it stands:" >&2
  echo "            reset_lab11.sh clears the checkpoint and the next run replays it." >&2
  echo "            To rebuild the topic from scratch: ./scripts/reset_lab11.sh --all" >&2
  exit 0
fi

echo "produce.sh: producing $(wc -l < "$STREAM") records at ${RATE}/s into '$TOPIC'"
awk -v rate="$RATE" '{print} NR % rate == 0 {system("sleep 1")}' "$STREAM" \
  | $DC exec -T broker "$BIN/kafka-console-producer.sh" \
      --bootstrap-server broker:9092 --topic "$TOPIC" >/dev/null

echo "produce.sh: done. Committed end offset:"
$DC exec -T broker "$BIN/kafka-get-offsets.sh" \
  --bootstrap-server broker:9092 --topic "$TOPIC"
