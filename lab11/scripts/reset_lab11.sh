#!/usr/bin/env bash
# reset_lab11.sh - return the lab to a clean slate between runs.
#
# By default it clears the CHECKPOINTS, the SINK output, and the ORACLE on MinIO,
# but LEAVES the Kafka topic alone. That is deliberate: the topic is your
# replayable log, and clearing the checkpoint makes the next run replay it from
# the earliest offset into a clean sink, which is exactly the setup every part
# after Part 0 wants. Pass --all to also drop and forget the topic, which forces
# a re-produce and is only needed if you suspect the stream itself is wrong.
set -euo pipefail

# Run from the LAB ROOT: .env is there, and so is the compose file that every
# `docker compose` call below resolves against. This script lives in scripts/,
# so that is one level up. Without the cd, a run from inside scripts/ finds no
# compose file, and the getenv helper silently falls through to its defaults
# instead of reading the stamped .env.
cd "$(cd "$(dirname "$0")/.." && pwd)" || { echo "reset_lab11: cannot reach the lab root" >&2; exit 64; }
env_file=".env"
getenv() { grep -E "^$1=" "$env_file" 2>/dev/null | head -n1 | cut -d= -f2- || true; }

BUCKET="$(getenv S3_BUCKET)";     BUCKET="${BUCKET:-sd411}"
TOPIC="$(getenv LAB11_TOPIC)";    TOPIC="${TOPIC:-pitches}"
MU="$(getenv MINIO_ROOT_USER)";   MU="${MU:-sd411admin}"
MP="$(getenv MINIO_ROOT_PASSWORD)"; MP="${MP:-sd411password}"

DROP_TOPIC=0
[ "${1:-}" = "--all" ] && DROP_TOPIC=1

echo "reset_lab11: clearing checkpoints and sinks under bucket '$BUCKET' (oracle preserved)"
# Clear the checkpoints and the three sink prefixes, but NOT lab11/oracle (or
# lab11/oracle_wm30). An oracle is a function of the topic, which does not
# change between parts, so computing it once and keeping it is correct.
# verify_probe.py needs it to still be there after every reset-crash-resume
# cycle in Parts 2 and 3.
docker compose run --rm --no-deps --entrypoint sh minio-init -c "
  mc alias set local http://minio:9000 '$MU' '$MP' >/dev/null
  mc rm -r --force local/$BUCKET/checkpoints/lab11 2>/dev/null || true
  mc rm -r --force local/$BUCKET/lab11/agg_idempotent 2>/dev/null || true
  mc rm -r --force local/$BUCKET/lab11/agg_naive 2>/dev/null || true
  mc rm -r --force local/$BUCKET/lab11/agg_drops 2>/dev/null || true
  echo 'reset_lab11: checkpoints and sinks cleared, oracle left intact'
"

if [ "$DROP_TOPIC" -eq 1 ]; then
  echo "reset_lab11: --all given, deleting topic '$TOPIC' (you will re-produce)"
  docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker:9092 --delete --topic "$TOPIC" 2>/dev/null || true
else
  echo "reset_lab11: topic '$TOPIC' left intact (still replayable)"
fi

echo "reset_lab11: done. Next run replays from the earliest offset into a clean sink."
