#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 09 - stack verification.
#
#   cd lab09 && bash scripts/verify_lab09.sh
#
# Gating checks (0 through 3) run first and abort on failure, because every
# later check produces a confusing error rather than a useful one when the
# environment is not stamped or the base file is missing.
#
# Containers are resolved by compose SERVICE name, never by container_name.
# Nothing in this course sets container_name any more, and a hardcoded name
# false-passes after an extends: conversion.
# =============================================================================
set -uo pipefail

PASS=0; FAIL=0; WARN=0
LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$LAB_DIR" || exit 1

pass() { printf '  [ PASS ] %s\n' "$1"; PASS=$((PASS+1)); }
fail() { printf '  [ FAIL ] %s\n' "$1"; FAIL=$((FAIL+1)); }
warn() { printf '  [ WARN ] %s\n' "$1"; WARN=$((WARN+1)); }
head2() { printf '\n%s\n' "$1"; }

# Read a value from the stamped .env without sourcing it. Sourcing a file with
# unquoted spaces in a value executes the remainder as a command.
envval() {
  grep -E "^${1}=" .env 2>/dev/null | tail -n1 | cut -d= -f2- | sed -e 's/^"//' -e 's/"$//'
}

kx() { docker compose exec -T kafka "$@" 2>/dev/null; }

echo "SD411 Lab 09 verification"
echo "lab dir: $LAB_DIR"

# ---------------------------------------------------------------------------
head2 "Gating checks"

# 0 -------------------------------------------------------------------------
if [ ! -f .env ]; then
  fail "check 0: lab09/.env is missing. Run vm-base/scripts/sync_env.sh from the repo root."
  echo; echo "ABORT: nothing below can pass without a stamped environment."; exit 1
fi
MISSING=""
for VAR in KAFKA_IMAGE KAFKA_CLUSTER_ID KAFKA_BOOTSTRAP PORT_KAFKA_EXTERNAL \
           KAFKA_HEAP_OPTS KAFKA_RETENTION_CHECK_MS STREAM_DIR STREAM_FILE \
           STREAM_MIN_LINES SD411_DATA SD411_JARS; do
  if [ -z "$(envval "$VAR")" ]; then MISSING="$MISSING $VAR"; fi
done
if [ -n "$MISSING" ]; then
  fail "check 0: .env is stamped but missing:$MISSING"
  echo "         Those variables live in the Module 3 block of vm-base/common.env."
  echo "         Restore them there, then re-run vm-base/scripts/sync_env.sh."
  echo; echo "ABORT."; exit 1
fi
pass "check 0: .env stamped and carries every lab09 variable"

# 1 -------------------------------------------------------------------------
if [ -f ../vm-base/docker-compose.base.yml ]; then
  pass "check 1: ../vm-base/docker-compose.base.yml reachable"
else
  fail "check 1: ../vm-base/docker-compose.base.yml not found (needed by the spark profile)"
fi

# 2 -------------------------------------------------------------------------
if docker compose config >/dev/null 2>&1; then
  pass "check 2: docker compose config resolves"
else
  fail "check 2: docker compose config failed. Bad extends: path or unstamped variable."
  docker compose config 2>&1 | tail -n 5 | sed 's/^/         /'
  echo; echo "ABORT."; exit 1
fi

# 3 -------------------------------------------------------------------------
MISSING_TOOLS=""
command -v docker  >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS docker"
command -v python3 >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS python3"
if [ -z "$MISSING_TOOLS" ]; then
  pass "check 3: host tools present (docker, python3). The lab09 harness runs on the host."
else
  fail "check 3: missing host tools:$MISSING_TOOLS"
fi

# ---------------------------------------------------------------------------
head2 "Compose hygiene"

# 4 -------------------------------------------------------------------------
HARDCODED=$(grep -nE '^[[:space:]]*(image:[[:space:]]*[a-z0-9]|.*:[0-9]{4}:[0-9]{4})' docker-compose.yml \
            | grep -v '\${' || true)
if [ -z "$HARDCODED" ]; then
  pass "check 4: no hardcoded image tag or host:container port pair in docker-compose.yml"
else
  fail "check 4: hardcoded values in docker-compose.yml:"
  echo "$HARDCODED" | sed 's/^/         /'
fi

# 5 -------------------------------------------------------------------------
if grep -q 'container_name' docker-compose.yml; then
  fail "check 5: docker-compose.yml sets container_name. Resolve by service name instead."
else
  pass "check 5: no container_name (service-name resolution stays valid)"
fi

# ---------------------------------------------------------------------------
head2 "Broker"

# 6 -------------------------------------------------------------------------
KID=$(docker compose ps -q kafka 2>/dev/null)
if [ -n "$KID" ]; then
  STATE=$(docker inspect -f '{{.State.Health.Status}}' "$KID" 2>/dev/null || echo unknown)
  if [ "$STATE" = "healthy" ]; then
    pass "check 6: kafka container is up and healthy"
  else
    fail "check 6: kafka container is up but health is '$STATE' (give it 60s, then check logs)"
  fi
else
  fail "check 6: kafka container is not running. docker compose up -d"
  echo; echo "ABORT: broker checks below cannot run."
  printf '\n%d passed, %d failed, %d warnings\n' "$PASS" "$FAIL" "$WARN"; exit 1
fi

BOOT=$(envval KAFKA_BOOTSTRAP)

# 7 -------------------------------------------------------------------------
if kx /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --list >/dev/null; then
  pass "check 7: broker answers a metadata request on $BOOT"
else
  fail "check 7: broker did not answer on $BOOT"
fi

# 8 -------------------------------------------------------------------------
QUORUM=$(kx /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BOOT" describe --status || true)
if echo "$QUORUM" | grep -qi 'LeaderId'; then
  pass "check 8: KRaft quorum reports a leader (no ZooKeeper in this stack)"
else
  warn "check 8: could not read KRaft quorum status. Not fatal; the broker answered check 7."
fi

# 9 -------------------------------------------------------------------------
AUTOC=$(kx /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOT" \
        --entity-type brokers --entity-name 1 --describe --all 2>/dev/null \
        | grep -i 'auto.create.topics.enable' | head -n1 || true)
if echo "$AUTOC" | grep -qi 'auto.create.topics.enable=false'; then
  pass "check 9: topic auto-creation is OFF (a typo cannot silently make a 1-partition topic)"
else
  warn "check 9: could not confirm auto.create.topics.enable=false"
fi

# ---------------------------------------------------------------------------
head2 "Data and JARs"

# 10 ------------------------------------------------------------------------
STREAM_PATH="$(envval STREAM_DIR)/$(envval STREAM_FILE)"
MIN_LINES=$(envval STREAM_MIN_LINES)
if [ -f "$STREAM_PATH" ]; then
  LINES=$(wc -l < "$STREAM_PATH" | tr -d ' ')
  if [ "$LINES" -ge "$MIN_LINES" ] 2>/dev/null; then
    pass "check 10: replay file staged ($LINES lines, floor $MIN_LINES)"
  else
    fail "check 10: replay file has $LINES lines, below the $MIN_LINES floor"
  fi
else
  fail "check 10: replay file missing at $STREAM_PATH"
  echo "         Instructor pre-term step. See lab09/data/README.md."
  echo "         This is a provisioning failure, not something you did."
  printf '\n%d passed, %d failed, %d warnings\n' "$PASS" "$FAIL" "$WARN"
  exit 64
fi

# 11 ------------------------------------------------------------------------
if kx test -f "/seed/stream/$(envval STREAM_FILE)"; then
  pass "check 11: the broker container can see the replay file at /seed/stream/"
else
  fail "check 11: replay file exists on the host but is not visible inside the kafka container"
fi

# 12 ------------------------------------------------------------------------
JARS=$(envval SD411_JARS)
NEEDED=0; FOUND=0
for J in "spark-sql-kafka-0-10_2.12-$(envval SPARK_KAFKA_VERSION).jar" \
         "spark-token-provider-kafka-0-10_2.12-$(envval SPARK_KAFKA_VERSION).jar" \
         "kafka-clients-$(envval KAFKA_CLIENTS_VERSION).jar" \
         "commons-pool2-$(envval COMMONS_POOL2_VERSION).jar"; do
  NEEDED=$((NEEDED+1))
  [ -f "$JARS/$J" ] && FOUND=$((FOUND+1))
done
if [ "$FOUND" -eq "$NEEDED" ]; then
  pass "check 12: all $NEEDED Spark-Kafka connector JARs staged in $JARS"
else
  warn "check 12: $FOUND of $NEEDED Spark-Kafka JARs staged. Parts 1-5 do not need them."
  echo "         The Part 6 stretch does, and lab10 will not run without them."
fi

# 13 ------------------------------------------------------------------------
if [ -d work ]; then
  fail "check 13: a host work/ directory exists. Scratch belongs on the spark-work volume."
else
  pass "check 13: no host work/ directory (workdir fix compliant)"
fi

printf '\n%d passed, %d failed, %d warnings\n' "$PASS" "$FAIL" "$WARN"
if [ "$FAIL" -gt 0 ]; then
  echo "Stack is NOT ready. Work the failures top down; the first one usually explains the rest."
  exit 1
fi
echo "Stack ready. Start with: python3 scripts/lab09_kafka.py --part 1"
exit 0
