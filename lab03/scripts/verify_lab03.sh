#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 3 -- verify_lab03.sh  (reconciled: labNN naming, env-driven)
# Run BEFORE you touch the benchmark scaffold. Every check prints
# PASS / FAIL / WARN with a pointer at the next thing to look at.
# Usage:  ./scripts/verify_lab03.sh        (from the lab03/ directory)
# =============================================================================
set -u

# Pull ports/creds/paths from the stamped .env so this script never hardcodes
# a value that lives in common.env.
if [ -f ./.env ]; then
  set -a; . ./.env; set +a
fi
: "${PORT_SPARK_MASTER_UI:=8080}"
: "${MINIO_ROOT_USER:=sd411admin}"
: "${MINIO_ROOT_PASSWORD:=sd411password}"
: "${S3_ENDPOINT:=http://minio:9000}"
: "${S3_BUCKET:=sd411}"
: "${SEED_CSV:=statcast_2025.csv}"
: "${SEED_MIN_MB:=100}"

PASS=0; FAIL=0; WARN=0
ok()   { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL  $1"; echo "        -> $2"; FAIL=$((FAIL+1)); }
warn() { echo "  WARN  $1"; echo "        -> $2"; WARN=$((WARN+1)); }
dc()   { docker compose "$@"; }

echo "SD411 Lab 3 stack verification"
echo "=============================="

# [0] this lab is stamped and its base is reachable
if [ -f ./.env ]; then
  ok "[0a] lab03/.env present (sync_env.sh has run)"
else
  bad "[0a] lab03/.env missing" "Run ../vm-base/scripts/sync_env.sh to stamp common.env into this lab, then re-check."
fi
if [ -f ../vm-base/docker-compose.base.yml ]; then
  ok "[0b] vm-base/docker-compose.base.yml reachable (extends: resolves)"
else
  bad "[0b] ../vm-base/docker-compose.base.yml not found" "This lab inherits from the base. Confirm the vm-base/ directory sits next to lab03/."
fi

# [1] docker present
if command -v docker >/dev/null 2>&1; then ok "[1] docker binary found"
else bad "[1] docker binary not found" "See Lab 1 Part 0 for the install steps."; fi

# [2] compose v2 plugin
if dc version >/dev/null 2>&1; then ok "[2] docker compose v2 plugin present"
else bad "[2] docker compose v2 plugin missing" "Run 'docker compose version'. If it fails, see Lab 1 README Part 0."; fi

# [3] free disk -- Lab 3 writes ~6 copies of a ~300 MB dataset plus shuffle scratch
FREE_KB=$(df -Pk . | awk 'NR==2 {print $4}')
if [ "${FREE_KB:-0}" -ge 10485760 ]; then ok "[3] >= 10 GB free disk in this directory"
else bad "[3] less than 10 GB free disk" "Lab 3 writes the season six times. Free space or run 'docker system prune'."; fi

# [4-6] services running (service-based, not hardcoded container names --
#       container names now derive from the compose project 'lab03').
check_svc() {
  local svc="$1" n="$2"
  if dc ps --status running --services 2>/dev/null | grep -qx "$svc"; then
    ok "[$n] service ${svc} is running"
  else
    bad "[$n] service ${svc} is NOT running" "Run 'docker compose up -d' from lab03/ and check 'docker compose ps'."
  fi
}
check_svc spark-master 4
check_svc spark-worker 5
check_svc minio        6

# [7] minio-init exited 0 (seed loaded) / 64 (seed absent)
INIT_CID=$(dc ps -aq minio-init 2>/dev/null | head -1)
INIT_STATE=$(docker inspect -f '{{.State.ExitCode}}' "$INIT_CID" 2>/dev/null || echo "missing")
if [ "$INIT_STATE" = "0" ]; then
  ok "[7] minio-init exited 0 (buckets created, season CSV seeded)"
elif [ "$INIT_STATE" = "64" ]; then
  bad "[7] minio-init exited 64: provisioned seed absent" "The seed at \${SD411_DATA}/${SEED_CSV} was not found on this VM. See data/README.md, then 'docker compose up minio-init'."
else
  bad "[7] minio-init state is '${INIT_STATE}'" "Run 'docker compose logs minio-init' and read the last line."
fi

# [8] master UI reachable
if curl -fsS --max-time 5 "http://localhost:${PORT_SPARK_MASTER_UI}" >/dev/null 2>&1; then
  ok "[8] Spark master UI reachable on :${PORT_SPARK_MASTER_UI}"
else
  bad "[8] Spark master UI not reachable" "Check 'docker compose logs spark-master' and confirm port ${PORT_SPARK_MASTER_UI} is free (a stale lab stack collides; run the down-all script)."
fi

# [9] worker registered with master
if curl -fsS --max-time 5 "http://localhost:${PORT_SPARK_MASTER_UI}" 2>/dev/null | grep -qi "ALIVE"; then
  ok "[9] at least one ALIVE worker registered with the master"
else
  warn "[9] could not confirm an ALIVE worker" "Open http://localhost:${PORT_SPARK_MASTER_UI}; Workers table should show 1 ALIVE. If not: 'docker compose restart spark-worker'."
fi

# [10] seed object present and plausibly full-season sized
SEED_BYTES=$(dc --profile tools run --rm --entrypoint /bin/sh mc -c \
  "mc alias set local ${S3_ENDPOINT} ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} >/dev/null 2>&1 && mc stat --json local/${S3_BUCKET}/raw/${SEED_CSV} 2>/dev/null" \
  2>/dev/null | grep -o '"size":[0-9]*' | grep -o '[0-9]*' | head -1)
MIN_BYTES=$(( SEED_MIN_MB * 1048576 ))
if [ "${SEED_BYTES:-0}" -ge "$MIN_BYTES" ]; then
  ok "[10] seed object raw/${SEED_CSV} present ($((SEED_BYTES / 1048576)) MB)"
elif [ "${SEED_BYTES:-0}" -gt 0 ]; then
  warn "[10] seed object present but only $((SEED_BYTES / 1048576)) MB" "That looks like the Lab 1 sample, not the full season (floor is ${SEED_MIN_MB} MB). Your timings will be meaningless. Re-seed per data/README.md."
else
  bad "[10] seed object raw/${SEED_CSV} not found in MinIO" "Check the minio-init logs, then data/README.md."
fi

# [11] pyarrow available in the master (Part C runs in-container now)
if dc exec -T spark-master python3 -c "import pyarrow.parquet" >/dev/null 2>&1; then
  ok "[11] spark-master can import pyarrow (Part C ready in-container)"
else
  warn "[11] pyarrow not importable in spark-master" "Unexpected: it ships with apache/spark:3.5.3-python3. Fallback: 'docker compose exec spark-master pip install pyarrow'."
fi

echo "=============================="
echo "Result: ${PASS} pass, ${FAIL} fail, ${WARN} warn"
if [ "$FAIL" -gt 0 ]; then
  echo "Fix every FAIL before starting Part A. The 20-minute rule starts now."
  exit 1
fi
echo "Stack is ready. Open scripts/lab03_benchmark.py and start with Part A."
