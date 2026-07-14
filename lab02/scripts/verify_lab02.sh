#!/usr/bin/env bash
# =============================================================================
# SD411 lab02 -- verify_lab02.sh
# 13 named checks. Run from the lab02/ directory after `docker compose up -d`.
# Every check prints PASS, FAIL, or WARN and points at the next thing to look
# at. Do not start Part 1 until checks 1-12 all PASS.
# =============================================================================
set -uo pipefail

PASS=0; FAIL=0; WARN=0
ok()   { echo "PASS  [$1] $2"; PASS=$((PASS+1)); }
bad()  { echo "FAIL  [$1] $2"; echo "      -> $3"; FAIL=$((FAIL+1)); }
warn() { echo "WARN  [$1] $2"; echo "      -> $3"; WARN=$((WARN+1)); }

echo "SD411 lab02 stack verification -- $(date)"
echo "------------------------------------------------------------"

# 1. docker present
if command -v docker >/dev/null 2>&1; then
  ok 1 "docker CLI found ($(docker --version | cut -d',' -f1))"
else
  bad 1 "docker CLI not found" "Install Docker Engine or fix PATH. See lab01 README Part 0."
fi

# 2. compose v2 plugin
if docker compose version >/dev/null 2>&1; then
  ok 2 "docker compose v2 plugin found"
else
  bad 2 "docker compose v2 not available" "Legacy docker-compose v1 will not honor extends: as written. Install the compose plugin."
fi

# 3. .env stamped from common.env
if [[ -f .env ]]; then
  set -a; . ./.env; set +a
  if [[ -n "${SPARK_IMAGE:-}" && -n "${MINIO_IMAGE:-}" && -n "${SD411_DATA:-}" ]]; then
    ok 3 ".env stamped from common.env (SPARK_IMAGE=${SPARK_IMAGE})"
  else
    bad 3 ".env present but missing expected keys" "Re-stamp it: ../vm-base/scripts/sync_env.sh"
  fi
else
  bad 3 "./.env not found" "Every \${VAR} in docker-compose.yml resolves from it. Stamp it: ../vm-base/scripts/sync_env.sh"
fi

# 4. vm-base reachable (extends: source)
if [[ -f ../vm-base/docker-compose.base.yml ]]; then
  ok 4 "vm-base/docker-compose.base.yml found (extends: source)"
else
  bad 4 "../vm-base/docker-compose.base.yml missing" "The compose file inherits its Spark and MinIO services from it. Pull the full course repo, do not copy lab02/ alone."
fi

# 5. disk space
free_kb=$(df -k . | awk 'NR==2 {print $4}')
if (( free_kb >= 5*1024*1024 )); then
  ok 5 "disk space OK ($((free_kb/1024/1024)) GB free)"
else
  bad 5 "less than 5 GB free" "HDFS blocks + MinIO objects both hold a copy of the seed. Clear space."
fi

# 6. shared seed present and full-season sized
SEED="${SD411_DATA:-/opt/sd411/data}/${SEED_CSV:-statcast_2025.csv}"
if [[ -f "$SEED" ]]; then
  seed_mb=$(( $(wc -c < "$SEED") / 1024 / 1024 ))
  if (( seed_mb >= ${SEED_MIN_MB:-100} )); then
    ok 6 "shared seed present (${seed_mb} MB) at $SEED"
  else
    bad 6 "seed is only ${seed_mb} MB, under the ${SEED_MIN_MB:-100} MB floor" "That is a lab01-sized sample, not the season. Re-seed; your E1-E3 numbers are meaningless otherwise."
  fi
else
  bad 6 "shared seed not found at $SEED" "The VM provisioner places it once for all labs. Re-run the seeding step."
fi

# 7. small-files corpus generated
if [[ -d data/small ]]; then
  n_small=$(ls data/small 2>/dev/null | wc -l)
  ok 7 "small-files corpus present (${n_small} files)"
else
  bad 7 "data/small/ missing" "Run ./scripts/gen_data.sh"
fi

# 8-10. core containers running
# namenode/datanode/mc set container_name: explicitly; minio is extended from
# vm-base with no container_name, so compose names it lab02-minio-1 (or -2,
# ...) rather than lab02-minio. Resolve every service through `compose ps -q`
# instead of guessing the container name, so this doesn't false-fail on a
# healthy stack.
for pair in "namenode:8" "datanode:8" "minio:9" "mc:10"; do
  svc="${pair%%:*}"; n="${pair##*:}"
  cid=$(docker compose ps -q "$svc" 2>/dev/null)
  state=$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || echo "absent")
  if [[ "$state" == "running" ]]; then
    ok "$n" "$svc running ($(docker inspect -f '{{.Name}}' "$cid" 2>/dev/null | sed 's#^/##'))"
  else
    bad "$n" "$svc is '$state'" "docker compose up -d, then: docker compose logs $svc --tail 50. If a port is taken, a stale stack is up: ../vm-base/scripts/sd411_down_all.sh"
  fi
done

# 11. HDFS healthy: out of safemode, DataNode registered
report=$(docker exec lab02-namenode hdfs dfsadmin -report 2>/dev/null)
live=$(echo "$report" | grep -c "^Name:" || true)
safemode=$(docker exec lab02-namenode hdfs dfsadmin -safemode get 2>/dev/null || echo "unknown")
if [[ "$safemode" == *"OFF"* && "$live" -ge 1 ]]; then
  ok 11 "HDFS healthy: safemode OFF, $live live DataNode(s). UI: http://localhost:${PORT_HDFS_NN_UI:-9870}"
elif [[ "$safemode" == *"ON"* ]]; then
  warn 11 "NameNode in safemode" "Normal for ~60 s after bring-up. Re-run this script. Persisting past 3 min: docker compose down -v && up -d"
else
  bad 11 "DataNode not registered with the NameNode" "docker logs lab02-datanode --tail 50. Confirm the apache/hadoop image honors the HDFS-SITE.XML_* env-var config convention."
fi

# 12. MinIO live and the buckets bootstrapped by minio-init
if curl -sf -m 5 "http://localhost:${PORT_MINIO_API:-9000}/minio/health/live" >/dev/null 2>&1; then
  buckets=$(docker exec lab02-mc mc ls local 2>&1)
  if echo "$buckets" | grep -q "${S3_BUCKET:-sd411}"; then
    ok 12 "MinIO live, bucket '${S3_BUCKET:-sd411}' bootstrapped. Console: http://localhost:${PORT_MINIO_CONSOLE:-9001}"
  else
    bad 12 "MinIO up but bucket '${S3_BUCKET:-sd411}' missing" "minio-init did not complete. docker logs lab02-minio-init. Actual mc output was: ${buckets}"
  fi
else
  bad 12 "MinIO health check failed" "docker logs lab02-minio --tail 50; check for a port conflict on ${PORT_MINIO_API:-9000}."
fi

# 13. (Stretch only) Spark profile + the centrally provisioned S3A JARs
JARDIR="${SD411_JARS:-/opt/sd411/jars}"
if docker inspect -f '{{.State.Status}}' lab02-spark-master >/dev/null 2>&1; then
  state=$(docker inspect -f '{{.State.Status}}' lab02-spark-master)
  n_jars=$(ls "$JARDIR"/hadoop-aws-*.jar "$JARDIR"/aws-java-sdk-bundle-*.jar 2>/dev/null | wc -l)
  if [[ "$state" == "running" && "$n_jars" -eq 2 ]]; then
    ok 13 "spark-master running, S3A JARs present in $JARDIR (stretch ready)"
  elif [[ "$state" == "running" ]]; then
    warn 13 "spark-master running but S3A JARs missing from $JARDIR" "The VM provisioner fetches them once host-side (Spark cannot: USNA TLS interception breaks --packages). Re-run the provisioner."
  else
    warn 13 "spark-master present but '$state'" "docker logs lab02-spark-master --tail 50"
  fi
else
  warn 13 "Spark profile not started (fine unless attempting the stretch goal)" "docker compose --profile spark up -d"
fi

echo "------------------------------------------------------------"
echo "Result: $PASS pass, $FAIL fail, $WARN warn"
if (( FAIL > 0 )); then
  echo "Fix FAILs before starting Part 1. Stuck for 20 minutes? Follow the help protocol in README.md."
  exit 1
fi
echo "Stack verified. Proceed to Part 1."
