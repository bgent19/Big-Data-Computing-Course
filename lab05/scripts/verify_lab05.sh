#!/usr/bin/env bash
# =============================================================================
# SD411 lab05 -- stack verification
# Run from the lab05/ directory AFTER `docker compose up -d`.
# Every check is named. PASS means proceed. FAIL prints a triage hint.
# WARN means the lab can start but a later part needs attention.
#
# Post-reconciliation notes:
#   - No ./jars check. The S3A JARs are provisioned once on the VM at
#     ${SD411_JARS} and inherited through the base compose. C05 checks they
#     actually landed inside the container.
#   - mc calls use the MC_HOST_local env pattern with `mc` as entrypoint.
#     Shell-wrapping the minio/mc image silently false-passes; do not
#     "simplify" these back into `sh -c "mc alias set ..."`.
# =============================================================================
set -u

PASS=0; FAIL=0; WARN=0
ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
bad()  { echo "  [FAIL] $1"; echo "         triage: $2"; FAIL=$((FAIL+1)); }
warn() { echo "  [WARN] $1"; echo "         note:   $2"; WARN=$((WARN+1)); }

echo "SD411 lab05 stack verification"
echo "=============================="

# C01 .env stamped (everything downstream depends on this)
if [ -f ./.env ]; then
  set -a; . ./.env; set +a
  ok "C01 .env present (sync_env.sh has run)"
else
  bad "C01 .env missing" "run vm-base/scripts/sync_env.sh; every \${VAR} in the compose file resolves from it"
  echo "Cannot continue without .env."; exit 1
fi

# C02 base compose reachable (extends: target)
if [ -f ../vm-base/docker-compose.base.yml ]; then
  ok "C02 vm-base/docker-compose.base.yml found"
else
  bad "C02 base compose not found at ../vm-base/" "this lab extends: it; check out the full course repo, do not copy lab05/ alone"
fi

# C03 compose file resolves with no unset variables
if docker compose config >/dev/null 2>&1; then
  ok "C03 docker compose config resolves cleanly"
else
  bad "C03 compose config failed" "run 'docker compose config' and read the error; an unresolved \${VAR} means a stale .env"
fi

# C04 containers running
for c in lab05-spark-master lab05-spark-worker lab05-minio; do
  if docker ps --format '{{.Names}}' | grep -q "^${c}$"; then
    ok "C04 container ${c} running"
  else
    bad "C04 container ${c} NOT running" "docker compose up -d, then docker logs ${c}; a stale stack from another lab holds the same ports (../vm-base/scripts/sd411_down_all.sh)"
  fi
done

# C05 centralized S3A JARs present INSIDE the container
if docker exec lab05-spark-master bash -c \
   "[ -f /opt/spark/extra-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar ] && [ -f /opt/spark/extra-jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar ]" 2>/dev/null; then
  ok "C05 S3A connector JARs mounted at /opt/spark/extra-jars"
else
  bad "C05 S3A JARs not visible in the container" "the VM's ${SD411_JARS} is empty or unmounted; re-run the VM provisioner's JAR fetch. --packages will NOT work behind the proxy"
fi

# C06 master UI answers
if curl -fsS "http://localhost:${PORT_SPARK_MASTER_UI}" >/dev/null 2>&1; then
  ok "C06 Spark master UI answers on :${PORT_SPARK_MASTER_UI}"
else
  bad "C06 Spark master UI silent" "docker logs lab05-spark-master; check the port is not held by an old stack"
fi

# C07 worker registered
if curl -fsS "http://localhost:${PORT_SPARK_MASTER_UI}" 2>/dev/null | grep -qi 'ALIVE'; then
  ok "C07 worker registered and ALIVE"
else
  bad "C07 no ALIVE worker on the master" "docker logs lab05-spark-worker; the worker needs ~15 s after bring-up"
fi

# C08 MinIO health
if curl -fsS "http://localhost:${PORT_MINIO_API}/minio/health/live" >/dev/null 2>&1; then
  ok "C08 MinIO live on :${PORT_MINIO_API}"
else
  bad "C08 MinIO not answering" "docker logs lab05-minio"
fi

MCHOST="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000"

# C09 season seed present
if docker compose run --rm --no-deps -e MC_HOST_local="$MCHOST" \
     --entrypoint mc minio-init stat "local/${S3_BUCKET}/raw/${SEED_CSV}" >/dev/null 2>&1; then
  ok "C09 season seed present at ${S3_BUCKET}/raw/${SEED_CSV}"
else
  bad "C09 season seed missing in MinIO" "confirm ${SD411_DATA}/${SEED_CSV} exists on the VM, then: docker compose up minio-init"
fi

# C10 Parquet copy present (required from Part B onward)
if docker compose run --rm --no-deps -e MC_HOST_local="$MCHOST" \
     --entrypoint mc minio-init ls "local/${S3_BUCKET}/parquet/statcast_2025/" 2>/dev/null | grep -q parquet; then
  ok "C10 Parquet copy present at ${S3_BUCKET}/parquet/statcast_2025/"
else
  warn "C10 Parquet copy not found" "run scripts/make_parquet.py in Part 0 (command in the script header); the lab cannot pass Part B without it"
fi

# C11 plan output directory writable inside the work volume
if docker exec lab05-spark-master bash -c "mkdir -p \${SPARK_WORK_DIR}/plans && touch \${SPARK_WORK_DIR}/plans/.probe && rm \${SPARK_WORK_DIR}/plans/.probe" 2>/dev/null; then
  ok "C11 plan directory writable in the spark-work volume"
else
  bad "C11 cannot write to the work volume" "confirm SPARK_WORK_DIR matches the base compose's spark-work mount point (pre-term checklist item 1)"
fi

# C12 spark-submit smoke test (local, no S3A, ~20 s)
if docker exec lab05-spark-master /opt/spark/bin/spark-submit \
     --master 'local[1]' \
     /opt/spark/examples/src/main/python/pi.py 2 >/dev/null 2>&1; then
  ok "C12 spark-submit smoke test passed"
else
  bad "C12 spark-submit smoke test failed" "docker exec -it lab05-spark-master bash, rerun by hand, read the traceback"
fi

echo "------------------------------"
echo "PASS=${PASS}  WARN=${WARN}  FAIL=${FAIL}"
if [ "$FAIL" -gt 0 ]; then
  echo "Resolve FAILs before starting Part A. Remember the 20-minute rule."
  exit 1
fi
echo "Stack verified. Proceed to Part 0 of the README."
