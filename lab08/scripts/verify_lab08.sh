#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 08 - stack verification (checks 0 through 13)
#
# Run from the lab08/ directory AFTER `docker compose up -d`:
#     bash scripts/verify_lab08.sh
#
# Design rules this script follows (learned the hard way in earlier labs):
#   - containers are resolved by compose SERVICE name, never by a hardcoded
#     container_name. The base sets none, so hardcoded names false-fail.
#   - ports, bucket, JAR versions, and paths are read from the stamped .env.
#   - `mc` is the minio/mc ENTRYPOINT, so nothing here wraps it in a shell.
# =============================================================================
set -uo pipefail

PASS=0; FAIL=0; WARN=0
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; [ $# -gt 1 ] && printf '        %s\n' "$2"; FAIL=$((FAIL+1)); }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; [ $# -gt 1 ] && printf '        %s\n' "$2"; WARN=$((WARN+1)); }
hdr()  { printf '\n\033[1m%s\033[0m\n' "$1"; }

cd "$(dirname "$0")/.." || exit 1

hdr "Lab 08 - Skew and UI Forensics : stack verification"

# --- 0: .env stamped ---------------------------------------------------------
if [ -f .env ]; then
  set -a; . ./.env; set +a
  ok "0. .env is stamped (sync_env.sh has run)"
else
  bad "0. .env is missing" "Run scripts/sync_env.sh from the repo root. Every \${VAR} below reads it."
  echo; echo "Stopping: nothing downstream can be checked without .env."; exit 1
fi

S3_BUCKET="${S3_BUCKET:-sd411}"
FACT_PREFIX="${FACT_PREFIX:-fact/pitches}"
SD411_JARS="${SD411_JARS:-/opt/sd411/jars}"
PORT_SPARK_MASTER_UI="${PORT_SPARK_MASTER_UI:-8080}"
PORT_DRIVER_UI="${PORT_DRIVER_UI:-4040}"
PORT_MINIO_API="${PORT_MINIO_API:-9000}"

# --- 1: base compose reachable ----------------------------------------------
if [ -f ../vm-base/docker-compose.base.yml ]; then
  ok "1. ../vm-base/docker-compose.base.yml is reachable"
else
  bad "1. base compose not found" "lab08 extends: it. Check out the whole course repo, not just this lab."
fi

# --- 2: merged config resolves ----------------------------------------------
if docker compose config >/dev/null 2>&1; then
  ok "2. docker compose config resolves"
else
  bad "2. docker compose config failed" "$(docker compose config 2>&1 | tail -n 3)"
fi

# --- 3: services running -----------------------------------------------------
svc_id() { docker compose ps -q "$1" 2>/dev/null; }
for s in spark-master spark-worker minio; do
  if [ -n "$(svc_id "$s")" ]; then ok "3. service '$s' is up"
  else bad "3. service '$s' is not running" "docker compose up -d, then re-run this script."; fi
done
SM="$(svc_id spark-master)"

# --- 4: centralized S3A JARs -------------------------------------------------
HA="${SD411_JARS}/hadoop-aws-${HADOOP_AWS_VERSION:-3.3.4}.jar"
SDKJ="${SD411_JARS}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION:-1.12.262}.jar"
if [ -f "$HA" ] && [ -f "$SDKJ" ]; then
  ok "4. S3A JARs present in ${SD411_JARS} (versions from .env)"
else
  bad "4. S3A JARs missing from ${SD411_JARS}" \
      "Expected $HA and $SDKJ. This is a VM provisioning failure, not something you download per lab. Escalate."
fi

# --- 5: JARs visible inside the container ------------------------------------
if [ -n "$SM" ] && docker compose exec -T spark-master sh -c \
     '[ -f /opt/spark/extra-jars/hadoop-aws-*.jar ]' >/dev/null 2>&1; then
  ok "5. JARs are mounted inside spark-master at /opt/spark/extra-jars"
else
  warn "5. could not confirm /opt/spark/extra-jars inside the container" \
       "If the base mounts \${SD411_JARS} elsewhere, update the --jars paths in the README and both scripts."
fi

# --- 6: master UI up with a registered worker --------------------------------
if curl -fsS "http://localhost:${PORT_SPARK_MASTER_UI}" 2>/dev/null | grep -qi "Workers"; then
  W=$(curl -fsS "http://localhost:${PORT_SPARK_MASTER_UI}" | grep -c "worker-" || true)
  if [ "$W" -ge 1 ]; then ok "6. Spark master UI up with a registered worker"
  else bad "6. master UI up but NO worker registered" "docker compose logs spark-worker"; fi
else
  bad "6. Spark master UI not reachable on :${PORT_SPARK_MASTER_UI}" "Another sd411 stack may hold the port. Run sd411_down_all.sh."
fi

# --- 7: driver UI port published ---------------------------------------------
if docker compose config 2>/dev/null | grep -q "published:.*\"\?${PORT_DRIVER_UI}\"\?"; then
  ok "7. driver UI port ${PORT_DRIVER_UI} is published"
else
  bad "7. driver UI port ${PORT_DRIVER_UI} is NOT published" \
      "Every graded part of Lab 08 is verified in the driver UI. Uncomment the ports block in docker-compose.yml."
fi

# --- 8: MinIO reachable and bucket exists ------------------------------------
if curl -fsS "http://localhost:${PORT_MINIO_API}/minio/health/live" >/dev/null 2>&1; then
  ok "8. MinIO is live on :${PORT_MINIO_API}"
else
  bad "8. MinIO not reachable on :${PORT_MINIO_API}" "docker compose logs minio"
fi

# --- 9: upstream fact table present (the lab06 dependency) -------------------
INIT_RC=$(docker compose ps -a --format '{{.Service}} {{.ExitCode}}' 2>/dev/null | awk '$1=="minio-init"{print $2}' | head -n1)
if [ "${INIT_RC:-0}" = "64" ]; then
  bad "9. minio-init exited 64: no pitch fact at s3a://${S3_BUCKET}/${FACT_PREFIX}" \
      "Lab 08 reads the fact Lab 06 writes. Bring up lab06, run its writer, then bring lab08 up again."
elif [ -n "$SM" ]; then
  ok "9. minio-init did not report a missing upstream fact"
else
  warn "9. could not read minio-init exit status"
fi

# --- 10: lab08 dimensions built ----------------------------------------------
# `mc ls` exits 0 even on a missing prefix, so RC cannot gate this. The minio/mc
# image has no grep, so test for non-empty output with sh builtins instead.
if docker compose --profile tools run --rm -T mc-shell -c \
     "[ -n \"\$(mc ls local/${S3_BUCKET}/dim/pitch_type/ 2>/dev/null)\" ]" >/dev/null 2>&1; then
  ok "10. dim/pitch_type exists"
else
  warn "10. dim/pitch_type not found (expected before Part 0 step 4)" \
       "Run: docker compose exec spark-master spark-submit --master spark://spark-master:7077 --jars \$JARS /opt/lab08/scripts/build_skew_dims.py"
fi

# --- 11: scripts mounted read-only -------------------------------------------
if docker compose config 2>/dev/null | grep -q "/opt/lab08/scripts"; then
  if docker compose config 2>/dev/null | grep -A2 "/opt/lab08/scripts" | grep -qi "read_only: true"; then
    ok "11. ./scripts mounted read-only at /opt/lab08/scripts"
  else
    warn "11. /opt/lab08/scripts is mounted but not clearly read-only" "Confirm the ':ro' suffix survived the merge."
  fi
else
  bad "11. /opt/lab08/scripts is not in the merged config" "volumes should MERGE under extends:. Check the base."
fi

# --- 12: spark-work named volume in the merged config ------------------------
if docker compose config 2>/dev/null | grep -q "spark-work"; then
  ok "12. spark-work named volume is inherited"
else
  bad "12. spark-work named volume missing from the merged config" \
      "Scratch must not live on a host bind mount. Check vm-base."
fi

# --- 13: no host work/ directory ---------------------------------------------
if [ -d ./work ]; then
  bad "13. a host ./work directory exists" \
      "Spark runs as root and will leave root-owned files here. Delete it; scratch belongs on the spark-work volume."
else
  ok "13. no host work/ directory (workdir fix intact)"
fi

hdr "Summary"
printf '  %d pass, %d warn, %d fail\n\n' "$PASS" "$WARN" "$FAIL"
if [ "$FAIL" -gt 0 ]; then
  echo "  Do not start Part 1 with a FAIL on the board. If you cannot clear it in"
  echo "  20 minutes, log it on the worksheet and raise your hand."
  exit 1
fi
echo "  Stack is green. Record predictions P1 through P6 before you run anything."
exit 0
