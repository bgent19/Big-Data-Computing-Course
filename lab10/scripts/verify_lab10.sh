#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 10 - stack verification
#
# Run from the lab10/ directory BEFORE you write any code:
#     scripts/verify_lab10.sh
#
# Gating checks run first and exit immediately. There is no point testing the
# streaming source if the .env was never stamped.
#
# Containers are resolved by Docker Compose SERVICE name, never by container
# name. The base compose sets no container_name, so a hardcoded name check
# would pass against a container from a different lab.
# =============================================================================
set -uo pipefail

# This script lives in scripts/ but must run from the lab root: that is where
# .env is stamped and where docker-compose.yml lives. Resolve the lab root
# rather than the script's own directory, so it works from anywhere.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${HERE}/.."

PASS=0; FAIL=0; WARN=0

ok()   { printf "  PASS  %s\n" "$*"; PASS=$((PASS+1)); }
bad()  { printf "  FAIL  %s\n" "$*"; FAIL=$((FAIL+1)); }
warn() { printf "  WARN  %s\n" "$*"; WARN=$((WARN+1)); }
hdr()  { printf "\n[%s] %s\n" "$1" "$2"; }
die()  { printf "\n  FAIL  %s\n\n  This is a gating check. Fix it, then re-run.\n\n" "$*"; exit 1; }

DC="docker compose"

echo "==============================================================="
echo "SD411 Lab 10 - Windowing on Wikipedia Pageviews"
echo "Stack verification"
echo "==============================================================="

# --- C00 gating: .env stamped -----------------------------------------------
hdr C00 ".env stamped from common.env"
[[ -f ./.env ]] || die ".env not found. Run scripts/sync_env.sh from the repo root."
set -a; source ./.env; set +a
for V in SPARK_IMAGE MINIO_IMAGE MC_IMAGE S3_BUCKET SD411_JARS SD411_DATA \
         HADOOP_AWS_VERSION AWS_SDK_BUNDLE_VERSION WORKER_MEM WORKER_CORES; do
  [[ -n "${!V:-}" ]] || die "${V} is empty in .env. The stamp is stale or partial."
done
ok ".env present and the required variables resolve"

PV_MIN_SHARDS="${PV_MIN_SHARDS:-168}"
PV_HOST_DIR="${SD411_DATA}/pageviews"
PV_CONTAINER_DIR="/opt/lab10/pageviews"
JAR_DIR_CONTAINER="/opt/spark/extra-jars"

# --- C01 gating: base compose reachable -------------------------------------
hdr C01 "vm-base compose reachable"
[[ -f ../vm-base/docker-compose.base.yml ]] \
  || die "../vm-base/docker-compose.base.yml not found. This lab extends it."
ok "../vm-base/docker-compose.base.yml present"

# --- C02 gating: compose config resolves ------------------------------------
hdr C02 "docker compose config resolves"
if CONFIG="$(${DC} config 2>&1)"; then
  ok "merged configuration renders"
else
  echo "${CONFIG}" | sed 's/^/        /'
  die "docker compose config failed. Bad extends: path or unstamped .env."
fi

# --- C03: no hardcoded infrastructure in the lab compose --------------------
hdr C03 "no hardcoded tags, credentials, buckets, ports, or sizing"
BAD=0
grep -nE 'image:[[:space:]]*(apache/spark|minio/minio|minio/mc|apache/hadoop|bitnami)' docker-compose.yml >/dev/null 2>&1 && { bad "hardcoded image tag"; BAD=1; }
grep -nE 'sd411admin|sd411password' docker-compose.yml >/dev/null 2>&1 && { bad "hardcoded credential"; BAD=1; }
grep -nE '"(8080|7077|9000|9001|9870):' docker-compose.yml >/dev/null 2>&1 && { bad "hardcoded host port"; BAD=1; }
grep -nE '\-\-(cores|memory)[[:space:]]+[0-9]' docker-compose.yml >/dev/null 2>&1 && { bad "hardcoded worker sizing"; BAD=1; }
[[ ${BAD} -eq 0 ]] && ok "compose file is clean (all values arrive from .env)"

# --- C04: centralized JARs on the host --------------------------------------
hdr C04 "S3A JARs centralized at \${SD411_JARS}"
HA="${SD411_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
SDKJ="${SD411_JARS}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"
if [[ -s "${HA}" && -s "${SDKJ}" ]]; then
  ok "both JARs present at pinned versions"
else
  bad "missing ${HA} and/or ${SDKJ}. VM provisioning is incomplete."
fi

# --- C05 gating: Pageviews shards provisioned on the host -------------------
hdr C05 "Pageviews shards provisioned on this VM"
if [[ ! -d "${PV_HOST_DIR}" ]]; then
  die "${PV_HOST_DIR} does not exist. Run provision/prepare_pageviews.py."
fi
SHARDS=$(find "${PV_HOST_DIR}" -maxdepth 1 -name 'shard-*.parquet' | wc -l | tr -d ' ')
if [[ "${SHARDS}" -lt "${PV_MIN_SHARDS}" ]]; then
  die "found ${SHARDS} shards, expected at least ${PV_MIN_SHARDS}."
fi
ok "${SHARDS} shards present"
if [[ -s "${PV_HOST_DIR}/_manifest.json" ]]; then
  ok "_manifest.json present"
else
  bad "_manifest.json missing; provisioning did not finish."
fi

# --- C06: shard modification times increase monotonically -------------------
hdr C06 "shard mtimes increase in shard order (determinism gate)"
# The streaming file source orders by modification time when
# maxFilesPerTrigger is set. Out-of-order mtimes make every student's batch
# boundaries different and every expected number wrong.
OUT_OF_ORDER=$(find "${PV_HOST_DIR}" -maxdepth 1 -name 'shard-*.parquet' -printf '%f %T@\n' \
  | sort \
  | awk '{ if (NR>1 && $2 < prev) c++; prev=$2 } END { print c+0 }')
if [[ "${OUT_OF_ORDER}" -eq 0 ]]; then
  ok "modification times are monotonically non-decreasing"
else
  warn "${OUT_OF_ORDER} shards have out-of-order mtimes. Re-run prepare_pageviews.py."
fi

# --- C07: stack is up -------------------------------------------------------
hdr C07 "compose services running"
for SVC in spark-master spark-worker minio; do
  CID="$(${DC} ps -q "${SVC}" 2>/dev/null)"
  if [[ -n "${CID}" ]] && [[ "$(docker inspect -f '{{.State.Running}}' "${CID}" 2>/dev/null)" == "true" ]]; then
    ok "service ${SVC} is running"
  else
    bad "service ${SVC} is not running. Try: ${DC} up -d"
  fi
done

# --- C08: minio-init provisioning gate --------------------------------------
hdr C08 "minio-init completed without the exit-64 provisioning signal"
INIT_CID="$(${DC} ps -aq minio-init 2>/dev/null)"
if [[ -z "${INIT_CID}" ]]; then
  bad "minio-init never ran."
else
  CODE="$(docker inspect -f '{{.State.ExitCode}}' "${INIT_CID}" 2>/dev/null)"
  case "${CODE}" in
    0)  ok "minio-init exited 0 (buckets ready, shards visible)" ;;
    64) bad "minio-init exited 64: the Pageviews shards are not visible to the stack." ;;
    *)  bad "minio-init exited ${CODE}. Check: ${DC} logs minio-init" ;;
  esac
fi

# --- C09: shards visible inside BOTH Spark containers -----------------------
hdr C09 "shards visible inside the driver and the executor"
# The streaming file source reads a LOCAL path. If the worker cannot see the
# same path as the driver, tasks fail at scan time with a confusing message
# about a missing file that the driver just listed successfully.
for SVC in spark-master spark-worker; do
  N="$(${DC} exec -T "${SVC}" sh -c "ls -1 ${PV_CONTAINER_DIR}/shard-*.parquet 2>/dev/null | wc -l" 2>/dev/null | tr -d '\r ')"
  if [[ "${N:-0}" -ge "${PV_MIN_SHARDS}" ]]; then
    ok "${SVC} sees ${N} shards at ${PV_CONTAINER_DIR}"
  else
    bad "${SVC} sees ${N:-0} shards at ${PV_CONTAINER_DIR} (expected >= ${PV_MIN_SHARDS})"
  fi
done

# --- C10: JARs visible inside the container ---------------------------------
hdr C10 "S3A JARs visible inside spark-master"
JN="$(${DC} exec -T spark-master sh -c "ls -1 ${JAR_DIR_CONTAINER}/*.jar 2>/dev/null | wc -l" 2>/dev/null | tr -d '\r ')"
if [[ "${JN:-0}" -ge 2 ]]; then
  ok "${JN} JARs at ${JAR_DIR_CONTAINER}"
else
  bad "expected at least 2 JARs at ${JAR_DIR_CONTAINER}, found ${JN:-0}"
fi

# --- C11: spark-work is a named volume, and no host work/ exists ------------
hdr C11 "scratch lives on the spark-work named volume"
if echo "${CONFIG}" | grep -q 'spark-work'; then
  ok "spark-work appears in the merged configuration"
else
  bad "spark-work named volume missing from the merged configuration"
fi
if [[ -d ./work ]]; then
  bad "a host ./work directory exists. Delete it; Spark runs as root and will root-own it."
else
  ok "no host ./work directory"
fi

# --- C12: MinIO reachable and buckets present -------------------------------
hdr C12 "MinIO buckets"
# mc is the image ENTRYPOINT. Never wrap it in a shell; use MC_HOST_local.
MINIO_CID="$(${DC} ps -q minio 2>/dev/null)"
if [[ -z "${MINIO_CID}" ]]; then
  bad "minio container not found; skipping bucket check"
else
  NET="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "${MINIO_CID}" 2>/dev/null)"
  MC_OUT="$(docker run --rm --network "${NET}" \
    -e "MC_HOST_local=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
    "${MC_IMAGE}" ls local 2>&1)"
  if echo "${MC_OUT}" | grep -q "${S3_BUCKET}"; then
    ok "bucket ${S3_BUCKET} exists"
  else
    bad "bucket ${S3_BUCKET} not listed. mc said: $(echo "${MC_OUT}" | head -1)"
  fi
fi

# --- C13: end-to-end read through Spark -------------------------------------
hdr C13 "Spark can read the shards from the cluster (not local[*])"
PROBE=$(${DC} exec -T spark-master /opt/spark/bin/spark-submit \
  --master "spark://spark-master:7077" \
  --conf spark.sql.session.timeZone=UTC \
  --jars "${JAR_DIR_CONTAINER}/hadoop-aws-${HADOOP_AWS_VERSION}.jar,${JAR_DIR_CONTAINER}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
  /opt/lab10/scripts/verify_probe.py 2>&1 | tail -5)
if echo "${PROBE}" | grep -q "PROBE_OK"; then
  ok "cluster read succeeded: $(echo "${PROBE}" | grep PROBE_OK)"
else
  bad "cluster probe failed. Last lines:"
  echo "${PROBE}" | sed 's/^/        /'
fi

# --- C14: driver UI reachable -----------------------------------------------
hdr C14 "driver UI port published"
if curl -sf -o /dev/null "http://localhost:${PORT_DRIVER_UI:-4040}" 2>/dev/null; then
  ok "driver UI answering on ${PORT_DRIVER_UI:-4040}"
else
  warn "nothing on ${PORT_DRIVER_UI:-4040} right now. That is expected when no"
  warn "job is running, but every measurement in Parts 1, 2, and 4 uses the"
  warn "Structured Streaming tab. Re-check while a query is live."
fi

echo
echo "==============================================================="
printf "PASS %d   FAIL %d   WARN %d\n" "${PASS}" "${FAIL}" "${WARN}"
echo "==============================================================="
if [[ ${FAIL} -gt 0 ]]; then
  echo "Do not start Part 0 until FAIL is zero. Bring this output to the"
  echo "instructor if you have been stuck for 20 minutes."
  exit 1
fi
echo "Stack is good. Open WORKSHEET.md and write your Part 0 prediction first."
exit 0
