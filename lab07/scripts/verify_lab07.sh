#!/usr/bin/env bash
# SD411 lab07 — stack verification. 13 named checks.
# PASS = ready, FAIL = must fix before proceeding, WARN = note and continue.
# Run from the lab07/ directory after `docker compose up -d`.
#
# Reconciled: checks 0-2 gate the extends: plumbing BEFORE any stack check, and
# container resolution uses compose SERVICE names. The vm-base services set no
# container_name, so the old hardcoded-name checks would false-fail after the
# extends: conversion.
#
# Exit status: 0 if no FAILs, 1 otherwise.

set -uo pipefail

PASS=0; FAIL=0; WARN=0
ok()   { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
warn() { echo "  WARN  $1"; WARN=$((WARN+1)); }

echo "=== SD411 lab07 stack verification ==="

# --- 0. .env stamped -------------------------------------------------------
# Everything downstream reads these values. Fail fast rather than letting
# ${...} silently resolve to empty strings.
if [[ -f .env ]]; then
  ok ".env is stamped"
  set -a; . ./.env; set +a
else
  bad ".env missing — run ../vm-base/scripts/sync_env.sh to stamp it from common.env"
fi

# Defaults so the rest of the script is still readable if .env is missing.
S3_BUCKET="${S3_BUCKET:-sd411}"
FACT_PREFIX="${FACT_PREFIX:-fact/pitches}"
SD411_JARS="${SD411_JARS:-/opt/sd411/jars}"
HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.3.4}"
AWS_SDK_BUNDLE_VERSION="${AWS_SDK_BUNDLE_VERSION:-1.12.262}"
PORT_SPARK_MASTER_UI="${PORT_SPARK_MASTER_UI:-8080}"
JARS_CTR="/opt/spark/extra-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# --- 1. vm-base reachable --------------------------------------------------
if [[ -f ../vm-base/docker-compose.base.yml ]]; then
  ok "../vm-base/docker-compose.base.yml reachable"
else
  bad "../vm-base/docker-compose.base.yml not found — clone the course repo with vm-base as a sibling of lab07/"
fi

# --- 2. compose config resolves -------------------------------------------
# The cheap catch for a bad extends: or an unstamped .env.
if docker compose config >/dev/null 2>&1; then
  ok "docker compose config resolves (extends: + .env OK)"
else
  bad "docker compose config failed — run it directly to see the error"
fi

# --- 3. Docker reachable ---------------------------------------------------
if docker info >/dev/null 2>&1; then ok "Docker daemon reachable"
else bad "Docker daemon not reachable"; fi

# --- 4. centralized S3A JARs present on the VM ----------------------------
# Provisioned once at ${SD411_JARS} by vm-base. This lab no longer ships its
# own download_jars.sh, so a missing JAR is a PROVISIONING failure, not a
# student action.
if [[ -f "${SD411_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
   && -f "${SD411_JARS}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" ]]; then
  ok "centralized S3A JARs present at ${SD411_JARS}"
else
  bad "S3A JARs missing at ${SD411_JARS} — VM provisioning issue, escalate (do not download per-lab)"
fi

# --- 5. services up --------------------------------------------------------
for svc in spark-master spark-worker minio; do
  cid=$(docker compose ps -q "$svc" 2>/dev/null)
  if [[ -n "$cid" && "$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null)" == "true" ]]; then
    ok "service $svc running"
  else
    bad "service $svc not running (docker compose up -d)"
  fi
done

# --- 6. master healthy -----------------------------------------------------
mcid=$(docker compose ps -q spark-master 2>/dev/null)
HS=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$mcid" 2>/dev/null || echo "none")
if [[ "$HS" == "healthy" ]]; then ok "spark-master healthy"
elif [[ "$HS" == "starting" ]]; then warn "spark-master still starting; re-run in ~15s"
else bad "spark-master health=$HS"; fi

# --- 7. worker registered ALIVE -------------------------------------------
# A trailing `<<<'...'` on the same command as `< /dev/tcp/...` clobbers the
# /dev/tcp redirect (last redirect on fd 0 wins), so the old version never
# actually talked to the socket. Open the socket on its own fd, write the
# request, then read the response back from that fd.
WORKERS=$(docker compose exec -T spark-master bash -c \
  "exec 3<>/dev/tcp/127.0.0.1/8080; printf 'GET /json/ HTTP/1.0\r\n\r\n' >&3; cat <&3" 2>/dev/null \
  | grep -o '"state" : "ALIVE"' | wc -l | tr -d ' ')
if [[ "${WORKERS:-0}" -ge 1 ]]; then ok "at least one ALIVE worker registered"
else warn "could not confirm an ALIVE worker (UI: http://localhost:${PORT_SPARK_MASTER_UI})"; fi

# --- 8. MinIO healthy ------------------------------------------------------
nid=$(docker compose ps -q minio 2>/dev/null)
MHS=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$nid" 2>/dev/null || echo "none")
if [[ "$MHS" == "healthy" ]]; then ok "minio healthy"
else bad "minio health=$MHS"; fi

# --- 9. bucket bootstrapped ------------------------------------------------
if docker compose run --rm --entrypoint mc mc-shell ls "local/${S3_BUCKET}" >/dev/null 2>&1; then
  ok "bucket ${S3_BUCKET} exists"
else
  warn "could not confirm bucket ${S3_BUCKET} (mc-shell is behind the tools profile)"
fi

# --- 10. lab06 fact present (the dependency gate) -------------------------
# minio-init exits 64 when the upstream fact is absent. That code is the
# distinct "re-run lab06" signal, kept apart from a generic failure.
# NOTE: `mc ls` on a valid-but-empty prefix still exits 0 with no output, so
# a bare exit-code check always reports "present" even on a fresh bucket.
# Check that it actually printed a listing.
icid=$(docker compose ps -aq minio-init 2>/dev/null)
INIT_RC=$(docker inspect -f '{{.State.ExitCode}}' "$icid" 2>/dev/null || echo "-1")
if [[ -n "$(docker compose run --rm --entrypoint mc mc-shell ls "local/${S3_BUCKET}/${FACT_PREFIX}/" 2>/dev/null)" ]]; then
  ok "lab06 fact present at s3a://${S3_BUCKET}/${FACT_PREFIX}/"
elif [[ "$INIT_RC" == "64" ]]; then
  bad "lab06 fact absent (minio-init exit 64) — re-run the lab06 writer; see data/README.md"
else
  warn "could not confirm the lab06 fact; check with the tools profile"
fi

# --- 11. dimension tables built (Part 0 output) ---------------------------
if [[ -n "$(docker compose run --rm --entrypoint mc mc-shell ls "local/${S3_BUCKET}/dim/teams/" 2>/dev/null)" ]] \
 && [[ -n "$(docker compose run --rm --entrypoint mc mc-shell ls "local/${S3_BUCKET}/fact/pa_events/" 2>/dev/null)" ]]; then
  ok "dimension tables present (dim/teams, fact/pa_events)"
else
  warn "dimension tables not built yet — run build_dims.py (README Part 0)"
fi

# --- 12. end-to-end S3A read through the centralized JARs -----------------
# spark-submit sniffs the resource type from the file extension, which
# /dev/stdin does not have ("Failed to get main class in JAR"). Write the
# script to a real file, copy it in, and submit that path instead.
VERIFY_PY="$(mktemp /tmp/lab07_verify_XXXX.py)"
trap 'rm -f "$VERIFY_PY"' EXIT
cat > "$VERIFY_PY" <<PY
import os
from pyspark.sql import SparkSession
s = (SparkSession.builder.appName("lab07-verify")
     .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT","http://minio:9000"))
     .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER","sd411admin"))
     .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD","sd411password"))
     .config("spark.hadoop.fs.s3a.path.style.access","true")
     .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
     .getOrCreate())
try:
    n = s.read.parquet("s3a://${S3_BUCKET}/${FACT_PREFIX}").limit(10).count()
    print(f"VERIFY_OK rows={n}")
except Exception as e:
    print(f"VERIFY_ERR {e}")
s.stop()
PY
docker compose cp "$VERIFY_PY" spark-master:/tmp/lab07_verify.py >/dev/null 2>&1
READ=$(docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master "spark://spark-master:7077" \
  --jars "$JARS_CTR" \
  /tmp/lab07_verify.py 2>/dev/null)
if grep -q "VERIFY_OK" <<<"$READ"; then ok "Spark reads the fact through S3A with the centralized JARs"
else warn "in-container S3A read did not confirm (fact may be unbuilt; see check 10)"; fi

# --- 13. no host work/ dir (workdir fix compliance) -----------------------
# Scratch lives on the spark-work named volume. A host work/ directory means
# the packet predates the vm-base v1.1 fix and will permission-deny.
if [[ -d work ]]; then
  bad "host work/ directory present — this packet must use the inherited spark-work named volume; delete ./work"
else
  ok "no host work/ bind mount (spark-work named volume inherited)"
fi

echo
echo "=== summary: $PASS PASS / $WARN WARN / $FAIL FAIL ==="
if [[ "$FAIL" -gt 0 ]]; then
  echo "Resolve the FAIL items above before starting lab07."
  exit 1
fi
echo "Stack looks good. Proceed to README Part 0."
exit 0
