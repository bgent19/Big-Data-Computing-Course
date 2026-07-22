#!/usr/bin/env bash
# SD411 Lab 6 — Setup verification (v2.0, extends: base + common.env)
# Run from the lab06/ directory after `docker compose up -d`.
# 12 named checks. Each prints PASS, FAIL, or WARN with a reason.
# Do not start Part 0 until checks 1-11 PASS. Check 12 may WARN.
#
# This lab downloads nothing. The S3A JARs are provisioned on the VM at
# ${SD411_JARS} and mounted into the containers by the base compose file.

set -u
PASS=0; FAIL=0; WARN=0

report () {
  printf "%-4s | %-36s | %s\n" "$1" "$2" "$3"
  case "$1" in PASS) PASS=$((PASS+1));; FAIL) FAIL=$((FAIL+1));; WARN) WARN=$((WARN+1));; esac
}

# Load the stamped .env so this script sees the same values compose does.
if [ -f .env ]; then set -a; . ./.env; set +a; fi
: "${S3_BUCKET:=sd411}"; : "${SEED_CSV:=statcast_2025.csv}"
: "${SD411_JARS:=/opt/sd411/jars}"; : "${SD411_DATA:=/opt/sd411/data}"
: "${PORT_SPARK_MASTER_UI:=8080}"; : "${WORKER_CORES:=2}"
: "${HADOOP_AWS_VERSION:=3.3.4}"; : "${AWS_SDK_BUNDLE_VERSION:=1.12.262}"

echo "SD411 Lab 6 verification — $(date)"
echo "--------------------------------------------------------------------"

# 1. stamped .env present (everything else depends on it resolving)
if [ -f .env ]; then
  report PASS "01 env-stamped" ".env present"
else
  report FAIL "01 env-stamped" "run scripts/sync_env.sh — compose \${VARS} will not resolve"
fi

# 2. vm-base reachable from this lab directory
if [ -f ../vm-base/docker-compose.base.yml ]; then
  report PASS "02 vm-base-present" "../vm-base/docker-compose.base.yml"
else
  report FAIL "02 vm-base-present" "base compose missing — extends: cannot resolve"
fi

# 3. compose config renders (catches extends/merge and unset-var errors)
if docker compose config >/dev/null 2>&1; then
  report PASS "03 compose-config" "renders clean"
else
  report FAIL "03 compose-config" "$(docker compose config 2>&1 | head -1)"
fi

# 4. no hardcoded image tag crept back into this lab's compose
if grep -Eq '^[[:space:]]*image:[[:space:]]*[a-z]' docker-compose.yml 2>/dev/null; then
  report FAIL "04 no-hardcoded-tags" "an image: literal is present — tags belong in common.env"
else
  report PASS "04 no-hardcoded-tags" "images inherited from base/common.env"
fi

# 5. spark master + worker running
UP=$(docker compose ps --services --filter status=running 2>/dev/null | tr '\n' ' ')
if echo "$UP" | grep -q spark-master && echo "$UP" | grep -q spark-worker; then
  report PASS "05 spark-containers" "master + worker running"
else
  report FAIL "05 spark-containers" "running: ${UP:-none} — docker compose up -d"
fi

# 6. worker registered, correct core count
ALIVE=$(docker compose exec -T spark-master bash -c \
  'exec 3<>/dev/tcp/127.0.0.1/8080; printf "GET /json/ HTTP/1.0\r\n\r\n" >&3; cat <&3' 2>/dev/null \
  | python3 -c '
import sys, json, re
raw = sys.stdin.read(); m = re.search(r"\{.*\}", raw, re.S)
try:
    d = json.loads(m.group(0)) if m else {}
    w = [x for x in d.get("workers", []) if x.get("state") == "ALIVE"]
    print("%d,%d" % (len(w), w[0]["cores"] if w else 0))
except Exception: print("0,0")' 2>/dev/null)
NW=${ALIVE%,*}; NC=${ALIVE#*,}
if [ "$NW" = "1" ] && [ "$NC" = "$WORKER_CORES" ]; then
  report PASS "06 worker-registered" "1 ALIVE worker, $NC cores"
elif [ "$NW" = "1" ]; then
  report WARN "06 worker-registered" "1 worker, $NC cores (common.env says $WORKER_CORES) — Part A numbers shift"
else
  report FAIL "06 worker-registered" "$NW ALIVE workers — check worker logs"
fi

# 7. centralized S3A JARs visible INSIDE the container (not on the host)
JARCHK=$(docker compose exec -T spark-master bash -c \
  "ls /opt/spark/extra-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      /opt/spark/extra-jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar >/dev/null 2>&1 && echo ok" 2>/dev/null)
if [ "$JARCHK" = "ok" ]; then
  report PASS "07 s3a-jars-mounted" "both JARs at /opt/spark/extra-jars"
else
  report FAIL "07 s3a-jars-mounted" "provision ${SD411_JARS} on the VM (base mounts it; this lab does not download)"
fi

# 8. workdir fix in effect: spark-work is a named volume, not a host bind
WD=$(docker compose config --format json 2>/dev/null | python3 -c '
import sys, json
try:
    c = json.load(sys.stdin)
    v = c["services"]["spark-master"].get("volumes", [])
    print("named" if any(x.get("type")=="volume" and x.get("source")=="spark-work" for x in v) else "missing")
except Exception: print("unknown")' 2>/dev/null)
case "$WD" in
  named)   report PASS "08 workdir-named-vol" "spark-work is a named volume" ;;
  missing) report FAIL "08 workdir-named-vol" "spark-work not inherited — root-owned bind mount will break writes" ;;
  *)       report WARN "08 workdir-named-vol" "could not inspect merged config" ;;
esac

# 9. minio healthy + seed uploaded (exit 64 = shared seed absent)
HS=$(docker inspect --format '{{.State.Health.Status}}' "$(docker compose ps -q minio)" 2>/dev/null)
EC=$(docker inspect --format '{{.State.ExitCode}}' "$(docker compose ps -aq minio-init)" 2>/dev/null)
if [ "$HS" = "healthy" ] && [ "$EC" = "0" ]; then
  report PASS "09 minio-seeded" "minio healthy, minio-init exited 0"
elif [ "$EC" = "64" ]; then
  report FAIL "09 minio-seeded" "exit 64: ${SD411_DATA}/${SEED_CSV} absent — see data/README.md"
else
  report FAIL "09 minio-seeded" "minio=$HS init-exit=$EC"
fi

# 10. seed object present and above the common.env size floor
: "${SEED_MIN_MB:=100}"
BYTES=$(docker compose run --rm --entrypoint mc \
  -e MC_HOST_local="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
  minio-init stat --json "local/${S3_BUCKET}/raw/${SEED_CSV}" 2>/dev/null \
  | python3 -c 'import sys,json
try: print(json.load(sys.stdin)["size"])
except Exception: print(0)' 2>/dev/null)
BYTES=${BYTES:-0}
if [ "$BYTES" -ge $((SEED_MIN_MB*1024*1024)) ]; then
  report PASS "10 seed-size" "$((BYTES/1024/1024)) MB (floor ${SEED_MIN_MB} MB)"
else
  report FAIL "10 seed-size" "$((BYTES/1024/1024)) MB is under the ${SEED_MIN_MB} MB floor — re-seed"
fi

# 11. disk headroom: Part B writes three layout copies of the fact table
AVAIL_GB=$(df -BG --output=avail . 2>/dev/null | tail -1 | tr -dc '0-9')
if [ "${AVAIL_GB:-0}" -ge 6 ]; then
  report PASS "11 disk-headroom" "${AVAIL_GB} GB free"
else
  report FAIL "11 disk-headroom" "${AVAIL_GB:-?} GB free — Part B needs room for 3 layouts"
fi

# 12. master UI reachable from the host (Parts A and C require UI evidence)
if curl -s -o /dev/null -w '%{http_code}' "http://localhost:${PORT_SPARK_MASTER_UI}" 2>/dev/null | grep -q '^200$'; then
  report PASS "12 master-ui" "http://localhost:${PORT_SPARK_MASTER_UI}"
else
  report WARN "12 master-ui" "no answer on ${PORT_SPARK_MASTER_UI} — a stale lab stack may hold the port (sd411_down_all.sh)"
fi

echo "--------------------------------------------------------------------"
echo "PASS=$PASS  FAIL=$FAIL  WARN=$WARN"
if [ "$FAIL" -gt 0 ]; then
  echo "Do not start Part 0 with FAILs outstanding. Apply the 20-minute rule if stuck."
  exit 1
fi
exit 0
