#!/usr/bin/env bash
# SD411 lab04 -- verify_lab04.sh
# Run from the lab04/ directory AFTER `docker compose up -d`.
# 12 named checks. PASS/FAIL/WARN with a pointed next step on every failure.
#
# Post-reconciliation notes:
#  - Checks resolve containers by COMPOSE SERVICE, not by container_name. The
#    extends: base does not set container_name, so names are project-derived
#    and must never be hardcoded here again.
#  - JARs are provisioned once on the VM at ${SD411_JARS}; there is no
#    per-lab download_jars.sh any more.
set -u
PASS=0; FAIL=0; WARN=0

ok()   { echo "PASS  [$1] $2"; PASS=$((PASS+1)); }
bad()  { echo "FAIL  [$1] $2"; echo "      -> $3"; FAIL=$((FAIL+1)); }
warn() { echo "WARN  [$1] $2"; echo "      -> $3"; WARN=$((WARN+1)); }

echo "SD411 lab04 stack verification"
echo "=============================="

# 0. stamped .env must exist, and everything downstream depends on its values
if [ -f .env ]; then
  set -a; . ./.env; set +a
  ok 0 ".env present (stamped from common.env)"
else
  bad 0 ".env missing" "Run vm-base/scripts/sync_env.sh to stamp common.env into every lab."
  echo "Cannot continue without .env."
  exit 1
fi

# 1. docker present
if command -v docker >/dev/null 2>&1; then
  ok 1 "docker is installed"
else
  bad 1 "docker not found" "Install Docker Engine; see the course Hardware Requirements page."
fi

# 2. compose v2 plugin
if docker compose version >/dev/null 2>&1; then
  ok 2 "docker compose v2 plugin available"
else
  bad 2 "docker compose v2 not available" "Install the compose plugin (docker-compose-plugin package)."
fi

# 3. the base file this lab inherits from must be reachable
if [ -f ../vm-base/docker-compose.base.yml ]; then
  ok 3 "vm-base/docker-compose.base.yml found"
else
  bad 3 "vm-base/docker-compose.base.yml not found" "This lab inherits every service from it. Clone the full course repo, don't copy a single lab directory."
fi

# 4. compose file actually resolves (catches unresolved \${...} and bad extends)
if docker compose config >/dev/null 2>&1; then
  ok 4 "docker compose config resolves"
else
  bad 4 "docker compose config failed" "Run 'docker compose config' and read the error; usually a missing .env value or an unreachable base file."
fi

# 5. free disk
AVAIL_GB=$(df -BG --output=avail . 2>/dev/null | tail -1 | tr -dc '0-9')
if [ -n "${AVAIL_GB}" ] && [ "${AVAIL_GB}" -ge 5 ]; then
  ok 5 "disk: ${AVAIL_GB} GB free"
else
  warn 5 "disk: ${AVAIL_GB:-unknown} GB free (<5 GB)" "Free space before Part 3; shuffle spill needs room."
fi

# 6. centralized S3A connector JARs staged on the VM
JARDIR="${SD411_JARS:-/opt/sd411/jars}"
HJ="hadoop-aws-${HADOOP_AWS_VERSION}.jar"
AJ="aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"
if [ -f "${JARDIR}/${HJ}" ] && [ -f "${JARDIR}/${AJ}" ]; then
  ok 6 "S3A JARs staged in ${JARDIR}"
else
  bad 6 "S3A JARs missing from ${JARDIR}" "Re-run the VM provisioner's JAR stage (vm-base). We use --jars, not --packages: the proxy breaks in-container Maven resolution."
fi

# 7-9. services running (by compose service name)
for spec in "7:spark-master" "8:spark-worker" "9:minio"; do
  n="${spec%%:*}"; svc="${spec#*:}"
  if docker compose ps --status running --services 2>/dev/null | grep -qx "$svc"; then
    ok "$n" "$svc is running"
  else
    bad "$n" "$svc is not running" "docker compose up -d, then: docker compose logs $svc"
  fi
done

# 10. minio-init exited 0 (64 means the shared seed was never provisioned)
INIT_ID=$(docker compose ps -aq minio-init 2>/dev/null | head -1)
INIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' "${INIT_ID}" 2>/dev/null)
if [ "${INIT_CODE:-1}" = "0" ]; then
  ok 10 "minio-init bootstrap exited 0"
elif [ "${INIT_CODE:-1}" = "64" ]; then
  bad 10 "minio-init exited 64: season seed missing" "The seed lives at ${SD411_DATA:-/opt/sd411/data}/${SEED_CSV:-statcast_2025.csv}. See data/README.md."
else
  bad 10 "minio-init exited ${INIT_CODE:-unknown}" "docker compose logs minio-init"
fi

# 11. worker registered with master
if curl -fsS --max-time 5 "http://localhost:${PORT_SPARK_MASTER_UI:-8080}/json/" 2>/dev/null | grep -q '"status" *: *"ALIVE"'; then
  ok 11 "worker registered with master (ALIVE)"
else
  bad 11 "no ALIVE worker registered" "docker compose logs spark-worker. Also check no stale sd411 stack holds the ports: scripts/sd411_down_all.sh"
fi

# 12. corpus generated (WARN only: Part 0 generates it)
# minio's image ships without grep, so check for a part- file via a shell glob.
if docker compose exec -T minio sh -c "ls /data/${S3_BUCKET:-sd411}/corpus/plays/part-* >/dev/null 2>&1"; then
  ok 12 "text corpus present at corpus/plays/"
else
  warn 12 "corpus not yet generated" "Run gen_corpus.py (Part 0, step 2 in the README). Expected on first boot."
fi

echo "=============================="
echo "Result: ${PASS} pass, ${FAIL} fail, ${WARN} warn"
if [ "$FAIL" -gt 0 ]; then
  echo "Fix FAILs before starting Part 1. Apply the 20-minute rule."
  exit 1
fi
exit 0
