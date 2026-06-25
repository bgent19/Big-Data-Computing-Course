#!/usr/bin/env bash
# SD411 Lab 0 — Stack Verification
# Runs a series of named checks against the running course stack.
# Exit code 0 = all PASS; non-zero = at least one FAIL.
#
# Run from the repo root:  ./scripts/verify_stack.sh

set -u  # treat unset vars as errors; do NOT set -e — we want all checks to run.

# --- colors (degrade gracefully if tput missing or no tty) ---
if [ -t 1 ]; then
  GREEN=$(tput setaf 2 2>/dev/null || echo '')
  RED=$(tput setaf 1   2>/dev/null || echo '')
  YELLOW=$(tput setaf 3 2>/dev/null || echo '')
  BOLD=$(tput bold     2>/dev/null || echo '')
  RESET=$(tput sgr0    2>/dev/null || echo '')
else
  GREEN='' ; RED='' ; YELLOW='' ; BOLD='' ; RESET=''
fi

PASS=0 ; FAIL=0 ; WARN=0
pass() { echo "${GREEN}[PASS]${RESET} $1"; PASS=$((PASS+1)); }
fail() { echo "${RED}[FAIL]${RESET} $1"; FAIL=$((FAIL+1)); }
warn() { echo "${YELLOW}[WARN]${RESET} $1"; WARN=$((WARN+1)); }
info() { echo "       $1"; }

echo "${BOLD}==========================================${RESET}"
echo "${BOLD}SD411 — Stack Verification${RESET}"
echo "Date: $(date)"
echo "Host: $(hostname)"
echo "${BOLD}==========================================${RESET}"

# --- 1. Docker installed ---
if command -v docker >/dev/null 2>&1; then
  pass "docker available — $(docker --version)"
else
  fail "docker not found in PATH"
  info "Install: https://docs.docker.com/engine/install/ubuntu/"
fi

# --- 2. Docker Compose v2 plugin ---
if docker compose version >/dev/null 2>&1; then
  pass "docker compose plugin — $(docker compose version | head -1)"
else
  fail "docker compose plugin not found"
  info "sudo apt-get install docker-compose-plugin"
fi

# --- 3. Disk space available to Docker ---
DOCKER_ROOT=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo /var/lib/docker)
AVAIL_GB=$(df -BG "$DOCKER_ROOT" 2>/dev/null | awk 'NR==2 {gsub(/G/,"",$4); print $4}')
if [ -n "${AVAIL_GB:-}" ] && [ "$AVAIL_GB" -ge 10 ]; then
  pass "disk space at $DOCKER_ROOT: ${AVAIL_GB}G available"
elif [ -n "${AVAIL_GB:-}" ]; then
  warn "disk space at $DOCKER_ROOT: only ${AVAIL_GB}G available (recommend >= 10G)"
else
  warn "could not determine available disk space"
fi

# --- 4. Each expected container is running ---
for svc in sd411-spark-master sd411-spark-worker sd411-minio; do
  state=$(docker inspect -f '{{.State.Status}}' "$svc" 2>/dev/null || echo "missing")
  case "$state" in
    running) pass "container $svc is running" ;;
    missing) fail "container $svc not found — did 'docker compose up -d' complete?" ;;
    *)       fail "container $svc is in state: $state"
             info "Logs: docker logs $svc" ;;
  esac
done

# --- 5. minio-init exited cleanly (it's a one-shot) ---
init_state=$(docker inspect -f '{{.State.Status}}' sd411-minio-init 2>/dev/null || echo "missing")
init_exit=$(docker inspect -f '{{.State.ExitCode}}' sd411-minio-init 2>/dev/null || echo "?")
if [ "$init_state" = "exited" ] && [ "$init_exit" = "0" ]; then
  pass "minio-init exited cleanly (buckets created, sample seeded)"
elif [ "$init_state" = "running" ]; then
  warn "minio-init still running — wait 10s and re-run this script"
else
  fail "minio-init in state '$init_state' with exit code $init_exit"
  info "View logs: docker logs sd411-minio-init"
fi

# --- 6. Spark master UI responds ---
if curl -sf -o /dev/null --max-time 5 http://localhost:8080; then
  pass "Spark master UI reachable at http://localhost:8080"
else
  fail "Spark master UI not reachable at http://localhost:8080"
  info "Try: docker logs sd411-spark-master  (look for 'Started Spark Master')"
fi

# --- 7. Spark worker UI responds ---
if curl -sf -o /dev/null --max-time 5 http://localhost:8081; then
  pass "Spark worker UI reachable at http://localhost:8081"
else
  fail "Spark worker UI not reachable at http://localhost:8081"
fi

# --- 8. MinIO API health ---
if curl -sf -o /dev/null --max-time 5 http://localhost:9000/minio/health/live; then
  pass "MinIO API healthy at http://localhost:9000"
else
  fail "MinIO API not reachable at http://localhost:9000"
fi

# --- 9. MinIO console responds ---
if curl -sf -o /dev/null --max-time 5 http://localhost:9001; then
  pass "MinIO console reachable at http://localhost:9001"
else
  fail "MinIO console not reachable at http://localhost:9001"
fi

# --- 10. Seed object present in MinIO ---
# Pre-configure the alias via the MC_HOST_<alias> env var so mc can run as a
# one-shot command. The minio/mc image has 'mc' as its ENTRYPOINT, so passing
# 'sh -c ...' as args silently fails (mc treats 'sh' as an unknown subcommand).
# Capture stderr too so a future failure surfaces a real diagnostic.
bucket_listing=$(docker run --rm --network sd411-net \
  -e MC_HOST_local="http://sd411admin:sd411password@minio:9000" \
  minio/mc:RELEASE.2024-11-21T17-21-54Z \
  ls local/sd411/raw/ 2>&1)
if echo "$bucket_listing" | grep -q "statcast_sample.csv"; then
  pass "bucket 'sd411' contains raw/statcast_sample.csv"
else
  fail "bucket 'sd411' missing the seeded sample"
  # Show what mc actually said so the failure is debuggable.
  echo "$bucket_listing" | head -3 | sed 's/^/       mc: /'
  info "Check ./data/statcast_sample.csv exists, then:"
  info "  docker compose up -d --force-recreate minio-init"
fi

# --- 11. Worker registered with master ---
worker_count=$(curl -s --max-time 5 http://localhost:8080/json/ 2>/dev/null \
  | grep -o '"aliveworkers" : [0-9]*' | grep -o '[0-9]*' || echo 0)
if [ -n "${worker_count:-}" ] && [ "$worker_count" -ge 1 ]; then
  pass "Spark master shows $worker_count worker(s) registered"
else
  fail "Spark master shows 0 registered workers"
  info "Logs: docker logs sd411-spark-worker"
fi

# --- 12. S3A connector JARs present (offline workaround for Maven SSL) ---
# Because --packages fails behind USNA's TLS-inspecting proxy (Java truststore
# doesn't carry the institutional CA), we pre-download the JARs on the host
# and mount them into the Spark containers. Lab 0 step 3.3 uses --jars to
# reference them.
JARS_DIR="$(cd "$(dirname "$0")/.." && pwd)/jars"
REQUIRED_JARS=("hadoop-aws-3.3.4.jar" "aws-java-sdk-bundle-1.12.262.jar")
missing_jars=()
for j in "${REQUIRED_JARS[@]}"; do
  [ -f "$JARS_DIR/$j" ] || missing_jars+=("$j")
done
if [ "${#missing_jars[@]}" -eq 0 ]; then
  pass "S3A connector JARs present in ./jars/"
else
  fail "${#missing_jars[@]} of ${#REQUIRED_JARS[@]} S3A JAR(s) missing: ${missing_jars[*]}"
  info "Run: ./scripts/download_jars.sh"
fi

echo "${BOLD}==========================================${RESET}"
echo "Summary: ${GREEN}${PASS} passed${RESET}, ${RED}${FAIL} failed${RESET}, ${YELLOW}${WARN} warnings${RESET}"
echo "${BOLD}==========================================${RESET}"

if [ "$FAIL" -eq 0 ]; then
  echo "${GREEN}${BOLD}Stack verification PASSED.${RESET}  Proceed to Part 3 of the lab."
  exit 0
else
  echo "${RED}${BOLD}Stack verification FAILED.${RESET}  See failures above."
  echo "If stuck >= 20 minutes, follow the help protocol in README.md."
  exit 1
fi