#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 2 -- verify_stack.sh
# 12 named checks. Run from the lab02/ directory after `docker compose up -d`.
# Every check prints PASS, FAIL, or WARN with a pointer at the next thing to
# look at. Do not start Part 1 until checks 1-11 all PASS.
# =============================================================================
set -uo pipefail

PASS=0; FAIL=0; WARN=0
ok()   { echo "PASS  [$1] $2"; PASS=$((PASS+1)); }
bad()  { echo "FAIL  [$1] $2"; echo "      -> $3"; FAIL=$((FAIL+1)); }
warn() { echo "WARN  [$1] $2"; echo "      -> $3"; WARN=$((WARN+1)); }

echo "SD411 Lab 2 stack verification -- $(date)"
echo "------------------------------------------------------------"

# 1. docker present
if command -v docker >/dev/null 2>&1; then
  ok 1 "docker CLI found ($(docker --version | cut -d',' -f1))"
else
  bad 1 "docker CLI not found" "Install Docker Engine, or check your PATH. See lab0 README Part 0."
fi

# 2. compose v2 plugin
if docker compose version >/dev/null 2>&1; then
  ok 2 "docker compose v2 plugin found"
else
  bad 2 "docker compose v2 not available" "You may have legacy docker-compose v1. Install the compose plugin."
fi

# 3. disk space (>= 5 GB free on the partition holding the working dir)
free_kb=$(df -k . | awk 'NR==2 {print $4}')
if (( free_kb >= 5*1024*1024 )); then
  ok 3 "disk space OK ($((free_kb/1024/1024)) GB free)"
else
  bad 3 "less than 5 GB free" "big.csv + HDFS blocks + MinIO objects need headroom. Clear space or shrink TARGET_MB."
fi

# 4. test data generated
if [[ -f data/big.csv && -d data/small ]]; then
  n_small=$(ls data/small 2>/dev/null | wc -l)
  ok 4 "test data present (big.csv + ${n_small} small files)"
else
  bad 4 "test data missing" "Run ./scripts/gen_data.sh first (needs data/statcast_sample.csv)."
fi

# 5-8. containers running
for svc in lab02-namenode lab02-datanode lab02-minio lab02-mc; do
  case $svc in
    lab02-namenode) n=5;; lab02-datanode) n=6;; lab02-minio) n=7;; lab02-mc) n=8;;
  esac
  state=$(docker inspect -f '{{.State.Status}}' "$svc" 2>/dev/null || echo "absent")
  if [[ "$state" == "running" ]]; then
    ok "$n" "$svc running"
  else
    bad "$n" "$svc is '$state'" "docker compose up -d, then: docker logs $svc --tail 50"
  fi
done

# 9. NameNode web UI reachable
if curl -sf -m 5 http://localhost:9870/ >/dev/null 2>&1; then
  ok 9 "NameNode UI reachable at http://localhost:9870"
else
  bad 9 "NameNode UI not reachable on :9870" "Port conflict or NameNode still starting. docker logs lab02-namenode --tail 50"
fi

# 10. NameNode out of safemode and DataNode registered
report=$(docker exec lab02-namenode hdfs dfsadmin -report 2>/dev/null)
live=$(echo "$report" | grep -c "^Name:" || true)
safemode=$(docker exec lab02-namenode hdfs dfsadmin -safemode get 2>/dev/null || echo "unknown")
if [[ "$safemode" == *"OFF"* && "$live" -ge 1 ]]; then
  ok 10 "HDFS healthy: safemode OFF, $live live DataNode(s)"
elif [[ "$safemode" == *"ON"* ]]; then
  warn 10 "NameNode in safemode" "Usually resolves within 60 s of startup. Re-run this script. If it persists: docker logs lab02-namenode"
else
  bad 10 "DataNode not registered with NameNode" "docker logs lab02-datanode --tail 50; check fs.defaultFS env in compose."
fi

# 11. MinIO live
if curl -sf -m 5 http://localhost:9000/minio/health/live >/dev/null 2>&1; then
  ok 11 "MinIO live at http://localhost:9000 (console :9001)"
else
  bad 11 "MinIO health check failed" "docker logs lab02-minio --tail 50; check for a port conflict on 9000/9001."
fi

# 12. (Stretch only) Spark master + S3A JARs, if the spark profile was started
if docker inspect -f '{{.State.Status}}' lab02-spark-master >/dev/null 2>&1; then
  state=$(docker inspect -f '{{.State.Status}}' lab02-spark-master)
  jars_present=$(ls jars/hadoop-aws-*.jar jars/aws-java-sdk-bundle-*.jar 2>/dev/null | wc -l)
  if [[ "$state" == "running" && "$jars_present" -eq 2 ]]; then
    ok 12 "spark-master running with S3A JARs present (stretch goal ready)"
  elif [[ "$state" == "running" ]]; then
    warn 12 "spark-master running but S3A JARs missing in ./jars/" "Run ./scripts/download_jars.sh before the stretch spark-submit (--packages is blocked by TLS interception)."
  else
    warn 12 "spark-master present but '$state'" "docker logs lab02-spark-master --tail 50"
  fi
else
  warn 12 "Spark profile not started (fine unless attempting the stretch goal)" "docker compose --profile spark up -d  (and ./scripts/download_jars.sh first)"
fi

echo "------------------------------------------------------------"
echo "Result: $PASS pass, $FAIL fail, $WARN warn"
if (( FAIL > 0 )); then
  echo "Fix FAILs before starting Part 1. Stuck for 20 minutes? Follow the help protocol in README.md."
  exit 1
fi
echo "Stack verified. Proceed to Part 1."
