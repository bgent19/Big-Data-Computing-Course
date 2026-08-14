#!/usr/bin/env bash
# verify_lab11.sh - preflight and proof checks for lab11.
#
# Checks are numbered 0..14. The first block (0..4) GATES: any failure there
# aborts immediately, because nothing downstream can be trusted if the env is
# not stamped or the compose will not resolve. The rest accumulate PASS / WARN /
# FAIL and the script exits non-zero if any FAIL remains. Containers are always
# addressed by their compose SERVICE name (broker, spark-master, minio), never a
# hardcoded container name, so this survives a `name:` change.
#
# .env values are read with a grep helper, never by sourcing: KAFKA_HEAP_OPTS
# contains spaces, and sourcing a line whose value has a space runs the tail as
# a command (lab09 finding).
set -uo pipefail

# Run from the LAB ROOT, not from scripts/, whichever directory the caller is
# standing in. Every path below is relative to the lab root: .env, the compose
# it must resolve, ../vm-base/ for the extends: target, and scripts/ for the
# shell checks. This script lives in scripts/, so that is one level up.
cd "$(dirname "$0")/.." || { printf 'verify_lab11: cannot reach the lab root\n' >&2; exit 64; }
ENVF=".env"
FAILS=0; WARNS=0

pass() { printf '  [PASS] %s\n' "$1"; }
warn() { printf '  [WARN] %s\n' "$1"; WARNS=$((WARNS+1)); }
fail() { printf '  [FAIL] %s\n' "$1"; FAILS=$((FAILS+1)); }
gate() { printf '  [FAIL] %s\n' "$1"; printf '\nverify_lab11: gating check failed, aborting.\n'; exit 64; }
getenv() { grep -E "^$1=" "$ENVF" 2>/dev/null | head -n1 | cut -d= -f2- || true; }

echo "verify_lab11: gating checks (0-4)"

# 0 - .env present and stamped
if [ ! -f "$ENVF" ]; then gate "0 .env missing. Run scripts/sync_env.sh from common.env."; fi
pass "0 .env present"

# 1 - .env carries the lab09 Kafka names AND the lab11 names
missing=""
for k in KAFKA_IMAGE KAFKA_HEAP_OPTS PORT_KAFKA_EXTERNAL \
         LAB11_TOPIC LAB11_WINDOW_SECONDS LAB11_WATERMARK_SECONDS \
         S3_BUCKET MINIO_ROOT_USER MINIO_ROOT_PASSWORD SD411_DATA SD411_JARS; do
  [ -z "$(getenv "$k")" ] && missing="$missing $k"
done
if [ -n "$missing" ]; then
  gate "1 .env is missing required names:$missing  (append lab09 Kafka block + lab11 addendum, re-run sync_env.sh)"
fi
pass "1 .env carries lab09 Kafka names and lab11 names"

# 2 - base compose reachable
if [ ! -f "../vm-base/docker-compose.base.yml" ]; then
  gate "2 ../vm-base/docker-compose.base.yml not found (extends: target missing)."
fi
pass "2 base compose reachable"

# 3 - docker compose config resolves with no unresolved ${VAR}
if command -v docker >/dev/null 2>&1; then
  cfg="$(docker compose config 2>&1)" || gate "3 docker compose config failed:
$cfg"
  # A LITERAL $${VAR} is not an unresolved reference: minio-init's inline script
  # escapes its variables on purpose so the shell INSIDE the container expands
  # them (MINIO_ROOT_USER, S3_BUCKET), and `config` renders the escape back as
  # $${VAR}. Match only a ${ that is NOT preceded by a $, or every run FAILs on
  # the bucket-init command that is doing exactly the right thing.
  if printf '%s' "$cfg" | grep -qP '(?<!\$)\$\{'; then
    gate "3 unresolved \${VAR} remains after config:
$(printf '%s' "$cfg" | grep -nP '(?<!\$)\$\{')"
  fi
  pass "3 docker compose config resolves clean"
else
  gate "3 docker not found on host."
fi

# 4 - host tools
for t in awk grep sed; do command -v "$t" >/dev/null 2>&1 || gate "4 host tool missing: $t"; done
pass "4 host tools present"

echo "verify_lab11: content checks (5-14)"

# 5 - hardcode audit on the compose. Only container-internal service addresses
#     (broker:909x, minio:9000) are allowed; no image tags, credentials, or
#     host:container port pairs may be literal.
badtag="$(grep -nE 'bitnami|RELEASE\.[0-9]|apache/(spark|kafka|hadoop):[0-9]|minio/(minio|mc):' docker-compose.yml || true)"
if [ -n "$badtag" ]; then fail "5 hardcoded image tag in compose:
$badtag"; else pass "5 no hardcoded image tags"; fi
badcred="$(grep -nE 'sd411password|sd411admin' docker-compose.yml || true)"
if [ -n "$badcred" ]; then fail "5b hardcoded credential in compose:
$badcred"; else pass "5b no hardcoded credentials"; fi

# 6 - no container_name (resolve by service)
if grep -qE '^\s*container_name:' docker-compose.yml; then
  fail "6 container_name present; address services by name instead"
else pass "6 no container_name"; fi

# 7 - JAR gate. lab11 is the FIRST lab that drives Kafka THROUGH Spark, so the
#     Spark-Kafka connector JARs are required, not optional (lab09 WARNed; here
#     they FAIL). The S3A pair is required too, for the MinIO checkpoint.
JARS="$(getenv SD411_JARS)"; JARS="${JARS:-/opt/sd411/jars}"
if [ -d "$JARS" ]; then
  need_kafka="spark-sql-kafka-0-10 spark-token-provider-kafka-0-10 kafka-clients commons-pool2"
  need_s3a="hadoop-aws aws-java-sdk-bundle"
  miss=""
  for j in $need_kafka $need_s3a; do
    ls "$JARS" 2>/dev/null | grep -q "$j" || miss="$miss $j"
  done
  if [ -n "$miss" ]; then fail "7 required JARs missing from $JARS:$miss (add to scripts/download_jars.sh)"; \
    else pass "7 Spark-Kafka + S3A JARs staged in $JARS"; fi
else
  warn "7 $JARS not present on this host; cannot check JARs here (provision on the golden VM)"
fi

# 8 - compile all Python, SyntaxWarning promoted to error. compile() rather
#     than py_compile: py_compile writes __pycache__ directories into the
#     packet, which then show up as untracked litter in every student's repo.
if command -v python3 >/dev/null 2>&1; then
  pyerr=0
  for f in $(find . -name '*.py' | sort); do
    python3 -W error::SyntaxWarning \
      -c "import sys; src=open(sys.argv[1]).read(); compile(src, sys.argv[1], 'exec')" \
      "$f" 2>/tmp/pyc.$$ || { fail "8 compile failed: $f
$(cat /tmp/pyc.$$)"; pyerr=1; }
  done
  [ "$pyerr" -eq 0 ] && pass "8 all Python compiles clean"
  rm -f /tmp/pyc.$$
else
  warn "8 python3 not on host; skipping compile check"
fi

# 9 - shell syntax
sh -n scripts/produce.sh 2>/tmp/shn.$$ && pass "9 produce.sh sh -n clean" || fail "9 produce.sh sh -n:
$(cat /tmp/shn.$$)"
bash -n scripts/reset_lab11.sh 2>/tmp/bn.$$ && pass "9b reset_lab11.sh bash -n clean" || fail "9b reset_lab11.sh bash -n:
$(cat /tmp/bn.$$)"
rm -f /tmp/shn.$$ /tmp/bn.$$

# 10 - compose parses as YAML
if command -v python3 >/dev/null 2>&1; then
  python3 -c "import yaml,sys; yaml.safe_load(open('docker-compose.yml'))" 2>/tmp/yl.$$ \
    && pass "10 docker-compose.yml parses as YAML" \
    || fail "10 YAML parse error:
$(cat /tmp/yl.$$)"
  rm -f /tmp/yl.$$
else
  warn "10 python3 not on host; skipping YAML parse"
fi

# 11 - em-dash audit, packet-wide
emhits="$(LC_ALL=C grep -rlP '\xe2\x80\x94' --include='*.py' --include='*.sh' --include='*.md' --include='*.yml' . 2>/dev/null || true)"
if [ -n "$emhits" ]; then fail "11 em-dash found in:
$emhits"; else pass "11 no em-dashes packet-wide"; fi

# 12 - every ${VAR} referenced in the compose is defined in .env
undef=""
for v in $(grep -oE '\$\{[A-Z0-9_]+\}' docker-compose.yml | tr -d '${}' | sort -u); do
  [ -z "$(getenv "$v")" ] && undef="$undef $v"
done
if [ -n "$undef" ]; then fail "12 compose references names not in .env:$undef"; else pass "12 all compose \${VAR} defined in .env"; fi

# 12b - every shell script the README asks a student to run as ./scripts/foo.sh
#       must be executable. A packet committed at mode 644 fails on the FIRST
#       line of the README with "Permission denied", which reads to a student
#       like a broken VM.
notx=""
for s in scripts/*.sh provision/*.sh; do
  [ -f "$s" ] || continue
  [ -x "$s" ] || notx="$notx $s"
done
if [ -n "$notx" ]; then
  fail "12b shell scripts are not executable:$notx  (git update-index --chmod=+x <file>)"
else pass "12b all shell scripts executable"; fi

# 12c - the event-time stream this lab replays must be staged on the VM. This is
#       instructor provisioning, not a student step; produce.sh exits 64 without
#       it, so catching it here turns a mid-period surprise into a preflight WARN.
STREAM_DIR="$(getenv SD411_DATA)"; STREAM_DIR="${STREAM_DIR:-/opt/sd411/data}"
STREAM_FILE="$STREAM_DIR/lab11/pitch_stream.jsonl"
if [ -s "$STREAM_FILE" ]; then
  lines="$(wc -l < "$STREAM_FILE" | tr -d ' ')"
  if [ "$lines" -ge 20000 ]; then pass "12c pitch stream staged ($lines lines)"
  else fail "12c pitch stream at $STREAM_FILE has only $lines lines (expected 20000); re-run sudo ./provision/stage_pitch_stream.sh"; fi
else
  warn "12c $STREAM_FILE not staged; run 'sudo ./provision/stage_pitch_stream.sh' (instructor step, offline, ~1 s)"
fi

# 13 - no stray host work dir committed
if [ -d work ] && [ -n "$(ls -A work 2>/dev/null)" ]; then
  warn "13 host work/ is non-empty; make sure it is git-ignored and not submitted"
else pass "13 no stray work/ artifacts"; fi

# 14 - terminal proof probe, only if the stack is up and an oracle exists
if command -v docker >/dev/null 2>&1 && docker compose ps 2>/dev/null | grep -q 'spark-master'; then
  if docker compose exec -T spark-master sh -lc 'true' 2>/dev/null; then
    warn "14 stack is up: run 'spark-submit ... scripts/verify_probe.py' after Part 2 to prove exactly-once"
  else
    warn "14 stack present but spark-master not exec-able yet"
  fi
else
  warn "14 stack not up; the exactly-once probe runs in Part 2, not here"
fi

echo
if [ "$FAILS" -gt 0 ]; then
  echo "verify_lab11: $FAILS FAIL, $WARNS WARN. Fix the FAILs before the period."
  exit 1
else
  echo "verify_lab11: 0 FAIL, $WARNS WARN. Preflight clean."
  exit 0
fi
