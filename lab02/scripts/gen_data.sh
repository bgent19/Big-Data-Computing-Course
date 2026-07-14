#!/usr/bin/env bash
# =============================================================================
# SD411 lab02 -- gen_data.sh
#
# Builds ONE thing: data/small/, the 500-tiny-files corpus for E2 and E3.
#
# The "big file" is no longer generated. It is the shared provisioned season
# seed at ${SD411_DATA}/${SEED_CSV} (~700-800K rows, >=100 MB), mounted
# read-only into the namenode and mc containers at /seed. That is the same
# seed every other lab uses, so nobody re-downloads Statcast per lab and
# nobody's numbers are measured against a differently-sized file.
#
# Run from the lab02/ directory BEFORE `docker compose up -d`:
#   ./scripts/gen_data.sh
# =============================================================================
set -euo pipefail

# Values come from the stamped .env (sync_env.sh), never from hardcoded literals.
if [[ ! -f .env ]]; then
  echo "FAIL: ./.env not found."
  echo "      Stamp it from common.env:  ../scripts/sync_env.sh"
  exit 1
fi
set -a; . ./.env; set +a

N_SMALL="${N_SMALL:-500}"
N_ROWS="${N_ROWS:-5000}"          # rows drawn off the seed to slice into files
SEED="${SD411_DATA}/${SEED_CSV}"

# --- seed present and full-season sized? ------------------------------------
if [[ ! -f "$SEED" ]]; then
  echo "FAIL: shared seed not found at $SEED"
  echo "      The VM provisioner places it there once for all labs."
  echo "      Re-run the seeding step, or ask the instructor. Do NOT copy a"
  echo "      Lab 01-sized sample in its place; the floors below exist to catch"
  echo "      exactly that."
  exit 1
fi

seed_mb=$(( $(wc -c < "$SEED") / 1024 / 1024 ))
if (( seed_mb < SEED_MIN_MB )); then
  echo "FAIL: $SEED is ${seed_mb} MB, under the ${SEED_MIN_MB} MB floor."
  echo "      This looks like a lab01-sized single-game sample, not the full"
  echo "      2025 season. Re-seed before measuring anything."
  exit 1
fi
echo "Seed OK: $SEED (${seed_mb} MB). This is your 'big file' for E1-E3."

# --- small/: N_ROWS of the seed sliced into N_SMALL tiny files ---------------
echo "Building data/small/ (${N_SMALL} files from the first ${N_ROWS} seed rows)..."
rm -rf data/small
mkdir -p data/small
# awk (not `tail | head`) reads the seed file directly and `exit`s once it has
# enough rows, so nothing upstream ever gets SIGPIPE'd by an early-closing
# reader -- `tail -n +2 "$SEED" | head -n "$N_ROWS"` looks equivalent but under
# `set -o pipefail` reports exit 141 (tail's SIGPIPE) even on success, which
# aborts the script before the summary below ever prints.
awk -v n="$N_ROWS" 'NR==1{next} NR<=n+1{print} NR>n+1{exit}' "$SEED" \
  | split -n "l/${N_SMALL}" -d -a 4 --additional-suffix=.csv - data/small/part_

n_files=$(ls data/small | wc -l)
total_kb=$(( $(du -sb data/small | cut -f1) / 1024 ))
echo "      data/small/ written: ${n_files} files, ${total_kb} KB total."
echo

# The whole point of E2, previewed:
echo "Note the shape of what you just built:"
echo "  big file  : ${seed_mb} MB in 1 file"
echo "  small dir : ${total_kb} KB in ${n_files} files  (~$(( total_kb / n_files )) KB each)"
echo "The small corpus is a rounding error next to the seed. Hold onto that"
echo "when you write your E2 prediction."
echo
echo "Next: docker compose up -d && ./scripts/verify_lab02.sh"
