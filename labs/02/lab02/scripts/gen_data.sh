#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 2 -- gen_data.sh
# Builds the two test corpora for the measurement experiments:
#   data/big.csv        one large CSV (~TARGET_MB, default 200 MB) made by
#                       repeating the Statcast sample body under one header
#   data/small/         the same sample split into 500 tiny CSV files
#
# Run from the lab02/ directory BEFORE `docker compose up -d`:
#   ./scripts/gen_data.sh
# =============================================================================
set -euo pipefail

TARGET_MB="${TARGET_MB:-200}"
N_SMALL="${N_SMALL:-500}"
SRC="data/statcast_sample.csv"

if [[ ! -f "$SRC" ]]; then
  echo "FAIL: $SRC not found."
  echo "      This is the same seed file used in Lab 0. Copy it here:"
  echo "      cp ../lab01/data/statcast_sample.csv data/"
  exit 1
fi

src_bytes=$(wc -c < "$SRC")
if (( src_bytes < 100000 )); then
  echo "WARN: $SRC is under 100 KB. Generation will still work, but check"
  echo "      that you copied the full sample, not a truncated file."
fi

# --- big.csv: header once, body repeated until >= TARGET_MB -----------------
echo "[1/2] Building data/big.csv (target ${TARGET_MB} MB)..."
head -n 1 "$SRC" > data/big.csv
tail -n +2 "$SRC" > /tmp/lab02_body.csv
body_bytes=$(wc -c < /tmp/lab02_body.csv)
target_bytes=$(( TARGET_MB * 1024 * 1024 ))
repeats=$(( (target_bytes / body_bytes) + 1 ))
for (( i=0; i<repeats; i++ )); do
  cat /tmp/lab02_body.csv >> data/big.csv
done
rm -f /tmp/lab02_body.csv
big_mb=$(( $(wc -c < data/big.csv) / 1024 / 1024 ))
echo "      data/big.csv written: ${big_mb} MB ($repeats copies of the sample body)."

# --- small/: the original sample split into N_SMALL line-chunks --------------
echo "[2/2] Building data/small/ (${N_SMALL} files)..."
rm -rf data/small
mkdir -p data/small
tail -n +2 "$SRC" | split -n "l/${N_SMALL}" -d -a 4 --additional-suffix=.csv - data/small/part_
n_files=$(ls data/small | wc -l)
total_kb=$(( $(du -sb data/small | cut -f1) / 1024 ))
echo "      data/small/ written: ${n_files} files, ${total_kb} KB total."

echo
echo "Done. Sanity check:"
ls -lh data/big.csv
echo "smallest small file: $(ls -S data/small | tail -1)"
echo
echo "Next: docker compose up -d && ./scripts/verify_stack.sh"
