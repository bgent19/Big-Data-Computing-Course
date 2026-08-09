#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 10 - provision step 1 of 2: fetch raw Wikipedia Pageviews dumps
#
# RUN ONCE, ON THE GOLDEN VM, BEFORE THE TERM. Not a student step.
# Students never touch the network in this lab.
#
# Downloads 168 consecutive hourly dump files (one week) from
# dumps.wikimedia.org into a staging directory. Files run 50-75 MB gzipped, so
# budget about 11 GB of download. Measured on a campus link: roughly 50 s per
# file serially, so BUDGET 2-3 HOURS, not the hour this script used to claim.
# The upstream host rate limits, which caps what parallelism can buy you.
# The script is restartable: files already present and non-empty are skipped,
# so a killed run resumes where it stopped.
#
# TLS: the USNA proxy intercepts HTTPS, so curl is pointed at the system CA
# bundle (${SYSTEM_CA_BUNDLE} from common.env), which is the one trust store
# that already contains the USNA root. Do NOT add -k. If this script fails
# with a certificate error, the trust store is the bug, not the server.
#
# Usage:
#   ./fetch_pageviews.sh                      # uses PV_WEEK_START from .env
#   PV_WEEK_START=2026-02-02 ./fetch_pageviews.sh
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Prefer the stamped lab .env, fall back to the repo-level common.env.
for CANDIDATE in "${HERE}/../.env" "${HERE}/../../common.env"; do
  if [[ -f "${CANDIDATE}" ]]; then
    # shellcheck disable=SC1090
    set -a; source "${CANDIDATE}"; set +a
    echo "[env] sourced ${CANDIDATE}"
    break
  fi
done

SD411_DATA="${SD411_DATA:-/opt/sd411/data}"
SYSTEM_CA_BUNDLE="${SYSTEM_CA_BUNDLE:-/etc/ssl/certs/ca-certificates.crt}"
PV_WEEK_START="${PV_WEEK_START:-}"
PV_HOURS="${PV_HOURS:-168}"
RAW_DIR="${PV_RAW_DIR:-${SD411_DATA}/pageviews_raw}"

if [[ -z "${PV_WEEK_START}" ]]; then
  echo "FATAL: PV_WEEK_START is not set (expected YYYY-MM-DD, UTC)."
  echo "       Set it in common.env. See COMMON_ENV_ADDENDUM.md."
  exit 2
fi

if ! date -u -d "${PV_WEEK_START}" >/dev/null 2>&1; then
  echo "FATAL: PV_WEEK_START='${PV_WEEK_START}' is not a date this system can parse."
  exit 2
fi

mkdir -p "${RAW_DIR}"

echo "[plan] week start (UTC) : ${PV_WEEK_START} 00:00"
echo "[plan] hours to fetch   : ${PV_HOURS}"
echo "[plan] staging directory: ${RAW_DIR}"
echo "[plan] CA bundle        : ${SYSTEM_CA_BUNDLE}"
echo

FETCHED=0
SKIPPED=0
FAILED=0

for (( H=0; H<PV_HOURS; H++ )); do
  STAMP=$(date -u -d "${PV_WEEK_START} 00:00 UTC + ${H} hours" +"%Y%m%d-%H0000")
  YYYY=${STAMP:0:4}
  MM=${STAMP:4:2}
  FNAME="pageviews-${STAMP}.gz"
  URL="https://dumps.wikimedia.org/other/pageviews/${YYYY}/${YYYY}-${MM}/${FNAME}"
  DEST="${RAW_DIR}/${FNAME}"

  if [[ -s "${DEST}" ]] && gzip -t "${DEST}" 2>/dev/null; then
    SKIPPED=$((SKIPPED+1))
    continue
  fi

  printf "[%3d/%3d] %s ... " "$((H+1))" "${PV_HOURS}" "${FNAME}"
  # --retry-all-errors is what makes 429 retryable. dumps.wikimedia.org rate
  # limits, and without it a 429 is a hard failure that silently leaves a hole
  # in the feed which only the PV_MIN_SHARDS gate catches, much later.
  # Do NOT parallelise this loop past about 3 concurrent connections; 10 earns
  # 429s on most of the requests.
  if curl --fail --silent --show-error --location \
          --cacert "${SYSTEM_CA_BUNDLE}" \
          --retry 5 --retry-delay 5 --retry-all-errors --connect-timeout 30 \
          -o "${DEST}.part" "${URL}"; then
    mv "${DEST}.part" "${DEST}"
    printf "ok (%s)\n" "$(du -h "${DEST}" | cut -f1)"
    FETCHED=$((FETCHED+1))
  else
    rm -f "${DEST}.part"
    printf "FAILED\n"
    FAILED=$((FAILED+1))
  fi
done

echo
echo "[done] fetched=${FETCHED} skipped=${SKIPPED} failed=${FAILED}"
if (( FAILED > 0 )); then
  echo "[warn] re-run this script; it resumes. Persistent failures on the same"
  echo "       hour usually mean that hour is genuinely absent upstream."
  exit 1
fi
echo "[next] run provision/prepare_pageviews.py to build the lab shards."
