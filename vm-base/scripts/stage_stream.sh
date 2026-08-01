#!/usr/bin/env bash
# =============================================================================
# stage_stream.sh
# Produces the Module 3 replay file at ${STREAM_DIR}/${STREAM_FILE} once, on
# the golden VM, pre-term, with network. Students never download anything and
# never run this; lab09/replay.sh only reads what this leaves behind.
#
# Order of attempts:
#   1. gharchive  — stage ${SD411_DATA}/gharchive/${GH_ARCHIVE_FILE} (fetched
#                   from data.gharchive.org if not already present), then run
#                   make_stream.py --source gharchive over it.
#   2. statcast   — make_stream.py --source statcast over ${SD411_DATA}/${SEED_CSV}.
#                   Needs no new provisioning at all. The lab's event vocabulary
#                   changes and nothing else does (see lab09/data/README.md).
#
# Idempotent: a replay file already at or above ${STREAM_MIN_LINES} is left
# alone. FORCE=1 rebuilds it.
#
# The printed type-key distribution is the number that belongs in
# lab09/data/README.md — paste it there, replacing the reference table.
#
# Usage:  ./stage_stream.sh                   (reads ../common.env)
#         FORCE=1 ./stage_stream.sh
#         SOURCE=statcast ./stage_stream.sh   (skip the gharchive attempt)
# =============================================================================
set -euo pipefail

log()  { printf '[stream] %s\n' "$*"; }
warn() { printf '[stream][WARN] %s\n' "$*" >&2; }
die()  { printf '[stream][FAIL] %s\n' "$*" >&2; exit 1; }

HERE="$(cd "$(dirname "$0")" && pwd)"
VM_BASE="$(cd "${HERE}/.." && pwd)"
# shellcheck disable=SC1091
[ -f "${VM_BASE}/common.env" ] && . "${VM_BASE}/common.env"

SD411_DATA="${SD411_DATA:-/opt/sd411/data}"
SD411_REPO="${SD411_REPO:-/opt/sd411/repo}"
SEED_CSV="${SEED_CSV:-statcast_2025.csv}"
STREAM_DIR="${STREAM_DIR:-${SD411_DATA}/stream}"
STREAM_FILE="${STREAM_FILE:-gh_events.tsv}"
STREAM_EVENTS="${STREAM_EVENTS:-200000}"
STREAM_MIN_LINES="${STREAM_MIN_LINES:-60000}"
GH_ARCHIVE_FILE="${GH_ARCHIVE_FILE:-}"
GH_ARCHIVE_HOUR="${GH_ARCHIVE_HOUR:-}"

OUT="${STREAM_DIR}/${STREAM_FILE}"
GH_DIR="${SD411_DATA}/gharchive"
GH_PATH="${GH_DIR}/${GH_ARCHIVE_FILE:-none}"
GH_URL="https://data.gharchive.org/${GH_ARCHIVE_FILE:-none}"

# make_stream.py ships with lab09. Accept both the flat layout and the
# lab09/scripts/ layout the compose file mounts, plus a copy staged next to the
# seed scripts, so this keeps working whichever the repo settles on.
MAKE_STREAM=""
for cand in "${HERE}/make_stream.py" \
            "${SD411_REPO}/lab09/scripts/make_stream.py" \
            "${SD411_REPO}/lab09/make_stream.py" \
            "${VM_BASE}/../lab09/scripts/make_stream.py" \
            "${VM_BASE}/../lab09/make_stream.py"; do
  [ -f "${cand}" ] && { MAKE_STREAM="${cand}"; break; }
done
[ -n "${MAKE_STREAM}" ] || die "make_stream.py not found. Place the lab repo in ${SD411_REPO} first."
log "using ${MAKE_STREAM}"

install -d "${STREAM_DIR}"

# ---- already staged? -------------------------------------------------------
if [ -z "${FORCE:-}" ] && [ -s "${OUT}" ]; then
  cur_lines="$(wc -l < "${OUT}" | tr -d ' ')"
  if [ "${cur_lines}" -ge "${STREAM_MIN_LINES}" ]; then
    log "replay file already present with ${cur_lines} lines (floor ${STREAM_MIN_LINES}); skipping"
    exit 0
  fi
  warn "replay file present but only ${cur_lines} lines (< ${STREAM_MIN_LINES}); rebuilding"
fi

build() {
  # $1 = source name, $2 = input path
  log "building replay file from $1: $2"
  python3 "${MAKE_STREAM}" --source "$1" --input "$2" \
    --out "${OUT}" --events "${STREAM_EVENTS}"
}

check_floor() {
  [ -s "${OUT}" ] || return 1
  local lines
  lines="$(wc -l < "${OUT}" | tr -d ' ')"
  [ "${lines}" -ge "${STREAM_MIN_LINES}" ] || {
    warn "only ${lines} lines written (floor ${STREAM_MIN_LINES})"
    return 1
  }
  log "replay file ready: ${OUT} (${lines} lines)"
  publish_sample
  return 0
}

# make_stream.py drops sample_events.json next to the replay file. Part 0 tells
# students to open lab09/data/sample_events.json, so put a copy where the lab
# text says it is. LAB09_DIR is derived from wherever make_stream.py was found.
publish_sample() {
  local sample lab09_dir
  sample="${STREAM_DIR}/sample_events.json"
  [ -s "${sample}" ] || return 0
  lab09_dir="$(cd "$(dirname "${MAKE_STREAM}")" && pwd)"
  case "${lab09_dir}" in
    */lab09/scripts) lab09_dir="$(dirname "${lab09_dir}")" ;;
    */lab09)         ;;
    *)               lab09_dir="${SD411_REPO}/lab09" ;;
  esac
  [ -d "${lab09_dir}" ] || return 0
  install -d "${lab09_dir}/data"
  cp -f "${sample}" "${lab09_dir}/data/sample_events.json"
  log "sample events copied to ${lab09_dir}/data/sample_events.json"
}

# ---- attempt 1: GitHub Archive hour ---------------------------------------
if [ "${SOURCE:-gharchive}" = "gharchive" ] && [ -n "${GH_ARCHIVE_FILE}" ]; then
  install -d "${GH_DIR}"
  if [ ! -s "${GH_PATH}" ]; then
    log "fetching ${GH_URL}"
    # Host system CA bundle handles the USNA proxy; no -k / --insecure.
    set +e
    if command -v curl >/dev/null 2>&1; then
      curl --fail --location --silent --show-error -o "${GH_PATH}" "${GH_URL}"
    else
      wget -q -O "${GH_PATH}" "${GH_URL}"
    fi
    rc=$?
    set -e
    [ "${rc}" -eq 0 ] || { warn "could not fetch hour ${GH_ARCHIVE_HOUR} (exit ${rc})"; rm -f "${GH_PATH}"; }
  else
    log "gharchive hour already staged: ${GH_PATH}"
  fi

  if [ -s "${GH_PATH}" ]; then
    set +e
    build gharchive "${GH_PATH}"
    rc=$?
    set -e
    if [ "${rc}" -eq 0 ] && check_floor; then exit 0; fi
    warn "gharchive build did not produce a usable replay file"
  fi
fi

# ---- attempt 2: Statcast seed fallback ------------------------------------
SEED_PATH="${SD411_DATA}/${SEED_CSV}"
if [ -s "${SEED_PATH}" ]; then
  warn "falling back to --source statcast over ${SEED_PATH}"
  build statcast "${SEED_PATH}"
  check_floor && exit 0
  die "statcast fallback produced fewer than ${STREAM_MIN_LINES} lines"
fi

die "no usable source. GitHub Archive hour ${GH_ARCHIVE_HOUR} could not be staged and
${SEED_PATH} does not exist. Run scripts/seed_statcast.sh first, or stage
${GH_PATH} by hand from a networked machine."
