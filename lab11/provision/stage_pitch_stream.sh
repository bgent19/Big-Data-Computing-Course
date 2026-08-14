#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 11 - provision: stage the event-time pitch stream
#
# RUN ONCE, ON THE GOLDEN VM, BEFORE THE TERM. Not a student step. No network
# is touched: the stream is a pure function of the Statcast seed CSV that
# seed_statcast.sh already staged, so this runs offline and after a snapshot
# restore just as well as on the first build.
#
# What it does:
#   1. resolves SD411_DATA and SEED_CSV from the stamped lab .env (falling back
#      to ../../vm-base/common.env, then to hardcoded defaults)
#   2. creates ${SD411_DATA}/lab11
#   3. runs make_pitch_stream.py with the analytic defaults published in
#      data/README.md and COMMON_ENV_ADDENDUM.md
#   4. leaves the file student-readable and the report on stdout
#
# The generator's printed report is what belongs in data/README.md; the numbers
# there (20000 records, 1600 late tail, ~3600 s span, ~60 windows) are analytic,
# so a run that prints anything else means an argument drifted. Check before you
# accept the image.
#
# Idempotent: an existing stream file at or above ${LAB11_STREAM_MIN_LINES} is
# left alone. FORCE=1 rebuilds it. Same contract as vm-base/scripts/
# stage_stream.sh, which stages the lab09 replay file.
#
# ${SD411_DATA} is root-owned on the golden image, so this normally runs under
# sudo. It chowns the output back to the login user afterwards.
#
# Usage:
#   sudo ./provision/stage_pitch_stream.sh
#   sudo FORCE=1 ./provision/stage_pitch_stream.sh
# =============================================================================
set -euo pipefail

log()  { printf '[pitch-stream] %s\n' "$*"; }
warn() { printf '[pitch-stream][WARN] %s\n' "$*" >&2; }
die()  { printf '[pitch-stream][FAIL] %s\n' "$*" >&2; exit 1; }

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Values are read with a grep helper, never by sourcing: KAFKA_HEAP_OPTS in the
# same file contains a space, and sourcing it runs the tail as a command
# (lab09 finding, same reason produce.sh and verify_lab11.sh use a helper).
ENVF=""
for cand in "${HERE}/../.env" "${HERE}/../../vm-base/common.env"; do
  [ -f "${cand}" ] && { ENVF="${cand}"; break; }
done
getenv() {
  [ -n "${ENVF}" ] || { printf '%s' "${2:-}"; return; }
  local v
  v="$(grep -E "^$1=" "${ENVF}" 2>/dev/null | head -n1 | cut -d= -f2- || true)"
  [ -z "${v}" ] && v="${2:-}"
  printf '%s' "${v}"
}
[ -n "${ENVF}" ] && log "reading defaults from ${ENVF}" \
                 || warn "no .env or common.env found; using built-in defaults"

SD411_DATA="$(getenv SD411_DATA /opt/sd411/data)"
SEED_CSV="$(getenv SEED_CSV statcast_2025.csv)"
SEED_PATH="${SD411_DATA}/${SEED_CSV}"
OUT_DIR="${SD411_DATA}/lab11"
OUT="${OUT_DIR}/pitch_stream.jsonl"

# Analytic defaults. These fix the late tail at 0.08 * 20000 = 1600 records
# landing exactly 90 s behind the frontier, which is the invariant Part 1 grades
# against. Keep them in lockstep with LAB11_WATERMARK_SECONDS (generous, 180 s,
# > 90) and the Part 1 tight override (30 s, < 90). If you change the tail here,
# change both watermarks in common.env and the table in data/README.md.
ROWS="${LAB11_STREAM_ROWS:-20000}"
SEED="${LAB11_STREAM_SEED:-411}"
SPACING_MS="${LAB11_STREAM_SPACING_MS:-180}"
WINDOW_SECONDS="$(getenv LAB11_WINDOW_SECONDS 60)"
LATE_FRACTION="${LAB11_STREAM_LATE_FRACTION:-0.08}"
LATE_LATENESS="${LAB11_STREAM_LATE_LATENESS:-90}"
MIN_LINES="${LAB11_STREAM_MIN_LINES:-${ROWS}}"

GEN="${HERE}/make_pitch_stream.py"
[ -f "${GEN}" ] || die "make_pitch_stream.py not found next to this script (${GEN})"

# ---- already staged? -------------------------------------------------------
if [ -z "${FORCE:-}" ] && [ -s "${OUT}" ]; then
  cur_lines="$(wc -l < "${OUT}" | tr -d ' ')"
  if [ "${cur_lines}" -ge "${MIN_LINES}" ]; then
    log "stream already staged: ${OUT} (${cur_lines} lines, floor ${MIN_LINES}); skipping"
    log "re-run with FORCE=1 to rebuild"
    exit 0
  fi
  warn "stream present but only ${cur_lines} lines (< ${MIN_LINES}); rebuilding"
fi

# ---- preconditions ---------------------------------------------------------
[ -s "${SEED_PATH}" ] || die "seed CSV not found: ${SEED_PATH}
Run vm-base/scripts/seed_statcast.sh first. lab11 reads the same single-season
seed every Module 1 and 2 lab reads; it downloads nothing of its own."
command -v python3 >/dev/null 2>&1 || die "python3 not on PATH"

install -d "${OUT_DIR}" 2>/dev/null || die "cannot create ${OUT_DIR}
${SD411_DATA} is root-owned on the golden image. Re-run this script with sudo."
[ -w "${OUT_DIR}" ] || die "${OUT_DIR} is not writable by $(id -un). Re-run with sudo."

# ---- build -----------------------------------------------------------------
# Standard library only, so no venv and no pip: this is the same discipline that
# keeps the generator runnable inside the pinned Spark image (lab09).
log "building ${OUT} from ${SEED_PATH}"
python3 "${GEN}" \
  --seed-csv "${SEED_PATH}" \
  --out "${OUT}" \
  --rows "${ROWS}" \
  --seed "${SEED}" \
  --spacing-ms "${SPACING_MS}" \
  --window-seconds "${WINDOW_SECONDS}" \
  --late-fraction "${LATE_FRACTION}" \
  --late-lateness-seconds "${LATE_LATENESS}"

lines="$(wc -l < "${OUT}" | tr -d ' ')"
[ "${lines}" -ge "${MIN_LINES}" ] || die "only ${lines} lines written (floor ${MIN_LINES})"

# ---- ownership -------------------------------------------------------------
# produce.sh reads this file as the student, from the host, outside any
# container. Under sudo the write lands root-owned; hand it back.
LOGIN_USER="${SUDO_USER:-$(id -un)}"
chown "${LOGIN_USER}" "${OUT}" 2>/dev/null || true
chmod 0644 "${OUT}" 2>/dev/null || true

log "staged ${OUT} (${lines} lines, owner ${LOGIN_USER})"
log "next: paste the report above over the reference block in lab11/data/README.md,"
log "      then check the pre-term box in instructor/INSTRUCTOR_KEY.md."
