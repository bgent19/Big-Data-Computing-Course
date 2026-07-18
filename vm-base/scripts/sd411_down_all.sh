#!/usr/bin/env bash
# =============================================================================
# sd411_down_all.sh
# Tears down any running SD411 lab stack so the next lab can claim the shared
# host ports (8080/7077/9000/9001/4040/4041/9870). All labs reuse these, so
# only one stack runs at a time; a stale stack is triage item #1.
#
# Usage:  ./sd411_down_all.sh           (down, keep volumes)
#         ./sd411_down_all.sh -v        (down AND remove volumes — wipes MinIO)
# =============================================================================
set -euo pipefail
log() { printf '[down] %s\n' "$*"; }

WIPE=""
[ "${1:-}" = "-v" ] && WIPE="-v"

# Compose project names default to the lab directory name; bring down every
# project whose containers are tagged with the sd411 image set.
mapfile -t projects < <(docker ps -a --format '{{.Label "com.docker.compose.project"}}' \
  | grep -i 'lab' | sort -u || true)

if [ "${#projects[@]}" -eq 0 ]; then
  log "no running sd411 lab stacks found"
fi

for p in "${projects[@]}"; do
  [ -n "${p}" ] || continue
  log "stopping compose project: ${p} ${WIPE}"
  docker compose -p "${p}" down ${WIPE} || true
done

# Belt-and-braces: report anything still holding the shared ports.
for port in 8080 7077 9000 9001 4040 4041 9870; do
  if command -v ss >/dev/null 2>&1 && ss -ltn "( sport = :${port} )" 2>/dev/null | grep -q ":${port}"; then
    log "WARNING: port ${port} still in use after teardown"
  fi
done
log "done."
