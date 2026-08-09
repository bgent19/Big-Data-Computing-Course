#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 10 - checkpoint reset
#
#   scripts/reset_lab10_checkpoints.sh            # every lab10 checkpoint
#   scripts/reset_lab10_checkpoints.sh part1_t1h  # just one query's checkpoint
#
# A checkpoint is a progress record, not a cache. It remembers which shards a
# named query has already consumed. Re-running a query against a checkpoint
# that already covers all 168 shards produces zero batches and an empty result
# table, which looks like a broken query and is not one.
#
# Deleting a checkpoint is the streaming equivalent of "start over". It is
# correct here because the feed is a fixed directory that never changes. It
# would be a serious operational mistake against a live Kafka topic, where the
# checkpoint is the only record of where the consumer got to.
# =============================================================================
set -uo pipefail

# Runs from the lab root, not scripts/: .env and docker-compose.yml live there.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${HERE}/.."

[[ -f ./.env ]] || { echo "FATAL: .env not stamped. Run scripts/sync_env.sh."; exit 2; }
set -a; source ./.env; set +a

CHK_PREFIX="${CHK_PREFIX:-chk/lab10}"
TARGET="${1:-}"
DC="docker compose"

MINIO_CID="$(${DC} ps -q minio 2>/dev/null)"
if [[ -z "${MINIO_CID}" ]]; then
  echo "FATAL: minio is not running. Start the stack first: ${DC} up -d"
  exit 1
fi
NET="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "${MINIO_CID}")"

if [[ -n "${TARGET}" ]]; then
  PATH_TO_CLEAR="local/${S3_BUCKET}/${CHK_PREFIX}/${TARGET}"
else
  PATH_TO_CLEAR="local/${S3_BUCKET}/${CHK_PREFIX}"
fi

echo "Removing ${PATH_TO_CLEAR}"
docker run --rm --network "${NET}" \
  -e "MC_HOST_local=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
  "${MC_IMAGE}" rm --recursive --force --quiet "${PATH_TO_CLEAR}" \
  || echo "(nothing there, which is fine)"

echo "Done. The next run of that query starts from shard 0."
