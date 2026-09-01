#!/usr/bin/env bash
# =============================================================================
# provision_ubuntu_vm.sh
# Turns a fresh Ubuntu 24.04 LTS machine into an SD411 lab image. Idempotent:
# safe to re-run.
#
# Assumes an ordinary network with working public TLS: no enterprise root CA is
# injected and no trust-store repair is performed. On a network that intercepts
# TLS (corporate/campus proxy), that CA work is an extra stage you must supply;
# see step 2 of PROVISIONING_STEPS.md for what it has to cover.
#
# Stages (each is skippable for re-runs and logged):
#   1. APT base packages (ca-certificates, python3-venv, build tools, jq, ss)
#   2. Docker Engine + Compose v2 plugin, add the login user to the docker group
#   3. Python env: pip install pybaseball pyarrow pandas into a shared venv
#   4. Lay down /opt/sd411 (repo, data, jars) and copy the lab repo in
#   5. download_jars.sh — S3A + Spark-Kafka connector JARs to /opt/sd411/jars
#   6. pull every pinned image from common.env
#   7. seed_statcast.sh — write the shared season CSV (real, else synthetic)
#   8. stage_stream.sh — GitHub Archive hour + Module 3 Kafka replay file
#   9. sync_env.sh — stamp common.env into every lab directory
#  10. ensure any lab work/ dir is student-owned (scratch ownership safety net)
#
# Usage:
#   sudo ./provision_ubuntu_vm.sh [--repo /path/to/SD411-repo] \
#        [--skip-seed] [--skip-stream]
#
# Run as root (sudo). The target login user is detected from SUDO_USER.
# =============================================================================
set -euo pipefail

log()   { printf '\n=== [provision] %s ===\n' "$*"; }
info()  { printf '[provision] %s\n' "$*"; }
warn()  { printf '[provision][WARN] %s\n' "$*" >&2; }
die()   { printf '[provision][FAIL] %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run with sudo"

HERE="$(cd "$(dirname "$0")" && pwd)"
. "${HERE}/common.env"

REPO_SRC=""
SKIP_SEED=""
SKIP_STREAM=""
while [ $# -gt 0 ]; do
  case "$1" in
    --repo)        REPO_SRC="$2"; shift 2 ;;
    --skip-seed)   SKIP_SEED=1; shift ;;
    --skip-stream) SKIP_STREAM=1; shift ;;
    *) die "unknown arg: $1" ;;
  esac
done

LOGIN_USER="${SUDO_USER:-$(logname 2>/dev/null || echo root)}"
info "target login user: ${LOGIN_USER}"

# ---- 1. APT base -----------------------------------------------------------
log "1/10 base packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
  ca-certificates curl gnupg lsb-release \
  python3 python3-venv python3-pip \
  jq iproute2 unzip
update-ca-certificates >/dev/null || true

# ---- 2. Docker + Compose v2 ------------------------------------------------
log "2/10 Docker Engine + Compose v2"
if ! command -v docker >/dev/null 2>&1; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${VERSION_CODENAME}") stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin
else
  info "docker present, skipping install"
fi
docker --version
docker compose version
systemctl enable --now docker || warn "could not enable docker via systemctl"
if [ "${LOGIN_USER}" != "root" ]; then
  usermod -aG docker "${LOGIN_USER}" || warn "could not add ${LOGIN_USER} to docker group"
  info "added ${LOGIN_USER} to docker group (log out/in for it to take effect)"
fi

# ---- 3. Python env ---------------------------------------------------------
log "3/10 Python data env (shared venv)"
VENV="${SD411_HOME}/venv"
install -d "${SD411_HOME}"
if [ ! -d "${VENV}" ]; then
  python3 -m venv "${VENV}"
fi
"${VENV}/bin/pip" install --upgrade pip
"${VENV}/bin/pip" install pybaseball pyarrow pandas
info "venv ready at ${VENV}"

# ---- 4. /opt/sd411 layout + repo -------------------------------------------
log "4/10 on-VM layout + lab repo"
install -d "${SD411_DATA}" "${SD411_JARS}" "${SD411_REPO}"
if [ -n "${REPO_SRC}" ] && [ -d "${REPO_SRC}" ]; then
  cp -r "${REPO_SRC}/." "${SD411_REPO}/"
  info "copied lab repo from ${REPO_SRC} into ${SD411_REPO}"
else
  warn "no --repo given; place the SD411 lab repo into ${SD411_REPO} before sync_env"
fi
# Carry the Lab 0 synthetic fallback next to the seed scripts so seeding can
# degrade gracefully on a machine where the real pull is blocked.
if [ -f "${SD411_REPO}/lab0/scripts/synthetic_statcast.py" ]; then
  cp -f "${SD411_REPO}/lab0/scripts/synthetic_statcast.py" "${HERE}/scripts/"
  info "synthetic fallback staged for seed_statcast.sh"
fi
chown -R "${LOGIN_USER}:${LOGIN_USER}" "${SD411_HOME}" 2>/dev/null || true

# ---- 5. Connector JARs -----------------------------------------------------
log "5/10 connector JARs (S3A + Spark-Kafka)"
JARS_DIR="${SD411_JARS}" bash "${HERE}/scripts/download_jars.sh"

# ---- 6. Pull pinned images -------------------------------------------------
log "6/10 pull pinned images"
for img in "${SPARK_IMAGE}" "${HADOOP_IMAGE}" "${MINIO_IMAGE}" "${MC_IMAGE}" \
           "${KAFKA_IMAGE}"; do
  info "pulling ${img}"
  docker pull "${img}" || die "pull failed: ${img} (verify the tag in common.env)"
done

# ---- 7. Seed dataset -------------------------------------------------------
if [ -n "${SKIP_SEED}" ]; then
  warn "7/10 seed SKIPPED (--skip-seed); run seed_statcast.sh before term"
else
  log "7/10 seed Statcast season CSV"
  PATH="${VENV}/bin:${PATH}" bash "${HERE}/scripts/seed_statcast.sh" \
    || warn "seed did not complete; run scripts/seed_statcast.sh manually"
fi

# ---- 8. Module 3 stream source ---------------------------------------------
# Stages ${SD411_DATA}/gharchive/${GH_ARCHIVE_FILE} and runs make_stream.py
# once to produce ${STREAM_DIR}/${STREAM_FILE}. Both steps happen here, on the
# instructor image, with network. Students never download anything.
# Falls back to --source statcast over the seed CSV if GitHub Archive is
# unreachable; the lab's event vocabulary changes and nothing else does.
if [ -n "${SKIP_STREAM}" ]; then
  warn "8/10 stream SKIPPED (--skip-stream); run scripts/stage_stream.sh before term"
else
  log "8/10 Module 3 Kafka replay file"
  bash "${HERE}/scripts/stage_stream.sh" \
    || warn "replay file not staged; lab09 verify check 10 will FAIL until you run
            scripts/stage_stream.sh manually. See lab09/data/README.md."
  chown -R "${LOGIN_USER}:${LOGIN_USER}" "${STREAM_DIR}" 2>/dev/null || true
fi

# ---- 9. Sync per-lab .env --------------------------------------------------
log "9/10 stamp common.env into each lab"
bash "${HERE}/scripts/sync_env.sh" "${SD411_REPO}" || warn "sync_env reported no labs yet"

# ---- 10. Scratch-dir ownership safety net ----------------------------------
# Migrated labs use the `spark-work` NAMED VOLUME for container scratch, so no
# host work/ dir exists to go root-owned. For any lab not yet migrated that
# still ships a host work/ bind mount, make sure it exists and the student
# owns it BEFORE first `compose up`, so Docker does not create it as root.
log "10/10 ensure lab scratch dirs are student-owned"
shopt -s nullglob
for wd in "${SD411_REPO}"/lab[0-9]*/work; do
  install -d "${wd}"
  chown -R "${LOGIN_USER}:${LOGIN_USER}" "${wd}" 2>/dev/null || true
  info "student-owns $(dirname "${wd}" | xargs basename)/work"
done
shopt -u nullglob

log "PROVISION COMPLETE"
info "Next: as ${LOGIN_USER} (after re-login for the docker group), bring up a lab"
info "and run its checks, e.g.  cd ${SD411_REPO}/lab03 && docker compose up -d \\"
info "  && ./scripts/verify_lab03.sh"
info "Before exporting the golden image, tear down cleanly with scratch removed:"
info "  bash ${HERE}/scripts/sd411_down_all.sh -v   # -v drops spark-work + minio-data"
exit 0
