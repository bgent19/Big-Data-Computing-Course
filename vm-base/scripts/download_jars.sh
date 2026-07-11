#!/usr/bin/env bash
# =============================================================================
# download_jars.sh
# Fetches the two S3A connector JARs on the HOST so the spark containers never
# need to reach Maven
# Central through the intercepted-TLS Java cacerts. Labs --jars these from the
# mounted /opt/sd411/jars instead of using --packages.
#
# Versions come from common.env (HADOOP_AWS_VERSION / AWS_SDK_BUNDLE_VERSION)
# and MUST move in lockstep with any Spark/Hadoop bump — a mismatch surfaces as
# NoSuchMethodError at runtime, not at load time.
#
# Idempotent: skips a JAR that is already present and non-empty.
#
# Usage:  ./download_jars.sh            (reads ../common.env)
#         JARS_DIR=/tmp/j ./download_jars.sh
# =============================================================================
set -euo pipefail

log()  { printf '[jars] %s\n' "$*"; }
die()  { printf '[jars][FAIL] %s\n' "$*" >&2; exit 1; }

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
[ -f "${HERE}/../common.env" ] && . "${HERE}/../common.env"

JARS_DIR="${JARS_DIR:-${SD411_JARS:-/opt/sd411/jars}}"
HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.3.4}"
AWS_SDK_BUNDLE_VERSION="${AWS_SDK_BUNDLE_VERSION:-1.12.262}"

MAVEN="https://repo1.maven.org/maven2"
HADOOP_AWS_URL="${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
AWS_SDK_URL="${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

install -d "${JARS_DIR}"

fetch() {
  local url="$1" dest="$2"
  if [ -s "${dest}" ]; then
    log "present, skipping: $(basename "${dest}")"
    return 0
  fi
  log "downloading $(basename "${dest}")"
  # --fail so a 404 (wrong version) is an error, not a saved HTML body.
  if command -v curl >/dev/null 2>&1; then
    curl --fail --location --silent --show-error -o "${dest}" "${url}" \
      || die "download failed: ${url} (check version pin and network)"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "${dest}" "${url}" \
      || die "download failed: ${url} (check version pin and network)"
  else
    die "neither curl nor wget available"
  fi
  [ -s "${dest}" ] || die "downloaded file is empty: ${dest}"
}

fetch "${HADOOP_AWS_URL}" "${JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
fetch "${AWS_SDK_URL}"    "${JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

log "S3A JARs ready in ${JARS_DIR}:"
ls -lh "${JARS_DIR}"/*.jar
exit 0
