#!/usr/bin/env bash
# =============================================================================
# download_jars.sh
# Fetches the connector JARs on the HOST (where the system CA bundle trusts the
# USNA proxy) so the spark containers never need to reach Maven Central through
# the intercepted-TLS Java cacerts. Labs --jars these from the mounted
# /opt/sd411/jars instead of using --packages.
#
# Two sets:
#   S3A          (labs 3-8)        HADOOP_AWS_VERSION / AWS_SDK_BUNDLE_VERSION
#   Spark-Kafka  (lab09 stretch;   SPARK_KAFKA_VERSION / KAFKA_CLIENTS_VERSION /
#                 lab10/11 hard)   COMMONS_POOL2_VERSION
#
# Versions come from common.env and MUST move in lockstep with any Spark/Hadoop
# bump — a mismatch surfaces as NoSuchMethodError at runtime, not at load time.
# Spark 3.5.3 is built against kafka-clients 3.4.1; those four move together.
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
SPARK_KAFKA_VERSION="${SPARK_KAFKA_VERSION:-3.5.3}"
KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION:-3.4.1}"
COMMONS_POOL2_VERSION="${COMMONS_POOL2_VERSION:-2.11.1}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-2.12}"

MAVEN="https://repo1.maven.org/maven2"
HADOOP_AWS_URL="${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
AWS_SDK_URL="${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

SQL_KAFKA_JAR="spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar"
TOKEN_PROVIDER_JAR="spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar"
KAFKA_CLIENTS_JAR="kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
COMMONS_POOL2_JAR="commons-pool2-${COMMONS_POOL2_VERSION}.jar"

SQL_KAFKA_URL="${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_KAFKA_VERSION}/${SQL_KAFKA_JAR}"
TOKEN_PROVIDER_URL="${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_KAFKA_VERSION}/${TOKEN_PROVIDER_JAR}"
KAFKA_CLIENTS_URL="${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/${KAFKA_CLIENTS_JAR}"
COMMONS_POOL2_URL="${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/${COMMONS_POOL2_JAR}"

install -d "${JARS_DIR}"

fetch() {
  local url="$1" dest="$2"
  if [ -s "${dest}" ]; then
    log "present, skipping: $(basename "${dest}")"
    return 0
  fi
  log "downloading $(basename "${dest}")"
  # --fail so a 404 (wrong version) is an error, not a saved HTML body.
  # The host system CA bundle handles the USNA proxy; no -k / --insecure.
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

log "S3A connector (labs 3-8)"
fetch "${HADOOP_AWS_URL}" "${JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
fetch "${AWS_SDK_URL}"    "${JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# Spark-Kafka connector. lab09 only WARNs when these are absent (Parts 1-5
# touch no Spark), but lab10 and lab11 cannot run without them, so this is a
# hard failure here — the pre-term window with network is the place to find out.
log "Spark-Kafka connector (lab09 stretch; required by lab10/lab11)"
fetch "${SQL_KAFKA_URL}"       "${JARS_DIR}/${SQL_KAFKA_JAR}"
fetch "${TOKEN_PROVIDER_URL}"  "${JARS_DIR}/${TOKEN_PROVIDER_JAR}"
fetch "${KAFKA_CLIENTS_URL}"   "${JARS_DIR}/${KAFKA_CLIENTS_JAR}"
fetch "${COMMONS_POOL2_URL}"   "${JARS_DIR}/${COMMONS_POOL2_JAR}"

log "JARs ready in ${JARS_DIR}:"
ls -lh "${JARS_DIR}"/*.jar
exit 0
