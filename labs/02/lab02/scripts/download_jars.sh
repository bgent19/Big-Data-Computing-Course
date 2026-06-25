#!/usr/bin/env bash
# =============================================================================
# SD411 Lab 2 -- download_jars.sh  (STRETCH GOAL ONLY)
# Fetches the two S3A connector JARs on the HOST, where the system CA bundle
# trusts the USNA interception cert. Inside the Spark containers, Java's
# cacerts does not, so Spark's own `--packages` download from repo1.maven.org
# fails with "PKIX path building failed". Same workaround as Lab 0 v1.4.
#
# Run once before the stretch goal (from any directory):
#   ./scripts/download_jars.sh          # from 02.HDFS/
#   bash scripts/download_jars.sh       # also fine
#
# The JARs land in jars/ relative to this script's parent directory (the lab
# root), which is git-ignored and mounted read-only into the Spark containers
# at /opt/spark/extra-jars. The path is resolved from the script location so
# the JARs end up in the right place regardless of where you invoke the script.
#
# VERSION PINS: these MUST match the Hadoop version the Spark image was built
# against (Hadoop 3.3.4 for Spark 3.5.x). A mismatch surfaces as
# NoSuchMethodError at runtime, not at load time. If the Spark image version
# ever moves, both pins move together with it.
# =============================================================================
set -euo pipefail

HADOOP_AWS_VER="3.3.4"
AWS_SDK_VER="1.12.262"
BASE="https://repo1.maven.org/maven2"

# Resolve the lab root (parent of this scripts/ dir) so JARs always land in
# the right place for docker-compose, regardless of invocation directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(dirname "$SCRIPT_DIR")"
JAR_DIR="$LAB_DIR/jars"

mkdir -p "$JAR_DIR"

fetch() {
  local url="$1" out="$JAR_DIR/$2"
  if [[ -f "$out" ]]; then
    echo "  already present: $out"
    return
  fi
  echo "  downloading: $2"
  if ! curl -fSL --retry 3 -o "$out" "$url"; then
    echo "FAIL: could not download $2"
    echo "      The host needs outbound HTTPS to repo1.maven.org with the"
    echo "      USNA CA in its system trust store. If the host itself cannot"
    echo "      reach Maven Central, ask the instructor for the JAR bundle."
    rm -f "$out"
    exit 1
  fi
}

echo "Fetching S3A connector JARs (host-side)..."
fetch "$BASE/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VER/hadoop-aws-$HADOOP_AWS_VER.jar" \
      "hadoop-aws-$HADOOP_AWS_VER.jar"
fetch "$BASE/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VER/aws-java-sdk-bundle-$AWS_SDK_VER.jar" \
      "aws-java-sdk-bundle-$AWS_SDK_VER.jar"

echo
echo "Done. JARs in $JAR_DIR:"
ls -lh "$JAR_DIR/"
echo
echo "These mount into the Spark containers at /opt/spark/extra-jars."
echo "The stretch spark-submit uses: --jars /opt/spark/extra-jars/hadoop-aws-$HADOOP_AWS_VER.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-$AWS_SDK_VER.jar"
