#!/usr/bin/env bash
# SD411 Lab 0 — Download S3A connector JARs for offline use.
#
# Why this exists:
#   Spark inside the container runs --packages via Ivy, which uses Java's
#   truststore ($JAVA_HOME/lib/security/cacerts). That truststore does
#   NOT include the USNA interception CA, so Maven Central downloads fail
#   with "PKIX path building failed".
#
# What this does:
#   Downloads the two JARs from Maven Central using the HOST's curl, which
#   trusts the institutional CA via the system bundle. The JARs land in
#   ./jars/, which the v1.3 compose mounts read-only into the Spark
#   containers as /opt/spark/extra-jars. At lab time we use --jars instead
#   of --packages, sidestepping Ivy entirely.
#
# Usage:
#   ./scripts/download_jars.sh
#
# Versions are pinned to what Spark 3.5.3 / Hadoop 3.3.4 was built against.
# Mismatched versions cause NoSuchMethodError at *runtime*, not at load —
# do not change these without checking the Hadoop release notes.

set -euo pipefail

# Resolve repo root from script location, regardless of cwd
JARS_DIR="$(cd "$(dirname "$0")/.." && pwd)/jars"
mkdir -p "$JARS_DIR"

REPO="https://repo1.maven.org/maven2"

download() {
  local name="$1"
  local url="$2"
  local dest="$JARS_DIR/$name"
  if [ -f "$dest" ]; then
    echo "[skip] $name (already present, $(du -h "$dest" | cut -f1))"
    return 0
  fi
  echo "[get ] $name"
  echo "       <- $url"
  curl -fL --connect-timeout 15 -o "$dest" "$url"
  echo "       -> $dest ($(du -h "$dest" | cut -f1))"
}

download "hadoop-aws-3.3.4.jar" \
  "$REPO/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

download "aws-java-sdk-bundle-1.12.262.jar" \
  "$REPO/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

echo
echo "JARs in $JARS_DIR:"
ls -la "$JARS_DIR"/*.jar 2>/dev/null || echo "  (none — downloads failed)"

echo
echo "If curl above failed with SSL errors, your host's CA bundle does not"
echo "trust Maven Central's chain either. Try:"
echo "  curl --cacert /etc/ssl/certs/ca-certificates.crt -fL -O <url>"
echo "or download the JARs on a personal machine and scp them into ./jars/."
