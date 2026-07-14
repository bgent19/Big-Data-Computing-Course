"""
SD411 lab02 -- stretch goal: read the SAME file through both storage layers.

Spark does not care where the bytes live. The URI scheme picks the filesystem
connector: hdfs:// goes through the bundled HDFS client, s3a:// goes through
hadoop-aws + the AWS SDK.

Those two S3A JARs are NOT downloaded by Spark. USNA's TLS interception breaks
Ivy/Maven resolution from inside the container (PKIX path building failed), so
the JARs are fetched host-side once by the VM provisioner into ${SD411_JARS}
and mounted by the base compose at /opt/spark/extra-jars. We pass --jars, never
--packages.

Prerequisites (all covered by the README stretch section):
  1. docker compose --profile spark up -d
  2. big file already in BOTH stores from Parts 1-2:
       HDFS  : hdfs dfs -put /seed/statcast_2025.csv /lab02/big.csv
       MinIO : mc cp /seed/statcast_2025.csv local/sd411/lab02/big.csv

Submit (the scripts/ dir is mounted at /opt/lab02/scripts; the apache image
does not put spark-submit on the docker-exec PATH, so use the full path):

  docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
    /opt/lab02/scripts/hdfs_vs_s3a.py
"""

import os
import time

from pyspark.sql import SparkSession

# Everything configurable resolves from the environment the base compose
# injects out of common.env. No credentials are hardcoded in this file.
S3_BUCKET = os.environ.get("S3_BUCKET", "sd411")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "sd411admin")
SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "sd411password")

HDFS_PATH = "hdfs://namenode:8020/lab02/big.csv"
S3A_PATH = f"s3a://{S3_BUCKET}/lab02/big.csv"

spark = (
    SparkSession.builder.appName("sd411-lab02-hdfs-vs-s3a")
    # Without .master(...) spark-submit silently runs local[*] against the
    # driver and the worker container does nothing. Lab 01 learned this the
    # hard way. The Spark master UI is the proof: your app must appear under
    # Completed Applications.
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)


def timed_count(label: str, path: str) -> None:
    """Full-scan count with wall-clock timing. count() forces a complete read."""
    t0 = time.time()
    n = spark.read.csv(path, header=True).count()
    dt = time.time() - t0
    print(f"{label:>8}  rows={n:,}  wall={dt:.1f}s  ({path})")


print("-" * 72)
print("Same bytes, two storage layers, one unchanged line of application code.")
print("Each path runs twice. The second run is where the page cache shows up.")
print("-" * 72)
timed_count("HDFS", HDFS_PATH)
timed_count("S3A", S3A_PATH)
timed_count("HDFS(2)", HDFS_PATH)
timed_count("S3A(2)", S3A_PATH)
print("-" * 72)
print("Record all four in WORKSHEET.md (Stretch), then answer the question that")
print("matters: which gap is the storage layer, and which gap is the cache?")

spark.stop()
