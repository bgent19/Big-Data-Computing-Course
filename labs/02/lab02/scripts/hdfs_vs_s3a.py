"""
SD411 Lab 2 -- stretch goal: read the SAME file through both storage layers.

Spark does not care where the bytes live. The URI scheme selects the
filesystem connector: hdfs:// goes through the bundled HDFS client,
s3a:// goes through hadoop-aws + the AWS SDK (pulled via --packages).

Prerequisites (all from the README stretch section):
  1. Fetch the S3A JARs on the host (once):  ./scripts/download_jars.sh
  2. Stack up with the spark profile:  docker compose --profile spark up -d
  3. big.csv loaded into BOTH stores:
       HDFS:   hdfs dfs -put /data/big.csv /lab02/big.csv      (inside namenode)
       MinIO:  mc cp /data/big.csv local/lab02/big.csv          (inside mc)

Submit from the host. The scripts/ dir is mounted into the master at
/opt/lab02, and the S3A JARs are mounted at /opt/spark/extra-jars, so:

  docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
    /opt/lab02/hdfs_vs_s3a.py

We use --jars (host-downloaded JARs) rather than --packages because USNA TLS
interception breaks Spark's Maven resolution from inside the container. The
apache/spark image needs the full spark-submit path; binaries are not on the
default docker-exec PATH.
"""

import time
from pyspark.sql import SparkSession

HDFS_PATH = "hdfs://namenode:8020/lab02/big.csv"
S3A_PATH = "s3a://lab02/big.csv"

spark = (
    SparkSession.builder.appName("sd411-lab02-hdfs-vs-s3a")
    # s3a -> MinIO wiring, same values as Lab 0's hello_statcast.py
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)


def timed_count(label: str, path: str) -> None:
    """Full-scan count with wall-clock timing. Count forces a complete read."""
    t0 = time.time()
    n = spark.read.csv(path, header=True).count()
    dt = time.time() - t0
    print(f"{label:>8}  rows={n:,}  wall={dt:.1f}s  ({path})")


print("-" * 70)
print("Reading the same bytes through two different storage layers.")
print("Run each twice; the second run shows you what OS page cache does.")
print("-" * 70)
timed_count("HDFS", HDFS_PATH)
timed_count("S3A", S3A_PATH)
timed_count("HDFS(2)", HDFS_PATH)
timed_count("S3A(2)", S3A_PATH)
print("-" * 70)
print("Record all four numbers in WORKSHEET.md (Stretch section) and answer:")
print("which difference is the storage layer, and which is the cache?")

spark.stop()
