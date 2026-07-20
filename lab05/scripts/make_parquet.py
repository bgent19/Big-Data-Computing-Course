#!/usr/bin/env python3
# =============================================================================
# SD411 lab05 -- Part 0 prep: ensure a Parquet copy of the season exists.
#
# Reads  s3a://<bucket>/raw/statcast_2025.csv
# Writes s3a://<bucket>/parquet/statcast_2025/   (snappy, 4 files)
#
# If you completed lab03/lab04 and kept your MinIO volume, this copy already
# exists and the script exits immediately. Run inside the master container:
#
#   docker compose exec spark-master /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
#     /opt/lab05/scripts/make_parquet.py
#
# The JARs live at /opt/spark/extra-jars, inherited from the VM's shared
# /opt/sd411/jars through the base compose. There is no per-lab JAR fetch and
# no --packages: the institutional proxy breaks in-container Maven resolution.
#
# Expected runtime on the lab VM: 2-4 minutes for a ~350 MB season CSV.
# =============================================================================
import os

from pyspark.sql import SparkSession

# S3A configuration comes from the environment (common.env -> .env -> compose).
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
BUCKET   = os.environ.get("S3_BUCKET", "sd411")
ACCESS   = os.environ.get("MINIO_ROOT_USER", "sd411admin")
SECRET   = os.environ.get("MINIO_ROOT_PASSWORD", "sd411password")

RAW = f"s3a://{BUCKET}/raw/statcast_2025.csv"
PQ  = f"s3a://{BUCKET}/parquet/statcast_2025"

spark = (
    SparkSession.builder.appName("lab05-make-parquet")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# Skip if the Parquet copy already exists and is non-empty.
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
Path = sc._jvm.org.apache.hadoop.fs.Path
fs = Path(PQ).getFileSystem(hadoop_conf)
if fs.exists(Path(PQ)) and fs.listStatus(Path(PQ)):
    print(f"Parquet copy already present at {PQ}, nothing to do.")
    spark.stop()
    raise SystemExit(0)

print(f"Reading {RAW} with schema inference (one extra pass, prep only)...")
df = spark.read.csv(RAW, header=True, inferSchema=True)
print(f"  rows={df.count():,}  cols={len(df.columns)}")

print(f"Writing {PQ} (snappy, 4 files)...")
df.coalesce(4).write.mode("overwrite").parquet(PQ)

print("Done. Verify with check C10 in scripts/verify_lab05.sh.")
spark.stop()
