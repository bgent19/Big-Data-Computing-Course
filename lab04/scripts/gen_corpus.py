#!/usr/bin/env python3
"""
SD411 lab04 -- gen_corpus.py (INSTRUCTOR-PROVIDED PLUMBING)

Builds the lab04 text corpus from the season Statcast seed already sitting
in MinIO (raw/statcast_2025.csv, staged by the VM provisioner and measured in lab03).

What it does:
  1. Reads the season CSV.
  2. Keeps one copy of each plate-appearance description (the `des` column,
     "Plate appearance description from game day" per the Savant docs).
     Statcast repeats `des` on every pitch row of a plate appearance, so we
     deduplicate on (game_pk, at_bat_number) to get one line per PA.
  3. Stacks REPLICATE copies of the season (default 80) so the shuffle in
     Part 3 has something to chew on, then writes plain text to
     s3a://sd411/corpus/plays/ . REPLICATE=80 is calibrated so that, with
     the Part 3 scaffold pinning the read to 8 partitions (see
     lab04_rdds.py), the groupByKey/reduceByKey Shuffle Write ratio clears
     the course's 10x floor (re-baselined; see instructor/INSTRUCTOR_KEY.md).

NOTE TO STUDENTS: this script uses DataFrames, which we cover NEXT week.
Today it is plumbing. You are not responsible for how it works yet; you are
responsible for what it produces. Run it once and move on:

  docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
    /opt/lab04/scripts/gen_corpus.py

Expected runtime on the lab stack: 2-5 minutes (dominated by the S3A read of
the season CSV; the connector JARs are mounted, not resolved at runtime).
The corpus written at REPLICATE=80 is roughly 900MB-1GB across 8 files.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SEED_PATH = "s3a://sd411/raw/statcast_2025.csv"
CORPUS_PATH = "s3a://sd411/corpus/plays"
REPLICATE = int(os.environ.get("REPLICATE", "80"))  # reasoned default; see README

spark = (
    SparkSession.builder.appName("lab04-gen-corpus")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "sd411admin")
    .config("spark.hadoop.fs.s3a.secret.key", "sd411password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = spark.read.csv(SEED_PATH, header=True)

required = {"des", "game_pk", "at_bat_number"}
missing = required - set(df.columns)
if missing:
    print(f"FATAL: seed is missing columns {sorted(missing)}. "
          f"Re-seed per data/README.md.", file=sys.stderr)
    spark.stop()
    sys.exit(65)

plays = (
    df.select("game_pk", "at_bat_number", "des")
    .where(F.col("des").isNotNull() & (F.trim(F.col("des")) != ""))
    .dropDuplicates(["game_pk", "at_bat_number"])
    .select("des")
)

n_pa = plays.count()
if n_pa < 50_000:
    print(f"WARNING: only {n_pa} plate appearances found. A full-season seed "
          f"should yield ~150K-190K. Check that the staged seed is the full "
          f"season, not the lab01 smoke-test sample.", file=sys.stderr)

stacked = plays
for _ in range(REPLICATE - 1):
    stacked = stacked.union(plays)

# 8 output files: enough parallelism for a 2-core worker without creating
# a small-files mess (remember lab03, Part D).
(
    stacked.repartition(8)
    .write.mode("overwrite")
    .text(CORPUS_PATH)
)

print(f"Corpus written to {CORPUS_PATH}")
print(f"  plate appearances (unique): {n_pa}")
print(f"  lines written (x{REPLICATE}): {n_pa * REPLICATE}")
spark.stop()
