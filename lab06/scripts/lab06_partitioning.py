#!/usr/bin/env python3
"""
SD411 Lab 6 — Partitioning Experiments
Module 2 · Week 6 · R6 · Thu 24 Sep 2026

Student scaffold. The measurement harness is provided so the lab measures
partitioning, not your ability to write a timing loop. The experiments are
yours: every TODO block is graded against the rubric in README.md.

UNFORGEABILITY RULE (same as Labs 1-5): replace ALPHA_CODE below with the
per-section code written on the board at the start of lab. It is baked into
every results line the harness prints. Output containing the placeholder, or
a code from another section, scores zero for that part.

Run (from the lab06/ directory). Jobs run INSIDE the master container so the
centralized JARs and the spark://spark-master:7077 URL resolve:

    SUBMIT="docker compose exec spark-master /opt/spark/bin/spark-submit \\
      --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,\\
/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar"

    $SUBMIT /opt/lab06/scripts/lab06_partitioning.py --part 0
    $SUBMIT /opt/lab06/scripts/lab06_partitioning.py --part A
    $SUBMIT /opt/lab06/scripts/lab06_partitioning.py --part B
    $SUBMIT /opt/lab06/scripts/lab06_partitioning.py --part C

Always --jars, never --packages: USNA TLS interception breaks in-container
Maven resolution (PKIX path building failed). The JARs are provisioned on the
VM at /opt/sd411/jars and mounted for you; you do not download anything.
"""

import argparse
import os
import statistics
import sys
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession, functions as F

ALPHA_CODE = "REPLACE_ME"          # <-- board code, per section, per day

# Paths. Bucket and seed name come from common.env via the environment so this
# script never hardcodes what the stack owns.
BUCKET = os.environ.get("S3_BUCKET", "sd411")
SEED_CSV = os.environ.get("SEED_CSV", "statcast_2025.csv")

RAW = f"s3a://{BUCKET}/raw/{SEED_CSV}"

# CANONICAL FACT PATH — Part 0 writes it, Lab 7 (joins) reads it.
# Do not rename this. Lab 7's minio-init exits 64 if this object is absent,
# which is the dependency chain between the two labs made mechanical.
FACT_PATH = f"s3a://{BUCKET}/fact/pitches"

WORK = f"s3a://{BUCKET}/work/lab06"   # your experiment output


# ---------------------------------------------------------------------------
# Provided: session builder. Sizing comes from the stack, not from here.
# ---------------------------------------------------------------------------

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName(f"{app_name}-{ALPHA_CODE}")
        .config("spark.hadoop.fs.s3a.endpoint",
                os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ.get("MINIO_ROOT_USER", "sd411admin"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ.get("MINIO_ROOT_PASSWORD", "sd411password"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.adaptive.enabled", "false")   # AQE OFF — README gotcha 1
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Provided: measurement harness (do not modify between trials of one part)
# ---------------------------------------------------------------------------

@contextmanager
def timer(label: str, sink: list):
    t0 = time.perf_counter()
    yield
    dt = time.perf_counter() - t0
    sink.append(dt)
    print(f"    trial: {label} = {dt:.2f} s")


def median_of_trials(label: str, fn, trials: int = 3) -> float:
    """Run fn() `trials` times, report the median. The first run includes JVM
    and connector warm-up, which is why trials matter. Trial spread beyond
    about 25 percent of the median is worth a sentence in your writeup, not
    silent deletion."""
    times = []
    for i in range(trials):
        with timer(f"{label} #{i + 1}", times):
            fn()
    med = statistics.median(times)
    print(f"RESULT[{ALPHA_CODE}] {label}: median={med:.2f}s "
          f"trials={['%.2f' % t for t in times]}")
    return med


def load_fact(spark: SparkSession):
    """The Part 0 output. Every experiment reads this, so all three parts
    measure partitioning against identical input."""
    return spark.read.parquet(FACT_PATH)


# The fixed workload for Parts A and C. Provided so the only thing changing is
# the partitioning. Do not "improve" the query; that changes the experiment.
def workload(df) -> None:
    (df.groupBy("pitch_type")
       .agg(F.avg("release_speed").alias("avg_velo"),
            F.count("*").alias("n"))
       .collect())


# ===========================================================================
# PART 0 — Build the fact table                          (in lab, ~15 min)
# ===========================================================================
# The stack seeds ONE object: the 2025 regular season as CSV. Everything else
# in this lab, and the join lab next week, reads the Parquet fact you write
# here. This is the first time in the course you are the producer rather than
# the consumer, and the path you write is a contract with Lab 7.
#
# Run this once. If you re-run it, everything downstream changes underneath
# your measurements, so finish A, B, and C before touching it again.

def part_0(spark: SparkSession) -> None:
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(RAW))

    # Derived columns the experiments partition on. game_date is the natural
    # date grain; game_month is the coarse grain.
    fact = (df
            .withColumn("game_date", F.to_date("game_date"))
            .withColumn("game_month", F.month("game_date")))

    n = fact.count()
    print(f"RESULT[{ALPHA_CODE}] part0 rows={n}")
    if n < int(os.environ.get("SEED_MIN_ROWS", "600000")):
        sys.exit(f"Fact table only {n} rows — seed looks truncated, see data/README.md")

    # Written flat and modestly sized. Lab 7 reads exactly this path.
    (fact.repartition(8)
         .write.mode("overwrite")
         .parquet(FACT_PATH))
    print(f"RESULT[{ALPHA_CODE}] part0 wrote {FACT_PATH}")


# ===========================================================================
# PART A — Partition count sweep                         (in lab, ~30 min)
# ===========================================================================
# Question: on THIS hardware (2 cores), what partition count makes the fixed
# workload fastest, and what does the whole curve look like?
#
# CHECKPOINT: your predicted curve is on the worksheet and initialed BEFORE
# you run this part. Post-hoc predictions score zero.

def part_a(spark: SparkSession) -> None:
    df = load_fact(spark)
    counts = [1, 2, 4, 8, 32, 200, 1000]

    for n in counts:
        # TODO(A1): create a DataFrame with exactly n partitions from df.
        #           One method call. Confirm with rdd.getNumPartitions().
        # TODO(A2): force the repartition to actually happen BEFORE you time
        #           the workload, so you are not timing a shuffle plus a
        #           workload together. Think about what laziness (Lab 4) does
        #           to the naive version of this experiment. cache() plus a
        #           cheap action is one route; document whichever you use.
        # TODO(A3): time the workload with median_of_trials, label f"A n={n}".
        raise NotImplementedError("Part A: remove this line once A1-A3 are done")

    # TODO(A4): while a sweep runs, open the driver UI (port 4040) and record
    #           on the worksheet: the scan-stage task count at n=8 and at
    #           n=1000, and the post-shuffle-stage task count at any n. That
    #           last number does not move. Why not? That belongs in the writeup.


# ===========================================================================
# PART B — Partition column: the on-disk layout experiment  (in lab, ~40 min)
# ===========================================================================
# Now the variable is not how many partitions, but where the boundaries fall
# on disk. Same rows, three layouts, two queries raced across all three.
#
# CHECKPOINT: predictions for both query winners and for the by_date directory
# count, initialed BEFORE any race runs.

def part_b(spark: SparkSession) -> None:
    df = load_fact(spark)

    # Layout 1 — provided: flat, no partition column, 8 files.
    (df.repartition(8)
       .write.mode("overwrite")
       .parquet(f"{WORK}/flat"))

    # TODO(B1): Layout 2 — write df partitioned by game_month to
    #           {WORK}/by_month. Hint: .write.partitionBy(...). Then look at
    #           the directory structure with the mc shell and sketch it on the
    #           worksheet.

    # TODO(B2): Layout 3 — write df partitioned by game_date to
    #           {WORK}/by_date. Before running it, predict on the worksheet
    #           how many directories this creates. Afterward, count them and
    #           note the write time relative to B1. You have met this failure
    #           mode before, in Module 1. Name it in your writeup.

    # The race queries. Q1 is month-scoped; Q2 cuts across the whole season.
    def q1(path):
        d = spark.read.parquet(path)
        d.filter(F.col("game_month") == 7).agg(F.count("*")).collect()

    def q2(path):
        d = spark.read.parquet(path)
        (d.filter(F.col("pitcher") == PITCHER_ID)
          .agg(F.avg("release_spin_rate")).collect())

    # TODO(B3): pick PITCHER_ID: any pitcher with 2000+ pitches in the season
    #           (a quick groupBy finds one). Record the id and the pitch count
    #           on the worksheet so your numbers are reproducible.

    # TODO(B4): race Q1 and Q2 on all three layouts with median_of_trials.
    #           Six medians, labels like "B q1 flat", "B q2 by_month".

    # TODO(B5): for Q1 on the by_month layout, capture the physical plan
    #           (.explain() on the filtered frame) and find the line telling
    #           you how much data Spark decided NOT to read. Paste it into the
    #           worksheet. We have not named this mechanism in lecture yet.
    #           Monday we will. Describe what you observed in your own words.
    raise NotImplementedError("Part B: remove this line once B1-B5 are done")


# ===========================================================================
# PART C — Skew: observe it, name it, do not fix it       (in lab, ~20 min)
# ===========================================================================
# Roughly a third of all pitches are four-seam fastballs. Partition by
# pitch_type and no partition count in the world balances that.

def part_c(spark: SparkSession) -> None:
    df = load_fact(spark)

    # TODO(C1): print the pitch_type frequency table (count, descending).
    #           Record the top value and its share of all rows on the
    #           worksheet.

    # TODO(C2): repartition df into 8 partitions BY THE COLUMN pitch_type
    #           (not by a number alone), then get per-partition row counts:
    #           df.rdd.glom().map(len).collect() is the blunt instrument.
    #           Record all 8 sizes on the worksheet.

    # TODO(C3): time workload() on the key-partitioned frame with
    #           median_of_trials, label "C skewed". Then open the driver UI,
    #           find the stage, and record max task duration against median
    #           task duration. One number should embarrass the other.
    raise NotImplementedError("Part C: remove this line once C1-C3 are done")

    # There is no C4 asking you to fix this. Detection today; salting and AQE
    # in Week 8. Your writeup's last paragraph: given what you measured, what
    # would a fix have to accomplish?


# ---------------------------------------------------------------------------

PITCHER_ID = None  # TODO(B3) lives here


def main() -> None:
    if ALPHA_CODE == "REPLACE_ME":
        sys.exit("Set ALPHA_CODE to the board code before running. See header.")
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", choices=["0", "A", "B", "C"], required=True)
    args = ap.parse_args()
    spark = build_spark(f"lab06-part{args.part}")
    try:
        {"0": part_0, "A": part_a, "B": part_b, "C": part_c}[args.part](spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
