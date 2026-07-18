#!/usr/bin/env python3
# =============================================================================
# SD411 Lab 3 -- lab03_benchmark.py (STUDENT SCAFFOLD)
#
# Build-and-measure: CSV vs Parquet on a full season of Statcast.
#
# Run from the lab03/ directory:
#
#   docker compose exec spark-master spark-submit \
#     --master spark://spark-master:7077 \
#     --jars $(echo /opt/spark/extra-jars/*.jar | tr ' ' ',') \
#     /opt/lab03/scripts/lab03_benchmark.py
#
# Note: we pass --jars from the centralized JAR mount, NOT --packages. The
# USNA proxy breaks Maven resolution, so the S3A JARs are fetched host-side
# once (into /opt/sd411/jars) and mounted into the containers at
# /opt/spark/extra-jars -- use that container path, not the host path.
# See download_jars.sh / the course setup notes.
#
# The harness below (Timer, path_size_mb, run_trials, results table) is
# provided so your measurements are comparable to everyone else's. Your job
# is the parts marked TODO. Do not modify the harness; if you believe the
# harness is measuring the wrong thing, say so in your report and defend it.
# =============================================================================

import time
import statistics
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# REQUIRED EDIT: your alphanumeric code. Output paths are derived from this,
# which makes your artifacts yours. An unedited alpha is an automatic
# resubmission, same rule as Lab 0.
# -----------------------------------------------------------------------------
ALPHA = "m000000"  # <-- EDIT ME

RAW_CSV = "s3a://sd411/raw/statcast_2025.csv"
OUT     = f"s3a://sd411/lab03/{ALPHA}"

# =============================================================================
# Harness -- do not modify below this line (until the TODOs start)
# =============================================================================

spark = (
    SparkSession.builder
    .appName(f"SD411-Lab3-{ALPHA}")
    .master("spark://spark-master:7077")   # explicit: guards against a silent local[*] run
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "sd411admin")
    .config("spark.hadoop.fs.s3a.secret.key", "sd411password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

RESULTS = []  # rows of (section, label, value, unit)

def record(section, label, value, unit):
    RESULTS.append((section, label, value, unit))
    print(f"[{section}] {label}: {value} {unit}")

@contextmanager
def timer(section, label):
    """Wall-clock a block and record it. Wrap exactly the work you intend
    to measure and nothing else."""
    t0 = time.perf_counter()
    yield
    record(section, label, round(time.perf_counter() - t0, 2), "s")

def run_trials(section, label, fn, n=3):
    """Run fn() n times, record each trial and the median. Trial 1 is your
    cold(ish) run; trials 2..n tell you what caching is doing to you."""
    times = []
    for i in range(1, n + 1):
        t0 = time.perf_counter()
        fn()
        dt = round(time.perf_counter() - t0, 2)
        times.append(dt)
        record(section, f"{label} trial {i}", dt, "s")
    record(section, f"{label} MEDIAN", round(statistics.median(times), 2), "s")

def path_size_mb(path):
    """Total size of every object under an s3a:// path, in MB, via the
    Hadoop FileSystem API. Counts data bytes as stored (post-compression)."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(conf)
    summary = fs.getContentSummary(p)
    return round(summary.getLength() / (1024 * 1024), 1)

# =============================================================================
# Part A -- write the season in six formats, measure write time and size
# =============================================================================
print("=" * 70)
print("PART A: formats")
print("=" * 70)

with timer("A", "read raw CSV + count"):
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)   # see README gotcha #2 before you defend this
          .csv(RAW_CSV))
    n_rows = df.count()
record("A", "row count", n_rows, "rows")
record("A", "column count", len(df.columns), "cols")
record("A", "raw CSV size", path_size_mb(RAW_CSV), "MB")

# Target formats. F1 is the seeded raw CSV itself (already measured above).
FORMATS = {
    # key                (writer-fn description)
    "csv_gzip":          "CSV, gzip codec",
    "parquet_none":      "Parquet, compression=none",
    "parquet_snappy":    "Parquet, compression=snappy (the default)",
    "parquet_gzip":      "Parquet, compression=gzip",
    "parquet_zstd":      "Parquet, compression=zstd",
}

# TODO(A1): for each key in FORMATS, write `df` to f"{OUT}/{key}" in the
# right format with the right codec, inside `with timer("A", f"write {key}")`.
# Use .coalesce(4) on every write so file counts are identical across
# formats and Part D's small-files comparison stays clean.
# Hints: df.write.mode("overwrite"); CSV codec option is "compression";
# Parquet codec is .option("compression", <codec>).
#
# TODO(A2): after each write, record its size:
#   record("A", f"size {key}", path_size_mb(f"{OUT}/{key}"), "MB")
#
# TODO(A3): compute and record compression ratio vs raw CSV for each format:
#   record("A", f"ratio {key}", <raw_mb / format_mb rounded to 2>, "x")

# =============================================================================
# Part B -- scan benchmarks: the same three queries against CSV and Parquet
# =============================================================================
print("=" * 70)
print("PART B: scans")
print("=" * 70)

# Readers. Note the explicit schema reuse: reading CSV with the schema we
# already inferred keeps Part B from re-paying the inference cost and keeps
# the comparison about scan work, not schema work.
schema = df.schema

def read_csv_raw():
    return spark.read.option("header", True).schema(schema).csv(RAW_CSV)

def read_csv_gzip():
    return spark.read.option("header", True).schema(schema).csv(f"{OUT}/csv_gzip")

def read_parquet_snappy():
    return spark.read.parquet(f"{OUT}/parquet_snappy")

# The three queries. Each returns when the action completes.
def q1_full_count(reader):
    reader().count()

def q2_one_column(reader):
    reader().agg(F.avg("release_speed")).collect()

def q3_filtered(reader):
    reader().filter(F.col("release_speed") > 100).count()

# TODO(B1): using run_trials(...), benchmark q1, q2, and q3 against
# read_csv_raw and read_parquet_snappy. That is 6 benchmark cells, 3 trials
# each. Label them clearly, e.g. run_trials("B", "q2 parquet_snappy",
# lambda: q2_one_column(read_parquet_snappy)).
#
# TODO(B2): benchmark q1 against read_csv_gzip. Before you run it, write
# down your prediction: faster or slower than raw CSV, and why? Check the
# Spark application UI (http://localhost:4040) while it runs and note the
# number of tasks in the scan stage. The answer is in the Week 3 Day 2
# notes under splittability.
#
# TODO(B3): pushdown evidence. Print the physical plan for the q3 filter
# on Parquet and find the PushedFilters line:
#   read_parquet_snappy().filter(F.col("release_speed") > 100).explain()
# Paste the plan excerpt into your report and explain, in terms of row-group
# statistics from the Week 3 Day 1 notes, why Parquet can skip work here
# that CSV cannot.

# =============================================================================
# Part D -- the small-files problem, measured (Part C lives in
# inspect_parquet.py and runs on your host, not in Spark)
# =============================================================================
print("=" * 70)
print("PART D: small files")
print("=" * 70)

# TODO(D1): write parquet_snappy two more ways:
#   f"{OUT}/parquet_4files"    using .coalesce(4)      (you already have this one as parquet_snappy)
#   f"{OUT}/parquet_400files"  using .repartition(400)
# Time both writes. Record both sizes. Yes, the bytes are nearly the same.
#
# TODO(D2): benchmark q1_full_count against each layout (3 trials each).
# Record the task counts you observe in the :4040 UI for one trial of each.
#
# TODO(D3): in your report, connect the result to BOTH halves of the
# small-files argument from the Week 3 Day 2 notes: the per-file open/list
# overhead you just measured, and the metadata-server memory argument you
# could not measure on this stack. Why couldn't you measure the second one
# here? (One sentence. Think about what MinIO is and is not.)

# =============================================================================
# Results table -- prints everything recorded, in order. Paste this block
# verbatim into your report appendix.
# =============================================================================
print("=" * 70)
print(f"RESULTS TABLE -- {ALPHA}")
print("=" * 70)
for section, label, value, unit in RESULTS:
    print(f"{section:>2} | {label:<38} | {value:>12} {unit}")

spark.stop()
