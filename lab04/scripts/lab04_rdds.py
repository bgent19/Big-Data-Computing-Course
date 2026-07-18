#!/usr/bin/env python3
"""
SD411 lab04 -- lab04_rdds.py (STUDENT SCAFFOLD, Parts 3 and 4)

Parts 1 and 2 happen in the pyspark shell; this script is for the
measured comparison (Part 3) and the distribution analysis (Part 4).

Run it on the cluster:

  docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
    /opt/lab04/scripts/lab04_rdds.py

While it runs, keep http://localhost:4040 open. The wall-clock numbers
come from this script; the shuffle-write numbers come from the UI. You
need BOTH on your worksheet, and you need to be able to explain why
they disagree in magnitude. AI cannot replace measurement.

REQUIRED EDIT: set ALPHA_CODE to your alpha code before running. The
results header embeds it; a missing or shared alpha code is an
integrity conversation, not a style points deduction.
"""

import statistics
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# REQUIRED STUDENT EDIT
ALPHA_CODE = "CHANGE_ME"   # e.g. "265432"
# ---------------------------------------------------------------------------

CORPUS_PATH = "s3a://sd411/corpus/plays"
TRIALS = 3   # report the median; do not change without documenting why

spark = (
    SparkSession.builder.appName(f"lab04-{ALPHA_CODE}")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "sd411admin")
    .config("spark.hadoop.fs.s3a.secret.key", "sd411password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
sc = spark.sparkContext

if ALPHA_CODE == "CHANGE_ME":
    raise SystemExit("Set ALPHA_CODE before running (see header).")


# --------------------------- measurement harness ---------------------------

@contextmanager
def timer(label, bucket):
    """Wall-clock a block; append seconds to bucket."""
    t0 = time.perf_counter()
    yield
    dt = time.perf_counter() - t0
    bucket.append(dt)
    print(f"    {label}: {dt:.2f}s")


def median_of(fn, label):
    """Run fn() TRIALS times, return (median_seconds, last_result)."""
    times, result = [], None
    for i in range(TRIALS):
        with timer(f"{label} trial {i + 1}", times):
            result = fn()
    med = statistics.median(times)
    print(f"  {label}: median {med:.2f}s over {TRIALS} trials")
    return med, result


def tokenize(line):
    """One tokenizer for the whole lab. Declare it; defend it.

    Lowercase, split on whitespace, strip leading/trailing punctuation.
    If you change this, say so in your worksheet: different tokenizers
    give different counts, and your numbers must be reproducible.
    """
    out = []
    for tok in line.lower().split():
        tok = tok.strip(".,;:!?\"'()")
        if tok:
            out.append(tok)
    return out


# ------------------------------- Part 3 ------------------------------------
# Word count two ways over the same corpus. Identical answers, different
# shuffles. Your job: measure the difference, then explain the mechanism.

print("=" * 70)
print(f"SD411 lab04 results -- alpha {ALPHA_CODE}")
print("=" * 70)

lines = sc.textFile(CORPUS_PATH).coalesce(8)  # pin to gen_corpus.py's 8 output files
pairs = lines.flatMap(tokenize).map(lambda w: (w, 1))

print("\nPart 3a -- reduceByKey")


def count_reduce():
    # TODO(3a): produce (word, count) using reduceByKey, then return
    # the number of distinct words via .count().
    # counts = pairs.________________________
    # return counts.count()
    raise NotImplementedError("Part 3a")


t_reduce, n_words_reduce = median_of(count_reduce, "reduceByKey")

print("\nPart 3b -- groupByKey")


def count_group():
    # TODO(3b): produce the SAME (word, count) result using groupByKey
    # followed by a map that sums (or measures the length of) each
    # group, then return .count().
    # counts = pairs.________________________
    # return counts.count()
    raise NotImplementedError("Part 3b")


t_group, n_words_group = median_of(count_group, "groupByKey")

assert n_words_reduce == n_words_group, (
    "The two pipelines disagree on distinct-word count. Same input, same "
    "logic, same answer -- find the bug before measuring anything."
)

print(f"\ndistinct words: {n_words_reduce}")
print(f"reduceByKey median: {t_reduce:.2f}s | groupByKey median: {t_group:.2f}s")
print("Now go to http://localhost:4040 -> Stages and record, for ONE trial")
print("of each: Shuffle Write bytes (map stage) and Shuffle Read bytes")
print("(reduce stage). Worksheet table P3, then mechanism question M3.")

# ------------------------------- Part 4 ------------------------------------
# The shape of the distribution. No collect()-everything allowed.

print("\nPart 4 -- top of the distribution")

# TODO(4a): rebuild counts with reduceByKey (cheap now, you wrote it above)
# and use takeOrdered to fetch the 20 most frequent words.
# top20 = counts.________________________
# for word, n in top20:
#     print(f"  {n:>9,}  {word}")

# Worksheet question M4 asks: why takeOrdered(20, ...) instead of
# counts.collect() followed by a Python sort? Answer in terms of WHERE
# the work happens and what has to fit in the driver.

spark.stop()
