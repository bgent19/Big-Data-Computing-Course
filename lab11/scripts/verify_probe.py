#!/usr/bin/env python3
"""
verify_probe.py  -  compare a sink against the oracle, row for row.

Reads the oracle at s3a://<bucket>/lab11/oracle and a sink prefix (default the
idempotent aggregate) and reports three things the exactly-once claim depends on:
  1. do the two aggregates match on every (window, pitch_type) key,
  2. is the sink grand total equal to the oracle (nothing lost, nothing doubled),
  3. how many keys differ, and in which direction, if any.

Exit code 0 only when they match exactly. verify_lab11.sh calls this as its
terminal check; you also run it yourself after every crash-and-recover to prove
the run. It is deliberately dumb: it does not know which run produced the sink,
only whether the sink equals the truth.

Usage:
    spark-submit ... verify_probe.py [sink_prefix] [oracle_prefix]
where sink_prefix defaults to agg_idempotent and oracle_prefix to oracle. For
Part 3 pass agg_naive. For Part 1 compare the tight-watermark run against the
tight-watermark oracle:

    verify_probe.py agg_drops oracle_wm30
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def env(name, default=None):
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


S3_BUCKET   = env("S3_BUCKET", "sd411")
S3_ENDPOINT = env("S3_ENDPOINT", "http://minio:9000")
S3_KEY      = env("MINIO_ROOT_USER", "sd411admin")
S3_SECRET   = env("MINIO_ROOT_PASSWORD", "sd411password")


def load(spark, path):
    # recursiveFileLookup is REQUIRED, not a nicety. The naive sink writes each
    # batch into its own randomly named subdirectory, and those names are not
    # key=value, so Spark's default partition discovery never descends into
    # them and the read fails with UNABLE_TO_INFER_SCHEMA on a directory that
    # is visibly full of Parquet. It also makes the idempotent sink's
    # batch_id=N directories read as plain data instead of a partition column,
    # which is what we want here: the probe compares aggregates, not layout.
    try:
        return spark.read.option("recursiveFileLookup", "true").parquet(path)
    except Exception as exc:
        # Print it. A swallowed exception here reads as "the sink is missing"
        # when the sink is fine and the reader was wrong.
        print("  (read failed for %s: %s: %s)" % (path, type(exc).__name__, str(exc).split("\n")[0]))
        return None


def norm(df):
    # Both the oracle and the sink carry w_start, w_end, pitch_type, count.
    return (
        df.groupBy("w_start", "w_end", "pitch_type")
        .agg(F.sum("count").alias("count"))
    )


def main():
    sink_prefix = sys.argv[1] if len(sys.argv) > 1 else "agg_idempotent"
    oracle_prefix = sys.argv[2] if len(sys.argv) > 2 else "oracle"
    spark = (
        SparkSession.builder.appName("lab11_probe")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    oracle = load(spark, "s3a://%s/lab11/%s" % (S3_BUCKET, oracle_prefix))
    sink = load(spark, "s3a://%s/lab11/%s" % (S3_BUCKET, sink_prefix))

    if oracle is None:
        print("PROBE FAIL: no oracle found at prefix '%s'. Run oracle.py (Part 0) first."
              % oracle_prefix)
        spark.stop(); sys.exit(2)
    if sink is None:
        print("PROBE FAIL: no sink found at prefix '%s'." % sink_prefix)
        spark.stop(); sys.exit(2)

    o = norm(oracle)
    s = norm(sink)
    o_total = o.agg(F.sum("count").alias("t")).first()["t"] or 0
    s_total = s.agg(F.sum("count").alias("t")).first()["t"] or 0

    # keys where the counts disagree (full outer join, null-safe)
    joined = o.alias("o").join(
        s.alias("s"),
        on=["w_start", "w_end", "pitch_type"], how="fullouter",
    )
    diff = joined.where(
        F.coalesce(F.col("o.count"), F.lit(-1)) != F.coalesce(F.col("s.count"), F.lit(-1))
    )
    n_diff = diff.count()

    print("PROBE sink prefix ...... %s" % sink_prefix)
    print("  oracle prefix ........ %s" % oracle_prefix)
    print("  oracle grand total ... %d" % o_total)
    print("  sink   grand total ... %d" % s_total)
    print("  keys that disagree ... %d" % n_diff)
    if s_total > o_total:
        print("  direction ............ sink OVER oracle by %d (duplication)" % (s_total - o_total))
    elif s_total < o_total:
        print("  direction ............ sink UNDER oracle by %d (loss)" % (o_total - s_total))

    if n_diff == 0 and s_total == o_total:
        print("PROBE PASS: sink equals oracle. Nothing lost, nothing doubled.")
        spark.stop(); sys.exit(0)
    else:
        print("PROBE FAIL: sink does not equal oracle.")
        spark.stop(); sys.exit(1)


if __name__ == "__main__":
    main()
