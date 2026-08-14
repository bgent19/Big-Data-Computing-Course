#!/usr/bin/env python3
"""
oracle.py  -  the ground truth the streaming sink must reproduce, exactly.

Reads the SAME Kafka topic the streaming query reads, but as a bounded batch, and
computes the windowed count directly. Then it keeps only the CLOSED windows: the
ones whose window_end is at or below (max event time - watermark). Append mode
never emits any other window during a bounded replay, so this is precisely the
set the streaming sink should contain, no more and no less.

This is why the exactly-once proof is a clean equality and not a fuzzy "about the
same": event-time windowing makes the answer a function of the data, so the batch
oracle, the clean streaming run, and the crash-and-recover run all have to agree
row for row. If they do not, exactly-once was violated and the probe will say so.

You run this once in Part 0 with no arguments, which uses the generous watermark
from the environment and writes the canonical oracle. The number it prints is
yours to record; the packet does not hardcode it, because it is baselined on
your VM's stream, not asserted.

Part 1 runs it a second time with a TIGHT allowance:

    oracle.py --watermark 30 --suffix oracle_wm30

which closes windows against the same frontier minus 30 s instead of minus 180 s
and writes to its own prefix, leaving the canonical oracle untouched. That second
oracle is what makes the Part 1 drop count measurable IN RECORDS: it is the
answer a tight-watermark run would have produced if nothing were ever dropped,
so (tight oracle - tight sink) is exactly the abandoned late tail.
"""
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType


def env(name, default=None):
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


TOPIC       = env("LAB11_TOPIC", "pitches")
BROKER      = env("LAB11_BROKER", "broker:9092")
WINDOW_S    = int(env("LAB11_WINDOW_SECONDS", "60"))
WATERMARK_S = int(env("LAB11_WATERMARK_SECONDS", "180"))
S3_BUCKET   = env("S3_BUCKET", "sd411")
S3_ENDPOINT = env("S3_ENDPOINT", "http://minio:9000")
S3_KEY      = env("MINIO_ROOT_USER", "sd411admin")
S3_SECRET   = env("MINIO_ROOT_PASSWORD", "sd411password")

VALUE_SCHEMA = StructType([
    StructField("rid", LongType()),
    StructField("pitch_type", StringType()),
    StructField("event_time_ms", LongType()),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watermark", type=int, default=WATERMARK_S,
                    help="allowance in seconds used to decide which windows are "
                         "closed (default: LAB11_WATERMARK_SECONDS)")
    ap.add_argument("--suffix", default="oracle",
                    help="prefix under s3a://<bucket>/lab11/ to write (default: oracle)")
    args = ap.parse_args()
    watermark_s = args.watermark

    spark = (
        SparkSession.builder.appName("lab11_oracle")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Match the streaming query so the oracle is not 200 tiny Parquet files.
        .config("spark.sql.shuffle.partitions", env("LAB11_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    rows = (
        raw.select(F.from_json(F.col("value").cast("string"), VALUE_SCHEMA).alias("j"))
        .select("j.*")
        .withColumn("event_time", (F.col("event_time_ms") / 1000).cast("timestamp"))
    )

    t_max_ms = rows.agg(F.max("event_time_ms").alias("m")).first()["m"]
    frontier = t_max_ms / 1000.0
    cutoff = frontier - watermark_s            # closed iff window_end <= cutoff (seconds)

    windowed = (
        rows.groupBy(F.window("event_time", "%d seconds" % WINDOW_S), "pitch_type")
        .count()
        .select(
            F.col("window.start").alias("w_start"),
            F.col("window.end").alias("w_end"),
            "pitch_type", "count",
        )
    )
    closed = windowed.filter(F.col("w_end").cast("double") <= F.lit(cutoff))

    out = "s3a://%s/lab11/%s" % (S3_BUCKET, args.suffix)
    closed.write.mode("overwrite").parquet(out)

    total = closed.agg(F.sum("count").alias("s")).first()["s"] or 0
    n_rows = closed.count()
    print("oracle written: %s" % out)
    print("  watermark allowance .... %d s" % watermark_s)
    print("  frontier T_max ......... %d ms" % t_max_ms)
    print("  closed-window cutoff ... window_end <= %.3f s" % cutoff)
    print("  closed (window,type) rows %d" % n_rows)
    print("  closed-window grand total %d  (records inside closed windows)" % total)
    spark.stop()


if __name__ == "__main__":
    main()
