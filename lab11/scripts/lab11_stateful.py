#!/usr/bin/env python3
"""
lab11_stateful.py  -  SD411 lab11 student scaffold

A stateful streaming aggregation you will run, crash on purpose, restart, and
then prove exactly-once from the output and the checkpoint. The plumbing (Spark
session, the Kafka read, the crash instrument, the CLI) is written for you. The
three lines that carry the lesson are marked TODO(you): the watermark, the
window group-by, and the idempotent sink path. Fill those in, and make every
prediction with predict() BEFORE you run the action it is about.

Run it like:
    spark-submit --jars <spark-kafka + s3a jars> lab11_stateful.py --action baseline
    spark-submit ...                              lab11_stateful.py --action crash
    spark-submit ...                              lab11_stateful.py --action resume
See README.md for the exact submit line and the per-part invocations.

Every action prints a per-batch progress table when it finishes. That table, not
the driver UI, is the instrument you are graded on: the Structured Streaming tab
dies with the driver a few seconds after the query stops, and the LAST progress
record alone is a no-data batch whose counters are all zero.
"""
import argparse
import datetime as dt
import os
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType


# ---------------------------------------------------------------------------
# predict-first harness. Every prediction is timestamped into
# lab11_predictions.log the moment you make it, so a prediction written AFTER
# the measurement is visible in your submission as such and scores zero. Call it
# before you run the action it concerns.
# ---------------------------------------------------------------------------
def predict(tag, text):
    line = "%s  [%s]  %s" % (dt.datetime.now().isoformat(timespec="seconds"), tag, text)
    with open("lab11_predictions.log", "a") as fh:
        fh.write(line + "\n")
    print("PREDICT " + line)


# ---- config from environment (stamped from common.env) ---------------------
def env(name, default=None):
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


TOPIC        = env("LAB11_TOPIC", "pitches")
BROKER       = env("LAB11_BROKER", "broker:9092")
WINDOW_S     = int(env("LAB11_WINDOW_SECONDS", "60"))
WATERMARK_S  = int(env("LAB11_WATERMARK_SECONDS", "180"))
MAX_OFFSETS  = env("LAB11_MAX_OFFSETS_PER_TRIGGER", "4000")
# Part 1 only. The late tail has to arrive at least one whole micro-batch AFTER
# the batch that carried the frontier, or the watermark that filters it is still
# too far behind to call it late. See the Part 1 note in README.md.
DROPS_MAX_OFFSETS = env("LAB11_DROPS_MAX_OFFSETS", "200")
CRASH_BATCH  = int(env("LAB11_CRASH_BATCH", "3"))

S3_BUCKET    = env("S3_BUCKET", "sd411")
S3_ENDPOINT  = env("S3_ENDPOINT", "http://minio:9000")
S3_KEY       = env("MINIO_ROOT_USER", "sd411admin")
S3_SECRET    = env("MINIO_ROOT_PASSWORD", "sd411password")

CKPT_ROOT    = "s3a://%s/checkpoints/lab11" % S3_BUCKET
SINK_ROOT    = "s3a://%s/lab11" % S3_BUCKET


def build_spark():
    return (
        SparkSession.builder.appName("lab11_stateful")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # One state-store partition per shuffle partition, per batch. The Spark
        # default of 200 makes every micro-batch write 200 state files and 200
        # tiny Parquet parts on a single-worker VM, which dominates the runtime
        # of the many-batch Part 1 run. This value is RECORDED IN THE CHECKPOINT
        # (look in offsets/N), so it cannot be changed on a resume: reset first.
        .config("spark.sql.shuffle.partitions", env("LAB11_SHUFFLE_PARTITIONS", "8"))
        # Keep every batch's progress record, not the default last 100: the
        # Part 1 run has more batches than that and summarize() reads them all.
        .config("spark.sql.streaming.numRecentProgressUpdates", "2000")
        # RocksDB is flipped on from the CLI in Part 4; default provider otherwise.
        .getOrCreate()
    )


VALUE_SCHEMA = StructType([
    StructField("rid", LongType()),
    StructField("pitch_type", StringType()),
    StructField("event_time_ms", LongType()),
])


def read_pitches(spark, max_offsets):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), VALUE_SCHEMA).alias("j"))
        .select("j.*")
        .withColumn("event_time", (F.col("event_time_ms") / 1000).cast("timestamp"))
    )
    return parsed


def windowed_counts(pitches, watermark_s):
    # ===================================================================
    # TODO(you) 1: apply the event-time watermark, allowance = watermark_s
    #              seconds, on the event_time column. Without it, append mode
    #              has no signal for when a window is final and this query
    #              cannot run in append mode at all (you saw that failure in
    #              lab10). Replace the pass-through below.
    watermarked = pitches  # <-- TODO(you): .withWatermark("event_time", f"{watermark_s} seconds")

    # TODO(you) 2: group by a TUMBLING window of WINDOW_S seconds on event_time
    #              AND by pitch_type, then count(). This (window, pitch_type)
    #              pair is exactly the key of the state store. Replace the
    #              placeholder aggregation below.
    agg = watermarked.groupBy("pitch_type").count()  # <-- TODO(you): add window(...) to the groupBy
    # ===================================================================
    return agg


# ---- sinks -----------------------------------------------------------------
# Both sinks go through foreachBatch so the crash lands at a place you control:
# after the sink write, before the batch commits. That is the exact failure
# window from Wednesday's lecture, the one that turns an at-least-once sink into
# duplicates and leaves an exactly-once sink unharmed.
#
# `prefix` is the sink directory this action owns (agg_idempotent, agg_naive,
# agg_drops). Each part writes under its own prefix so a Part 1 run can never
# contaminate the Part 2 proof.
def make_sink(mode, crash_after_write, prefix):
    def foreach_batch(bdf, bid):
        bdf = bdf.select(
            F.col("window.start").alias("w_start"),
            F.col("window.end").alias("w_end"),
            "pitch_type", "count",
        )
        if mode == "idempotent":
            # TODO(you) 3: write to a path DERIVED FROM bid so a replay of this
            #              batch overwrites the same object instead of adding a
            #              new one. That determinism is the whole of leg three.
            #              Replace the None below with the batch-id path and use
            #              mode("overwrite").
            path = None  # <-- TODO(you): f"{SINK_ROOT}/{prefix}/batch_id={bid}"
            bdf.write.mode("overwrite").parquet(path)
        elif mode == "naive":
            # Given, as the counter-example: a fresh random name every write, so
            # a replayed batch writes NEW files next to the old ones.
            path = "%s/%s/part-%s" % (SINK_ROOT, prefix, uuid.uuid4().hex)
            bdf.write.mode("append").parquet(path)
        else:
            raise SystemExit("unknown sink mode: %s" % mode)

        if crash_after_write and bid == CRASH_BATCH:
            raise RuntimeError(
                "lab11: injected fault AFTER the batch %d write, BEFORE commit" % bid)

    return foreach_batch


def summarize(q):
    # The Structured Streaming tab shows this live, but the driver UI is gone
    # seconds after the query stops, so the graded numbers are printed here from
    # the query's own progress records.
    #
    # Read q.recentProgress, NOT q.lastProgress: the final micro-batch of an
    # AvailableNow run is a no-data batch that exists only to apply the last
    # watermark, so its dropped count is always 0 and its state-row count is
    # whatever is left over, never the peak.
    #
    # One honest caveat on `dropped`: numRowsDroppedByWatermark is counted at
    # the state operator, which sees rows AFTER partial aggregation. A batch in
    # which 200 late records are abandoned reports a handful of dropped GROUPS,
    # not 200. The record-level drop count is Part 1's oracle comparison.
    progs = q.recentProgress
    if not progs:
        print("progress: no batches ran (is the topic empty? see produce.sh)")
        return
    print("progress: per-batch state metrics")
    print("  %-6s %-8s %-9s %-11s %-9s %s" %
          ("batch", "input", "emitted", "stateRows", "dropped", "watermark"))
    peak = 0
    total_in = 0
    total_emitted = 0
    total_dropped = 0
    for p in progs:
        ops = p.get("stateOperators", [])
        rows = sum(int(o.get("numRowsTotal", 0)) for o in ops)
        emitted = sum(int(o.get("numRowsRemoved", 0)) for o in ops)
        dropped = sum(int(o.get("numRowsDroppedByWatermark", 0)) for o in ops)
        nin = int(p.get("numInputRows", 0))
        peak = max(peak, rows)
        total_in += nin
        total_emitted += emitted
        total_dropped += dropped
        print("  %-6s %-8d %-9d %-11d %-9d %s" %
              (p.get("batchId"), nin, emitted, rows, dropped,
               p.get("eventTime", {}).get("watermark", "-")))
    print("progress: batches=%d  input records=%d  window rows emitted=%d"
          % (len(progs), total_in, total_emitted))
    print("progress: PEAK state rows=%d  state rows dropped by watermark=%d"
          % (peak, total_dropped))


def run_query(spark, mode, crash_after_write, watermark_s, ckpt_suffix,
              sink_prefix, max_offsets):
    pitches = read_pitches(spark, max_offsets)
    agg = windowed_counts(pitches, watermark_s)
    ckpt = "%s/%s" % (CKPT_ROOT, ckpt_suffix)
    q = (
        agg.writeStream.outputMode("append")
        .foreachBatch(make_sink(mode, crash_after_write, sink_prefix))
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)   # replay the retained log in fixed-size batches, then stop
        .start()
    )
    q.awaitTermination()
    summarize(q)
    print("query finished. watermark=%ds  maxOffsetsPerTrigger=%s" % (watermark_s, max_offsets))
    print("  sink ....... %s/%s" % (SINK_ROOT, sink_prefix))
    print("  checkpoint . %s" % ckpt)


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--action", required=True,
                    choices=["baseline", "crash", "resume",
                             "naive-crash", "naive-resume", "drops"])
    ap.add_argument("--watermark", type=int, default=None,
                    help="override watermark seconds (Part 1 drop experiment)")
    ap.add_argument("--max-offsets", default=None,
                    help="override maxOffsetsPerTrigger (Part 1 uses a smaller "
                         "batch so the late tail arrives after the frontier)")
    args = ap.parse_args()
    wm = args.watermark if args.watermark is not None else WATERMARK_S

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.action == "baseline":
        # Part 0/2 control: clean run, idempotent sink, no crash. Its output is
        # the oracle the crash runs must reproduce.
        run_query(spark, "idempotent", False, wm, "agg_idempotent",
                  "agg_idempotent", args.max_offsets or MAX_OFFSETS)
    elif args.action == "crash":
        # Part 2: idempotent sink, fault after write on CRASH_BATCH. Dies.
        run_query(spark, "idempotent", True, wm, "agg_idempotent",
                  "agg_idempotent", args.max_offsets or MAX_OFFSETS)
    elif args.action == "resume":
        # Part 2: same checkpoint, no fault. Resumes from the committed offset.
        run_query(spark, "idempotent", False, wm, "agg_idempotent",
                  "agg_idempotent", args.max_offsets or MAX_OFFSETS)
    elif args.action == "naive-crash":
        run_query(spark, "naive", True, wm, "agg_naive",
                  "agg_naive", args.max_offsets or MAX_OFFSETS)
    elif args.action == "naive-resume":
        run_query(spark, "naive", False, wm, "agg_naive",
                  "agg_naive", args.max_offsets or MAX_OFFSETS)
    elif args.action == "drops":
        # Part 1: tight watermark, one clean pass, into its OWN sink prefix so
        # nothing here can pollute the Part 2 proof. Smaller batches, because
        # the late tail is only abandoned once the frontier has propagated into
        # the watermark that filters late records (README, Part 1).
        run_query(spark, "idempotent", False, wm, "agg_drops",
                  "agg_drops", args.max_offsets or DROPS_MAX_OFFSETS)

    spark.stop()


if __name__ == "__main__":
    main()
