#!/usr/bin/env python3
"""
SD411 Lab 10 - Windowing on Wikipedia Pageviews  (STUDENT SCAFFOLD)

Run one part at a time from the driver container:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
      /opt/lab10/scripts/lab10_windowing.py part0

Parts: part0 part1 part2 part3 part4

Everything below marked TODO is yours. Everything else is harness: the Spark
session, the metrics collector, the ASCII plotting, and the file plumbing.
Read the harness anyway. run_stream() is where the numbers on your worksheet
come from, and you will be asked at the oral station how a state row count
gets from the executor into your terminal.

WRITE YOUR PREDICTION ON THE WORKSHEET BEFORE YOU RUN ANY PART.
A prediction recorded after the measurement scores zero.
"""

import json
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# =============================================================================
# Knobs. All resolve from the environment, which the compose file populates
# from the stamped .env. Do not hardcode a bucket, a credential, or a path.
# =============================================================================
PV_DIR = os.environ.get("PV_DIR", "file:///opt/lab10/pageviews")
S3_BUCKET = os.environ.get("S3_BUCKET", "sd411")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
CHK_PREFIX = os.environ.get("CHK_PREFIX", "chk/lab10")
ORACLE_PREFIX = os.environ.get("ORACLE_PREFIX", "lab10/oracle")
SPARK_WORK_DIR = os.environ.get("SPARK_WORK_DIR", "/opt/spark/work")

MAX_FILES_PER_TRIGGER = int(os.environ.get("MAX_FILES_PER_TRIGGER", "8"))
TRIGGER_SECONDS = int(os.environ.get("TRIGGER_SECONDS", "1"))
PV_SESSION_MIN_VIEWS = int(os.environ.get("PV_SESSION_MIN_VIEWS", "500"))

CHK_ROOT = "s3a://%s/%s" % (S3_BUCKET, CHK_PREFIX)
ORACLE_PATH = "s3a://%s/%s/hourly" % (S3_BUCKET, ORACLE_PREFIX)
METRICS_DIR = os.path.join(SPARK_WORK_DIR, "lab10", "metrics")

# Harness safety rails.
IDLE_POLLS_TO_STOP = 6          # consecutive polls with no new batch id
MAX_RUN_SECONDS = 420           # hard ceiling on any single streaming run

# The streaming file source will not infer a schema. This is not the engine
# being difficult: an inferred schema would change under you as new files
# arrive, and a query whose schema is not fixed is not a query.
SCHEMA = StructType([
    StructField("ts", TimestampType(), True),
    StructField("domain", StringType(), True),
    StructField("page", StringType(), True),
    StructField("views", IntegerType(), True),
])


def todo(what):
    raise NotImplementedError("TODO: " + what)


# =============================================================================
# Harness
# =============================================================================
def build_spark(app_name):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        # Every timestamp in this lab is UTC. Window boundaries are aligned to
        # the Unix epoch in UTC whatever this is set to - a window duration is
        # a fixed count of microseconds, and the session zone only controls how
        # timestamps are rendered. Pinning it to UTC keeps the window labels you
        # read matching the hours the dumps are named after.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_stream(spark):
    """The lab's one and only source. Same shape every part."""
    return (
        spark.readStream
        .schema(SCHEMA)
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .parquet(PV_DIR)
    )


def _snapshot(progress):
    """Pull the handful of fields we actually grade out of a progress dict."""
    ops = progress.get("stateOperators") or []
    op = ops[0] if ops else {}
    return {
        "batchId": progress.get("batchId"),
        "numInputRows": progress.get("numInputRows", 0),
        "triggerExecutionMs": (progress.get("durationMs") or {}).get("triggerExecution"),
        "stateRowsTotal": op.get("numRowsTotal"),
        "stateRowsUpdated": op.get("numRowsUpdated"),
        "stateMemoryBytes": op.get("memoryUsedBytes"),
        "watermark": (progress.get("eventTime") or {}).get("watermark"),
    }


def run_stream(spark, agg_df, name, mode="complete"):
    """
    Start a streaming query against the memory sink, poll it until the feed is
    exhausted, record per-batch metrics, stop it, and return the metrics list.

    The memory sink is a teaching sink. It holds the result table in the
    driver's heap so you can SELECT it. Never use it for anything real.
    """
    os.makedirs(METRICS_DIR, exist_ok=True)
    chk = "%s/%s" % (CHK_ROOT, name)

    query = (
        agg_df.writeStream
        .queryName(name)
        .outputMode(mode)
        .format("memory")
        .option("checkpointLocation", chk)
        .trigger(processingTime="%d seconds" % TRIGGER_SECONDS)
        .start()
    )

    metrics = []
    seen = set()
    idle = 0
    t0 = time.time()

    def harvest():
        """
        Drain every batch that finished since we last looked, and return how
        many were new.

        WHY NOT lastProgress: it is a single snapshot of the most recent batch.
        Poll it once per trigger and any batch that starts and finishes between
        two samples is never recorded, so the state curve silently grows holes
        and the batch count depends on how fast the machine is. recentProgress
        retains the last spark.sql.streaming.numRecentProgressUpdates batches
        (default 100, and this feed produces ~21), so draining it and
        de-duplicating on batchId records every batch exactly once.
        """
        found = 0
        for progress in query.recentProgress:
            snap = _snapshot(progress)
            if snap["batchId"] is None or snap["batchId"] in seen:
                continue
            seen.add(snap["batchId"])
            metrics.append(snap)
            found += 1
            print("  batch %-4s in=%-8s state=%-8s stateMB=%-7s trigMs=%s" % (
                snap["batchId"], snap["numInputRows"], snap["stateRowsTotal"],
                _mb(snap["stateMemoryBytes"]), snap["triggerExecutionMs"]))
        return found

    while query.isActive:
        time.sleep(TRIGGER_SECONDS)
        idle = 0 if harvest() else idle + 1
        if idle >= IDLE_POLLS_TO_STOP:
            break
        if time.time() - t0 > MAX_RUN_SECONDS:
            print("  [harness] hit MAX_RUN_SECONDS; stopping early.")
            break

    harvest()          # final drain: the last batches land after the last poll
    query.stop()
    metrics.sort(key=lambda m: m["batchId"])

    if not metrics:
        print()
        print("  [harness] ZERO batches ran for query '%s'." % name)
        print("  [harness] The usual cause is a checkpoint that already records")
        print("  [harness] every shard as consumed. A checkpoint is a progress")
        print("  [harness] record, not a cache. Reset it and re-run:")
        print("  [harness]     scripts/reset_lab10_checkpoints.sh %s" % name)
        print()

    path = os.path.join(METRICS_DIR, "%s.jsonl" % name)
    with open(path, "w") as fh:
        for m in metrics:
            fh.write(json.dumps(m) + "\n")
    print("  [harness] %d batches, metrics -> %s" % (len(metrics), path))
    return metrics


def _mb(nbytes):
    if nbytes is None:
        return "?"
    return "%.1f" % (float(nbytes) / (1024.0 * 1024.0))


def bars(pairs, width=48, title=""):
    """ASCII bar chart. pairs is a list of (label, numeric value)."""
    if title:
        print("\n" + title)
    values = [v for _, v in pairs if v is not None]
    if not values:
        print("  (no data)")
        return
    top = max(values) or 1
    for label, value in pairs:
        n = int(width * float(value) / float(top)) if value else 0
        print("  %-26s %12d %s" % (str(label)[:26], value, "#" * n))


def state_curve(metrics, title):
    bars([(m["batchId"], m["stateRowsTotal"] or 0) for m in metrics],
         title=title)


# =============================================================================
# PART 0 - the oracle and the evidence  (target 15 min)
# =============================================================================
def part0(spark):
    print("=" * 72)
    print("PART 0 - batch ground truth, and what the shards actually contain")
    print("=" * 72)

    # Batch read. Same files, no streaming, no state. This is the answer the
    # streaming query has to reproduce in Part 4.
    batch = spark.read.schema(SCHEMA).parquet(PV_DIR)

    # ---- TODO 0.1 -----------------------------------------------------------
    # Report the size and event-time extent of the whole feed: total rows,
    # total views, min(ts), max(ts), and the number of distinct pages.
    # One action, not five. Build a single aggregate row.
    summary = todo("0.1 build a one-row summary of the whole feed")
    summary.show(truncate=False)

    # ---- TODO 0.2 -----------------------------------------------------------
    # A shard is a file. Is a shard an hour?
    # Derive the shard index from the file path with input_file_name() and the
    # regex shard-(\d+)\.parquet, then report per shard: row count, min(ts),
    # max(ts), and the span in hours between them.
    # Show the first 12 shards and the last 12.
    per_shard = todo("0.2 per-shard row count and event-time min/max/span")
    per_shard.orderBy("shard").show(12, truncate=False)
    per_shard.orderBy(F.col("shard").desc()).show(12, truncate=False)

    # ---- TODO 0.3 -----------------------------------------------------------
    # The oracle. Group the batch read into 1-hour event-time windows and sum
    # views. Write it to ORACLE_PATH as Parquet, overwrite mode.
    # Part 4 joins the streaming answer against this, so the column names
    # matter: produce exactly (window_start TIMESTAMP, views BIGINT).
    oracle = todo("0.3 hourly oracle keyed by window_start")
    oracle.write.mode("overwrite").parquet(ORACLE_PATH)

    rows = oracle.orderBy("window_start").collect()
    bars([(r["window_start"].strftime("%m-%d %Hz"), r["views"]) for r in rows[:72]],
         title="Total views per event hour (first 72 hours)")
    print("\nWrote oracle -> %s  (%d hourly rows)" % (ORACLE_PATH, len(rows)))


# =============================================================================
# PART 1 - tumbling windows and the resolution sweep  (target 30 min)
# =============================================================================
def part1(spark):
    print("=" * 72)
    print("PART 1 - tumbling windows; window size as a resolution decision")
    print("=" * 72)

    stream = read_stream(spark)

    # ---- TODO 1.1 -----------------------------------------------------------
    # Build a function that takes a window size string ("1 hour", "3 hours",
    # ...) and returns the aggregation: group by a TUMBLING event-time window
    # over ts, sum views, and project (window_start, window_end, views).
    def tumbling(size):
        return todo("1.1 tumbling aggregation at size=%s" % size)

    for size, tag in [("1 hour", "t1h"), ("3 hours", "t3h"),
                      ("6 hours", "t6h"), ("24 hours", "t24h")]:
        print("\n--- tumbling %s ---" % size)
        metrics = run_stream(spark, tumbling(size), "part1_%s" % tag, mode="complete")
        result = spark.sql(
            "select window_start, views from part1_%s order by window_start" % tag
        ).collect()
        print("  windows emitted: %d" % len(result))
        bars([(r["window_start"].strftime("%m-%d %Hz"), r["views"]) for r in result],
             title="tumbling %s" % size)
        state_curve(metrics, "state rows per batch, tumbling %s" % size)

    # ---- TODO 1.2 -----------------------------------------------------------
    # Pick the single burstiest article you can find in the feed and repeat the
    # sweep for that page alone, at 1 hour and at 24 hours only. Two runs, not
    # four. Filter BEFORE the aggregation.
    # Finding the article is part of the task. One reasonable approach: use the
    # batch read to rank pages by peak-hour views over median-hour views.
    page = todo("1.2 choose one bursty page")
    print("\nSweeping page: %s" % page)

    for size, tag in [("1 hour", "p1h"), ("24 hours", "p24h")]:
        agg = todo("1.2 tumbling %s restricted to page=%s" % (size, page))
        run_stream(spark, agg, "part1_%s" % tag, mode="complete")
        rows = spark.sql(
            "select window_start, views from part1_%s order by window_start" % tag
        ).collect()
        bars([(r["window_start"].strftime("%m-%d %Hz"), r["views"]) for r in rows],
             title="%s at tumbling %s" % (page, size))


# =============================================================================
# PART 2 - sliding windows and the multiplication  (target 25 min)
# =============================================================================
def part2(spark):
    print("=" * 72)
    print("PART 2 - sliding windows; what overlap costs")
    print("=" * 72)

    stream = read_stream(spark)

    # ---- TODO 2.1 -----------------------------------------------------------
    # A 24-hour window sliding every 3 hours, summing views over the whole feed.
    # Project (window_start, window_end, views).
    sliding = todo("2.1 sliding window, size 24 hours, slide 3 hours")

    metrics = run_stream(spark, sliding, "part2_s24h3h", mode="complete")
    result = spark.sql(
        "select window_start, window_end, views from part2_s24h3h order by window_start"
    ).collect()
    print("  windows emitted: %d" % len(result))
    bars([(r["window_start"].strftime("%m-%d %Hz"), r["views"]) for r in result],
         title="sliding 24h / 3h")
    state_curve(metrics, "state rows per batch, sliding 24h/3h")

    # ---- TODO 2.2 -----------------------------------------------------------
    # Now add a grouping key: the same sliding window, grouped additionally by
    # domain. Predict the state row count before you run it. Then run it.
    sliding_keyed = todo("2.2 same sliding window, additionally grouped by domain")
    metrics_keyed = run_stream(spark, sliding_keyed, "part2_s24h3h_dom", mode="complete")
    n_keyed = spark.sql("select count(*) c from part2_s24h3h_dom").collect()[0]["c"]
    print("  keyed result rows: %d" % n_keyed)
    state_curve(metrics_keyed, "state rows per batch, sliding 24h/3h by domain")

    # ---- TODO 2.3 -----------------------------------------------------------
    # Verify the conservation identity on the worksheet. Compare:
    #   (a) sum of views across all sliding windows
    #   (b) the total views from your Part 0 summary
    # State the exact integer ratio you expect and whether you got it.
    todo("2.3 compute the sliding-window sum and compare it to the batch total")


# =============================================================================
# PART 3 - session windows  (target 20 min)
# =============================================================================
def part3(spark):
    print("=" * 72)
    print("PART 3 - session windows")
    print("=" * 72)

    stream = read_stream(spark)

    # ---- TODO 3.1 -----------------------------------------------------------
    # Try the streaming version FIRST. Build a session-window aggregation over
    # the stream: filter to rows with views >= PV_SESSION_MIN_VIEWS, group by a
    # 3-hour-gap session window on ts and by page, and count.
    # Then run it below. Read whatever comes back very carefully.
    #
    # The harness runs this in APPEND mode, and that is not an arbitrary
    # choice. All three output modes behave differently here and only one of
    # them tells you anything: complete mode starts and runs, update mode
    # refuses for a reason that has nothing to do with what is missing, and
    # append mode refuses and says why. Try the other two after you have
    # recorded the append message if you want to see this for yourself.
    session_stream = todo("3.1 streaming session window, 3 hour gap, keyed by page")

    print("\n--- attempting the STREAMING session window ---")
    try:
        run_stream(spark, session_stream, "part3_sessions", mode="append")
        print("  the streaming session query started.")
    except Exception as exc:  # noqa: BLE001 - we want the message, whatever it is
        print("  the streaming session query did NOT start.")
        print("  exception type: %s" % type(exc).__name__)
        print("  message:")
        for line in str(exc).splitlines()[:8]:
            print("    " + line)
        print("\n  Copy that message onto the worksheet verbatim. It names the")
        print("  one thing this lab does not have yet.")

    # ---- TODO 3.2 -----------------------------------------------------------
    # Now do it in batch, where nothing is missing. Same filter, same 3-hour
    # gap, same key. Produce per session: page, session_start, session_end,
    # duration in hours, and total views.
    batch = spark.read.schema(SCHEMA).parquet(PV_DIR)
    sessions = todo("3.2 batch session windows, 3 hour gap, keyed by page")
    sessions.cache()

    total_sessions = sessions.count()
    distinct_pages = sessions.select("page").distinct().count()
    print("\n  sessions: %d over %d pages (mean %.2f sessions/page)"
          % (total_sessions, distinct_pages, total_sessions / max(distinct_pages, 1)))

    print("\n  longest sessions:")
    sessions.orderBy(F.col("duration_hours").desc()).show(10, truncate=False)
    print("\n  pages with the most sessions:")
    (sessions.groupBy("page").count()
     .orderBy(F.col("count").desc()).show(10, truncate=False))

    # ---- TODO 3.3 -----------------------------------------------------------
    # On the worksheet: you filtered to views >= PV_SESSION_MIN_VIEWS before
    # grouping. Using numbers you already have, estimate how many open sessions
    # the engine would have had to hold WITHOUT that filter. Do not run it
    # unless you have finished everything else.
    todo("3.3 estimate the unfiltered session key count (worksheet, no run)")


# =============================================================================
# PART 4 - state forensics and the reordering proof  (target 25 min)
# =============================================================================
def part4(spark):
    print("=" * 72)
    print("PART 4 - does the streaming answer equal the batch answer?")
    print("=" * 72)

    stream = read_stream(spark)

    # ---- TODO 4.1 -----------------------------------------------------------
    # Rebuild the 1-hour tumbling aggregation, projected as
    # (window_start, views), exactly matching the oracle's schema.
    tumbling_1h = todo("4.1 1-hour tumbling aggregation matching the oracle schema")

    metrics = run_stream(spark, tumbling_1h, "part4_t1h", mode="complete")
    streamed = spark.sql("select window_start, views from part4_t1h")

    # ---- TODO 4.2 -----------------------------------------------------------
    # Full outer join the streamed result against the Part 0 oracle on
    # window_start and count the rows where the two disagree, plus the rows
    # present in one and missing from the other.
    oracle = spark.read.parquet(ORACLE_PATH)
    mismatches = todo("4.2 full outer join streamed vs oracle; isolate disagreements")

    n_bad = mismatches.count()
    print("\n  disagreements between streaming and batch: %d" % n_bad)
    if n_bad:
        mismatches.show(20, truncate=False)
    else:
        print("  The out-of-order feed produced the batch answer exactly.")

    # Provided: the state curve and the extrapolation you will defend.
    state_curve(metrics, "state rows per batch, tumbling 1h (Part 4 run)")
    if metrics:
        final_state = metrics[-1]["stateRowsTotal"] or 0
        print("\n  final state rows: %d" % final_state)
        print("  batches run     : %d" % len(metrics))
        # P4.2 asks how many batches the count went DOWN in, so compare each
        # batch to the one before it. Comparing against the running peak
        # instead answers a different question ("how many batches sat below
        # the high-water mark") and happens to agree only because this curve
        # never falls.
        decreases = 0
        previous = None
        for m in metrics:
            value = m["stateRowsTotal"] or 0
            if previous is not None and value < previous:
                decreases += 1
            previous = value
        print("  batches in which state DECREASED: %d" % decreases)

    # ---- TODO 4.3 -----------------------------------------------------------
    # On the worksheet: this feed is one week. Extrapolate the final state row
    # count for the same query run continuously for one year, and again with a
    # per-page grouping key instead of a global one. Then answer in one
    # sentence: what single piece of information would let the engine throw a
    # window's state away?
    todo("4.3 extrapolate state to one year and name what would bound it")


# =============================================================================
PARTS = {"part0": part0, "part1": part1, "part2": part2, "part3": part3, "part4": part4}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in PARTS:
        print("usage: lab10_windowing.py {%s}" % "|".join(sorted(PARTS)))
        sys.exit(2)
    part = sys.argv[1]
    spark = build_spark("sd411-lab10-%s" % part)
    started = time.time()
    try:
        PARTS[part](spark)
    finally:
        print("\n[timer] %s wall clock: %.1f s" % (part, time.time() - started))
        spark.stop()


if __name__ == "__main__":
    main()
