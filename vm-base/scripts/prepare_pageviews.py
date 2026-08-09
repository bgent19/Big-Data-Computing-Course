#!/usr/bin/env python3
"""
SD411 Lab 10 - provision step 2 of 2: build the lab shards.

RUN ONCE, ON THE GOLDEN VM, BEFORE THE TERM. Not a student step.

Reads the raw hourly Wikipedia Pageviews dumps staged by fetch_pageviews.sh
and produces the dataset the lab actually streams:

    ${SD411_DATA}/pageviews/shard-0000.parquet .. shard-0167.parquet
    ${SD411_DATA}/pageviews/_manifest.json          (student-visible, safe)
    ${SD411_DATA}/pageviews_report/spike_report.md  (instructor only)

Shard semantics, which are the whole point of this script:

  A shard is an ARRIVAL slice, not an hour. Shard i carries the rows whose
  event hour is i, MINUS the rows randomly displaced forward, PLUS the rows
  displaced forward into i from hours i-1, i-2, and i-3. Every row keeps its
  true event timestamp. The result is a stream that is genuinely out of order
  in event time, reproducibly so (fixed seed), which is what makes Part 0's
  out-of-order evidence and Part 4's reordering proof real measurements rather
  than assertions.

  Rows in the final three hours are never displaced, so no row is ever pushed
  past the end of the feed and the batch total is exactly conserved.

Run it inside the pinned Spark image so the PySpark and Parquet versions match
the ones the lab uses:

  docker run --rm --user 0:0 \
    -v /opt/sd411/data:/data \
    -v "$(pwd)/provision":/provision:ro \
    apache/spark:3.5.3-python3 \
    /opt/spark/bin/spark-submit --master 'local[*]' \
      --driver-memory 4g /provision/prepare_pageviews.py

--user 0:0 is required: the image defaults to UID 185 and /opt/sd411/data is
root-owned, so the run otherwise dies with PermissionError on /data/pageviews.

Environment (all have defaults; see COMMON_ENV_ADDENDUM.md):
  PV_WEEK_START        YYYY-MM-DD, UTC, hour 00 of the first hour
  PV_HOURS             number of hourly shards to build (default 168)
  PV_MIN_VIEWS         drop rows below this hourly view count (default 50)
  PV_LATE_FRACTION     fraction of rows displaced forward (default 0.08)
  PV_LATE_MAX_SHIFT    maximum forward displacement, in shards (default 3)
  PV_SEED              RNG seed for displacement (default 411)
  PV_RAW_DIR           staged .gz dumps       (default /data/pageviews_raw)
  PV_OUT_DIR           shard output directory (default /data/pageviews)
  PV_REPORT_DIR        instructor report dir  (default /data/pageviews_report)
"""

import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WEEK_START = os.environ.get("PV_WEEK_START", "")
print(WEEK_START)
HOURS = int(os.environ.get("PV_HOURS", "168"))
MIN_VIEWS = int(os.environ.get("PV_MIN_VIEWS", "50"))
LATE_FRACTION = float(os.environ.get("PV_LATE_FRACTION", "0.08"))
LATE_MAX_SHIFT = int(os.environ.get("PV_LATE_MAX_SHIFT", "3"))
SEED = int(os.environ.get("PV_SEED", "411"))
RAW_DIR = os.environ.get("PV_RAW_DIR", "/data/pageviews_raw")
OUT_DIR = os.environ.get("PV_OUT_DIR", "/data/pageviews")
REPORT_DIR = os.environ.get("PV_REPORT_DIR", "/data/pageviews_report")
DOMAINS = ("en", "en.m")


def fail(msg: str) -> None:
    print("FATAL: " + msg, file=sys.stderr)
    sys.exit(2)


def main() -> None:
    if not WEEK_START:
        fail("PV_WEEK_START is not set (expected YYYY-MM-DD, UTC).")
    try:
        start_dt = datetime.strptime(WEEK_START, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        fail("PV_WEEK_START='%s' is not YYYY-MM-DD." % WEEK_START)
        return
    end_dt = start_dt + timedelta(hours=HOURS)

    spark = (
        SparkSession.builder.appName("sd411-lab10-provision")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ---- read every staged dump at once -------------------------------------
    # gzip is not splittable, so each file becomes one partition. The hour is
    # not in the file's contents, only in its name, which is exactly the
    # "event time is metadata, not a column" situation the lab discusses.
    raw = spark.read.text(os.path.join(RAW_DIR, "pageviews-*.gz"))
    if raw.rdd.isEmpty():
        fail("no dumps matched %s/pageviews-*.gz" % RAW_DIR)

    stamp = F.regexp_extract(F.input_file_name(), r"pageviews-(\d{8}-\d{6})\.gz", 1)
    parts = F.split(F.col("value"), " ")

    rows = (
        raw.select(
            F.to_timestamp(stamp, "yyyyMMdd-HHmmss").alias("ts"),
            parts.getItem(0).alias("domain"),
            parts.getItem(1).alias("page"),
            parts.getItem(2).cast("int").alias("views"),
        )
        .where(F.col("ts").isNotNull())
        .where(F.col("page").isNotNull())
        .where(F.col("views").isNotNull())
        .where(F.col("domain").isin(*DOMAINS))
        .where(F.col("views") >= F.lit(MIN_VIEWS))
        .where(F.col("ts") >= F.lit(start_dt.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
        .where(F.col("ts") < F.lit(end_dt.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    )

    hour_index = (
        (F.unix_timestamp("ts") - F.lit(int(start_dt.timestamp()))) / F.lit(3600)
    ).cast("int")

    # ---- displacement -------------------------------------------------------
    # Two independent seeded streams: one decides whether a row is displaced,
    # one decides how far. Rows in the last LATE_MAX_SHIFT hours are pinned so
    # nothing falls off the end of the feed and the total is conserved exactly.
    coin = F.rand(SEED)
    shift = (F.floor(F.rand(SEED + 1) * F.lit(LATE_MAX_SHIFT)) + F.lit(1)).cast("int")
    raw_delay = F.when(coin < F.lit(LATE_FRACTION), shift).otherwise(F.lit(0))
    delay = F.when(hour_index >= F.lit(HOURS - LATE_MAX_SHIFT), F.lit(0)).otherwise(raw_delay)

    staged = (
        rows.withColumn("hour_index", hour_index)
        .withColumn("arrival", hour_index + delay)
        .select("ts", "domain", "page", "views", "hour_index", "arrival")
        .cache()
    )
    total_rows = staged.count()
    distinct_pages = staged.select("page").distinct().count()
    displaced_rows = staged.where(F.col("arrival") != F.col("hour_index")).count()
    print("[prep] rows=%d distinct_pages=%d displaced=%d (%.2f%%)"
          % (total_rows, distinct_pages, displaced_rows, 100.0 * displaced_rows / max(total_rows, 1)))

    if total_rows == 0:
        fail("filter produced zero rows; check PV_MIN_VIEWS and PV_WEEK_START.")

    # ---- write one flat Parquet file per arrival shard ----------------------
    # Written in index order so modification times increase monotonically. The
    # streaming file source orders by modification time when maxFilesPerTrigger
    # is set, so this ordering is what makes the student runs deterministic.
    if os.path.isdir(OUT_DIR):
        shutil.rmtree(OUT_DIR)
    os.makedirs(OUT_DIR, exist_ok=True)
    tmp_dir = os.path.join(OUT_DIR, "_tmp")

    shard_counts = []
    for i in range(HOURS):
        chunk = (
            staged.where(F.col("arrival") == F.lit(i))
            .select("ts", "domain", "page", "views")
            .orderBy("ts", "page")
            .coalesce(1)
        )
        chunk.write.mode("overwrite").parquet(tmp_dir)
        produced = [f for f in os.listdir(tmp_dir) if f.endswith(".parquet")]
        if len(produced) != 1:
            fail("shard %d produced %d parquet parts, expected 1" % (i, len(produced)))
        dest = os.path.join(OUT_DIR, "shard-%04d.parquet" % i)
        shutil.move(os.path.join(tmp_dir, produced[0]), dest)
        shutil.rmtree(tmp_dir)
        n = spark.read.parquet(dest).count()
        shard_counts.append(n)
        if i % 24 == 0:
            print("[prep] shard %03d rows=%d" % (i, n))

    if sum(shard_counts) != total_rows:
        fail("shard row total %d != staged total %d" % (sum(shard_counts), total_rows))

    # ---- student-visible manifest ------------------------------------------
    # Underscore prefix so Spark's file listing ignores it. Deliberately does
    # NOT record the displacement parameters: discovering that the feed is out
    # of order is Part 0's job.
    manifest = {
        "dataset": "Wikimedia Pageviews (hourly dumps)",
        "source": "https://dumps.wikimedia.org/other/pageviews/",
        "license": "CC0 1.0 Public Domain Dedication",
        "week_start_utc": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "week_end_utc": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hours": HOURS,
        "shard_count": HOURS,
        "domains": list(DOMAINS),
        "min_hourly_views_filter": MIN_VIEWS,
        "total_rows": total_rows,
        "distinct_pages": distinct_pages,
        "schema": "ts TIMESTAMP (UTC), domain STRING, page STRING, views INT",
        "shard_naming": "shard-NNNN.parquet, one file per arrival slice",
        "note": "A shard is an arrival slice. It is not guaranteed to be an hour.",
    }
    with open(os.path.join(OUT_DIR, "_manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2)

    # ---- instructor spike report -------------------------------------------
    os.makedirs(REPORT_DIR, exist_ok=True)
    hourly_total = (
        staged.groupBy("hour_index").agg(F.sum("views").alias("views"))
        .orderBy("hour_index").collect()
    )
    per_page = (
        staged.groupBy("page")
        .agg(
            F.sum("views").alias("total_views"),
            F.max("views").alias("peak_hour_views"),
            F.expr("percentile_approx(views, 0.5)").alias("median_hour_views"),
            F.countDistinct("hour_index").alias("active_hours"),
        )
        .where(F.col("total_views") >= F.lit(20000))
        .withColumn("burstiness", F.col("peak_hour_views") / (F.col("median_hour_views") + F.lit(1)))
        .orderBy(F.col("burstiness").desc())
        .limit(25)
        .collect()
    )

    lines = []
    lines.append("# Lab 10 provisioning report (instructor only)\n")
    lines.append("Week: %s to %s (UTC). Rows: %d. Distinct pages: %d.\n"
                 % (manifest["week_start_utc"], manifest["week_end_utc"], total_rows, distinct_pages))
    lines.append("Displacement: fraction=%.3f, max_shift=%d shards, seed=%d, displaced=%d rows.\n"
                 % (LATE_FRACTION, LATE_MAX_SHIFT, SEED, displaced_rows))
    lines.append("\n## Diurnal check (total views per event hour)\n")
    peak = max((r["views"] for r in hourly_total), default=1)
    lines.append("```\n")
    for r in hourly_total:
        bar = "#" * int(48.0 * r["views"] / peak)
        lines.append("h%03d %12d %s\n" % (r["hour_index"], r["views"], bar))
    lines.append("```\n")
    lines.append("\nIf that column does not show a clear 24-hour cycle, the week or the\n"
                 "PV_MIN_VIEWS filter is wrong. Part 1's resolution sweep depends on it.\n")
    lines.append("\n## Burstiest articles (candidates for the Part 1 sweep)\n\n")
    lines.append("| page | total | peak hour | median hour | active hours | burstiness |\n")
    lines.append("|---|---:|---:|---:|---:|---:|\n")
    for r in per_page:
        lines.append("| %s | %d | %d | %d | %d | %.1f |\n" % (
            r["page"], r["total_views"], r["peak_hour_views"],
            r["median_hour_views"], r["active_hours"], r["burstiness"]))
    lines.append("\nPick one high-burstiness article for the INSTRUCTOR_KEY worked example.\n"
                 "Do NOT publish this list to students; finding the spike is their work.\n")

    with open(os.path.join(REPORT_DIR, "spike_report.md"), "w") as fh:
        fh.writelines(lines)

    print("[done] shards -> %s" % OUT_DIR)
    print("[done] report -> %s/spike_report.md" % REPORT_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
