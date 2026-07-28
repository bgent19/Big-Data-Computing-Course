#!/usr/bin/env python3
"""
SD411 Lab 08 - Skew and UI Forensics  (STUDENT SCAFFOLD)

Run one part at a time:
    docker compose exec -e LAB08_HOLD=240 spark-master spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
      /opt/lab08/scripts/lab08_skew.py --part 1

LAB08_HOLD is how many seconds the script sleeps before exiting, so the driver
UI at http://localhost:4040 stays alive while you read it. Use 0 when you are
only collecting a time, 240 or more when you need to walk the UI.

Every part writes a JSON results file stamped with YOUR alpha code. Set ALPHA
below before your first run. A results file carrying the placeholder is not a
submission.
"""

import argparse
import hashlib
import json
import os
import statistics
import sys
import time
import urllib.request

from pyspark.sql import SparkSession, functions as F

# =============================================================================
# EDIT THIS. Your alpha code, lowercase, no spaces. Example: "m280001"
# =============================================================================
ALPHA = "REPLACE_ME"
# =============================================================================

BUCKET = os.environ.get("S3_BUCKET", "sd411")
FACT_PREFIX = os.environ.get("FACT_PREFIX", "fact/pitches")
FACT_PATH = f"s3a://{BUCKET}/{FACT_PREFIX}"
DIM_EVENTS_PATH = f"s3a://{BUCKET}/dim/events"
DIM_PITCH_PATH = f"s3a://{BUCKET}/dim/pitch_type"
RESULTS_DIR = os.environ.get("LAB08_RESULTS", "/opt/spark/work-dir/results")

# The whole lab runs at this partition count. Lab 06 showed you why the 200
# default is a guess. At this data size 16 keeps every partition populated and
# keeps the task list short enough to read by eye in the UI. Remember this
# number in Part 3; it matters more than it looks.
SHUFFLE_PARTS = 16

SKEW_KEY = "events"


# -----------------------------------------------------------------------------
# Provided helpers. Nothing in this block needs editing.
# -----------------------------------------------------------------------------

def _rk(row):
    """Null-safe sort key. The left-outer join leaves is_out null on the rows
    whose events key never matched the dimension, and Python 3 refuses to order
    None against a bool. Sort on (is_none, str(value)) so the reference and the
    salted result sort identically for the Part 3 correctness assertion."""
    return tuple((v is None, str(v)) for v in row)

def mk_spark(app_name, conf=None):
    """Build a SparkSession against the cluster, not local[*]."""
    b = (SparkSession.builder
         .appName(f"lab08-{ALPHA}-{app_name}")
         .master("spark://spark-master:7077")
         .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
         .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "sd411admin"))
         .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "sd411password"))
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))
         # Memory pressure so the fat (null-events) partition SPILLS to disk and
         # the Part 2 spill columns are non-zero (they read zero under Spark's
         # default 60% execution pool). Env-tunable so you can adjust without
         # editing code:
         #   LAB08_EXECUTOR_MEM   (default 512m)
         #   LAB08_MEM_FRACTION   (default 0.08)
         # 0.08 is calibrated for the SINGLE-season fact (~4.8 MB fat partition).
         # On the multi-season fact (${SEED_CSV_LARGE}, ~50 MB fat partition) the
         # real data volume makes the straggler measurable on its own, so raise
         # LAB08_MEM_FRACTION back toward normal (0.3-0.6) and re-measure -- the
         # tiny pool otherwise over-spills and distorts the wall-clock you are
         # now able to read. See INSTRUCTOR_KEY section 4.
         .config("spark.executor.memory", os.environ.get("LAB08_EXECUTOR_MEM", "512m"))
         .config("spark.memory.fraction", os.environ.get("LAB08_MEM_FRACTION", "0.08")))
    for k, v in (conf or {}).items():
        b = b.config(k, str(v))
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    return s


def timed(fn, trials=3):
    """Median of `trials` wall-clock seconds. Median, not mean: one cold JVM or
    one GC pause should not decide your answer."""
    runs = []
    for i in range(trials):
        t0 = time.time()
        out = fn()
        dt = time.time() - t0
        runs.append(dt)
        print(f"    trial {i + 1}: {dt:8.2f}s  (result={out})")
    return {"median_s": round(statistics.median(runs), 3),
            "trials_s": [round(r, 3) for r in runs]}


def key_profile(df, key_col):
    """Distribution of the KEY itself, before any shuffle is involved.
    Nulls count as a key here, which is the entire point for one of the three."""
    kc = df.groupBy(key_col).count().orderBy(F.desc("count")).collect()
    counts = [r["count"] for r in kc]
    med = statistics.median(counts) if counts else 0
    top = kc[0] if kc else None
    return {"key": key_col, "distinct_keys": len(counts),
            "hottest_key": (str(top[key_col]) if top is not None else None),
            "hottest_key_rows": (top["count"] if top is not None else 0),
            "median_key_rows": med,
            "key_skew_ratio": round(counts[0] / med, 2) if med else None}


def partition_profile(df, key_col, n=SHUFFLE_PARTS):
    """Rows per shuffle partition if `df` were shuffled on `key_col`.

    This is how you see skew WITHOUT paying for the expensive job. It answers
    the only question that matters: after hashing this key into n partitions,
    how lopsided are the partitions?
    """
    prof = (df.repartition(n, F.col(key_col))
              .withColumn("_pid", F.spark_partition_id())
              .groupBy("_pid").count().orderBy(F.desc("count")).collect())
    counts = [r["count"] for r in prof]
    med = statistics.median(counts) if counts else 0
    return {"key": key_col, "partitions_nonempty": sum(1 for c in counts if c > 0),
            "max_rows": max(counts) if counts else 0, "median_rows": med,
            "min_rows": min(counts) if counts else 0,
            "partition_skew_ratio": round(max(counts) / med, 2) if med else None}


def salted(df, key_col, n, out_col="salted_key"):
    """Deterministic salt. rand() would also spread the key, but a recomputed
    partition would draw different salts and the join would silently lose rows.
    A hash of stable columns gives the same salt every time."""
    salt = F.pmod(F.hash(F.col("game_pk"), F.col("batter"),
                         F.coalesce(F.col("release_speed"), F.lit(0.0))), F.lit(n))
    return (df.withColumn("_salt", salt)
              .withColumn(out_col, F.concat_ws("#", F.col(key_col), F.col("_salt"))))


def explode_dim(dim, key_col, n, out_col="salted_key"):
    """Replicate every dimension row across all n salt values so a salted probe
    row still finds its match."""
    return (dim.withColumn("_salt", F.explode(F.sequence(F.lit(0), F.lit(n - 1))))
               .withColumn(out_col, F.concat_ws("#", F.col(key_col), F.col("_salt"))))


def rest_stage_summary():
    """Optional convenience: pull completed-stage metrics from the driver's REST
    API instead of transcribing them by hand. The UI is still the graded
    surface. Returns [] if the UI is not reachable, which is not an error."""
    try:
        base = "http://localhost:4040/api/v1/applications"
        apps = json.loads(urllib.request.urlopen(base, timeout=5).read())
        app_id = apps[0]["id"]
        stages = json.loads(urllib.request.urlopen(f"{base}/{app_id}/stages", timeout=5).read())
        out = []
        for st in stages:
            if st.get("status") != "COMPLETE":
                continue
            out.append({"stage_id": st["stageId"], "name": (st.get("name") or "")[:60],
                        "num_tasks": st.get("numTasks"),
                        "executor_run_time_ms": st.get("executorRunTime"),
                        "shuffle_read_bytes": st.get("shuffleReadBytes"),
                        "shuffle_write_bytes": st.get("shuffleWriteBytes"),
                        "memory_spill_bytes": st.get("memoryBytesSpilled"),
                        "disk_spill_bytes": st.get("diskBytesSpilled")})
        return sorted(out, key=lambda r: -(r["executor_run_time_ms"] or 0))[:6]
    except Exception as e:  # noqa: BLE001
        print(f"    (REST summary unavailable: {e}. Read the UI by hand.)")
        return []


def write_results(part, payload):
    """Stamped, fingerprinted results. The fingerprint covers your alpha code
    and the measured values, so a number edited afterward will not match the
    fingerprint you submitted."""
    if ALPHA == "REPLACE_ME":
        sys.exit("FATAL: set ALPHA at the top of this file before running.")
    os.makedirs(RESULTS_DIR, exist_ok=True)
    body = {"alpha": ALPHA, "part": part,
            "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "shuffle_partitions": SHUFFLE_PARTS, "data": payload}
    blob = json.dumps(body, sort_keys=True, default=str)
    body["fingerprint"] = hashlib.sha256((ALPHA + blob).encode()).hexdigest()[:12]
    path = os.path.join(RESULTS_DIR, f"lab08_{ALPHA}_part{part}.json")
    with open(path, "w") as fh:
        json.dump(body, fh, indent=2, default=str)
    print(f"\n  wrote {path}   fingerprint={body['fingerprint']}")
    return body


def hold():
    secs = int(os.environ.get("LAB08_HOLD", "0"))
    if secs:
        print(f"\n  holding the driver UI open for {secs}s at http://localhost:4040 ...")
        time.sleep(secs)


# -----------------------------------------------------------------------------
# PART 1 - Profile the keys before you pay for a shuffle.
# Predictions P1-P6 must already be initialed. Post-hoc predictions score zero.
# -----------------------------------------------------------------------------

def part1():
    spark = mk_spark("part1")
    fact = spark.read.parquet(FACT_PATH).cache()
    total = fact.count()
    print(f"  fact rows: {total:,}")

    results = {"fact_rows": total, "key_profiles": [], "partition_profiles": []}

    for key in ["pitch_type", "events", "batter"]:
        # TODO 1.1: call key_profile(fact, key), append to results["key_profiles"]
        # TODO 1.2: call partition_profile(fact, key), append to
        #           results["partition_profiles"]
        # TODO 1.3: print both so you can copy them onto the worksheet
        pass

    # TODO 1.4: one key has a large KEY skew ratio and a small PARTITION skew
    #           ratio. Record which, and one sentence on why. That is M1.
    results["anomaly"] = {"key": None, "why": ""}

    # TODO 1.5: compute the fair share (total rows / SHUFFLE_PARTS) and, for
    #           each key, the ratio of its hottest key to that fair share.
    #           Compare that column against partition_skew_ratio. One of them
    #           predicts the other.
    results["fair_share_rows"] = None

    write_results(1, results)
    hold()
    spark.stop()


# -----------------------------------------------------------------------------
# PART 2 - Induce the straggler and diagnose it from the outside in.
#
# AQE is OFF and broadcast is disabled on purpose. You are looking at the
# unmitigated shape.
#
# The join is a LEFT OUTER join, and that is not an arbitrary choice. Part 5.2
# is where you find out what happens to this same job as an inner join.
# -----------------------------------------------------------------------------

def build_job(spark, salt_n=None):
    """The graded job, unsalted when salt_n is None."""
    fact = spark.read.parquet(FACT_PATH)
    dim = spark.read.parquet(DIM_EVENTS_PATH)

    if salt_n is None:
        # TODO 2.1: left-outer join fact to dim on SKEW_KEY.
        raise NotImplementedError("TODO 2.1")
    else:
        # TODO 3.1: salt the fact side with salted(), replicate the dimension
        #           with explode_dim(), and join on "salted_key". Drop the
        #           duplicate key column from the dimension side first.
        raise NotImplementedError("TODO 3.1")

    # TODO 2.2: aggregate the joined rows by the dimension's is_out column,
    #           returning a count and the average release_speed. Referencing a
    #           dimension column is what stops Catalyst from deciding it does
    #           not need the join at all.


def part2():
    spark = mk_spark("part2", {
        "spark.sql.adaptive.enabled": "false",
        # Force the shuffle. Left alone, a dimension this small broadcasts and
        # there is no skew to see. That is a hint about Part 5.
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    })

    # TODO 2.3: print the static plan with explain(mode="formatted") and confirm
    #           SortMergeJoin, not BroadcastHashJoin.

    print("\n  unmitigated left outer join, 3 trials...")
    t = timed(lambda: build_job(spark).count())

    # TODO 2.4: with the hold running, open http://localhost:4040. Find the
    #           dominant stage. Record min / 25th / median / 75th / max task
    #           duration and shuffle read on the worksheet. Sort tasks by
    #           shuffle read and confirm which key value the fat task holds.
    stages = rest_stage_summary()
    for s in stages:
        print(f"    stage {s['stage_id']:>3}  tasks={s['num_tasks']:>4}  "
              f"runTime={s['executor_run_time_ms']}ms  read={s['shuffle_read_bytes']}  "
              f"spill(disk)={s['disk_spill_bytes']}")

    write_results(2, {"baseline": t, "stages": stages,
                      # TODO 2.5: transcribe from the UI. These are graded.
                      "task_min_s": None, "task_p25_s": None, "task_median_s": None,
                      "task_p75_s": None, "task_max_s": None,
                      "fat_partition_key_value": None,
                      "straggler_share_of_stage": None})
    hold()
    spark.stop()


# -----------------------------------------------------------------------------
# PART 3 - Salt it by hand. Three salt factors, one curve.
# -----------------------------------------------------------------------------

def part3():
    spark = mk_spark("part3", {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    })

    reference = sorted([tuple(r) for r in build_job(spark).collect()], key=_rk)
    results = {"reference_rows": reference}

    for n in [4, 16, 64]:
        # TODO 3.2: before timing, assert the salted result equals `reference`
        #           exactly. A fast wrong answer is worth nothing, and this is
        #           precisely the transform where rows go missing silently.
        print(f"\n  salt factor N={n}, 3 trials...")
        results[f"N={n}"] = timed(lambda n=n: build_job(spark, salt_n=n).count())

    # TODO 3.3: which N won, and by how much over the Part 2 baseline? Then the
    #           real question: N=64 almost certainly did not beat N=16. Explain
    #           that in terms of SHUFFLE_PARTS. That is M3.
    results["chosen_n"] = None
    results["why"] = ""

    write_results(3, results)
    hold()
    spark.stop()


# -----------------------------------------------------------------------------
# PART 4 - Hand it to AQE, then find out what AQE actually did.
# -----------------------------------------------------------------------------

def part4():
    out = {}

    # --- 4a: AQE at its shipped defaults ------------------------------------
    spark = mk_spark("part4a", {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    })
    # TODO 4.1: run the SAME job as Part 2 and time it.
    # TODO 4.2: open the SQL tab and read the FINAL plan, not explain(). Record
    #           whether any AQEShuffleRead reports skewed partitions, and
    #           whether it reports coalesced ones. They are different claims.
    # TODO 4.3: compare the max task duration against Part 2. If the total time
    #           moved but the straggler did not, name the mechanism responsible.
    out["aqe_default"] = None
    out["stages_default"] = rest_stage_summary()
    spark.stop()

    # --- 4b: AQE with thresholds moved to this data size --------------------
    spark = mk_spark("part4b", {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        # TODO 4.4: a partition is treated as skewed only when it is BOTH more
        #           than skewedPartitionFactor times the median AND larger than
        #           skewedPartitionThresholdInBytes. Look up both defaults, work
        #           out which one your fat partition fails, and set the two
        #           skewJoin knobs plus advisoryPartitionSizeInBytes so the
        #           split can actually happen here.
    })
    # TODO 4.5: re-run and time. Confirm in the SQL tab that AQEShuffleRead now
    #           reports skewed partitions. If it does not, your thresholds are
    #           still above the real partition size.
    out["aqe_tuned"] = None
    out["stages_tuned"] = rest_stage_summary()

    # --- 4c: the same key, as an aggregation --------------------------------
    # TODO 4.6: with AQE and skewJoin still on, run a groupBy on SKEW_KEY over
    #           the fact alone (no join) and time it. Record the task min,
    #           median and max. The straggler survives. Why? That is half of M5.
    out["agg_under_aqe"] = None

    out["final_plan_skewed_true"] = None    # TODO from the SQL tab
    out["final_plan_coalesced"] = None      # TODO from the SQL tab
    write_results(4, out)
    hold()
    spark.stop()


# -----------------------------------------------------------------------------
# PART 5 - STRETCH (+5). Two fixes that are not salting.
# -----------------------------------------------------------------------------

def part5():
    # TODO 5.1: re-run the Part 2 job with autoBroadcastJoinThreshold left at
    #           its default so the dimension broadcasts. Time it against your
    #           best salted run and your best AQE run, then write two sentences
    #           about what that means for Parts 2 through 4.
    # TODO 5.2: run the same join as an INNER join, broadcast still disabled.
    #           The skew disappears. Read both plans, find the extra operator
    #           the inner plan has that the left outer plan does not, and name
    #           the Catalyst rule. You met it in Lab 05.
    print("Part 5 is stretch. Implement 5.1 and 5.2, then call write_results(5, ...).")


PARTS = {1: part1, 2: part2, 3: part3, 4: part4, 5: part5}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", type=int, required=True, choices=sorted(PARTS))
    a = ap.parse_args()
    PARTS[a.part]()
