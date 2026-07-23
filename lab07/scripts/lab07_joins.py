#!/usr/bin/env python3
"""
SD411 lab07 — Join Strategy Lab (student scaffold).

You will induce all three of Spark's main equi-join strategies on purpose, then
verify each one in the Spark UI and in the physical plan:

  Part 1  Broadcast hash join   (small dimension auto-broadcasts)
  Part 2  Sort-merge join       (two large tables, the default)
  Part 3  Shuffle-hash join     (you have to fight Catalyst for this one)
  Part 4  The AQE trap          (the planned join is not the join that runs)

The S3A JARs are provisioned once on the VM at /opt/sd411/jars and mounted into
the containers. You do not download anything. Set $SUBMIT once per session:

  JARS="/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar"
  SUBMIT="docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 --jars $JARS"
  $SUBMIT /opt/lab07/scripts/lab07_joins.py

PREDICT-FIRST: for every measurement in the README, write your prediction in
WORKSHEET.md and get it initialed BEFORE you run it. Post-hoc predictions
score zero.

ALPHA-CODE: replace the value of ALPHA below with your section's alpha code
(handout, top of the README). A submission whose printed alpha does not match
your section is not your own work.
"""
import os
import time
import statistics
from pyspark.sql import SparkSession, functions as F

ALPHA = "TODO-REPLACE-WITH-YOUR-ALPHA-CODE"   # <-- EDIT THIS (Part 0)

# Paths and credentials resolve from the container environment, which compose
# populates from the stamped .env. Do not hardcode them.
BUCKET = os.environ.get("S3_BUCKET", "sd411")
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "sd411admin")
SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "sd411password")
FACT_PREFIX = os.environ.get("FACT_PREFIX", "fact/pitches")

FACT_PATH = f"s3a://{BUCKET}/{FACT_PREFIX}"      # written by lab06
PA_PATH = f"s3a://{BUCKET}/fact/pa_events"       # built in Part 0
TEAMS_PATH = f"s3a://{BUCKET}/dim/teams"         # built in Part 0

# Deliberately small so each shuffle produces a readable number of tasks in the
# UI. With 8 shuffle partitions you can count stages and tasks by eye. Do not
# change this without noting it in your worksheet; it changes every number.
SHUFFLE_PARTITIONS = 8


# ----------------------------------------------------------------------------
# Provided harness. Do not modify. Same measurement discipline as lab03, lab04,
# and lab06: a 3-trial median wall-clock and a plan-string reader.
# ----------------------------------------------------------------------------
def time_action(label, action, trials=3):
    """Run a zero-arg `action` `trials` times, return the median wall time (s)."""
    times = []
    for i in range(trials):
        t0 = time.perf_counter()
        action()
        dt = time.perf_counter() - t0
        times.append(dt)
        print(f"    [{label}] trial {i + 1}: {dt:.2f}s")
    med = statistics.median(times)
    print(f"    [{label}] MEDIAN: {med:.2f}s")
    return med


def static_plan_str(df):
    """The plan as Catalyst PLANS it, before execution (initial adaptive plan)."""
    return df._jdf.queryExecution().toString()


def executed_plan_str(df):
    """The plan that ACTUALLY RAN. Call only after an action that runs `df`'s
    OWN queryExecution, e.g. df.rdd.count() or df.foreach(...) -- NOT
    df.count(). Dataset.count() is implemented as
    `groupBy().count().collect()`, a SEPARATE derived query with its own
    queryExecution, so it never drives df's own AdaptiveSparkPlanExec to
    completion. Read this after df.count() and you will see the pre-AQE
    plan every time, even though AQE genuinely reoptimized -- just on a
    plan instance you never asked to see."""
    return df._jdf.queryExecution().executedPlan().toString()


def detect_join(plan_str):
    """Best-effort: name the join node present in a plan string."""
    for needle in ("BroadcastHashJoin", "SortMergeJoin",
                   "ShuffledHashJoin", "BroadcastNestedLoopJoin"):
        if needle in plan_str:
            return needle
    return "unknown"


def main():
    spark = (
        SparkSession.builder
        .appName(f"lab07-joins-{ALPHA}")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    print(f"=== SD411 lab07  alpha={ALPHA}  shuffle.partitions={SHUFFLE_PARTITIONS} ===")
    print(f"[load] fact = {FACT_PATH}")

    pitches = spark.read.parquet(FACT_PATH)
    pa = spark.read.parquet(PA_PATH)
    teams = spark.read.parquet(TEAMS_PATH)
    print(f"[load] pitches={pitches.count()}  pa_events={pa.count()}  teams={teams.count()}")

    # ========================================================================
    # PART 1 — BROADCAST HASH JOIN
    # Join the pitch fact to the 30-row teams dimension on home_team == team.
    # teams is far under the 10 MB threshold, so Catalyst should broadcast it.
    # ------------------------------------------------------------------------
    # TODO(P1): build `j1` = pitches joined to teams on pitches.home_team == teams.team.
    #           Then print static_plan_str(j1) and detect_join(...) on it.
    #           Measure j1.count() with time_action. Record which side broadcast.
    #
    #   j1 = pitches.join(teams, pitches.home_team == teams.team, "inner")
    #   print(detect_join(static_plan_str(j1)))
    #   time_action("P1 broadcast", lambda: j1.count())
    #
    # QUESTION P1: which table got broadcast, and how do you know from the plan?
    #              Was the BIG side (pitches) shuffled? Check the UI SQL tab.
    # ========================================================================
    # YOUR PART 1 CODE HERE


    # ========================================================================
    # PART 2 — SORT-MERGE JOIN
    # Join the pitch fact to pa_events on (game_pk, at_bat_number). Both sides
    # are above the broadcast threshold. To make the result deterministic (and
    # to keep AQE from converting it — that is Part 4's job), DISABLE broadcast
    # for this part only by setting autoBroadcastJoinThreshold to -1.
    # ------------------------------------------------------------------------
    # TODO(P2): set spark.sql.autoBroadcastJoinThreshold = -1
    #           build `j2` = pitches joined to pa on game_pk AND at_bat_number.
    #           print the plan + detect_join. Count Exchange and Sort nodes.
    #           Measure j2.count(). Record shuffle read/write bytes from the UI.
    #
    #   spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    #   j2 = pitches.join(pa, ["game_pk", "at_bat_number"], "inner")
    #
    # QUESTION P2: how many Exchange (shuffle) nodes? how many Sort nodes?
    #              why does sort-merge need BOTH sides sorted?
    # ========================================================================
    # YOUR PART 2 CODE HERE


    # ========================================================================
    # PART 3 — SHUFFLE-HASH JOIN
    # Same join as Part 2, but force a shuffle-hash join. Catalyst PREFERS
    # sort-merge, so a hint alone is not enough: you must also turn off that
    # preference. The pa side is the smaller (build) side.
    # ------------------------------------------------------------------------
    # TODO(P3): set spark.sql.join.preferSortMergeJoin = false
    #           keep autoBroadcastJoinThreshold = -1 (no broadcast)
    #           hint the SMALLER side: pa.hint("shuffle_hash")
    #           build `j3`, print plan + detect_join. Compare to Part 2:
    #           same shuffles? what is GONE from the plan vs sort-merge?
    #           Measure j3.count() and compare median to Part 2.
    #
    #   spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
    #   j3 = pitches.join(pa.hint("shuffle_hash"), ["game_pk", "at_bat_number"], "inner")
    #
    # QUESTION P3: what did the plan lose vs sort-merge? which side built the
    #              hash table? what is the risk if pa were not ~3x smaller?
    # ========================================================================
    # YOUR PART 3 CODE HERE


    # ========================================================================
    # PART 4 — THE AQE TRAP  (predict-first centerpiece)
    # Re-enable AQE's defaults: restore preferSortMergeJoin and the broadcast
    # threshold to their out-of-the-box values, leave AQE ON (it is on by
    # default). Now join pitches to a FILTERED pa: only home runs.
    #
    # PREDICT from the STATIC plan first (static_plan_str), write it down, get it
    # initialed. THEN run the action and read executed_plan_str + the UI.
    # ------------------------------------------------------------------------
    # TODO(P4): reset spark.sql.join.preferSortMergeJoin = true
    #           reset spark.sql.autoBroadcastJoinThreshold = 10MB (10485760)
    #           pa_hr = pa.where(F.col("events") == "home_run")
    #           j4 = pitches.join(pa_hr, ["game_pk", "at_bat_number"], "inner")
    #           print detect_join(static_plan_str(j4))   # what is PLANNED
    #           j4.rdd.count()                            # run it -- NOT
    #                                                      # j4.count(); see
    #                                                      # the note on
    #                                                      # executed_plan_str
    #           print detect_join(executed_plan_str(j4))  # what actually RAN
    #
    # QUESTION P4: do the planned and executed joins MATCH? if not, what changed
    #              them, and at what point in the query lifecycle? (This is your
    #              Week 8 preview: AQE.)
    # ========================================================================
    # YOUR PART 4 CODE HERE


    spark.stop()
    print("=== lab07 complete ===")


if __name__ == "__main__":
    main()
