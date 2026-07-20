#!/usr/bin/env python3
# =============================================================================
# SD411 lab05 -- Catalyst archaeology: same query, four ways
#
# The question, fixed for the whole lab:
#
#   Over the 2025 season, for pitches thrown at 95.0 mph or harder, report
#   the pitch count and the average spin rate by pitch_type, busiest type
#   first.
#
# Columns touched: pitch_type, release_speed, release_spin_rate.
# That is 3 columns out of roughly 80. Keep that ratio in mind all lab.
#
# You will produce four versions of this query and one variant of the data
# source. Run inside the master container:
#
#   docker compose exec spark-master /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
#     /opt/lab05/scripts/lab05_four_ways.py <part>
#
# where <part> is one of: a  b  c  rdd
#
# Plans are saved INSIDE the container, in the spark-work volume at
# $SPARK_WORK_DIR/plans. That volume is Docker-managed, not a host folder, so
# harvest them to your VM before you submit:
#
#   docker compose cp spark-master:/opt/spark/work/plans ./plans
#
# RULE THAT DOES NOT BEND: every measurement in this script has a prediction
# box on the worksheet. Fill the box in ink BEFORE you run the part.
# =============================================================================
import csv
import io
import os
import sys
import time
from contextlib import redirect_stdout

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

# S3A configuration comes from the environment (common.env -> .env -> compose).
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
BUCKET   = os.environ.get("S3_BUCKET", "sd411")
ACCESS   = os.environ.get("MINIO_ROOT_USER", "sd411admin")
SECRET   = os.environ.get("MINIO_ROOT_PASSWORD", "sd411password")

CSV = f"s3a://{BUCKET}/raw/statcast_2025.csv"
PQ  = f"s3a://{BUCKET}/parquet/statcast_2025"

# The work volume is Docker-managed (the workdir permission fix). Plans land
# here, not in a host folder; harvest with `docker compose cp` before submitting.
PLAN_DIR = os.path.join(os.environ.get("SPARK_WORK_DIR", "/opt/spark/work"), "plans")
os.makedirs(PLAN_DIR, exist_ok=True)

spark = (
    SparkSession.builder.appName("lab05-four-ways")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # Two deliberate lab settings. Both go on the worksheet.
    # AQE rewrites physical plans at runtime; today we want the static plan
    # Catalyst chose, so we turn AQE off. lab08 turns it back on and you
    # will see why it exists.
    .config("spark.sql.adaptive.enabled", "false")
    # 200 shuffle partitions is the historical default, sized for clusters
    # far larger than ours. 8 keeps task overhead from drowning the signal
    # on a 2-core worker.
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def save_plan(df, name):
    """Capture .explain('formatted') to work/plans/<name>.txt and echo it."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain("formatted")
    text = buf.getvalue()
    with open(f"{PLAN_DIR}/{name}.txt", "w") as f:
        f.write(text)
    print(f"--- plan saved: plans/{name}.txt " + "-" * 30)
    print(text)


def timed(label, fn, runs=3):
    """Run fn() `runs` times, print each wall time, return the result of the
    last run. Record the MEDIAN on your worksheet, and note run 1 separately
    (worksheet question M2 asks why run 1 is different)."""
    result = None
    for i in range(1, runs + 1):
        t0 = time.perf_counter()
        result = fn()
        dt = time.perf_counter() - t0
        print(f"  [{label}] run {i}: {dt:6.2f} s")
    return result


# Two readers for the same season. Note: the CSV reader is given an explicit
# schema-less header read with inferSchema. That inference itself costs a
# full pass; worksheet question M1 asks you to find that pass in the UI.
def read_csv():
    return spark.read.csv(CSV, header=True, inferSchema=True)

def read_parquet():
    return spark.read.parquet(PQ)


# =============================================================================
# PART A -- two front doors, one engine (predictions P1-P3 first)
# =============================================================================
def part_a():
    df = read_parquet()
    df.createOrReplaceTempView("pitches")

    # --- Version 1: SQL string -----------------------------------------------
    # TODO(A1): complete the query. Filter release_speed >= 95.0, group by
    # pitch_type, compute COUNT(*) AS n and AVG(release_spin_rate) AS avg_spin,
    # order by n descending.
    v_sql = spark.sql("""
        SELECT pitch_type,
               -- TODO(A1)
        FROM pitches
        WHERE -- TODO(A1)
        GROUP BY pitch_type
        ORDER BY n DESC
    """)

    # --- Version 2: DataFrame API --------------------------------------------
    # TODO(A2): express the same query with .where / .groupBy / .agg / .orderBy
    v_df = (
        df
        # .where( ... )
        # .groupBy( ... )
        # .agg( ... )
        # .orderBy( ... )
    )

    save_plan(v_sql, "v_sql")
    save_plan(v_df, "v_df")

    print("\nPart A results (both versions, should match row for row):")
    timed("v_sql", lambda: v_sql.collect())
    timed("v_df ", lambda: v_df.collect())
    for row in v_df.collect()[:10]:
        print(f"  {row['pitch_type']}: n={row['n']:,}  avg_spin={row['avg_spin']:.0f}")

    # Worksheet A: diff plans/v_sql.txt against plans/v_df.txt. Identical
    # modulo node numbering? That is prediction P1, settled.


# =============================================================================
# PART B -- the same query against CSV (predictions P4-P6 first)
# =============================================================================
def part_b():
    df_csv = read_csv()

    # TODO(B1): rebuild your Part A DataFrame version against df_csv.
    v_csv = (
        df_csv
        # same chain as v_df
    )

    save_plan(v_csv, "v_csv")
    print("\nPart B timing, CSV source:")
    timed("v_csv", lambda: v_csv.collect())

    # Worksheet B: in plans/v_df.txt find PushedFilters and ReadSchema on the
    # Scan node. Now find them (or their absence) in plans/v_csv.txt. The CSV
    # scan reports the filter too. Worksheet question M3 asks what "pushed"
    # can possibly mean for a row-oriented text file, and why the Parquet
    # number is still smaller.


# =============================================================================
# PART C -- breaking Catalyst on purpose (predictions P7-P8 first)
# =============================================================================
def part_c():
    df = read_parquet()

    # A Python UDF that does exactly what `release_speed >= 95.0` does.
    is_fast = F.udf(lambda v: v is not None and v >= 95.0, BooleanType())

    # TODO(C1): your Part A DataFrame chain, but with the filter expressed
    # through the UDF: .where(is_fast(F.col("release_speed")))
    v_udf = (
        df
        # .where(is_fast(F.col("release_speed")))
        # rest of the chain unchanged
    )

    save_plan(v_udf, "v_udf")
    print("\nPart C timing, UDF filter on Parquet:")
    timed("v_udf", lambda: v_udf.collect())

    # Worksheet C: in plans/v_udf.txt, find the node that did not exist in
    # plans/v_df.txt, and check what happened to PushedFilters on the Scan.
    # Then answer M4: the UDF computes the same boolean. What, precisely,
    # can Catalyst no longer see?


# =============================================================================
# PART RDD -- the version with no optimizer at all (prediction P2 covers this)
# =============================================================================
def part_rdd():
    sc = spark.sparkContext
    lines = sc.textFile(CSV)
    header = lines.first()
    cols = header.split(",")
    i_type = cols.index("pitch_type")
    i_speed = cols.index("release_speed")
    i_spin = cols.index("release_spin_rate")

    def parse(line):
        # A plain line.split(",") is NOT safe on this file: player_name is
        # quoted as "Last, First" in nearly every row, and that embedded
        # comma shifts every column after it (release_spin_rate included)
        # for almost the whole season. csv.reader respects the quoting so
        # the shift doesn't happen. (Worksheet M5 asks what would break the
        # naive approach -- this is it, not a hypothetical.)
        p = next(csv.reader([line]))
        try:
            return (p[i_type], float(p[i_speed]), float(p[i_spin]))
        except (ValueError, IndexError):
            return None

    # TODO(R1): build the pipeline. Filter out the header and bad rows,
    # keep speed >= 95.0, map to (pitch_type, (spin, 1)), reduceByKey to
    # (sum_spin, n), then mapValues to (n, sum_spin/n), and take the top
    # 10 by n with takeOrdered. lab04 muscles, same gym.
    def run():
        rows = (
            lines
            .filter(lambda l: l != header)
            .map(parse)
            .filter(lambda r: r is not None)
            # TODO(R1): the rest
        )
        # return rows.takeOrdered(10, key=lambda kv: -kv[1][0])
        return None

    print("\nPart RDD timing (no plan file: there is no plan):")
    top = timed("v_rdd", run)
    if top:
        for ptype, (n, avg_spin) in top:
            print(f"  {ptype}: n={n:,}  avg_spin={avg_spin:.0f}")

    # Worksheet R: call save_plan on an RDD. You can't. Write one sentence
    # on the worksheet about what that tells you.


if __name__ == "__main__":
    part = sys.argv[1] if len(sys.argv) > 1 else ""
    {"a": part_a, "b": part_b, "c": part_c, "rdd": part_rdd}.get(
        part, lambda: print("usage: lab05_four_ways.py [a|b|c|rdd]")
    )()
    spark.stop()
