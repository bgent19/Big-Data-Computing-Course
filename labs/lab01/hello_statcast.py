"""
SD411 Lab 0 — Hello Statcast
============================

Goal: read the seeded Statcast sample from MinIO via Spark, do a trivial
aggregation, and write the result back to MinIO as Parquet. If this script
runs end-to-end, your stack is fully wired up. From here on, the rest of
the course is just learning what to do with it.

Run from the host shell (after `docker compose up -d`):

    docker exec -it sd411-spark-master spark-submit \\
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \\
        /opt/work/hello_statcast.py

Notes
-----
* The S3A endpoint here is `http://minio:9000` — that's the address as
  seen from INSIDE the spark container, on the sd411-net network. If you
  run pyspark from your laptop instead, the endpoint becomes
  `http://localhost:9000`. We'll dig into why in Module 1.
* `--packages` pulls the hadoop-aws connector once on first run; the
  ivy-cache volume keeps it cached.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# -----------------------------------------------------------------------------
# 1. Configure Spark to talk to MinIO via the S3A connector.
#    These settings are baked in here for Lab 0; we'll examine each one in
#    Module 1 when we discuss object stores.
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("sd411-lab0-hello-statcast")
    # Submit to the standalone cluster running in the spark-master container.
    # Without this, spark-submit defaults to local[*] and the worker
    # container does nothing — which would defeat the point of Lab 0.
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint",            "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",          "sd411admin")
    .config("spark.hadoop.fs.s3a.secret.key",          "sd411password")
    .config("spark.hadoop.fs.s3a.path.style.access",   "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(f"Spark version:  {spark.version}")
print(f"Master:         {spark.sparkContext.master}")
print("=" * 60)


# -----------------------------------------------------------------------------
# 2. Read the seeded Statcast sample.
#    inferSchema is fine here — the sample is small. We'll do better in Lab 2.
# -----------------------------------------------------------------------------
src = "s3a://sd411/raw/statcast_sample.csv"
df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(src)
)
print(f"\nLoaded {src}")
print(f"  Rows:    {df.count():,}")
print(f"  Columns: {len(df.columns)}")
print("  First 10 columns / types:")
for f_ in df.schema.fields[:10]:
    print(f"    {f_.name:<25} {f_.dataType.simpleString()}")


# -----------------------------------------------------------------------------
# 3. Trivial aggregation: pitch counts and average velocity by pitch_type.
#    This is the simplest thing that proves both compute and S3 reads work.
# -----------------------------------------------------------------------------
by_type = (
    df.groupBy("pitch_type")
      .agg(
          F.count("*").alias("n"),
          F.round(F.avg("release_speed"), 1).alias("avg_velo_mph"),
      )
      .orderBy(F.desc("n"))
)
print("\nPitch type breakdown:")
by_type.show(truncate=False)


# -----------------------------------------------------------------------------
# 4. STUDENT TASK — one required edit.
#    Replace YOUR_ALPHA below with your actual alpha code (e.g. "m260042").
#    This is the only line in this file you MUST change. It puts your
#    output under a path unique to you, which is how the instructor
#    confirms in the lab check that this script actually ran on your stack.
# -----------------------------------------------------------------------------
YOUR_ALPHA = "REPLACE_ME"

if YOUR_ALPHA == "REPLACE_ME":
    raise RuntimeError(
        "Edit YOUR_ALPHA in hello_statcast.py before running. "
        "Lab 0 wants you to actually touch this file."
    )

dst = f"s3a://sd411/derived/{YOUR_ALPHA}/pitch_type_summary"
(by_type.write
        .mode("overwrite")
        .parquet(dst))

print(f"\nWrote summary to {dst}")
print("\nLab 0 hello-Statcast: COMPLETE.")

spark.stop()