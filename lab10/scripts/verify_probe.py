#!/usr/bin/env python3
"""
SD411 Lab 10 - verification probe. Called by verify_lab10.sh check C13.

Deliberately submits to spark://spark-master:7077. A probe that runs in
local[*] will pass on a stack whose worker never joined, which is the exact
failure this check exists to catch.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType,
)

PV_DIR = os.environ.get("PV_DIR", "file:///opt/lab10/pageviews")

SCHEMA = StructType([
    StructField("ts", TimestampType(), True),
    StructField("domain", StringType(), True),
    StructField("page", StringType(), True),
    StructField("views", IntegerType(), True),
])

spark = (
    SparkSession.builder
    .appName("sd411-lab10-verify-probe")
    .master("spark://spark-master:7077")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.schema(SCHEMA).parquet(PV_DIR)
    row = df.agg(
        F.count("*").alias("rows"),
        F.min("ts").alias("min_ts"),
        F.max("ts").alias("max_ts"),
    ).collect()[0]

    # .keys() returns a Scala keySet, which Py4J will not iterate. Ask Scala
    # for the size instead of trying to walk the collection from Python.
    executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()
    if row["rows"] == 0:
        print("PROBE_FAIL rows=0")
        sys.exit(1)
    print("PROBE_OK rows=%d span=%s..%s executors_including_driver=%d"
          % (row["rows"], row["min_ts"], row["max_ts"], executors))
except Exception as exc:  # noqa: BLE001
    print("PROBE_FAIL %s: %s" % (type(exc).__name__, str(exc).splitlines()[0]))
    sys.exit(1)
finally:
    spark.stop()
