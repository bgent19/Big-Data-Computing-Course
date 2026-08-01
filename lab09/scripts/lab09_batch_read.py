#!/usr/bin/env python3
r"""
SD411 Lab 09 - Part 6 STRETCH scaffold (+5 pts). Optional.

Kafka computes nothing. This is the first line of code in the course that
makes an engine subscribe to a log, and it does it in the easiest possible
mode: a BATCH read with fixed start and end offsets, which turns the stream
into an ordinary bounded DataFrame. Next week's lab drops the endingOffsets
and everything gets harder.

Bring up the Spark services first:

    docker compose --profile spark up -d

Then submit (one line; note the JARs are comma-separated explicit paths, not
a glob, because --jars takes a single argument and does not expand globs):

    docker compose exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/extra-jars/kafka-clients-3.4.1.jar,/opt/spark/extra-jars/commons-pool2-2.11.1.jar \
      /opt/lab09/scripts/lab09_batch_read.py

Report in the WORKSHEET: total row count, Spark's per-partition counts, and
whether they match the end_offsets() you recorded in Part 3.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TOPIC = "gh.keyed.type"
BOOTSTRAP = "kafka:9092"


def main():
    # .master() is not optional. Without it spark-submit silently runs
    # local[*], the worker idles, and the Spark UI has nothing to show at the
    # oral check.
    spark = (SparkSession.builder
             .appName("lab09-batch-read")
             .master("spark://spark-master:7077")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # TODO 6.1  Read TOPIC as a BATCH DataFrame.
    #           format("kafka"), option("kafka.bootstrap.servers", BOOTSTRAP),
    #           option("subscribe", TOPIC),
    #           option("startingOffsets", "earliest"),
    #           option("endingOffsets", "latest").
    #           Use spark.read, not spark.readStream. That single word is the
    #           difference between a bounded job and one that never ends.
    raw = None
    raise NotImplementedError("TODO 6.1")

    # TODO 6.2  Print the schema. Kafka hands Spark seven columns and not one
    #           of them knows anything about a GitHub event: five are log
    #           coordinates and two are opaque bytes. Name the column that
    #           holds your event body and the column that holds your key, and
    #           note their types.

    # TODO 6.3  Cast key and value from binary to string and count rows per
    #           Kafka partition. Compare against your Part 3 end offsets.

    # TODO 6.4  Parse the JSON value with from_json and a schema you declare,
    #           then count events by type. You now have an answer Kafka could
    #           not have given you. That is the boundary the lab is about.

    spark.stop()


if __name__ == "__main__":
    main()
