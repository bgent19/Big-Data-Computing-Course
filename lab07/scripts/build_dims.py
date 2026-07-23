#!/usr/bin/env python3
"""
SD411 lab07 — dimension builder (Part 0 plumbing).

CONSUMES the fact table lab06 (Partitioning Experiments) writes. Derives the
two tables lab07 joins against:

  ${S3_BUCKET}/dim/teams        30 rows. Tiny. The broadcast side (Part 1).
  ${S3_BUCKET}/fact/pa_events   one row per plate appearance (~180-190K for a
                                single season). Above the 10 MB broadcast
                                threshold, ~4x smaller than the pitch fact.
                                The "medium" side for Parts 2-4.

Instructor plumbing. Students are told what these tables ARE, not asked to
build them. DataFrame transforms here are lab05 material.

Paths, credentials, and the endpoint come from the environment (stamped .env
via sync_env.sh -> compose -> container env). Nothing is hardcoded.

Run from the master. The S3A JARs are provisioned once on the VM at
${SD411_JARS} and mounted by vm-base; this lab no longer fetches its own:

  docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars "$JARS" \
    /opt/lab07/scripts/build_dims.py

Exit 64 if the lab06 fact is absent (distinct from a generic crash), 65 if it
is present but missing required columns.
"""
import os
import sys
from pyspark.sql import SparkSession, functions as F

# ---- everything resolves from the environment -------------------------------
BUCKET = os.environ.get("S3_BUCKET", "sd411")
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "sd411admin")
SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "sd411password")

# CANONICAL cross-lab dependency. lab06 is the WRITER, lab07 follows it.
# Overridable so a lab06 path change is a one-line .env edit, not a code edit.
FACT_PREFIX = os.environ.get("FACT_PREFIX", "fact/pitches")

FACT_PATH = f"s3a://{BUCKET}/{FACT_PREFIX}"
TEAMS_OUT = f"s3a://{BUCKET}/dim/teams"
PA_OUT = f"s3a://{BUCKET}/fact/pa_events"

# Real, stable 30-team league/division map. Keyed by the Statcast 3-letter
# abbreviation used in home_team / away_team.
TEAM_MAP = [
    ("ATL", "NL", "East"), ("MIA", "NL", "East"), ("NYM", "NL", "East"),
    ("PHI", "NL", "East"), ("WSH", "NL", "East"),
    ("CHC", "NL", "Central"), ("CIN", "NL", "Central"), ("MIL", "NL", "Central"),
    ("PIT", "NL", "Central"), ("STL", "NL", "Central"),
    ("ARI", "NL", "West"), ("COL", "NL", "West"), ("LAD", "NL", "West"),
    ("SD", "NL", "West"), ("SF", "NL", "West"),
    ("BAL", "AL", "East"), ("BOS", "AL", "East"), ("NYY", "AL", "East"),
    ("TB", "AL", "East"), ("TOR", "AL", "East"),
    ("CWS", "AL", "Central"), ("CLE", "AL", "Central"), ("DET", "AL", "Central"),
    ("KC", "AL", "Central"), ("MIN", "AL", "Central"),
    ("ATH", "AL", "West"), ("HOU", "AL", "West"), ("LAA", "AL", "West"),
    ("SEA", "AL", "West"), ("TEX", "AL", "West"),
]


def main():
    spark = (
        SparkSession.builder
        .appName("lab07-build-dims")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    print(f"[build_dims] fact  = {FACT_PATH}")
    print(f"[build_dims] teams = {TEAMS_OUT}")
    print(f"[build_dims] pa    = {PA_OUT}")

    try:
        fact = spark.read.parquet(FACT_PATH)
    except Exception as e:  # noqa: BLE001 — any read failure here means "no fact"
        print(f"[build_dims] ERROR: could not read the lab06 fact at {FACT_PATH}",
              file=sys.stderr)
        print(f"[build_dims] underlying: {e}", file=sys.stderr)
        print("[build_dims] Re-run the lab06 writer, or see data/README.md.",
              file=sys.stderr)
        spark.stop()
        sys.exit(64)

    cols = set(fact.columns)
    required = {"game_pk", "at_bat_number", "events", "batter", "home_team"}
    missing = required - cols
    if missing:
        print(f"[build_dims] ERROR: fact is missing columns: {sorted(missing)}",
              file=sys.stderr)
        spark.stop()
        sys.exit(65)

    # --- teams dim: 30 rows. Broadcast candidate. -------------------------
    teams = spark.createDataFrame(TEAM_MAP, schema=["team", "league", "division"])
    teams.coalesce(1).write.mode("overwrite").parquet(TEAMS_OUT)
    print(f"[build_dims] wrote dim/teams ({teams.count()} rows)")

    # --- pa_events: one row per plate appearance. Medium side. ------------
    # The terminal pitch of a PA carries a non-null `events`. Dedup defensively
    # in case a PA spans rows oddly.
    #
    # Column set is intentionally wide (outcome, situational, batted-ball, and
    # pitch-characteristic columns of the terminal pitch) so the UNFILTERED
    # table lands above the 10 MB broadcast threshold, not just above it after
    # Part 4's home_run filter. A narrow ~7-column projection measures under
    # 1 MB at a season's row count and auto-broadcasts on its own, which
    # collapses Part 4: the static planner would already pick
    # BroadcastHashJoin with no filter-selectivity guesswork involved, so
    # there is nothing left for AQE to correct at runtime. Do not narrow this
    # back down without re-verifying s3a://<bucket>/fact/pa_events exceeds
    # 10 MB unfiltered (`mc du`) and that Part 4's home_run-filtered subset
    # still lands well under it.
    keep = [c for c in
            ["game_pk", "at_bat_number", "events", "des", "description",
             "batter", "pitcher", "stand", "p_throws", "home_team", "away_team",
             "bb_type", "hit_location", "launch_speed", "launch_angle",
             "hit_distance_sc", "estimated_ba_using_speedangle",
             "estimated_woba_using_speedangle", "woba_value", "woba_denom",
             "babip_value", "iso_value", "launch_speed_angle", "on_1b",
             "on_2b", "on_3b", "outs_when_up", "inning", "inning_topbot",
             "home_score", "away_score", "bat_score", "fld_score",
             "post_home_score", "post_away_score", "post_bat_score",
             "post_fld_score", "delta_home_win_exp", "delta_run_exp",
             "if_fielding_alignment", "of_fielding_alignment", "game_year",
             "game_month", "game_date", "pitch_number", "balls", "strikes",
             "pfx_x", "pfx_z", "plate_x", "plate_z", "effective_speed",
             "release_speed", "sz_top", "sz_bot", "vx0", "vy0", "vz0", "ax",
             "ay", "az", "hc_x", "hc_y", "spin_axis", "arm_angle",
             "attack_angle", "swing_length", "bat_speed",
             "delta_pitcher_run_exp", "home_win_exp", "bat_win_exp",
             "n_thruorder_pitcher", "release_spin_rate", "release_extension",
             "estimated_slg_using_speedangle", "pitch_name", "pitch_type"]
            if c in cols]
    pa = (fact
          .where(F.col("events").isNotNull())
          .select(*keep)
          .dropDuplicates(["game_pk", "at_bat_number"]))
    # coalesce to a sane file count; do NOT collapse to one file, which would
    # hide the parallel read in Part 2's scan stage.
    pa.coalesce(8).write.mode("overwrite").parquet(PA_OUT)

    n_pa = pa.count()
    n_fact = fact.count()
    ratio = n_fact / max(n_pa, 1)
    print(f"[build_dims] wrote fact/pa_events ({n_pa} rows)")
    print(f"[build_dims] pitch fact {n_fact} rows; fact:pa ratio = {ratio:.1f}x")
    if ratio < 3.0:
        print("[build_dims] WARN: fact:pa ratio < 3x. The Part 3 shuffle-hash "
              "build side needs to be roughly 3x smaller; check the seed size.",
              file=sys.stderr)

    spark.stop()
    print("[build_dims] done.")


if __name__ == "__main__":
    main()
