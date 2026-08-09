# Lab 10 data

**Nothing lives in this directory.** It exists so the packet documents where the
data actually is, and so `.gitignore` has something to point at when somebody
inevitably drops a 40 MB Parquet file here.

## Where the data is

The feed is provisioned once per VM image, outside the lab directory:

```
${SD411_DATA}/pageviews/shard-0000.parquet ... shard-0167.parquet
${SD411_DATA}/pageviews/_manifest.json
```

`docker-compose.yml` mounts that directory read only into both `spark-master`
and `spark-worker` at `/opt/lab10/pageviews`. Both mounts are required: the
streaming file source reads a local path, and a path the executor cannot see
fails at scan time with a message that blames a missing file the driver listed
successfully a second earlier.

## What the data is

Wikimedia Foundation pageviews dumps, `https://dumps.wikimedia.org/other/pageviews/`.
One file per hour, listing every page viewed on any Wikimedia project in that
hour and how many times. Released under CC0 1.0 Public Domain Dedication.

The raw dumps carry every project and the enormous one-view tail. The
provisioning step narrows them to what this lab needs:

| Step | Value | Set by |
|---|---|---|
| Week | 168 consecutive hours from hour 00 UTC | `PV_WEEK_START` |
| Projects | `en` and `en.m` | fixed in `prepare_pageviews.py` |
| Floor | at least `PV_MIN_VIEWS` views in the hour | `PV_MIN_VIEWS`, default 50 |
| Format | Parquet, one flat file per arrival slice | fixed |

Schema:

```
ts       TIMESTAMP   the hour the views happened, UTC
domain   STRING      "en" (desktop) or "en.m" (mobile web)
page     STRING      the article title, underscored
views    INT         views of that page in that hour
```

## A shard is not an hour

The shards are arrival slices. A fraction of each hour's rows is displaced
forward into a later shard, keeping its true event timestamp, so the feed
delivers event time out of order in a fixed and reproducible way. The exact
parameters are in the instructor report, not in `_manifest.json`, because
discovering the disorder is Part 0's job.

Displacement is forward only. No shard ever contains an event time from the
future, which is the property that makes Monday's lecture coherent.

## Rebuilding it

```bash
# 1. fetch the raw dumps (once; 8-9 GB)
./provision/fetch_pageviews.sh

# 2. build the shards (once; deterministic given PV_SEED)
#    --user 0:0 is required, not optional: the apache/spark image defaults to
#    the unprivileged `spark` user (UID 185) and ${SD411_DATA} is root-owned,
#    so without it the run dies with PermissionError on /data/pageviews.
docker run --rm --user 0:0 \
  -v ${SD411_DATA}:/data \
  -v "$(pwd)/provision":/provision:ro \
  ${SPARK_IMAGE} \
  /opt/spark/bin/spark-submit --master 'local[*]' --driver-memory 4g \
    /provision/prepare_pageviews.py
```

Then read `${SD411_DATA}/pageviews_report/spike_report.md` and confirm the
diurnal cycle is visible before you accept the week. Part 1 does not work
without it.

## Why not Statcast

Statcast is the running dataset for Modules 1 and 2 and it is the right one
there. It is the wrong one here for a specific reason: a pitch is an instant,
and the natural window sizes for pitches are minutes, which means a week of
event time is a week of wall clock to replay convincingly. Pageviews arrive
pre-aggregated to the hour, so 168 files carry a full week of event time and a
24-hour window is a real, visible thing rather than a simulation. The diurnal
cycle in the data is also a signal every student already has intuitions about,
which is the same reason Statcast earned its place in Module 2.
