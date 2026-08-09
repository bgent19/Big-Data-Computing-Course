# Lab 10 - Windowing on Wikipedia Pageviews

| | |
|---|---|
| **Lab session** | Thursday 29 October 2026 |
| **Report due** | Before the start of lab 11(Tuesday 05 November 2026) |
| **Weight** | counts in the Labs bucket (25% of term grade) |

---

## What this lab is about

Lecture gave you three window shapes and one API. Tumbling windows
tile the timeline, sliding windows overlap it, session windows let the data
draw its own boundaries, and all three are expressed as an ordinary `groupBy`
over a column built from event time. That is the mechanism. This lab is about the
consequences, which the was previously only asserted.

Three of them, specifically. Window size is a resolution decision, and choosing
it badly destroys the signal you built the pipeline to see. Overlap is not free,
and the multiplier is exactly arithmetic you can do in advance. And event-time
grouping is correct under reordering, which sounds like a small claim until you
measure how badly out of order the feed actually is and then discover the
streaming answer matches the batch answer to the row.

There is a fourth consequence, and it is the one that closes the lab. Nothing
we write today ever lets a window go. You are going to watch state grow, batch
after batch, with no mechanism anywhere in the query capable of stopping it.
That is not a bug in your code. It is the missing piece, and a future lecture is
about the thing that supplies it.

---

## The dataset

Wikimedia publishes one file per hour listing every page anybody looked at, how
many times. The provisioned feed on your VM is one week of it, filtered to
English Wikipedia (desktop and mobile web) and to pages with at least 50 views
in an hour, which cuts the enormous one-view tail without touching anything
you would want to analyze.

```
ts       TIMESTAMP   the hour the views happened, UTC
domain   STRING      "en" or "en.m"
page     STRING      the article title
views    INT         views of that page in that hour
```

168 files at `/opt/lab10/pageviews/shard-0000.parquet` through `shard-0167.parquet`,
plus `_manifest.json` describing the extract. The directory is mounted read
only. You cannot break it, and you should not try.

One thing about those files is deliberately not in the manifest, and Part 0
exists to make you find it.

Source and licence: Wikimedia Foundation pageviews dumps, CC0 1.0. Cite it in
your memo the way you would cite any dataset.

---

## Why there is no Kafka in this lab

Lab 09 stood up Kafka and you produced and consumed a real event stream through
it. This lab does not use it, on purpose.

Today's subject is what a window means, and a windowed result you cannot
reproduce is not a measurement. The file source over a fixed directory of 168
shards gives every one of you the same batches, the same window boundaries, and
the same numbers, so a prediction can be right or wrong rather than merely
different. It also removes a second network hop from a lab that already has
five streaming query runs in it.

The query itself does not care. Everything you write today is a `groupBy` over
an event-time column, and swapping `.parquet(dir)` for `.format("kafka")` at the
top changes the source and nothing else. That source independence is the
Structured Streaming claim from lecture, and you are about to use it
without noticing.

---

## Before you start

1. Download the [lab10 files](lab10.zip) and unzip them into the sd411 directory
1. `cd` into `lab10/` and confirm `.env` exists. If not, run `scripts/sync_env.sh ~`
   from the repository root.
2. `docker compose up -d`
3. `scripts/verify_lab10.sh`

Do not proceed past a single FAIL. C05 and C09 in particular are about whether
the Pageviews shards were provisioned on this VM and whether both the driver
and the executor can see them, and every part of this lab reads that directory.

Submit jobs like this:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/lab10/scripts/lab10_windowing.py part0
```

The Structured Streaming tab of the driver UI at `http://localhost:4040` is your
instrument for the whole lab. Keep it open. When a query is running, that tab
shows input rows per batch, batch duration, and the state row count, and Parts
1, 2, and 4 all ask you to read numbers off it.

---

## Part 0 - ground truth and shard forensics

Before you stream anything, read the same files in batch and establish what the
answer is. A streaming query that cannot reproduce the batch answer is wrong,
and you cannot make that comparison without the batch answer in hand.

Then look at what a shard actually contains. You will assume, because it is the
obvious assumption, that shard 37 holds hour 37. Check it. `input_file_name()`
gives you the file each row came from in a batch read, and the minimum and
maximum `ts` per shard will tell you whether the assumption survives.

Write down what you find before you go on. Part 4 depends on it.

**Deliverables:** the summary row, the per-shard table for the first and last
12 shards, the hourly oracle written to MinIO, and worksheet items P0.1 to P0.4.

---

## Part 1 - tumbling windows and the resolution sweep

Build the tumbling aggregation once, as a function of window size, and run it
at 1 hour, 3 hours, 6 hours, and 24 hours.

Predict the number of windows each size will emit before you run any of them.
There is one right answer for each and you can derive all four from the
manifest without touching Spark.

Then look at the bar charts. At 1 hour the diurnal cycle of English Wikipedia
is unmissable. Somewhere between there and 24 hours it stops being visible.
Find where, and be able to say why in terms of the size of the window relative
to the period of the thing you are trying to see.

Then find the burstiest article in the week and repeat at 1 hour and 24 hours.
Ranking pages by peak views will hand you the site's front page, which is
always the biggest thing on Wikipedia and never spikes. You want the pages that
were briefly enormous, which is a different statistic.

Compare those two charts carefully, and do not assume what you will find. The
question is not only whether the spike is still visible at 24 hours. It is what
the 24-hour chart can no longer tell you about it.

**Deliverables:** the four window counts predicted and measured, both bar
charts for your chosen article, and worksheet items P1.1 to P1.5.

---

## Part 2 - sliding windows and the multiplication

A 24-hour window sliding every 3 hours. Predict three numbers before you run it:

- how many windows each event lands in,
- how many windows the query will emit in total,
- the ratio of the sum of views across all windows to the batch total.

Two of those three are what almost everybody expects. One of them is not, and
the gap between the number you predicted and the number you got is the most
useful thing in this part. Do not paper over it on the worksheet. Explain it.

Then add `domain` as a second grouping key and predict the state row count
before running. State is keyed by the pair, and you already know both factors.

**Deliverables:** three predictions and three measurements, the keyed state row
count, and worksheet items P2.1 to P2.5.

---

## Part 3 - session window

Build the streaming session window first, exactly as lecture wrote
it, and run it. Read what comes back very carefully and copy it onto the
worksheet word for word.

Then do the same thing in batch, where nothing is missing, and look at the
sessions you get. Some articles produce one session that runs the entire week.
Some produce three or four. Be able to say what distinguishes them.

Last, without running it: you filtered to popular hours before grouping. Using
numbers you already have from Part 0, estimate how many open sessions the
engine would be holding without that filter. Do not run the unfiltered version
unless everything else is finished, and if you do, be ready to kill it.

**Deliverables:** the verbatim message from the streaming attempt, the batch
session statistics, the unfiltered estimate, and worksheet items P3.1 to P3.4.

---

## Part 4 - state forensics and the reordering proof

Re-run the 1-hour tumbling aggregation, then full outer join its result against
the Part 0 oracle and count the disagreements.

Predict that count first. You measured in Part 0 exactly how out of order this
feed is. Say what you think that reordering did to the answer, then find out.

Then read the state curve the harness prints. Every batch, the number of rows
the query is holding. Count the batches in which that number went down.

Finish with the extrapolation on the worksheet: this same query, run for a year,
and again with a per-page key instead of a global one. Then answer the question
the whole lab has been walking toward, in one sentence: what single piece of
information would let the engine throw a finished window's state away?

You do not have that information today. Monday's lecture is where you get it.

**Deliverables:** the disagreement count, the state curve, the two
extrapolations, and worksheet items P4.1 to P4.5.

---

## Friction log

Fill this in as you go, not at the end. It is worth part of your Part 5 memo
score and it is the only way the lab packet gets better next year.

| Time | What I was doing | What went wrong | How long it cost | How I got out |
|---|---|---|---|---|

---

## Submission

Push to your course repository under `lab10/`:

1. `WORKSHEET.md`, complete, with instructor initials on every prediction block.
2. `lab10_windowing.py`, your version, all TODOs closed.
3. `metrics/*.jsonl`, harvested from the work volume:
   ```bash
   docker compose cp spark-master:/opt/spark/work/lab10/metrics ./metrics
   ```
4. `MEMO.md`, one page maximum.

The memo follows the writing standard the same as every other one in this course.
Lead with the number, then prove it. Your thesis sentence should be a claim
about this pipeline that a reader could act on, supported by two or three
figures you actually measured. The state curve from Part 4 is the strongest
figure in the lab and most of you should build the memo around it.
