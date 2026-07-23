# SD411 Lab 07 - Join Strategy Lab

| | |
|---|---|
| **Lab session** | Thursday 01 October 2026 |
| **Report due** | before the start of Lab 08 (Thursday 08 October 2026) |
| **Weight** | counts in the Labs bucket (25% of term grade) |

---

## Where we are

In Lab 06 we made partitioning the thing we measure, and we watched the partition count move the wall clock around on the Statcast fact. In class we drew the line between repartition and coalesce, and then we put a name to the most expensive thing Spark does on your behalf: the shuffle. We know a shuffle moves every row across the network so that rows sharing a key land together.

A join is where shuffles come to live. Two tables, one key, and Spark has to get matching rows into the same place before it can pair them. The interesting part is that Spark has three different ways to do that, and they cost wildly different amounts. Catalyst usually picks for you. Today you take the pick away from it.

We are going to induce all three equi-join strategies on purpose, on real data, and confirm each one two ways: in the physical plan and in the Spark UI. Then we are going to run one more join where the plan you read and the join that actually runs are not the same thing, and you will have to explain why.

**Build-then-measure.** Predict every number before you run it. Post-hoc predictions score zero.

---

## Prerequisites

1. Lab 06 submitted, and its fact table still in MinIO. Lab 07 reads what Lab 06 wrote; the stack refuses to come up without it.

---

## Part 0 - Stack up and build the tables

### 0.1 Bring up the stack and verify

Download the [lab07 files](lab07.zip) and unzip them into the sd411 directory


```bash
cd lab07
bash ../vm-base/scripts/sync_env.sh ~
pose up -d
bash scripts/verify_lab07.sh
```

`verify_lab07.sh` runs 13 checks. The first three gate the plumbing: your `.env` is stamped, `vm-base` is reachable, and `docker compose config` resolves. Do not start Part 1 until it prints `0 FAIL`.

Check 10 is the one that matters most here. Lab 07 seeds no data of its own; it consumes the fact table Lab 06 wrote. If that fact is absent, `minio-init` exits with code 64 (a distinct signal, not a generic crash) and the check tells you to re-run the Lab 06 writer.

### 0.2 Set the submit command once

The S3A JARs live on the VM at `/opt/sd411/jars` and are mounted into the containers. Point `--jars` at them:

```bash
JARS="/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar"
SUBMIT="docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --jars $JARS"
```

Two JAR paths, separated by a comma, no spaces. `--jars` takes one comma-separated argument and does not expand wildcards, so `/opt/sd411/jars/*.jar` will not work.

### 0.3 Build the dimensions

```bash
$SUBMIT /opt/lab07/scripts/build_dims.py
```

This reads the pitch fact and writes two tables you will join against:

- **`dim/teams`** - 30 rows. One per club, with league and division. This is small. Hold onto that word, it is the whole point of Part 1.
- **`fact/pa_events`** - one row per plate appearance, on the order of 180,000 for a season. This is the outcome of each at-bat. It is much smaller than the pitch fact but still too big to broadcast. Hold onto that too.

The script prints the row counts and the fact-to-`pa` ratio. You want that ratio at least 3x; the script warns you if it is not, because Part 3 depends on it.

### 0.4 Edit your alpha code

Open `scripts/lab07_joins.py` and replace `ALPHA = "TODO-..."` with your section's alpha code from the top of this handout. Your printed output carries this code. A submission whose alpha does not match your section is not your own work.

---

## Part 1 - The broadcast hash join

MIDN A wants to know, for every pitch, which division the home club plays in. That is a join: the pitch fact on the left, the 30-row `teams` table on the right, matched on `home_team == team`.

Think about what Spark has to do. The pitch fact is hundreds of thousands of rows spread across the cluster. The `teams` table is 30 rows. Spark has two honest options. It could shuffle both tables by team so matching rows meet, which means dragging every pitch row across the network to sort it by a key with only 30 distinct values. Or it could ship one copy of the tiny 30-row table to every worker and let each worker join its local pitch rows against that copy, with no pitch rows moving at all. The second one is obviously cheaper, and Catalyst knows it. When one side is under `spark.sql.autoBroadcastJoinThreshold` (10 MB by default), Spark **broadcasts** that side and does a broadcast hash join.

> **Predict first.** In WORKSHEET.md, P1: which table gets broadcast? Will the pitch fact be shuffled? Get it initialed.

Now write the Part 1 code in the scaffold:

```python
j1 = pitches.join(teams, pitches.home_team == teams.team, "inner")
print(detect_join(static_plan_str(j1)))
time_action("P1 broadcast", lambda: j1.count())
```

Run it. Then open the Spark UI, click into the application, and open the **SQL / DataFrame** tab. Find the join node.

**What you are confirming:** the plan says `BroadcastHashJoin`. One side has a `BroadcastExchange` above it; the other does not. The pitch fact is read and scanned but never shuffled. The 30-row side is the one that moved.

Sounds great, right? One side is tiny, so we ship it and skip the expensive part. But what happens when both sides are big enough that broadcasting either one would blow up every worker's memory? That is Part 2.

---

## Part 2 - The sort-merge join

Now join the pitch fact to `pa_events` on the pair `(game_pk, at_bat_number)`. Both sides are above the broadcast threshold. Spark cannot ship either one to every worker without risking an out-of-memory failure, so it falls back to the strategy that is safe at any size: shuffle both sides by the join key, sort each partition, and merge.

This is the **sort-merge join**, and it is Spark's default for two large tables. It is safe because nothing has to fit in memory all at once. Both sides stream through sorted, and the merge walks them like a zipper.

To keep this part clean and deterministic, disable broadcast for Part 2 only:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
j2 = pitches.join(pa, ["game_pk", "at_bat_number"], "inner")
print(detect_join(static_plan_str(j2)))
time_action("P2 sort-merge", lambda: j2.count())
```

> **Predict first.** P2: how many `Exchange` (shuffle) nodes will the plan have? How many `Sort` nodes? Initial it.

Read the plan and the UI. A sort-merge join has two `Exchange hashpartitioning` nodes (one per side) and two `Sort` nodes (one per side), feeding one `SortMergeJoin`. In the UI's Stages tab you will see the shuffle read and shuffle write bytes on both sides. Record them; you will compare against Part 3.

This is the safe default. But notice what we paid for safety: we sorted both sides. Sorting is not free. If one side is small enough to build a hash table in memory, the sort is wasted work. Can we keep the safety of shuffling both sides but skip the sort? That is Part 3.

---

## Part 3 - The shuffle-hash join

Same join as Part 2, but this time we want a **shuffle-hash join**: shuffle both sides by key as before, then on each partition build an in-memory hash table from the smaller side and probe it with the larger side. No sort. When the small side genuinely fits per partition, this beats sort-merge.

Here is the catch, and it is the lesson. Catalyst does not want to give you this join. `spark.sql.join.preferSortMergeJoin` is `true` by default, because sort-merge is the memory-safe choice and the hash build side can OOM if it is too big. A hint by itself will not override that preference. You have to turn the preference off **and** point Catalyst at the smaller side.

```python
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
# leave autoBroadcastJoinThreshold at -1 so it cannot sneak a broadcast in
j3 = pitches.join(pa.hint("shuffle_hash"), ["game_pk", "at_bat_number"], "inner")
print(detect_join(static_plan_str(j3)))
time_action("P3 shuffle-hash", lambda: j3.count())
```

> **Predict first.** P3: compared to the sort-merge plan, what node disappears? Will the median time go up or down? Initial it.

Read the plan. You still have two `Exchange` nodes, because both sides still shuffle by key. But the two `Sort` nodes are gone, and the join node now says `ShuffledHashJoin`. The `pa` side is the build side; it is the one whose partitions become hash tables.

Compare your Part 2 and Part 3 medians and shuffle bytes side by side in the worksheet. The shuffle bytes should be about the same (both shuffle both sides). The time difference is the sort you skipped.

One more thing, and it is why Catalyst was reluctant. The hash build side has to fit in memory per partition. We hinted `pa` because it is the smaller side, about four times smaller than the fact. If you hinted the pitch fact instead, or if `pa` were not actually smaller, the build side would not fit on the worker and the task would OOM. The stretch goal lets you trigger that on purpose. Skew can do the same thing without you asking, more to follow...

---

## Part 4 - The AQE trap

Reset the two knobs you changed back to their defaults:

```python
spark.conf.set("spark.sql.join.preferSortMergeJoin", True)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10 MB
```

Now join the pitch fact to a **filtered** `pa`: only the plate appearances that ended in a home run.

```python
pa_hr = pa.where(F.col("events") == "home_run")
j4 = pitches.join(pa_hr, ["game_pk", "at_bat_number"], "inner")
```

Before you run anything, read the static plan and predict the join strategy:

```python
print("PLANNED:", detect_join(static_plan_str(j4)))
```

> **Predict first.** P4: from the static plan, which join is Spark going to use? Write it down and get it initialed. Do not run the action yet.

Home runs are rare, a few thousand in a season out of hundreds of thousands of plate appearances. The filtered `pa_hr` side, once the filter actually runs, is tiny, far under the broadcast threshold. But Catalyst plans the query before it knows that. Without table statistics it estimates the filtered side from the full `pa` size, which is over the threshold, so the planned join is a sort-merge join.

Now run the action and read the plan that actually ran. Use `j4.rdd.count()`,
not `j4.count()`. `Dataset.count()` runs a separate `groupBy().count()`
query under the hood, not `j4`'s own plan, so `executed_plan_str(j4)`
afterward would still show the pre-AQE plan even though AQE really did
reoptimize:

```python
j4.rdd.count()
print("EXECUTED:", detect_join(executed_plan_str(j4)))
```

Open the UI SQL tab and look at the join node. The planned join and the executed join are not the same. **Adaptive Query Execution** watched the filtered side's real size come out of its shuffle, saw it was tiny, and rewrote the sort-merge join into a broadcast hash join at runtime, after planning but before the join ran.

This is the thing to sit with. The plan you get from `explain()` is a plan, not a promise. When AQE is on, and it is on by default, the join you read is the join Spark intended at compile time, and the join in the UI is the join that ran. When they disagree, you trust the UI.

We are going to spend time on what AQE does and when it helps. For now, you have seen the headline: the optimizer keeps optimizing after the music starts.

---

## Submission

Your submission is the following:

1. `scripts/lab07_joins.py` with your four parts filled in and your alpha code set.
2. `WORKSHEET.md` with every prediction initialed before its measurement, the P1–P4 tables, and the mechanism answers M1–M5.
3. Four Spark UI screenshots, one per part, each showing the join node in the SQL tab. The Part 4 screenshot must show the executed (final) plan.
4. The analysis memo (see below).

### The analysis memo (Course writing standard)

One page. Lead with the number, then prove it. The number is your Part 2 vs Part 3 median comparison: how much did dropping the sort buy you, and why. Then, accorfiding to our writing standard, explain the Part 4 result to a teammate who has not taken this course: what they would see if they only read `explain()`, what actually ran, and how they would have caught it. Cite your own measured numbers. No marketing adjectives. If a plan node is doing work, name the work.
