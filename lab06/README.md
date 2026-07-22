# Lab 6 - Partitioning Experiments

| | |
|---|---|
| **Lab session** | Thursday 24 September 2026 |
| **Report due** | before the start of Lab 07 (Thursday 01 October 2026) |
| **Weight** | counts in the Labs bucket (25% of term grade) |

## What this lab is about

Lecture made a large claim: partitioning is the dominant performance variable in Spark. Not the optimizer, not the file format, not the hardware. The number of partitions, the layout of partitions on disk, and the choice of partition key together decide more about your job's runtime than anything else you control.

Sounds great, right? But a claim that big does not get to go uninvestigated. Today we put it on the bench. You will build a fact table from the season seed, then run three controlled experiments against it: sweep the partition count and map the runtime curve, race two queries across three different on-disk layouts, and partition by a lopsided key to watch what an unbalanced cluster looks like from the inside.

There is a second thing happening today. Until now every lab has handed you data someone else prepared. In Part 0 you become the producer: the Parquet fact table you write is the same one Lab 7 reads next Thursday when we go after join strategies. The path is a contract, which is why the scaffold names it and tells you not to rename it.

One of the things you observe in Part B has not been named in class yet. That is ok. Measure it today, describe it in your own words, and later you will find out what the rest of the industry calls it.

## Before you start (15 min budget)

1. Download the [lab06 files](lab06.zip) and unzip them into the sd411 directory
2. `cd lab06/`.
3. Clear any stale stack from another lab: `bash ../vm-base/scripts/sd411_down_all.sh`. Every lab reuses the same host ports, so only one SD411 stack runs at a time.
4. `bash ../vm-base/scripts/sync_env.sh ~` if you have not already. This stamps the course `common.env` into `lab06/.env`, which is where every image tag, credential, port, and worker size comes from.
5. `docker compose up -d`
6. `bash scripts/verify_lab06.sh`

All twelve checks are named and print PASS, FAIL, or WARN. Do not start Part 0 with a FAIL outstanding. Check 12 may WARN without blocking you, but read what it says, because Parts A and C require Spark UI evidence.

Jobs run inside the master container so the JARs and the `spark://spark-master:7077` URL resolve. Define this once per terminal:

```
SUBMIT="docker compose exec spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar"
```

then run each part with `$SUBMIT /opt/lab06/scripts/lab06_partitioning.py --part 0` (and `A`, `B`, `C`).

## The discipline, restated

Every experiment has a checkpoint where your predictions go on the worksheet and get initialed before you run it. A wrong prediction with honest reasoning is worth full credit. A correct prediction written after the measurement is worth nothing, because it measured your scrolling speed, not your mental model.

MIDN A predicts the Part A curve dips at 4 partitions and climbs steeply after 200. MIDN B predicts it is flat from 2 onward because "Spark handles it." One of them is more wrong than the other, and both of them will learn exactly where their model breaks when the numbers land. That is the point.

## Part 0 - Build the fact table

The stack seeds one object: the 2025 regular season as CSV. Part 0 reads it, derives `game_date` and `game_month`, and writes a Parquet fact table to the canonical path the scaffold names.

Run it once. Everything in Parts A, B, and C reads it, so all three experiments see identical input, and Lab 7 reads it next week. If you re-run it midway through the lab, your earlier measurements no longer describe the same table.

## Part A - The partition count sweep

The fixed workload is a groupBy aggregation over the whole fact table. You will run it at partition counts of 1, 2, 4, 8, 32, 200, and 1000 and record the median of three trials at each.

Checkpoint first: sketch your predicted runtime curve on the worksheet, mark where the minimum sits, and write one sentence of reasoning for each end. Get it initialed.

Then complete TODOs A1 through A4. A2 is where this lab is most likely to quietly lie to you. Lab 4 taught you that transformations are lazy; if you time `df.repartition(n)` followed by the workload as one block, you measured a shuffle plus a workload and your curve is contaminated. The scaffold tells you this is your problem to solve and asks you to document your solution. Your method goes in the writeup.

While a sweep runs, open the driver UI on port 4040 and collect the task counts A4 asks for. You will find that one stage refuses to care what `n` you chose. The configuration value responsible was mentioned in Wednesday's lecture; connecting it to what you see in the UI is part of your writeup.

## Part B - Three layouts, two queries

Now the variable is not how many partitions, but where the boundaries fall on disk. You will write the same rows three ways: flat (provided), partitioned by `game_month`, and partitioned by `game_date`. Then two queries race across all three: Q1 filters to a single month, Q2 follows a single pitcher across the whole season.

Checkpoint first: predict which layout wins each query and roughly by how much, and predict how many directories the by_date layout creates. Initialed before the races.

The by_date write is going to do something you have seen before. A season is roughly 185 game days, and 185 directories of small Parquet files should set off an alarm installed in Unit 1. Name the problem in your writeup and connect it to why "more partition columns" is not free.

B5 is the discovery task. Capture the physical plan for the month-filtered query on the by_month layout and find the evidence that Spark skipped data it never read. Describe the mechanism in your own words on the worksheet. Do not look up the name; an explanation that obviously paraphrases a blog post is worth less than a clumsy one in your own voice.

## Part C - Skew, observed and named

About one pitch in three is a four-seam fastball. Partition by `pitch_type` and no partition count in the world balances that. C1 through C3 have you measure the imbalance directly and then catch it in the act in the Spark UI: max task duration against median task duration in the aggregation stage.

You are not asked to fix it. Detection is today's skill. The final paragraph of your writeup answers: given the imbalance you measured, what would any fix have to accomplish? Hold that thought until Week 8, where salting and AQE pick it up.

## At home: the writeup

Submit a memo (Writing standards apply: claim up front, evidence behind it, figures labeled) containing:

1. The Part A curve, plotted, with your prediction overlaid, and an explanation of both ends and the minimum in terms of cores, task overhead, and the shuffle setting.
2. The Part B race table (six medians), your by_date directory count and write-time observation tied back to the Module 1 failure mode, and your own-words description of the B5 mechanism with the plan line as evidence.
3. The Part C partition-size table, the max-vs-median task numbers with a UI screenshot, and your "what must a fix accomplish" paragraph.
4. A lecture audit paragraph: one place where your measurements sharpen, complicate, or contradict a number or rule of thumb from lecture. Agreement is allowed but has to be earned with a specific comparison.
5. Methodology notes: your A2 solution, trial counts, anything you would distrust about your own numbers.

AI policy is the course standard: permitted with disclosure, and the oral check assumes you own every line you submit. Submission is not ownership.

## Submission

Push to your course repo by Thu 1 Oct, 1330:

- `scripts/lab06_partitioning.py` (completed)
- `WORKSHEET.md` (photographed or scanned if you wrote on paper; the initialed checkpoints must be legible)
- `results/` (raw RESULT lines, plan capture, UI screenshots)
- `memo.pdf`

