# SD411 Lab 5 - Catalyst Archaeology: Same Query, Four Ways

| | |
|---|---|
| **Lab session** | Thursday 17 September 2026 |
| **Report due** | before the start of Lab 6 (Thursday 24 September 2026) |
| **Weight** | counts in the Labs bucket (25% of term grade) |
| **Estimated time** | 100–110 min in lab + 60–90 min at home for the memo |

---

## Where we are

In lecture we met DataFrames and Spark SQL, and we made a claim that should have bothered you: a SQL string and a chain of DataFrame method calls are the same query to Spark, because both are handed to an optimizer that rewrites them before anything runs. We did not prove that claim. Today you dig up the evidence yourself.

The optimizer is named Catalyst, and we will walk through its machinery properly. Today comes first on purpose. You will excavate the plans Catalyst produces, annotate them by hand, and form your own theory of what it is doing. When we name the stages in class, you will already have seen the artifacts. That is why this lab is called archaeology: the dig comes before the textbook.

One question anchors the whole session. Over the 2025 season, for pitches thrown at 95.0 mph or harder, what is the pitch count and average spin rate by pitch type? Three columns out of roughly eighty. You will answer it four ways: as a SQL string, as a DataFrame chain, as a DataFrame chain with a Python UDF doing the filtering, and as a raw RDD pipeline with your Lab 4 muscles. Same answer every time. Wildly different costs. Your job is to predict the differences, measure them, and explain them from the plans.

One rule carries over from previous labs and it is not negotiable: predict first, then measure. Every measurement has a prediction box on the worksheet that you fill in ink before you run the command.

## Workflow

### Part 0 - Stack and data

1. Download the [lab05 files](lab05.zip) and unzip them into the sd411 directory
1. `docker compose up -d`, then `./scripts/verify_lab05.sh`. All FAILs fixed before proceeding.
1. If check C10 warned that the Parquet copy is missing, run `scripts/make_parquet.py` (the exact command is in the script header). It can take 2–4 minutes; use the time to fill in predictions P1–P3.

### Part A - Two front doors, one engine

Predictions P1–P3 in ink first. Then complete TODOs A1 and A2 in `scripts/lab05_four_ways.py` and run part `a`. The script saves `work/plans/v_sql.txt` and `work/plans/v_df.txt`. Diff them. Settle P1.

While you are in the plan files, find these five node types and circle them. This legend is all you need today; Monday explains where each comes from.

| Plan node | What it is |
|---|---|
| `Scan parquet` / `Scan csv` | The read. Look for two fields on it: `ReadSchema` (which columns actually leave the file) and `PushedFilters` (which predicates the reader was told about). |
| `Filter` | A row test applied after the scan. |
| `Project` | Column selection or computation. |
| `HashAggregate` | The group-by. It appears twice; note the words `partial` and `final` and think about Lab 4's combiner-style reduceByKey. |
| `Exchange` | Data crossing the network. Every `Exchange` is a shuffle. Count them. |

**Checkpoint.** The instructor initials your worksheet. Predictions P1–P8 must be in ink by now (P4–P8 are predictions about parts you have not run yet; that is the point).

### Part B - Same query, CSV source

Run part `b`. Compare `plans/v_csv.txt` against `plans/v_df.txt`, line by line at the Scan node. Record both timings. Settle P4–P6, then answer M1 and M3 on the worksheet.

### Part C - Breaking Catalyst on purpose

Run part `c`. The UDF computes exactly the same boolean as `release_speed >= 95.0`. Find the new node in `plans/v_udf.txt`, find what happened to `PushedFilters`, record the timing. Settle P7–P8, answer M4.

### Part RDD - No optimizer at all

Complete TODO R1 (this is the Lab 4 pattern with a different reducer) and run part `rdd`. One timing, no plan file, and the worksheet asks you why there is no plan file. Answer M5.

### Close-out

Harvest your plan files before you tear anything down. They are written inside the container's work volume, not in this folder:

```
docker compose cp spark-master:/opt/spark/work/plans ./plans
```

Then check in with your instructor, then `docker compose down` (keep the volume; Lab 6 reuses the Parquet copy).

## Submission

Push to your course repo by the deadline:

- `WORKSHEET.md` (or a scan of the paper worksheet) with all predictions, measurements, and answers M1–M5
- `plans/` with all four plan files (harvested with `docker compose cp`, see close-out)
- Annotated plans for `v_df` and `v_udf` (photo of marked-up printout or annotated PDF, your choice)
- Your completed `lab05_four_ways.py`
- `memo.md` or `memo.pdf` - one page, SD322 memo standard, addressed to a teammate who writes all their Spark filters as Python UDFs "because it's the same logic anyway." Make the case with your own measurements.
- `AI_USAGE.md` - what you used AI for, per the course policy. Permissive with disclosure; you defend everything you submit.

## AI policy reminder

Use AI however you like to understand plans, debug, or draft the memo, and disclose it. But the predictions are yours before any tool runs, the measurements come from your stack, and at the oral check the only processor answering is you.
