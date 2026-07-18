# SD411 Lab 3 - Build and Measure: CSV vs Parquet on Statcast

| | |
|---|---|
| **Lab session** | Thursday, 3 September 2026 (Week 3 lab slot, 120 min) |
| **Report due** | Before the start of Lab 4 (Thursday, 10 September 2026) |
| **Weight** | Graded against the rubric below; counts in the Labs bucket (25% of term grade) |
| **Prerequisites** | Lab 1 pass, Lab 2 submitted, Week 3 lecture notes read (row vs columnar; compression and small files) |
| **Estimated time** | 120 min in lab (Parts A and B, start of C) + 2 to 3 hours outside lab (finish C and D, write the report) |

## Before you start: prerequisite verification

Run the checks. All of them. The script tells you what to fix and where to look.

```bash
cd lab03
```

If this is the first time `lab03/` has existed on your VM (e.g. it was added to the repo after your VM was provisioned), stamp its `.env` first — `docker compose up` needs it to resolve image tags and credentials:

```bash
../vm-base/scripts/sync_env.sh
```

Then:

```bash
docker compose up -d
./scripts/verify_lab03.sh
```

Thirteen checks must come back PASS (two are allowed to WARN; the script explains which). If check 10 warns that your seed looks like the Lab 1 sample, stop and re-seed. Benchmarking 4,000 rows tells you nothing; the whole point of this lab is that the differences only show up at scale.

Two of the checks (0a, 0b) confirm the shared plumbing every lab this term relies on: your `lab03/.env` was stamped from the course `common.env`, and the `vm-base/` base compose that this lab inherits from is reachable. If either fails, you are missing a provisioning step, not a lab step; the check tells you which script to run.

## The problem

In lecture we made a claim: choosing the right file format on a large dataset is often a 20× speedup over choosing the wrong one. We showed you the machinery that makes the claim plausible. Row groups, column chunks, footer statistics, dictionary encoding, codec tradeoffs. We also gave you some rule-of-thumb numbers, like "snappy typically gets 2 to 3× compression on Statcast."

You should not believe any of it yet.

This course has a standing rule: every performance claim is empirically verified by the person making it. This week, that person is you. You have a full season of Statcast pitch data sitting in MinIO as one CSV, roughly 700,000 rows by about 120 columns. By the end of this lab you will have written that season in six different physical layouts, raced the same three queries across them, opened up a Parquet footer with your own hands, and produced numbers that either confirm the lecture or contradict it. Both outcomes are acceptable. Numbers you cannot defend are not.

One framing before you start. MIDN A runs a query and writes down "Parquet took 4.1 seconds." MIDN B runs the same query three times, notices the second run took half as long as the first, figures out why, and reports the median with a sentence about cache effects. MIDN A has a number. MIDN B has a measurement. This lab grades measurements.

## Part A - Write the season six ways (in lab, ~35 min)

Open `scripts/lab03_benchmark.py`. The timing harness, the size helper, and the results table are provided; your work is in the TODO blocks. First edit `ALPHA` to your alpha code. Your output paths derive from it, same rule as Lab 1: an unedited alpha is an automatic resubmission.

The scaffold reads the raw season CSV and counts it. Then TODO(A1) through TODO(A3): write the dataframe as gzip CSV, uncompressed Parquet, snappy Parquet, gzip Parquet, and zstd Parquet; time every write; record every size; compute every compression ratio against the raw CSV.

Run it:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars $(echo /opt/spark/extra-jars/*.jar | tr ' ' ',') \
  /opt/lab03/scripts/lab03_benchmark.py
```

We pass `--jars` from the centralized JAR mount, not `--packages`. The lab network intercepts TLS, so Maven cannot resolve the S3A connector on the fly; those JARs were fetched once on the host (into `/opt/sd411/jars`, per `common.env`'s `${SD411_JARS}`) and mounted into the containers at `/opt/spark/extra-jars` — that container path is what `--jars` needs, since `/opt/sd411/jars` only exists on the host. If you ever see a `ClassNotFoundException` for `s3a`, the JARs are the first thing to check, not your code.

What you should be looking at when the numbers come out: which codec wins on size, which wins on write time, and whether they are the same codec. They will not be. That tension is the entire reason codecs are a knob and not a constant.

## Part B - Race the queries (in lab, ~40 min)

Three queries, defined for you in the scaffold:

- **Q1**: full count of the season.
- **Q2**: average of a single column (`release_speed`).
- **Q3**: count of rows where `release_speed > 100`. Triple digits is rare; most row groups contain none.

TODO(B1): run each against the raw CSV and against snappy Parquet, three trials each, using the provided `run_trials` helper. Before each cell runs, write down a prediction. Q1 is a fair fight. Q2 is not, and you know exactly why from the Week 3 Day 1 notes. Q3 is where the footer statistics earn their keep.

TODO(B2) is the one students remember. Run Q1 against the gzip CSV you wrote in Part A and watch the task count in the application UI at `http://localhost:4040` while it runs. Compare it to the task count for the raw CSV. The Week 3 Day 2 notes told you gzip is not splittable. Now you get to watch what that costs.

TODO(B3): print the physical plan for the Q3 filter on Parquet and find the `PushedFilters` line. That line is the optimizer telling you it intends to use the row-group statistics you are about to inspect in Part C.

A measurement discipline note, and it is graded: trial 1 and trial 3 of the same query will differ. The OS is caching object bytes under MinIO; the JVM warms up. You do not need to eliminate these effects on a laptop stack. You need to notice them, report medians, and say one intelligent sentence about them. Pretending your three trials were identical when your own table shows they were not is the fastest way to lose methodology points.

## Part C - The metadata autopsy (start in lab, finish at home, ~30 min)

So far Parquet has been a black box that happens to be fast. Lets open it.

Pull one part-file of your snappy Parquet output into the shared work volume, then run the inspector inside the master container. pyarrow does not ship with our pinned `apache/spark:3.5.3-python3` image (despite what you may have heard); `verify_lab03.sh` check 11 will WARN if it's missing. Install it once with `docker compose exec spark-master pip3 install pyarrow`.

```bash
docker compose --profile tools run --rm mc
# inside the mc shell (this writes into the shared spark-work volume):
mc ls local/sd411/lab03/<your alpha>/parquet_snappy/
# mc does not glob wildcards in `cp`; copy the exact filename from the ls output above
mc cp local/sd411/lab03/<your alpha>/parquet_snappy/<exact-filename-from-ls-above> /work/
exit

docker compose exec spark-master python3 \
  /opt/lab03/scripts/inspect_parquet.py part-00000-*.parquet
```

Everything it prints comes from the footer. No row data is decoded. You will see the row groups, the encodings chosen for `pitch_type` and `release_speed`, the per-chunk min/max statistics, and the footer's own size in bytes. The script ends with four questions, C1 through C4. Answer them in your report. C3 is the one that ties Part B to Part C: you will point at a specific min/max pair and a specific query and explain the skip.

## Part D - Break it with small files (at home, ~20 min)

Sounds great, right? Parquet wins, snappy is a sensible default, ship it. But the format is only half of the layout decision. The other half is how many files you cut the data into, and that one is entirely in your hands every time you call `repartition`.

TODO(D1) and TODO(D2): write the same snappy Parquet as 4 files and as 400 files, then race Q1 across both layouts. The bytes are nearly identical. The times will not be. TODO(D3) asks you to connect what you measured to the small-files argument from lecture, including the half of the argument this stack cannot demonstrate, and to say in one sentence why it cannot.

## The 20-minute rule

Same protocol as Lab 1 and Lab 2. If you are stuck on the same error for 20 minutes:

1. Re-run `./scripts/verify_lab03.sh` and read what it says.
2. Check the Common Gotchas table below.
3. Ask the person next to you (allowed and encouraged; this is collaboration, not copying; your numbers and your report are your own).
4. Ask the instructor, in lab or by email, with the exact command you ran and the exact error text.

Two hours of silent suffering is not toughness, it is bad engineering process.

## Common gotchas

| # | Symptom | What is actually happening |
|---|---|---|
| 1 | Every query takes 0.1 s | You timed a transformation, not an action. Spark is lazy; nothing ran. The provided queries end in `count()` or `collect()` for exactly this reason. |
| 2 | Part A read takes wildly longer than your neighbor's | `inferSchema` makes a full extra pass over the CSV to guess types. The scaffold does it once on purpose so Part B can reuse the schema. If you re-infer in Part B you are benchmarking schema inference, not scans. |
| 3 | Trial 2 is twice as fast as trial 1 | Page cache and JVM warmup. Expected. Report medians and mention it. |
| 4 | gzip CSV scan uses 1 task no matter what | Not a bug. That is splittability, and it is the answer to TODO(B2). |
| 5 | `ClassNotFoundException: org.apache.hadoop.fs.s3a` | Either the `--jars` glob did not expand, or you globbed the wrong path. `/opt/sd411/jars` is a **host** path; inside the containers the JARs are mounted at `/opt/spark/extra-jars` — that's the path `--jars` needs. Do not switch to `--packages` (the proxy blocks it). |
| 6 | Writes succeed but `path_size_mb` returns ~0 | Check the path string. A typo writes to a new prefix; MinIO will happily create it. Use the mc shell and `mc ls local/sd411/lab03/` to see what you actually wrote. |
| 7 | zstd write fails or is missing | Codec support varies by Spark build. If `compression=zstd` errors on our pinned image, record the error in your report, drop zstd from your table, and say so. A documented limitation beats a fabricated number. |
| 8 | 400-file write is slow and the UI shows a shuffle | `repartition(400)` shuffles by design. That is part of the cost you are measuring; do not "fix" it with coalesce, which would defeat the experiment. |
| 9 | Part C: cannot find the part-file | The `mc cp` writes into the shared `spark-work` volume; the `exec` into spark-master lands in that same volume's working directory, so a bare `part-00000-*.parquet` resolves. If it does not, run `docker compose exec spark-master ls` to see what landed. |
| 10 | Port already in use on `up` | A previous lab's stack is still running; every lab reuses the same ports. Run the course down-all script before switching labs. |

## What you submit

One PDF report plus one results file, on the course submission portal, before the start of Lab 4.

**Report (3 to 5 pages, HDI standards apply):**

1. **Methodology** (half page): what you ran, how many trials, what you did about caching effects, anything you changed and why.
2. **Part A table**: format, write time, size MB, compression ratio. One paragraph: which codec would you pick as this course's default and what are you trading away.
3. **Part B table**: query × format × trials × median. Your three predictions, written before you ran, and whether they held. The gzip task-count observation. The `PushedFilters` excerpt with two sentences of explanation.
4. **Part C answers**: C1 through C4, each two to four sentences, citing actual numbers from your inspector output.
5. **Part D**: both timings, the task counts, and the two-halves answer from TODO(D3).
6. **The lecture audit** (one paragraph): the Week 3 notes claimed snappy gets roughly 2 to 3× on Statcast and that format choice can approach a 20× speedup on the right query. Do your numbers support, refine, or contradict those claims? Be specific. "Confirmed" with no numbers is worth zero.
7. **AI usage statement**: same policy as every artifact in this course. Disclose what you used and for what. You must be able to defend everything you submit. Submission ≠ ownership.

**Results file:** the full `RESULTS TABLE` block printed by your benchmark run, pasted verbatim into a text file named `<alpha>_lab03_results.txt`.

## Rubric (100 points, published now, same as it will be applied)

| Component | Points | What earns them |
|---|---|---|
| Pipeline correctness | 20 | All six formats written under your alpha prefix; benchmark script runs end to end; results table present and internally consistent. |
| Measurement methodology | 25 | Three trials per cell; medians reported; caching effects acknowledged honestly; predictions written before results; nothing timed lazily (gotcha #1). |
| Analysis quality | 25 | Part B explanations are mechanistic, not vibes. "Parquet was faster because it is columnar" earns half credit at best; naming column pruning, pushdown, or parsing cost with your own numbers earns the rest. |
| Metadata autopsy (Part C) | 15 | C1 to C4 answered with specific values from your own file, correctly connected to row groups, encodings, and statistics. |
| Small files (Part D) | 10 | Both layouts measured; both halves of the argument addressed; the unmeasurable half correctly identified. |
| Communication | 5 | HDI standards: labeled tables, units everywhere, no screenshots of terminals where a table belongs. |

**Oral spot checks**: during Lab 4, the instructor will pull several students at random and ask one question about their own Lab 3 numbers ("your Q2 sped up 14× on Parquet; walk me through where the 14 comes from"). Inability to explain your own submitted numbers caps the lab at 50 points pending a resubmission meeting. This is not a trap; it is the course philosophy. You measured it, you own it.

## Where this leaves us

You now have a season of Statcast sitting in MinIO in a format you chose on purpose, with measurements to justify the choice. Every Module 2 lab reads from that Parquet. And the first thing Module 2 will ask is a question this lab quietly raised and did not answer: you saw that 4 files and 400 files scan very differently, which means somebody has to decide the parallelism of every job. Next week we meet the engine that makes that decision, and we start learning how to argue with it.
