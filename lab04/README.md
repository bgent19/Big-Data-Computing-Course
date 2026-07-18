# SD411 Lab 04 - RDDs From First Principles

| | |
|---|---|
| **Lab session** | Thursday 10 September 2026 |
| **Report due** | before the start of Lab 05 (Thursday 17 September 2026) |
| **Weight** | counts in the Labs bucket (25% of term grade) |
| **Estimated time** | 100–110 min in lab + 60–90 min at home for the writeup |


---

## Where we are

In Wednesday's lecture we met MapReduce and the reason Spark replaced it: instead of writing every intermediate result to disk, Spark records the recipe that produced each dataset and keeps the data in memory when it can. That recipe is the lineage, and the dataset it describes is the RDD. We drew the pictures on the board. Today you run them.

The corpus is real. Every plate appearance in MLB generates a one-line description ("Aaron Judge strikes out swinging."), and the season seed you measured in lab03 carries all of them in the `des` column. A season of baseball, read as text. You will count its words three different ways, and by the end you will be able to look at a chain of RDD operations and predict, before running anything, how many stages Spark will make of it and roughly how many bytes will cross the network. That prediction skill is the whole point of Module 2.

One rule carries over from lab02 and lab03 and it is not negotiable: predict first, then measure. Every measurement in this lab has a prediction box on the worksheet that you fill in before you run the command. Wrong predictions cost you nothing. Missing predictions cost you methodology points. AI cannot replace measurement.

---

## Part 0 - Stack up and corpus generation

```bash
cd lab04
docker compose up -d
./scripts/verify_lab04.sh
```

There is no JAR download step in this lab. The S3A connector JARs were staged once when your VM was provisioned (`/opt/sd411/jars`) and every lab stack mounts them read-only. Check 6 of the verify script confirms they are there before anything tries to use them.

Fix any FAILs. Check 12 will WARN that the corpus is missing; that is your next step:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/lab04/scripts/gen_corpus.py
```

This is an instructor-provided plumbing script. It pulls one description per plate appearance out of the season seed, stacks 80 copies of the season so Part 3's shuffle has real work to do, and writes plain text to `s3a://sd411/corpus/plays/`. It uses DataFrames, which are next week's material; today you are responsible for what it produces, not how. Record the two numbers it prints (unique plate appearances, lines written) in worksheet box P0. Re-run `verify_lab04.sh` and confirm all 13 checks pass.

## Part 1 - Laziness, observed

Open a shell on the cluster:

```bash
docker compose exec spark-master pyspark \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=sd411admin \
  --conf spark.hadoop.fs.s3a.secret.key=sd411password \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
```

Worksheet box P1, prediction first: the corpus is close to a gigabyte. How long will this line take?

```python
lines = sc.textFile("s3a://sd411/corpus/plays").coalesce(8)
```

(The `.coalesce(8)` pins the read to the 8 files `gen_corpus.py` wrote, regardless of how large the corpus grows — otherwise Spark auto-splits large files into more partitions than intended, which distorts the shuffle-byte comparison in Part 3.)

Run it. It returns in milliseconds. Did Spark just read close to a gigabyte in milliseconds? It did not. Nothing has been read. You built a recipe, not a result. Keep building it, timing each line by feel (instant or not-instant is precise enough here):

```python
words = lines.flatMap(lambda l: [t.strip(".,;:!?\"'()") for t in l.lower().split()])
pairs = words.map(lambda w: (w, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
```

Three more instant lines. You have now "computed" a word count over a season of baseball without your worker doing any work at all. Sounds great, right? Free computation! But MIDN A eventually needs an answer, and the moment she asks for one, the bill comes due:

```python
counts.take(5)
```

That one is not instant. Record what you saw in P1 and answer mechanism question M1: which lines were transformations, which was an action, and what exactly did Spark do at the moment of the action that it had refused to do earlier?

One wrinkle before you move on, because it will bite your timings later if you do not understand it: run `lines.count()` and compare its duration to `counts.take(5)`. `take` is allowed to stop early: if five results can be served from one partition, Spark may not touch the rest of the corpus. `count` cannot stop early. When we measure in Part 3, we measure with operations that touch everything, which leads us to the question of what "touching everything" looks like from the inside...

## Part 2 - Stages, narrow and wide

Spark turned your recipe into stages, and the boundary between stages is the most expensive line in your program. Lets find it. First on paper:

```python
print(counts.toDebugString().decode())
```

Read the lineage bottom-up. Worksheet P2: copy the tree, circle where the ShuffledRDD appears, and predict (before running anything) how many stages `counts.count()` will produce. Commit to a number.

Now run `counts.count()` and open the driver UI at http://localhost:4040 (Jobs → click the job → DAG visualization, then the Stages tab). Count the stages. Two, and the boundary sits exactly at `reduceByKey`. `flatMap` and `map` are narrow: each output partition needs only one input partition, so Spark fuses them into a single pass. MIDN A's worker core streams a partition through tokenize-then-pair without ever talking to anyone. `reduceByKey` is wide: the counts for the word "swinging" live partly in every partition, so finishing the count requires moving data between partitions. That movement is the shuffle, and the UI shows you its price tag: find Shuffle Write on stage 0 and Shuffle Read on stage 1 and record both in P2.

Two more predictions, then verify each in the UI:

```python
counts.filter(lambda kv: kv[1] > 100).count()        # prediction: how many stages?
counts.map(lambda kv: (kv[1], kv[0])).sortByKey(False).take(10)   # and now?
```

The first appends a narrow operation, so the stage count holds. The second introduces `sortByKey`, a second wide operation, and the stage count grows. (You will also notice Spark "skipped" stages it recognized as already computed (note it in P2 when you see it; we return to caching in Week 9). When you can predict stage counts cold, record your final P2 answers. But knowing where the shuffle is only raises the real question: how big is it, and can we make it smaller without changing the answer?

## Part 3 - Two shuffles, same answer

Exit the shell (`Ctrl-D`). Open `scripts/lab04_rdds.py`, set your alpha code, and complete TODOs 3a and 3b: the same word count built with `reduceByKey` and with `groupByKey`. Predictions first in worksheet P3: which is faster, and by roughly what factor? Which moves more bytes in the shuffle, and why would byte counts differ at all when the input and the answer are identical?

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/lab04/scripts/lab04_rdds.py
```

The harness gives you three-trial medians for wall time. The shuffle bytes you collect yourself from the Stages tab at http://localhost:4040, one trial of each pipeline, into table P3. Then mechanism question M3: `reduceByKey` combines values on the map side before anything ships, so what crosses the network is at most one pair per distinct word per partition. `groupByKey` ships every single `(word, 1)` pair and combines on the far side. Your numbers should make that sentence concrete: cite them. If `groupByKey` also got uncomfortably close to your worker's 2 GB of memory, that is not a flaw in the lab. Say so in your writeup and explain which operation forced every value for a key to coexist in one place.

## Part 4 - The shape of the distribution

Complete TODO 4a: the top 20 words by count, via `takeOrdered`. Record them in P4 and answer two questions.

M4: why `takeOrdered(20, ...)` instead of `collect()` and a Python sort? Answer in terms of where the work happens and what has to fit in the driver. (Remember from SD311 what happens to a program that materializes a structure far larger than it needs.)

M5: look at what dominates the list. Function words, baseball verbs, and a handful of player names: the distribution has a vicious head and a long tail. Now imagine partitioning this dataset by word, the way `reduceByKey` just did. Are the partitions equal? Which key would you least want to be the worker holding? Write two or three sentences. You have just discovered, in your own measurements, the problem we spend Week 8 solving. Hold that thought.

## Stretch goal (+5 bonus) - Kill a worker, watch lineage pay off

The lecture claimed lineage makes recomputation, not replication, the recovery mechanism. Verify the claim. Start a long job (the groupByKey pipeline is conveniently slow), and mid-flight, from a second terminal:

```bash
docker compose stop spark-worker     # wait ~20 seconds
docker compose start spark-worker
```

Watch the driver console and the UI: task failures, the worker re-registering, lost tasks rescheduled and recomputed from lineage, job completes with the right answer. Screenshot the failed-then-rescheduled tasks and write a paragraph: what did Spark recompute, how did it know what to recompute, and what would MapReduce have done in the same situation? With one worker the job stalls until the worker returns. That is fine, and worth a sentence about why.

---

## Submission (one PDF + one .py)

1. Completed `WORKSHEET.md` exported to PDF: predictions, measurements, mechanism answers M1–M5, friction log.
2. Your completed `lab04_rdds.py` with your alpha code set.
3. Screenshots: the two-stage DAG from Part 2, and the Stages tab showing shuffle bytes for both Part 3 pipelines.
4. AI usage statement, same format as Labs 1–2: what you used, for what, and which parts of the submission you can defend without it. Permissive with disclosure; submission ≠ ownership.
