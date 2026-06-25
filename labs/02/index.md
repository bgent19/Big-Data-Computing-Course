# Lab 2 -- HDFS vs MinIO

| | |
|---|---|
| **Session** | Thursday, 27 Aug 2026 |
| **Due** | Before the start of Lab 3: Thursday, 3 Sep 2026 |
| **Estimated time** | lab time + 60-90 min for the synthesis memo |
| **Weight** | Standard lab weight within the Labs grade component (25%) |
| **Prerequisite** | Lab 01 passed. No working stack, no Lab 02. |
| **Deliverables** | Completed `WORKSHEET.md`, synthesis memo, two screenshots, AI usage statement |

---

## 1. Why we are doing this

We have spent two weeks of lecture on a single question: where should the
bytes live? In class we built HDFS from first principles, with
its NameNode holding all the metadata and its DataNodes holding all the
blocks. We tore three of its design decisions out and got object
storage. We put names (CAP, PACELC) on the tradeoffs those
designs are making.

So far this is all claims. We claimed renames are free in HDFS and expensive
in an object store. We claimed object storage punishes small files. We
claimed HDFS chooses consistency over availability when nodes die. Sounds
great, right? We said it, so it must be true!

But here is the philosophy for this course: every performance claim gets measured by you,
on your hardware, with your own hands on the clock. Today both systems run
side by side on your machine, and you will catch each one doing exactly what
the architecture diagrams said it would do. By the end of the session you
will have a worksheet of numbers that *you* produced, and a one-page memo
arguing from those numbers. That memo is the deliverable an employer would
care about; the numbers are how you earn the right to write it.

One discipline matters above all the others today: **predict before you
measure**. Every experiment in Part 2 has a prediction row in the worksheet,
and it gets filled in before you touch the keyboard. A wrong prediction
costs you nothing. A skipped one costs you the whole point of the exercise,
because the goal is not the numbers. The goal is calibrating the model in
your head that generates predictions, and that model only updates when it
has committed to a guess and been told it was wrong.

## 2. Part 0 -- Setup and verification

1. Confirm the seed file is in place (same file as Lab 01):

   ```
   ls -lh data/statcast_sample.csv
   # missing? -> cp ../lab01/data/statcast_sample.csv data/
   ```

2. Generate the test corpora (one ~200 MB file, and 500 tiny files):

   ```
   ./scripts/gen_data.sh
   ```

3. Bring the stack up and verify:

   ```
   docker compose up -d
   ./scripts/verify_stack.sh
   ```

All checks 1-11 must PASS before you continue. Check 10 commonly WARNs for
the first minute while the NameNode leaves safemode; re-run the script. If
anything FAILs, notify your intructor.

You now have two complete distributed storage systems running on your VM.
Take five seconds to appreciate that, then open both web UIs and leave them
in tabs; you will use them all session:

- NameNode UI: http://localhost:9870
- MinIO console: http://localhost:9001 (login `minioadmin` / `minioadmin`)

4. Bring up your `WORKSHEET.md` for you to answer as you go through this lab.

## 3. Part 1 -- Explore

The point of this part is to put your hands on the metadata each system
keeps, because the metadata is where the two designs differ most. The data
path is just bytes on disk in both cases.

### 3.1 HDFS tour

Everything HDFS happens inside the namenode container:

```
docker compose exec namenode bash
```

Load the big file and look at what HDFS did with it:

```
hdfs dfs -mkdir /lab02
hdfs dfs -put /data/big.csv /lab02/big.csv
hdfs dfs -ls -h /lab02
hdfs fsck /lab02/big.csv -files -blocks -locations
```

Read the fsck output slowly. It is the NameNode showing you its bookkeeping:
every block, its ID, its length, and which DataNode holds it. Recall from
class that this entire structure lives in the NameNode's heap,
about 150 bytes per file and per block. Answer worksheet questions 1.1 and
1.2 now. While you are in the UI tab, find the same file under Utilities,
then Browse the file system, and take **screenshot 1** of its block listing.

### 3.2 MinIO tour

The mc container is your S3 client (open a second terminal):

```
docker compose exec mc bash
mc alias set local http://minio:9000 minioadmin minioadmin
mc mb local/lab02
mc cp /data/big.csv local/lab02/big.csv
mc ls local/lab02
mc stat local/lab02/big.csv
```

Now compare `mc stat` against your fsck output. One of these systems is
showing you blocks, locations, and replication. The other is showing you a
key, a size, and an ETag. That difference is not MinIO being lazy; it is the
flat key-value model from class, where "which server holds which
bytes" is deliberately not your problem. Answer worksheet question 1.3, and
take **screenshot 2** of the MinIO console showing the lab02 bucket.

MIDN A looks at this and concludes the two systems are basically the same:
you put a file in, you get a file out, who cares about the bookkeeping, right?

## 4. Part 2 -- Measure

Three experiments. For each one: prediction in the worksheet FIRST, then
commands, then measurement, then a mechanism explanation. The mechanism
explanation is the graded part; it should name what happened to the bytes
and the metadata, in the vocabulary from lecture.

### E1 -- The rename test

Lecture claimed an object store has no rename. Lets see what that costs.
Prediction first: how long will each command below take on a 200 MB file,
and why?

In the namenode container:

```
time hdfs dfs -mv /lab02/big.csv /lab02/renamed.csv
time hdfs dfs -mv /lab02/renamed.csv /lab02/big.csv
```

In the mc container:

```
time mc mv local/lab02/big.csv local/lab02/renamed.csv
time mc mv local/lab02/renamed.csv local/lab02/big.csv
```

When you write the mechanism: think about what HDFS had to change (and in
whose memory), versus what MinIO had to do with 200 MB of object data to
make a "rename" appear to happen. This same cost is why Spark jobs that
write to S3 use a different commit protocol than jobs that write to HDFS,
which we will meet again later in the course.

### E2 -- The small-files test

Same total data, two shapes: one file versus 500 files. Predict both
systems' times for both shapes before running anything.

In the namenode container:

```
hdfs dfs -mkdir /small
time hdfs dfs -put /data/big.csv /lab02/big_copy.csv
time hdfs dfs -put /data/small/* /small/
hdfs dfs -rm /lab02/big_copy.csv
```

In the mc container:

```
time mc cp /data/big.csv local/lab02/big_copy.csv
time mc cp --recursive /data/small/ local/lab02/small/
mc rm local/lab02/big_copy.csv
```

Note what you are seeing: the 500 small files are a few megabytes *in
total*, a tiny fraction of big.csv, and yet. The per-file cost you are
measuring (an RPC to the NameNode, an HTTP PUT to MinIO) is paid regardless
of how few bytes ride along with it. Multiply your per-file overhead by the
millions of files a careless pipeline produces and you have the
small-files problem, which we will cover next week.
You are measuring it before we name it.

### E3 -- The LIST test

Who answers "what files are in this directory," and how fast? Predict,
then:

```
# namenode container
time hdfs dfs -ls /small > /dev/null
# mc container
time mc ls local/lab02/small/ > /dev/null
```

For the mechanism, recall where HDFS keeps the namespace (one data
structure, one machine, in RAM) versus how an object store LIST works
(paginated scan over a key range in the metadata service). At 500 objects
both are quick; the question your explanation should answer is which one
degrades gracefully at 50 million objects, and why. This is also exactly
the pain the lakehouse metadata layer (Delta, Iceberg, Hudi) exists to fix.

## 5. Part 3 -- Predict behaviors: the failure drill

We opened with the claim that failure is the common case, so a storage
system's behavior during failure is not an edge case; it is the product.
Lets break ours.

Before each step, predict (write it down) what
the next command will show. Then run it and fill in the E4 timeline in the
worksheet as you go. From the **host** terminal:

```
docker stop lab02-datanode
```

Immediately, in the namenode container:

```
hdfs dfs -cat /lab02/big.csv | head -3
```

Watch it fail, and read the error: the NameNode happily told the client
which block to fetch, and the fetch found nobody home. Now watch the
NameNode figure out what you did. Re-run the following every ~15 seconds
until the picture changes (we tuned heartbeats so death is declared in
roughly 30-60 seconds; production waits 10.5 minutes, worksheet E4.c asks
you why):

```
hdfs dfsadmin -report | head -30
hdfs fsck / | tail -15
```

When fsck starts reporting missing blocks, your data is gone. Or is it?

```
docker start lab02-datanode    # from the host
```

Give it ~30 seconds, re-run fsck, then answer E4.a and E4.b. The
distinction the drill is teaching, between data that is *lost* and data
that is *unavailable*, is the heart of the CAP discussion from class,
and you have now watched a real system land on one side of it.

Restore order before you leave Part 3: `./scripts/verify_stack.sh` should
fully PASS again.

## 6. Stretch goal (optional)(+5 points) -- Spark does not care

If you finish early: start the Spark profile and read the same file through
both storage layers with the same code.

One-time setup. Fetch the S3A connector JARs on the host (You did this in Lab 01):

```
./scripts/download_jars.sh
docker compose --profile spark up -d
```

The `scripts/` directory is already mounted into the master at `/opt/lab02`,
and the JARs at `/opt/spark/extra-jars`, so submit directly:

```
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/lab02/hdfs_vs_s3a.py
```

(Requires big.csv present in both stores, which Parts 1 and 2 already did.)
Record the four timings in the worksheet's stretch table. The interesting
part is not which is faster on your VM; it is that the application code is
identical and only the URI changed. Hold that thought for Unit 2, where
this indifference is what makes decoupled compute-and-storage work.

## 7. Deliverables

Submit, as one folder or archive per the course submission convention:

1. `WORKSHEET.md` -- fully completed, predictions visibly made before
   measurements.
2. `memo.pdf` or `memo.md` -- the synthesis memo described at the bottom of
   the worksheet (<= 1 page).
3. Screenshots 1 and 2.
4. AI usage statement (in the worksheet).

## 8. Glossary of services in this stack

| Service | What it is | UI |
|---|---|---|
| `namenode` | HDFS metadata server; all bookkeeping, no data | :9870 |
| `datanode` | HDFS block server; all data, no bookkeeping | :9864 |
| `minio` | S3-compatible object store | :9001 (console) |
| `mc` | Interactive shell with the MinIO client + lab data | -- |
| `spark-master` / `spark-worker` | Stretch goal only (profile `spark`) | :8080 |

