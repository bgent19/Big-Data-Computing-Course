# SD411 lab02 -- HDFS vs MinIO: Explore, Measure, Predict

| | |
|---|---|
| **Session** | R2, Thursday 27 Aug 2026, 120 minutes |
| **Due** | Before the start of lab03: R3, Thursday 3 Sep 2026 |
| **Estimated time** | 120 min in lab + 60-90 min for the synthesis memo |
| **Weight** | Standard lab weight within the Labs component (25%) |
| **Prerequisite** | lab01 passed. No working stack, no lab02. |
| **Deliverables** | Completed `WORKSHEET.md`, synthesis memo, two screenshots, AI usage statement |

---

## 1. Why we are doing this

We have spent two weeks of lecture on a single question: where should the
bytes live? In Week 1 we built HDFS from first principles, with its NameNode
holding all the metadata and its DataNodes holding all the blocks. We tore three of its design decisions out and got object storage. We put names on the tradeoffs those designs make: CAP, PACELC.

So far this is all claims. We claimed renames are free in HDFS and expensive
in an object store. We claimed object storage punishes small files. We
claimed HDFS chooses consistency over availability when nodes die. Sounds
great, right? I said it, so it must be true.

But this course has a rule: every performance claim gets measured by you, on
your hardware, with your own hands on the clock. Today both systems run side
by side on your VM, and you will catch each one doing exactly what the
architecture diagrams said it would do. By the end of the session you will
have a worksheet of numbers that *you* produced, and a one-page memo arguing
from those numbers. The memo is the deliverable an employer would actually
read. The numbers are how you earn the right to write it.

One discipline matters above all the others today: **predict before you
measure**. Every experiment in Part 2 has a prediction row, and it gets
filled in before you touch the keyboard. A wrong prediction costs you
nothing. A skipped one costs you the whole point of the exercise, because
the goal was never the numbers. The goal is calibrating the model in your
head that *generates* predictions, and that model only updates when it has
committed to a guess and been told it was wrong.

## 2. Part 0 -- Setup and verification

Download the [lab02 files](lab02.zip) and unzip them into the sd411 directory

Work from the `lab02/` directory.

Only one SD411 stack can run at a time; every lab reuses the same host ports.
If lab01's stack is still up, clear it first:

```
../vm-base/scripts/sd411_down_all.sh
```

Stamp this lab's environment from the course-wide `common.env` (it carries
the pinned images, the MinIO credentials, and the worker sizing; nothing in
the compose file is hardcoded):

```
../vm-base/scripts/sync_env.sh
```

Build the small-files corpus. Note what this script does *not* do: it does
not build a big file. The big file is the shared season seed the VM was
provisioned with once, at `/opt/sd411/data/statcast_2025.csv`, mounted into
the containers read-only at `/seed`. Every lab measures against the same
bytes.

```
./scripts/gen_data.sh
```

Bring the stack up and verify:

```
docker compose up -d
./scripts/verify_lab02.sh
```

All checks 1-12 must PASS before you continue. Check 11 commonly WARNs for
the first minute while the NameNode leaves safemode; re-run the script. If
anything FAILs, notify your instructor.

You now have two complete distributed storage systems running on one VM.
Take five seconds to appreciate that, then open both web UIs and leave them
in tabs:

- NameNode UI: http://localhost:9870
- MinIO console: http://localhost:9001 (login `sd411admin` / `sd411password`)

## 3. Part 1 -- Explore

The point of this part is to put your hands on the metadata each system
keeps, because the metadata is where the two designs differ most. The data
path is just bytes on a disk in both cases.

### 3.1 HDFS tour

Everything HDFS happens inside the namenode container:

```
docker compose exec namenode bash
```

Load the seed and look at what HDFS did with it:

```
hdfs dfs -mkdir /lab02
hdfs dfs -put /seed/statcast_2025.csv /lab02/big.csv
hdfs dfs -ls -h /lab02
hdfs fsck /lab02/big.csv -files -blocks -locations
```

Read the fsck output slowly. It is the NameNode showing you its bookkeeping:
every block, its ID, its length, and which DataNode holds it. Recall from
Week 1 that this entire structure lives in the NameNode's heap, roughly 150
bytes per file and per block. Answer worksheet questions 1.1 and 1.2 now.
While you are in the UI tab, find the same file under Utilities, then Browse
the file system, and take **screenshot 1** of its block listing.

### 3.2 MinIO tour

The `mc` container is your S3 client, and the `local` alias is already
registered for you (no `mc alias set` step, and therefore no credential to
typo):

```
docker compose exec mc mc ls local
docker compose exec mc mc cp /seed/statcast_2025.csv local/sd411/lab02/big.csv
docker compose exec mc mc ls local/sd411/lab02/
docker compose exec mc mc stat local/sd411/lab02/big.csv
```

Now compare `mc stat` against your fsck output. One of these systems is
showing you blocks, locations, and replication. The other is showing you a
key, a size, and an ETag. That difference is not MinIO being lazy; it is the
flat key-value model from Monday's lecture, in which "which server holds
which bytes" is deliberately not your problem. Answer worksheet question
1.3, and take **screenshot 2** of the MinIO console showing the `sd411`
bucket.

MIDN A looks at this and concludes the two systems are basically the same:
you put a file in, you get a file out, who cares about the bookkeeping. MIDN
A is about to spend Part 2 being measurably wrong.

## 4. Part 2 -- Measure

Three experiments. For each: prediction in the worksheet FIRST, then the
commands, then the measurement, then a mechanism explanation. The mechanism
explanation is the graded part. "It was faster" is a measurement, not an
explanation. An explanation names what the system did with the bytes and
with the metadata.

### E1 -- The rename test

Lecture claimed an object store has no rename. Lets see what that costs.
Predict first: how long will each command below take on a file this size, and
why?

Inside the namenode container:

```
time hdfs dfs -mv /lab02/big.csv /lab02/renamed.csv
time hdfs dfs -mv /lab02/renamed.csv /lab02/big.csv
```

From the host (each `exec` runs `mc` directly inside the live container, so
you are timing the operation, not a container starting up):

```
time docker compose exec mc mc mv local/sd411/lab02/big.csv local/sd411/lab02/renamed.csv
time docker compose exec mc mc mv local/sd411/lab02/renamed.csv local/sd411/lab02/big.csv
```

When you write the mechanism: think about what HDFS had to change, and in
whose memory, versus what MinIO had to do with a few hundred megabytes of
object data to make a "rename" appear to have happened. This same cost is
why Spark jobs that write to S3 use a different commit protocol than jobs
that write to HDFS, which we meet again in Unit 2.

### E2 -- The small-files test

Two shapes: one big file, versus 500 files that together are a rounding
error next to it. Predict both systems' times for both shapes before running
anything.

Inside the namenode container:

```
hdfs dfs -mkdir /small
time hdfs dfs -put /seed/statcast_2025.csv /lab02/big_copy.csv
time hdfs dfs -put /labdata/small/* /small/
hdfs dfs -rm /lab02/big_copy.csv
```

From the host:

```
time docker compose exec mc mc cp /seed/statcast_2025.csv local/sd411/lab02/big_copy.csv
time docker compose exec mc mc cp --recursive /data/small/ local/sd411/lab02/small/
docker compose exec mc mc rm local/sd411/lab02/big_copy.csv
```

Note what you are seeing. The 500 small files are a couple of megabytes *in
total*, a fraction of a percent of the seed, and yet. The per-file cost you
are measuring (an RPC to the NameNode, an HTTP PUT to MinIO) is paid
regardless of how few bytes ride along with it. Multiply that overhead by
the millions of files a careless pipeline produces and you have the
small-files problem, which next Wednesday's lecture treats in full. You are
measuring it a week before we name it.

### E3 -- The LIST test

Who answers "what files are in this directory," and how fast? Predict, then:

```
# inside the namenode container
time hdfs dfs -ls /small > /dev/null

# from the host
time docker compose exec mc mc ls local/sd411/lab02/small/ > /dev/null
```

For the mechanism, recall where HDFS keeps the namespace (one data structure,
on one machine, in RAM) versus how an object-store LIST works (a paginated
scan over a key range in the metadata service). At 500 objects both are
quick. The question your explanation must answer is which one degrades
gracefully at 50 million objects, and why. This is also precisely the pain
the lakehouse metadata layer (Delta, Iceberg, Hudi) exists to remove.

## 5. Part 3 -- Predict behaviors: the failure drill

We opened with the claim that failure is the common case. A storage
system's behavior during failure is therefore not an edge case; it is the
product. Lets break ours.

Before each step, predict out loud what the next command will show. Then run
it and fill in the E4 timeline as you go. From the **host**:

```
docker stop lab02-datanode
```

Immediately, inside the namenode container:

```
hdfs dfs -cat /lab02/big.csv | head -3
```

Watch it fail, and read the error. The NameNode happily told the client which
block to fetch, and the fetch found nobody home. Now watch the NameNode work
out what you did. Re-run the following every 15 seconds or so until the
picture changes. We tuned the heartbeats so death is declared in roughly
30-60 seconds; production waits 10 minutes 30 seconds, and worksheet E4.c
asks you why:

```
hdfs dfsadmin -report | head -30
hdfs fsck / | tail -15
```

When fsck starts reporting missing blocks, your data is gone. Or is it?

```
docker start lab02-datanode    # from the host
```

Give it ~30 seconds, re-run fsck, then answer E4.a and E4.b. The distinction
this drill is teaching, between data that is *lost* and data that is
*unavailable*, is the heart of the CAP discussion, and you have now
watched a real system land on one side of it.

Restore order before you leave Part 3: `./scripts/verify_lab02.sh` should
fully PASS again.

## 6. Stretch goal (optional) -- Spark does not care

If you finish early, start the Spark profile and read the same file through
both storage layers with identical code.

```
docker compose --profile spark up -d
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/lab02/scripts/hdfs_vs_s3a.py
```

The S3A connector JARs were fetched host-side
once when your VM was provisioned,
and the base compose mounts them at `/opt/spark/extra-jars` for every lab.
That is why we pass `--jars` and never `--packages`.

Requires the big file present in both stores, which Parts 1 and 2 already
did. Record the four timings in the worksheet's stretch table. The
interesting part is not which is faster on one VM; it is that the application
code is identical and only the URI changed. Hold that thought for Unit 2,
where exactly this indifference is what makes decoupled compute and storage
work.

## 7. Deliverables

Submit, per the course submission convention:

1. `WORKSHEET.md`, fully completed, with predictions visibly made before
   measurements.
2. `memo.pdf` or `memo.md`, the synthesis memo described at the bottom of the
   worksheet (one page maximum).
3. Screenshots 1 and 2.
4. AI usage statement (in the worksheet).


## 8. Common gotchas

1. **Check 3 fails: no `.env`.** Every image tag, credential, and port in the
   compose file resolves from it. Stamp it: `../vm-base/scripts/sync_env.sh`. Never
   hand-edit `.env`; edit `common.env` and re-stamp, or your lab drifts away
   from everyone else's.
2. **Check 4 fails: `../vm-base/` missing.** The compose file *inherits* its
   Spark and MinIO services from `vm-base/docker-compose.base.yml`. Pull the
   whole course repo; a copy of `lab02/` on its own cannot start.
3. **Port already in use.** Another SD411 stack (usually lab01) is still up.
   Every lab reuses the same host ports by design. `../vm-base/scripts/sd411_down_all.sh`.
4. **Check 11 stuck in safemode.** Normal for ~60 s after bring-up. Persisting
   past three minutes usually means a half-wiped volume from an interrupted
   run: `docker compose down -v && docker compose up -d` (this wipes HDFS and
   MinIO contents, so you re-run the puts).
5. **`mc: command not found`.** You exec'd into `lab02-minio` (the server)
   instead of `lab02-mc` (the client).
6. **`mc` behaves oddly when you wrap it in a shell.** `mc` is the image's
   ENTRYPOINT, so shell-wrapped invocations can fail *silently*. Use the
   `docker compose exec mc mc <args>` form in this handout, which runs the
   binary directly. If you want an interactive session, `docker compose exec
   mc sh` and then plain `mc ...` inside.
7. **`mc mv` is suspiciously instant.** You probably moved a file that was not
   there. `mc ls local/sd411/lab02/` first.
8. **`hdfs dfs -put /labdata/small/*` says "No such file".** Your host shell
   expanded the glob before the container saw it. Run that command from a
   shell *inside* the namenode container, as written.
9. **Seed check (6) fails or WARNs on size.** Do not substitute a lab01-sized
   single-game sample. The floors exist because E1-E3 timings against a 3 MB
   file measure nothing but noise.
10. **Timings vary run to run.** They will, by 10-30%. Run twice and report
    both, or note the variance. The effects you are measuring are
    order-of-magnitude; they survive the noise comfortably.
11. **E4: `cat` still fails after restarting the DataNode.** Give the block
    report ~30 s to land after re-registration. fsck going healthy is your
    signal, not the container being "up".



## 10. Services in this stack

| Service | What it is | UI |
|---|---|---|
| `namenode` | HDFS metadata server: all bookkeeping, no data | :9870 |
| `datanode` | HDFS block server: all data, no bookkeeping | :9864 |
| `minio` | S3-compatible object store (inherited from vm-base) | :9001 console |
| `minio-init` | One-shot bucket bootstrap; exits 0 and stays exited | -- |
| `mc` | Long-lived MinIO client, `local` alias pre-registered | -- |
| `spark-master` / `spark-worker` | Stretch goal only (profile `spark`) | :8080 |
