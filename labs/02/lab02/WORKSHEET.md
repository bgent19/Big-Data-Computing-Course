# SD411 Lab 2 Worksheet -- HDFS vs MinIO

**Name (alpha):** ______________  **Date:** ______________

Rules of engagement: the Prediction column is filled in BEFORE you run the
experiment, in pen-equivalent (no editing after measurement). A wrong
prediction with a correct explanation of why it was wrong earns full credit.
A "prediction" written after the fact earns zero for that row and a
conversation about why. We are training your performance intuition; the
intuition only improves if you commit to a guess first.

---

## Part 1 -- Exploration observations

**1.1** Run `hdfs fsck /lab02/big.csv -files -blocks -locations` inside the
namenode. How many blocks does big.csv occupy, and what is the block size?

> 

**1.2** The stack runs with `dfs.replication=1`. State why replication factor
3 is impossible in this stack, in one sentence.

> 

**1.3** Run `mc stat local/lab02/big.csv` inside the mc container. Name one
piece of metadata HDFS showed you in 1.1 that MinIO does NOT show here, and
say why an object store has no equivalent.

> 

---

## Part 2 -- Predict, measure, explain

For each experiment: write the prediction, run the commands from the README,
record the measurement, then explain the mechanism in 2-3 sentences. "It was
faster" is a measurement, not an explanation. The explanation names what the
system did with the bytes and the metadata.

### E1 -- Rename a 200 MB file

| | HDFS (`hdfs dfs -mv`) | MinIO (`mc mv`) |
|---|---|---|
| Prediction (time, and why) | | |
| Measured | | |

Mechanism (what actually happened to the bytes in each system?):

> 

### E2 -- Upload 500 small files vs 1 big file (same total path, wildly different shape)

| | 1 x big.csv | 500 x small files |
|---|---|---|
| Prediction: HDFS upload time | | |
| Measured: HDFS upload time | | |
| Prediction: MinIO upload time | | |
| Measured: MinIO upload time | | |

Mechanism (what cost is paid per file/object, independent of its size?):

> 

### E3 -- LIST at scale

| | HDFS `hdfs dfs -ls /small` | MinIO `mc ls local/lab02/small/` |
|---|---|---|
| Prediction (which is faster, why) | | |
| Measured | | |

Mechanism (who answers a LIST in each system, and from what data structure?):

> 

### E4 -- Kill the DataNode

Timeline of observations. Record actual clock times.

| Event | Time | What you observed (command + output summary) |
|---|---|---|
| `docker stop lab02-datanode` issued | | |
| First `hdfs dfs -cat` attempt | | |
| NameNode marks DataNode dead (UI or `dfsadmin -report`) | | |
| `fsck /` output after death detected | | |
| `docker start lab02-datanode` issued | | |
| `fsck /` healthy again | | |

**E4.a** During the outage, was the data *lost* or *unavailable*? What
specific evidence from your timeline supports your answer?

> 

**E4.b** In CAP terms from Wednesday's lecture: when the only DataNode died,
which property did HDFS give up, and which did it keep? One sentence each.

> 

**E4.c** Production HDFS waits 10 minutes 30 seconds before declaring a
DataNode dead (we shortened it to ~30 s for this lab). Give one reason the
production default is *deliberately* slow.

> 

---

## Stretch (optional)(+ 5 points) -- Spark reads both

| Run | HDFS wall time | S3A wall time |
|---|---|---|
| Cold (first) | | |
| Warm (second) | | |

Which difference is the storage layer and which is the cache?

> 

---

## Synthesis memo (attach separately, <= 1 page)

Scenario: a unit is standing up analytics on five years of Statcast-scale
data (~50 GB raw, growing ~10 GB/year, read-heavy, batch queries). They can
afford either a 3-node HDFS cluster or object storage with on-demand compute.
Recommend one. Your recommendation must cite at least two of YOUR OWN
measurements from this worksheet as evidence, acknowledge one way the localhost
environment limits how far your numbers generalize, and close with what you
would measure next on real hardware before committing. Publication-quality
prose per the Human Data Interaction writing standard; this is the part of the lab a future
employer would actually read.

---

## AI usage statement

Per the course AI policy: list any AI tools used, what you asked, and what
you kept. "None" is an acceptable answer. You may be required to defend every line of this
worksheet orally, so the statement protects you!

> 
