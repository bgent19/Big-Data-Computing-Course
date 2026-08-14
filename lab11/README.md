# lab11 - Streaming aggregation with state

Two labs ago we produced a stream and got no answers out of it. Last week we
finally aggregated one, and lab10 left us staring at a state curve that only ever
went up, with a single question written under it: what bounds this? Lecture named two answers at once. The watermark bounds the state, by giving
Spark permission to forget windows the frontier has passed. And exactly-once,
the guarantee we actually want for a count, turns out to be three legs holding up
one output: a replayable source, a checkpoint that commits offsets and state
together, and a sink that absorbs a retried write. Today we earn every clause of
that sentence, out loud, from the output and the checkpoint.

Here is the shape of the day. MIDN A runs a windowed count over a pitch stream,
kills it on purpose in the middle of a batch, restarts it, and proves the final
answer is exactly what a clean run would have produced: nothing lost, nothing
double counted. Then MIDN B breaks one leg on purpose, a sink that blindly
appends, and watches the same crash turn into duplicates, before fixing it with
one idea we already own from SD321. That contrast is the whole lab. A
**stateful** query is one whose current answer depends on records it saw in
earlier batches, held in a per-partition key-value store called the **state
store**, and the reason streaming is hard is that this state and the input
offsets have to move as one atomic step or the crash at 21:43 gives you the wrong
number.

## What you are building

One streaming query. It reads pitches from a Kafka topic, assigns each to a
tumbling event-time window, and counts pitches per `pitch_type` per window. It
runs in **append** mode, so a window's row is emitted once, when the watermark
declares it final. The `(window, pitch_type)` pair is the state store key, and
watching that key set grow and then get bounded is Part 4.

The scaffold is `scripts/lab11_stateful.py`. Three lines carry the lesson and are
marked `TODO(you)`: the watermark, the window group-by, and the idempotent sink
path. Everything else, including the crash instrument, is written for you.

## Stack and submit line

Download the [lab11 files](lab11.zip) and unzip them into the sd411 directory

Two shells, and it matters which one you are in. Everything that says `docker
compose` runs **on the host, from the lab root**; every `spark-submit` runs
**inside the spark-master container**. Below, host commands are marked `host$`
and container commands `spark$`.

```
host$ ../vm-base/scripts/sync_env.sh     # stamp .env from common.env (once)
host$ docker compose up -d               # broker, spark-master, spark-worker, minio
host$ ./scripts/verify_lab11.sh          # must be clean before you start
```

`docker compose up` re-runs the bucket init, which clears `lab11/` on MinIO,
including your oracle. Bring the stack up once, at the start, and leave it up;
if you do have to bring it up again mid-period, re-run `oracle.py`.

`spark-submit` needs the Spark-Kafka connector and the S3A jars. Set them once
inside the container shell:

```
host$  docker compose exec spark-master bash
spark$ export JARS=$(echo /opt/spark/extra-jars/*.jar | tr ' ' ',')
spark$ cd /opt/lab11/scripts
```

Then every run is `spark-submit --jars "$JARS" lab11_stateful.py --action <...>`.

Each run ends by printing a per-batch table: input records, window rows emitted,
state rows held, rows dropped, and the watermark in force for that batch. **That
table is the instrument for Parts 1, 2, and 4.** The Structured Streaming tab in
the driver UI shows the same numbers live, but the driver exits when the query
does, so the tab is gone seconds later. Read the tab if you can catch it; grade
yourself off the table.

## The parts

Each part tells you what to predict, what to run, and what to look at. Make the
prediction, in the worksheet and with `predict()`, before you run anything.

### Part 0 - the oracle

Produce the stream, then compute the ground truth.

```
host$  ./scripts/produce.sh                            # paces the stream into Kafka
spark$ spark-submit --jars "$JARS" oracle.py           # writes s3a://sd411/lab11/oracle
```

`produce.sh` is a once-per-topic step and refuses to run twice: a second produce
would append a second copy of the stream and quietly invalidate every number you
compute after it. The topic survives every reset, because it is your replayable
log.

Record the closed-window grand total it prints. Every later run has to reproduce
it. Predict, before you run `oracle.py`: will the oracle total equal the number
of records you produced? If not, which records are missing and why? (Look at what
"closed window" means in the file header.)

### Part 1 - the watermark earns its keep

```
spark-submit --jars "$JARS" lab11_stateful.py --action baseline
```

Read the per-batch table it prints. Predict first: with the generous watermark,
how many records will the watermark drop, and what will the peak state-row count
be? Now run the same query with a tight watermark, into its own sink:

```
host$  ./scripts/reset_lab11.sh
spark$ spark-submit --jars "$JARS" lab11_stateful.py --action drops --watermark 30
```

Predict, before this run, the exact drop count in RECORDS, and where it comes
from. The data README tells you the size of the late tail; that number is not a
coincidence. This run takes about a minute, because it deliberately uses much
smaller micro-batches (see the note below).

Now measure it. The `dropped` column in the table is counted at the state
operator, which sees rows only after partial aggregation, so it reports dropped
*groups*, not dropped records: useful as a yes/no, useless as the count. The
record-level number comes from comparing this run against the oracle for the
same allowance:

```
spark$ spark-submit --jars "$JARS" oracle.py --watermark 30 --suffix oracle_wm30
spark$ spark-submit --jars "$JARS" verify_probe.py agg_drops oracle_wm30
```

`oracle_wm30` is what a tight run would have produced had nothing been dropped,
so the probe's UNDER-by number IS the abandoned tail, to the record. Reconcile it
against the tail size in the data README.

**Why the small batches (this is worth understanding, and it is examinable).**
A record is discarded as late when its event time is behind the watermark, but
the watermark a batch filters against is not computed from that batch: it lags,
by roughly one micro-batch of event time. At the lab's normal batch size, one
batch spans about twelve minutes of event time, which is far more than the tail's
90 s of lateness, so the tail arrives while the filtering watermark is still
behind it and NOTHING is dropped no matter how tight the allowance. The tail is
only abandoned once the frontier has propagated into the filter, which needs
`(one batch of event time) + allowance < (the tail's lateness)`. `--action drops`
therefore runs at `maxOffsetsPerTrigger=200`, about 36 s of event time per batch:
36 + 30 < 90, so the whole tail is late-by-contract and all of it goes. Predict
what would happen at the default batch size, then confirm it with
`--max-offsets 4000` if you have time.

### Part 2 - crash and recover

This is the lab. Reset, then crash on purpose:

```
host$  ./scripts/reset_lab11.sh
spark$ spark-submit --jars "$JARS" lab11_stateful.py --action crash
```

It dies inside batch 3, after that batch's write, before the batch commits. That
is the worst possible moment, on purpose. Now restart, same checkpoint, no fault:

```
spark-submit --jars "$JARS" lab11_stateful.py --action resume
```

Predict, before the resume: will the final sink total be less than, equal to, or
greater than the oracle? Say why in terms of the three legs. Then prove it:

```
spark-submit --jars "$JARS" verify_probe.py agg_idempotent
```

`PROBE PASS` means sink equals oracle: nothing lost, nothing doubled. While you
are here, look at the checkpoint. The committed Kafka offset in
`s3a://sd411/checkpoints/lab11/agg_idempotent/offsets` and the state version are
the pair that made the recovery correct. Find the offset the restart resumed
from.

### Part 3 - break a leg

Now the same crash against a sink that blindly appends under a random filename
every write.

```
host$  ./scripts/reset_lab11.sh
spark$ spark-submit --jars "$JARS" lab11_stateful.py --action naive-crash
spark$ spark-submit --jars "$JARS" lab11_stateful.py --action naive-resume
spark$ spark-submit --jars "$JARS" verify_probe.py agg_naive
```

Predict, before the resume, the direction the probe will report. Then explain, in
one sentence, why the idempotent sink in Part 2 survived the identical crash and
this one did not. The fix is already in the scaffold: the idempotent sink writes
under a name derived from the batch id, so the replay overwrites instead of
adding. That is leg three.

### Part 4 - where state lives, and what bounds it

Re-run the baseline and read the `stateRows` column of the per-batch table (the
Streaming tab shows the same series live, if you can catch it). Predict first:
how many state rows does the query hold at its peak, and why does the column stop
climbing instead of growing forever the way lab10's did? Note what the run prints
as `PEAK state rows` and reconcile it against your prediction: open windows times
pitch types present, where "open" means everything the watermark has not yet
declared final, including the whole batch of event time the query has ingested
ahead of its own watermark.

Then flip the state store to RocksDB with one config line (see the instructor
note in the worksheet) and re-run. Predict whether the closed-window answer
changes, whether the state-row count changes, and whether the heap pressure
changes. Two of those do not move and one does.

### Part 5 - the memo

Write it to the class standard. See the spec below.

### Stretch - recognize and skip (+5 extra credit)

The batch-id-in-the-name trick is one flavor of idempotency. The other, the one
the lecture tied to a database transaction, is recognize-and-skip: before writing
batch `bid`, check whether a marker object for `bid` already exists, and if it
does, skip the write entirely. Implement it against MinIO in `foreachBatch` and
show it survives the Part 3 crash. Explain which flavor you would reach for
against a Postgres sink and why.


## The memo (Class standard)

One page, PDF, to the course communication standard. Audience: a teammate who
will inherit this pipeline and needs to trust its numbers. Lead with the answer
(Minto): state, in the first two sentences, whether the pipeline delivers
exactly-once and how you know. Then walk the three legs, naming for each the
concrete mechanism in this lab that satisfies it, and name the one you would
worry about first if you swapped the file sink for a dashboard. Include one
figure, your choice, at Tufte's data-to-ink discipline; a screenshot of the
Streaming tab is fine if the figure earns its space. Close with the honest
sentence from lecture: which output of a real pipeline you would run at
exactly-once and which you would deliberately run at at-least-once, and why that
is the right call and not a lazy one.

## What to submit

- `lab11_predictions.log` (timestamps are the grade)
- Your completed `scripts/lab11_stateful.py`
- The `PROBE PASS` output from Part 2 and the probe direction from Part 3
- Your worksheet with all initial gates signed
- The memo PDF
- Your friction log