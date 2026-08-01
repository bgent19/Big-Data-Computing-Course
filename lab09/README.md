# Lab 09 - Kafka, End to End

**SD411 Big Data Computing | Module 3 | Thursday 22 October 2026**

Class left us with a log, a set of promises about ordering, and a
claim that Kafka computes nothing. Today you stand the whole thing up and check
all three by hand. By the end of the period you will have produced a real event
stream into a topic, watched consumers divide it up, killed one of them mid
stream, rewound the log to the beginning, and expired records off the front of
it. You will also have an event pipeline that has produced exactly zero
answers, which is the point.

---

## What you need before you start

1. A clean stack. `bash vm-base/scripts/sd411_down_all.sh` from the repo root
   first. Every lab in this course reuses the same host ports and only one
   SD411 stack can be up at a time.
2. `lab09/.env` stamped by `vm-base/scripts/sync_env.sh`. Check 0 fails fast
   if not.
3. The replay file staged on the VM. This was provisioned for you before term.
   If check 10 fails, that is a provisioning failure and not something you did:
   tell the instructor, do not try to download anything.
4. Download the [lab09 files](lab09.zip) and unzip them into the sd411 directory

```bash
cd lab09
docker compose up -d
bash scripts/verify_lab09.sh
```

Fourteen checks, numbered 0 through 13. Work failures top down. The first
failure usually explains every failure under it.

---

## How you work

The harness is `scripts/lab09_kafka.py` and it runs **on the VM, not inside a
container**:

```bash
python3 scripts/lab09_kafka.py --part 1
```

This is the one lab that does not submit through `spark-submit`, because this
is the one lab with no Spark job in it. The instruments are the Kafka CLI
tools that ship inside the broker image, and the harness runs them with
`docker compose exec` and turns their output into tables you can put in a
memo. Read the plumbing section at the top of the file once before you start;
you will use `create_topic`, `replay`, `end_offsets`, `earliest_offsets`,
`partition_profile`, `consume`, `group_describe`, `group_reset`,
`start_consumer`, `stop_consumer`, and `wait_for_group_empty` all period.

The scaffold has TODOs for Parts 1 through 5. Fill them in. `predict()` is
called before each measurement and writes to `lab09_predictions.log`, which
you submit.

You are welcome to run the CLI tools directly while you work things out:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic gh.keyed.type
```

---

## The parts

### Part 0 - Stack up
Bring the stack up, run the verify script, and read `data/sample_events.json`.
That file holds three raw events exactly as the source system emitted them.

### Part 1 - The log
One partition, 5000 events. Establish that offsets are positions, that end
offset equals count on a fresh topic, and that reading a record does not
remove it. That last one is where the mental model from every message queue
you have used breaks.

### Part 2 - Partitions and ordering
Four partitions, unkeyed. Measure what the ordering guarantee actually is by
reading the whole topic, then reading partition 0 and partition 1 on their
own, and comparing the offset sequences. Both single-partition reads come back
numbered 0 through 19 and they are different records. That is the whole
lesson: an offset without a partition does not name anything.

### Part 3 - Keys and distribution
The same 60000 events keyed two different ways into two identical topics.
Predict both partition distributions from the type frequencies in
`data/README.md` before you produce anything, then measure. You have seen this
number before, in lab08, under a different name.

### Part 4 - Consumer groups
Grow a group from one member to five on a four-partition topic. Kill a member
and time the reassignment. Build a backlog against an idle group and measure
lag against it. Start a second, independent group on the same topic. This is
the part that pays for the entire architecture, and it is the part the oral
station will draw from.

### Part 5 - Replay and retention
Rewind a group to the beginning and prove the records are identical. Then set
a 30-second retention on a throwaway topic and watch records leave from the
front. You need two instruments here, not one: `end_offsets()` and
`earliest_offsets()`. Predict both before you look.

### Part 6 (stretch, +5 extra credit) - The log as a bounded table
`docker compose --profile spark up -d`, then submit
`scripts/lab09_batch_read.py`. See the docstring in that file for the exact
`spark-submit` line. This is next week's lab in its easiest possible form.

---

## The memo

One page: Lead with the number, then prove it.

You are the ingestion lead for a live Statcast pipeline. Fifteen stadiums
produce pitch events. Three consumers read them: a dashboard that needs
seconds, an hourly Parquet writer, and an alerting job. Using **your own
measured numbers from today**, recommend:

1. A partition count for the `statcast.pitches` topic, with the reasoning tied
   to your Part 4 measurement of the group-size ceiling.
2. A record key, with the reasoning tied to your Part 3 distribution numbers,
   and an explicit statement of what your key choice costs you.
3. A retention window, with the disk arithmetic shown. Retention times
   throughput is disk, and the pipeline runs 2430 games a season at roughly
   290 pitches a game.

Then answer one question in two sentences: what part of the dashboard's job
can Kafka do, and what part can it not?

Cite your own tables. A memo that recommends a partition count without a
number behind it earns half credit at most.

---

## Mechanism questions

Answer in the WORKSHEET. These are the questions the oral station draws from.

**M1.** A topic has 4 partitions. You produce 1,000,000 records keyed by a
field with exactly 3 distinct values. You then increase the topic to 40
partitions. What happens to the distribution, and why is the answer not "it
gets ten times more even"?

**M2.** Two consumers, same group, 4 partitions. One consumer is killed with
SIGKILL. Explain why its partitions are not reassigned instantly, and name the
setting that decides how long the wait is.

**M3.** You reset a group to the earliest offset and re-read. The records are
byte for byte identical. Explain, in terms of what a commit actually is, why
this is guaranteed and not luck.

**M4.** A topic has 5000 records. Retention expires the first 4000. What is the
end offset now? What is the earliest readable offset? Why did the end offset
not move?

**M5.** Your alerting job needs to detect a 3 mph velocity drop for a pitcher
across his last 10 pitches. State precisely which part of that requirement
Kafka satisfies and which part it does not, and name the property of the log
that lets you re-run the alerting job over yesterday's data after you fix a bug
in it.

---

## What to submit

Due 2359 the day before Lab 10.

```
lab09_<alpha>/
  lab09_kafka.py            your filled-in harness
  lab09_predictions.log     generated; do not edit it
  WORKSHEET.md              completed
                            and Part 4 checkpoints
  memo.md                   one page, IAW writing standard
  AI_Usage.md
  lab09_batch_read.py       only if you attempted the stretch
```
