# Lab 09 Worksheet - Kafka, End to End

**Name:** ___________________________  **Alpha:** _______________

Fill this in as you go. Predictions are written **before** the measurement they
sit above. Two checkpoints require instructor initials during the period, not
after it.

---

## Part 0 - Stack

Verify script result: ______ passed, ______ failed, ______ warnings

If anything failed, what and how you cleared it:

```
```

---

## Part 1 - The log (10 pts)

**P1.3 Prediction.** End offset of partition 0 after 5000 records: __________

Reasoning (one sentence):

```
```

**Measured.** End offset: __________

**P1.4 Prediction.** Does a second read from the beginning return the same
records? __________

**Measured.** First read offsets: ______________________
Second read offsets: ______________________ Identical? ______

**P1.5 Schema reading.** From `data/sample_events.json`:

| Question | Field |
|---|---|
| Carries event time | |
| Would key per-repository ordering | |
| A field that is state, not an event | |

One sentence on why the last one is different in kind from the other two:

```
```

---

## Part 2 - Partitions and ordering (15 pts)

**P2.3 Prediction.** Max/min partition ratio, 20000 unkeyed records, 4
partitions: __________x

Reasoning:

```
```

**Measured.**

| Partition | Records | Share |
|---|---|---|
| 0 | | |
| 1 | | |
| 2 | | |
| 3 | | |
| **max/min** | | |

**P2.4 Prediction.** How many partitions will a 20-record whole-topic read
touch? ______

Whole-topic read, first 20 (partition, offset) pairs:

```
```

How many distinct partitions did those 20 records actually come from? ______

**P2.5** Partition-0 read, first 20 offsets:

```
```

Partition-1 read, first 20 offsets:

```
```

Record at offset 0 of partition 0:

```
```

Record at offset 0 of partition 1:

```
```

Same offset, same record? ______ What does that tell you an offset is? 

```
```

**P2.6** State the ordering guarantee Kafka gives and the one it does not, in
two sentences, citing your two readings as evidence:

```
```

---

## Part 3 - Keys and distribution (20 pts)

Predict both ratios **before you produce anything**. The type frequencies are
printed in `data/README.md`, so this is arithmetic, not a guess.

**P3.2a Prediction.** Keyed by entity, max/min ratio: __________x

**P3.2b Prediction.** Keyed by type, max/min ratio: __________x

Show the arithmetic behind 3.2b:

```
```

**Measured.**

| Partition | entity-keyed records | share | type-keyed records | share |
|---|---|---|---|---|
| 0 | | | | |
| 1 | | | | |
| 2 | | | | |
| 3 | | | | |
| **max/min ratio** | | | | |

**P3.5** Keys observed on partition 0 of the type-keyed topic:

```
```

Keys observed on partition 1:

```
```

(a) Are all the records on a given partition the same key? ______
(b) Are all the records with a given key on the same partition? ______

One of those is a guarantee Kafka makes and one is an accident of this hour's
hash values. Which is which, and why can the other one never be promised?

```
```

**P3.6** You need per-repository ordering AND an even distribution. Are both
achievable at once with a single key here? If not, what do you do?

```
```

> **CHECKPOINT 1.** Show the instructor both ratio numbers and answer one
> question about them out loud before moving on.
>
> Instructor initials: __________

---

## Part 4 - Consumer groups (25 pts)

**P4.1 Prediction.** Partitions owned by 1 member: ______
**Measured:** ______

**P4.2 Prediction.** Partitions per member with 2 members: ______
What happens to the offsets already committed?

```
```

**Measured:** ______ per member. Did anything get re-read or skipped? ______

**P4.3 Prediction.** What does the 5th member of a group do on a 4-partition
topic?

```
```

**Measured.** Distinct members holding at least one partition: ______

**P4.4 Prediction.** Time until orphaned partitions are reassigned: ______ s

Reasoning:

```
```

**Measured.** Sample every 10 s until the assignment changes.

| Elapsed | Assignment changed? |
|---|---|
| 10 s | |
| 20 s | |
| 30 s | |
| 40 s | |
| 50 s | |
| 60 s | |

Reassignment observed after ______ s. Which member picked up the orphaned
partitions? ______________

**P4.5** Lag. A running console consumer keeps lag at 0, so you build a
backlog first: stop every member, wait for the group to go Empty, then replay.

| Reading | Partition 0 | 1 | 2 | 3 | Total |
|---|---|---|---|---|---|
| group idle, after replay | | | | | |
| after one member returns | | | | | |

LAG is the difference of ____________________ minus ____________________,
measured in ____________________ (units).

Why did the backlog have to be built against an idle group?

```
```

**P4.6 Prediction.** Where does a brand new group start?

```
```

**Measured.**

| | Partition 0 | 1 | 2 | 3 |
|---|---|---|---|---|
| `g-second` CURRENT-OFFSET | | | | |
| `g-solo` CURRENT-OFFSET | | | | |

g-second read only 20 records, so most of its partitions still sit at 0 and
its LAG is roughly the whole topic. g-solo is at the tail with LAG 0. What
does that pair of rows prove about how groups relate to each other?

```
```

> **CHECKPOINT 2.** Show the instructor your five-member group describe output
> and explain the idle member. This is the highest-value oral question in the
> lab and it will come back at the Lab 10 station.
>
> Instructor initials: __________

---

## Part 5 - Replay and retention (10 pts)

**P5.1** After reset to earliest: CURRENT-OFFSET ________ LAG ________

**P5.2 Prediction.** Are the re-read records identical? ______
**Measured.** First re-read offsets: ______________________
Second re-read offsets: ______________________

The second call returned different offsets than the first. Did the first read
delete anything? What did it change?

```
```

**P5.4 Prediction.** After 30 seconds with `retention.ms=30000`:

End offset will be: __________
Earliest readable offset will be: __________

Reasoning:

```
```

**Measured.**

| | End offset | Earliest readable | Offsets returned by a read at "earliest" |
|---|---|---|---|
| immediately after produce | | | |
| after 30 s | | | |

The earliest offset was already well above 0 in the first row, before you
waited at all. Why?

```
```

Why did the end offset not move? Answer in terms of what an offset names:

```
```

---

## Part 6 - Stretch (+5)

Total rows read by Spark: ____________

| Kafka partition | Spark count | Your Part 3 end offset | Match? |
|---|---|---|---|
| 0 | | | |
| 1 | | | |
| 2 | | | |
| 3 | | | |

The seven columns Kafka hands Spark are: _____________________________________

Which of them describes your event? ____________________

---

## Mechanism questions

**M1.**

```
```

**M2.**

```
```

**M3.**

```
```

**M4.**

```
```

**M5.**

```
```

---

## Friction log

Real entries only. Empty is not the goal.

| Time | What broke | What you tried | What fixed it |
|---|---|---|---|
| | | | |
| | | | |
| | | | |
