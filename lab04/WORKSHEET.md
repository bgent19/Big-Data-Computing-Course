# SD411 Lab 04 — Worksheet

**Name:** ____________________  **Alpha:** ____________  **Date:** ____________

Rule of the lab: every prediction box is filled BEFORE the command runs. An
honest wrong prediction scores full methodology credit. A prediction written
after the measurement scores none, and it is usually obvious.

---

## P0 — Corpus generation

| Field | Value |
|---|---|
| Unique plate appearances reported by gen_corpus.py | |
| Lines written (after ×4 stacking) | |
| verify_lab04.sh result after generation (pass/fail/warn counts) | |

## P1 — Laziness

**Prediction (before running `sc.textFile`):** the corpus is a few hundred MB.
This line will take approximately: ____________

| Line | Instant / not instant | Transformation or action? |
|---|---|---|
| `lines = sc.textFile(...)` | | |
| `words = lines.flatMap(...)` | | |
| `pairs = words.map(...)` | | |
| `counts = pairs.reduceByKey(...)` | | |
| `counts.take(5)` | | |

`lines.count()` duration vs `counts.take(5)` duration: which was longer, and
roughly by how much? ____________

**M1.** What did Spark do at the moment of the action that it refused to do
during the four earlier lines? Name the structure it had been building instead.

> _Answer:_

## P2 — Stages, narrow and wide

Paste (or photograph) the `toDebugString()` tree and circle the ShuffledRDD:

```
(paste here)
```

**Prediction:** `counts.count()` will produce ________ stages.
**Observed:** ________ stages.

| Metric (from the Stages tab) | Value |
|---|---|
| Stage 0 Shuffle Write | |
| Stage 1 Shuffle Read | |

**Prediction:** `counts.filter(...).count()` → ________ stages. **Observed:** ________
**Prediction:** `...sortByKey(False).take(10)` → ________ stages. **Observed:** ________

Did the UI mark any stage "skipped"? Where, and what is your best guess why?

> _Answer:_

**M2.** Explain narrow vs wide in one sentence each, in terms of which input
partitions an output partition depends on. No memorized labels.

> _Narrow:_
> _Wide:_

## P3 — reduceByKey vs groupByKey

**Predictions (before running the scaffold):**
Faster pipeline: ____________ by roughly a factor of ____________
Larger shuffle: ____________ because ____________________________________

| Pipeline | Median wall time (3 trials) | Shuffle Write (UI) | Shuffle Read (UI) | Distinct words |
|---|---|---|---|---|
| reduceByKey | | | | |
| groupByKey | | | | |

**M3.** Explain the byte-count gap via map-side combining. Cite your own two
Shuffle Write numbers in the explanation. If you hit gotcha 8 (OOM), document
the failed stage here and explain which operation forced it.

> _Answer:_

## P4 — The distribution

Top 20 words (word : count), from `takeOrdered`:

```
(paste here)
```

**M4.** Why `takeOrdered(20, ...)` rather than `collect()` + Python sort?
Answer in terms of where the work happens and what must fit in the driver.

> _Answer:_

**M5.** If this dataset were partitioned by word, would the partitions be
equal? Which key would you least want to hold, and what does that imply for
the worker that holds it? (2–3 sentences. We return to this in Week 8.)

> _Answer:_

## Stretch (+5) — Lineage recovery

Screenshot reference of failed-then-rescheduled tasks: ____________

What did Spark recompute, how did it know what to recompute, and what would
MapReduce have done instead?

> _Answer:_

## Friction log (required)

| Time lost | Symptom | What fixed it |
|---|---|---|
| | | |
| | | |

## AI usage statement (required)

Tools used, for what, and which parts of this submission you can defend
without them. Submission ≠ ownership.

> _Statement:_
