# SD411 Lab 07 — Worksheet

**Name:** ____________________  **Alpha code:** ____________  **Date:** __________

Predict-first is the rule. Each prediction box must be filled in and **initialed by the instructor BEFORE** you run the corresponding measurement. A measurement with no initialed prediction scores zero for that part.

---

## Part 1 — Broadcast hash join

**Prediction P1** (initial before running): _______ (instructor initials)

- Which table do you expect Spark to broadcast? ____________________
- Will the pitch fact be shuffled? (yes / no) ______
- Why? ____________________________________________

**Measurement P1**

| Field | Value |
|---|---|
| Join node in the plan | |
| Side with `BroadcastExchange` above it | |
| Pitch fact shuffled? (from UI) | |
| Median wall time (3 trials) | |

---

## Part 2 — Sort-merge join

**Prediction P2** (initial before running): _______

- How many `Exchange` (shuffle) nodes? ______
- How many `Sort` nodes? ______

**Measurement P2**

| Field | Value |
|---|---|
| Join node in the plan | |
| `Exchange` node count | |
| `Sort` node count | |
| Shuffle write bytes (left side) | |
| Shuffle write bytes (right side) | |
| Median wall time (3 trials) | |

---

## Part 3 — Shuffle-hash join

**Prediction P3** (initial before running): _______

- Which node disappears vs the sort-merge plan? ____________________
- Median time vs Part 2: up or down? ______

**Measurement P3**

| Field | Value |
|---|---|
| Join node in the plan | |
| `Sort` node count (expect 0) | |
| Which side built the hash table | |
| Shuffle write bytes (left side) | |
| Shuffle write bytes (right side) | |
| Median wall time (3 trials) | |
| Time delta vs Part 2 (P2 − P3) | |

---

## Part 4 — AQE trap

**Prediction P4** (initial before running, from the STATIC plan only): _______

- Which join strategy does the static plan show? ____________________
- Do you expect the executed join to match? (yes / no) ______

**Measurement P4**

| Field | Value |
|---|---|
| PLANNED join (static plan) | |
| EXECUTED join (after `.count()`) | |
| Do they match? | |
| What rewrote the plan? | |
| At what point in the query lifecycle? | |

---

## Mechanism questions

**M1.** In Part 1, broadcasting the 30-row table avoided shuffling millions of pitch rows. State the rule, in your own words, that tells Catalyst a table is small enough to broadcast.

_____________________________________________________________

**M2.** Sort-merge join sorts both sides. Shuffle-hash join sorts neither. Both shuffle both sides. So what exactly does the shuffle-hash join trade away to skip the sort, and when does that trade go wrong?

_____________________________________________________________

**M3.** A hint of `shuffle_hash` was not enough on its own to get a shuffle-hash join. What else did you have to change, and why is that setting `true` by default?

_____________________________________________________________

**M4.** In Part 4, the planned join and the executed join differed. Why did the static planner not just pick the broadcast join itself, given that the filtered side is tiny?

_____________________________________________________________

**M5.** You have a teammate who tunes joins by reading `df.explain()` and never opens the Spark UI. Give them one concrete case from today where that habit would mislead them, and what they should do instead.

_____________________________________________________________

---

## Friction log

Log every fight that lasted more than a couple of minutes. Symptom, what you tried, what fixed it. This is graded.

| Time | Symptom | Hypothesis | What fixed it |
|---|---|---|---|
| | | | |
| | | | |
| | | | |

---

## AI usage statement

Per course policy, AI use is permitted and must be disclosed. AI cannot replace measurement; every number above must be one you ran.

- Did you use an AI assistant on this lab? (yes / no) ______
- If yes, for what? (concept questions / debugging / writing / other) ____________________
- Attach or link the conversation log with your submission.

I can explain every result I submitted and the conversation log reflects my own work.

**Signature:** ____________________
