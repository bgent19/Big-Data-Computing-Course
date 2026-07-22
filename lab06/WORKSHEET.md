# Lab 6 Worksheet — Predict, Then Measure

Name: ______________________  Section: ______  Alpha code: ______________

Rule of the room: every Predict box is filled in and initialed by the
instructor or lab assistant BEFORE the corresponding Measure box has a single
number in it. Post-hoc predictions score zero. A wrong prediction with honest
reasoning is full credit.

---

## Part 0 — Fact table

Rows written: __________   Fact path written: ____________________________

(If this is not the canonical path the scaffold names, stop and fix it. Lab 7
reads it next week.)

---

## Part A — Partition count sweep

### Predict (initial: ______ time: ______)

Sketch your predicted runtime curve (x = partition count 1…1000, log scale;
y = median seconds):

```
  s |
    |
    |
    |
    +----------------------------------------
      1    2    4    8    32    200    1000
```

Where is the minimum, and why there? (one sentence)

> ____________________________________________________________________

Why does the left end (n=1) behave the way you predict? (one sentence)

> ____________________________________________________________________

Why does the right end (n=1000) behave the way you predict? (one sentence)

> ____________________________________________________________________

### Measure

| n | trial 1 (s) | trial 2 (s) | trial 3 (s) | median (s) |
|---|---|---|---|---|
| 1 | | | | |
| 2 | | | | |
| 4 | | | | |
| 8 | | | | |
| 32 | | | | |
| 200 | | | | |
| 1000 | | | | |

A4 UI evidence:

- Scan-stage task count at n=8: ______  at n=1000: ______
- Post-shuffle-stage task count (any n): ______
- The config value you believe explains the constant: ______________________

---

## Part B — Layout races

### Predict (initial: ______ time: ______)

Q1 (single-month filter) winner: flat / by_month / by_date (circle one)
By roughly what factor over flat? ______×
Reasoning (one sentence):

> ____________________________________________________________________

Q2 (single-pitcher, full season) winner: flat / by_month / by_date (circle one)
Reasoning (one sentence):

> ____________________________________________________________________

Number of directories the by_date layout will create: ______

### Measure

B3 pitcher chosen: id ____________ pitch count this season: ________

| | flat | by_month | by_date |
|---|---|---|---|
| Q1 median (s) | | | |
| Q2 median (s) | | | |

by_date directory count (actual): ______  write time vs by_month: ______×

Sketch of the by_month directory tree (from the mc shell):

> ____________________________________________________________________

B5 — paste the physical-plan line that shows Spark skipping data on the
by_month layout:

> ____________________________________________________________________

In your own words, no jargon you have not earned: what is the mechanism?

> ____________________________________________________________________
> ____________________________________________________________________

---

## Part C — Skew observation

### Predict (initial: ______ time: ______)

C1 top pitch_type and its share of all rows (predict before counting):
type ______ share ______%

If we repartition into 8 partitions by pitch_type, predict the largest
partition's share of all rows: ______%

### Measure

C1 actual: top type ______ count __________ share ______%

C2 per-partition row counts (8 values):

| p0 | p1 | p2 | p3 | p4 | p5 | p6 | p7 |
|---|---|---|---|---|---|---|---|
| | | | | | | | |

C3: workload median on skewed frame: ______ s
UI stage evidence — max task duration: ______ median task duration: ______

One sentence: why can no partition COUNT fix this?

> ____________________________________________________________________

---

## Friction log

Anything that cost you more than five minutes, and what fixed it. This feeds
next year's gotchas table.

> ____________________________________________________________________
> ____________________________________________________________________

## AI usage statement

Tools used, and for what. Disclosure is permitted and expected; the oral check
assumes you own every line you submit.

> ____________________________________________________________________

---

## Oral check (instructor use)

Prompt drawn: A-curve / constant-stage / B5-mechanism / skew-vs-count
Result: credit / partial / re-check at Lab 7     Initials: ______
