# Lab 08 Worksheet - Skew and UI Forensics

Name: ______________________  Alpha: ______________  Date: 8 Oct 2026

Bring this page with you to the oral spot-check. It is the only paper you may
have during the lab; you may not have it during the spot-check itself.

---

## Section A - Predictions (record BEFORE any measurement)

Instructor initials: __________   Time: __________

Predictions are graded on being present and reasoned, not on being right. A
wrong prediction with a stated reason earns full credit. A blank or a
prediction recorded after the run earns zero.

| # | Prediction | Your answer | One-line reason |
|---|---|---|---|
| P1 | Which of `pitch_type`, `events`, `batter` has the largest **key** skew ratio? | | |
| P2 | Which has the largest **partition** skew ratio at 16 partitions? | | |
| P3 | For the Part 2 job, ratio of max task duration to median task duration | | |
| P4 | Best salt factor N among 4, 16, 64 | | |
| P5 | Speedup from AQE at its default settings (x times faster) | | |
| P6 | Will AQE skew handling fix the `events` groupBy? Yes / No | | |

---

## Section B - Part 1: key profile vs partition profile

| Key | Distinct keys | Hottest key | Hottest key rows | Median key rows | Key skew ratio |
|---|---:|---|---:|---:|---:|
| `pitch_type` | | | | | |
| `events` | | | | | |
| `batter` | | | | | |

| Key | Non-empty partitions | Max partition rows | Median partition rows | Min | Partition skew ratio |
|---|---:|---:|---:|---:|---:|
| `pitch_type` | | | | | |
| `events` | | | | | |
| `batter` | | | | | |

The key whose two ratios diverge most: ________________

In one sentence, the quantity that actually predicts partition skew:

_______________________________________________________________________

---

## Section C - Part 2: the forensic walk

Dominant stage ID: ______   Stage wall clock: ______ s   Task count: ______

| Metric | min | 25th | median | 75th | max |
|---|---:|---:|---:|---:|---:|
| Task duration (s) | | | | | |
| Shuffle read (MB) | | | | | |

Max / median task duration ratio: ________

Spill (memory): __________  Spill (disk): __________

The key VALUE held by the fat partition, by name: ____________________

Straggler share of stage wall clock (max task / stage duration): ________

Join strategy shown in the static plan: ______________________________

---

## Section D - Part 3: salting

Unsalted baseline from Part 2 (median of 3): ________ s

| N | Median of 3 (s) | Result matches unsalted? | Task count in the join stage |
|---:|---:|---|---:|
| 4 | | | |
| 16 | | | |
| 64 | | | |

Chosen N: ______

Cost that falls as N rises: ____________________________________________

Cost that rises as N rises: ____________________________________________

What caps the useful value of N (look at SHUFFLE_PARTS): ________________

---

## Section E - Part 4: AQE

| Run | Median of 3 (s) | Max task (s) | `AQEShuffleRead` says |
|---|---:|---:|---|
| AQE off (Part 2 baseline) | | | n/a |
| AQE default | | | |
| AQE with lab-scale thresholds | | | |

The two conditions a partition must meet to be treated as skewed:

1. ____________________________________________________________________
2. ____________________________________________________________________

Which one your data failed, and by how much: __________________________

Values you set, and why those values:

_______________________________________________________________________

**Part 4c**, same key as a plain aggregation, AQE and skewJoin both on:

min ______  median ______  max ______   Straggler survived? Y / N

Why skewJoin did not apply:

_______________________________________________________________________

---

## Section F - Part 5 stretch (+5)

Broadcast join, median of 3: ________ s   vs best salted ________ s   vs best AQE ________ s

Left outer join (Part 2 baseline), median of 3: ________ s
Inner join on the same key, median of 3: ________ s

The Catalyst rule that explains the difference: ________________________

---

## Section G - Friction log (the 20-minute rule)

| Time | What you were doing | Exact error | What you tried | Resolved? |
|---|---|---|---|---|
| | | | | |
| | | | | |
| | | | | |

---

## Section H - Submission checklist

- [ ] `ALPHA` set in `lab08_skew.py`, no `REPLACE_ME` anywhere
- [ ] `results/lab08_<alpha>_part1.json` through `part4.json` present
- [ ] Every number in the memo appears in a results file
- [ ] Memo is a PDF, four pages or fewer, SD322 standard, figures labeled
- [ ] M1 through M5 answered in the memo body, in prose
- [ ] `AI_USAGE.md` included
- [ ] Oral spot-check completed before leaving
