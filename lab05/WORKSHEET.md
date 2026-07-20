# SD411 Lab 5 — Worksheet (predict first, then measure)

**Name / alpha:** ______________________  **Date:** 17 Sep 2026
**Instructor initials (0:40 checkpoint):** ________

Rules: predictions in ink, before the run, no exceptions. A wrong prediction
with a good post-measurement explanation earns full credit. An empty or
post-hoc prediction earns zero for that line. AI cannot replace measurement.

---

## Predictions (P1–P3 before Part A; P4–P8 by the 0:40 checkpoint)

**P1.** Will `plans/v_sql.txt` and `plans/v_df.txt` be structurally identical
(same nodes, same order, ignoring node ID numbers)?

> Prediction (yes/no + one sentence why): ______________________________

**P2.** Rank the four versions by median wall time, fastest to slowest:
`v_df` (Parquet), `v_csv` (CSV), `v_udf` (Parquet + Python UDF filter),
`v_rdd` (raw RDD on CSV).

> Prediction: 1.________ 2.________ 3.________ 4.________

**P3.** How many `Exchange` nodes will the `v_df` physical plan contain?
(Hint: count the operations in the query that cannot be done partition-local.
lab04 vocabulary: count the wide dependencies.)

> Prediction: ________ because ______________________________

**P4.** The `v_df` Scan node lists `ReadSchema`. How many columns will it
name, out of the ~80 in the file?

> Prediction: ________

**P5.** Will the `v_csv` Scan node show the speed predicate in
`PushedFilters` at all?

> Prediction (yes/no): ________

**P6.** Estimate the ratio of `v_csv` median time to `v_df` median time.

> Prediction: roughly ________ × slower

**P7.** Will the `v_udf` Scan node still show the speed predicate in
`PushedFilters`?

> Prediction (yes/no + why): ______________________________

**P8.** Estimate the ratio of `v_udf` median time to `v_df` median time.

> Prediction: roughly ________ × slower

---

## Measurements

Record three runs and the median for each. Note run 1 separately.

| Version | Source | Run 1 (s) | Run 2 (s) | Run 3 (s) | Median (s) |
|---|---|---|---|---|---|
| v_sql | Parquet | | | | |
| v_df  | Parquet | | | | |
| v_csv | CSV     | | | | |
| v_udf | Parquet | | | | |
| v_rdd | CSV     | | | | |

Plan facts (from the saved files, not from memory):

| | v_df | v_csv | v_udf |
|---|---|---|---|
| Columns in `ReadSchema` | | | |
| `PushedFilters` contains speed predicate? | | | |
| Number of `Exchange` nodes | | | |
| Node present here that v_df does not have | — | | |

Top-3 result rows (sanity check, must match across all versions):

| pitch_type | n | avg_spin |
|---|---|---|
| | | |
| | | |
| | | |

---

## Mechanism questions (two to four sentences each, your own words)

**M1.** The CSV runs make one more full pass over the file than the query
itself needs. Find that pass in the Spark UI (port 4040 during the run) and
name what it is for. Why does the Parquet read not need it?

> ______________________________

**M2.** Run 1 is consistently slower than runs 2 and 3 for every version.
Give the mechanism. (There is more than one contributor; name the one you
can defend.)

> ______________________________

**M3.** The `v_csv` plan may still list the predicate at the scan. What can
"pushing a filter" possibly mean for a row-oriented text file, and why does
Parquet turn the same pushdown into so much less I/O? Use Module 1
vocabulary: row groups, statistics, column chunks.

> ______________________________

**M4.** Your UDF computes exactly the same boolean as
`release_speed >= 95.0`. State precisely what Catalyst can no longer see
when the test lives inside a Python function, and name two distinct costs
that follow (one optimization lost, one runtime cost added).

> ______________________________

**M5.** There is no plan file for the RDD version. What does that tell you
about the contract you accept when you drop from DataFrames to RDDs? And
from the `parse` function: name one real Statcast field that would break
the naive `split(",")` approach.

> ______________________________

---

## Oral check (instructor use)

Question asked: ____________  Pass / Revisit: ________  Initials: ________
