# lab11 WORKSHEET - predict before you measure

Alpha-code: ____________________    Date: ____________

Every prediction below must also be logged with `predict()` before you run the
action it concerns. This sheet is where you reason; the log is where the
timestamp lives. A prediction made after the measurement scores zero, here and
in the log.

Two checkpoints need your instructor's wet initials before you continue. Do not
run past a gate without them.

---

## Part 0 - the oracle

P0. Before running `oracle.py`, predict: will the closed-window grand total equal
the number of records you produced into Kafka?  Circle: EQUAL / LESS / MORE

Why, in one sentence (think about what "closed window" excludes):

________________________________________________________________

Measured closed-window grand total: ____________
Records produced (from produce.sh): ____________
Reconcile the gap if there is one: ______________________________

---

## Part 1 - watermark and drops

P1a. Generous watermark (baseline run). Predict the number of records the
watermark will drop: ____________   Measured: ____________

P1b. Tight watermark (`--action drops --watermark 30`). Predict the exact drop
count IN RECORDS and its source (the data README gives you the late-tail size):

Predicted drop count: ____________  because ______________________

Measured, as (tight oracle - tight sink) from
`verify_probe.py agg_drops oracle_wm30`:

  oracle_wm30 total: __________  agg_drops total: __________  drop: __________

The `dropped` column in the run's own table reports something smaller. Why is it
not the record count?

________________________________________________________________

P1c. In one sentence, what did widening the watermark buy, and what did it cost?

________________________________________________________________

P1d. Part 1 runs at `maxOffsetsPerTrigger=200` instead of the lab's usual 4000.
Predict what the drop count would be at 4000, and say why in terms of how far
behind the watermark that filters late records actually is:

Predicted at 4000: __________   because ______________________________

### >>> GATE 1 (instructor initials): __________
Student can state, without notes, why the tight watermark dropped exactly the
late tail and the generous one dropped nothing, can point at the frontier in the
explanation, and can say why the micro-batch size changes the answer even though
the data and the allowance are unchanged. Do not initial for a matching number
alone.

---

## Part 2 - crash and recover

P2a. Before `--action crash`: which batch id will the fault fire on, and what
will be true of the sink at the moment it dies (has batch 3 been written? has it
been committed?)

Fault batch: ______   At death, batch 3 is: WRITTEN / COMMITTED / NEITHER

P2b. Before `--action resume`: the final sink total will be, versus the oracle:
Circle LESS / EQUAL / MORE, and name the leg that decides it:

________________________________________________________________

### >>> GATE 2 (instructor initials): __________
Student has made prediction P2b in the log with a timestamp BEFORE running the
resume, and can name which leg (replayable source, atomic checkpoint, idempotent
sink) makes each of LESS, EQUAL, and MORE the outcome. Only then, resume.

P2c. Probe result: PROBE PASS / PROBE FAIL
Offset the restart resumed from (from the checkpoint offsets file): ____________

---

## Part 3 - break a leg

P3. Before `--action naive-resume`, predict the probe direction against
`agg_naive`:  OVER / EQUAL / UNDER oracle, by roughly ____________

Measured direction: ____________ by ____________

One sentence: why did the identical crash leave the idempotent sink correct and
the naive sink wrong?

________________________________________________________________

---

## Part 4 - state

P4a. Predict the peak state-row count the query holds: ____________
Measured (`PEAK state rows` on the run's per-batch table; the Streaming tab
shows the same series live if you catch it): ____________

P4b. Lab10's state curve climbed forever. This one stops. In one sentence, what
evicts the old rows?

________________________________________________________________

P4c. Flip to RocksDB and re-run. Predict: does the closed-window answer change?
YES / NO.  Does the state-row count change? YES / NO.  Does JVM heap pressure
change? YES / NO.
Measured: answer changed? ______  state rows changed? ______  heap? ______

Instructor note (the one config line): re-run the baseline with the state store
provider set on the submit line, nothing else changes:

    spark-submit --jars "$JARS" \
      --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
      lab11_stateful.py --action baseline

The closed-window answer is a function of the data, so it must not move, and
neither should the state-row count: the same keys are open either way. What
moves is where those rows live: RocksDB holds them off the JVM heap and off the
garbage collector's books, which is what lets real jobs carry millions of open
keys. Compare both runs' per-batch tables and say which numbers changed and
which did not.

---

## Sign-off

Friction log attached: YES / NO
Memo attached: YES / NO
Both gates initialed: YES / NO
