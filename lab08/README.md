# Lab 08 - Skew and UI Forensics

| | |
|---|---|
| **Lab session** | Thursday 08 October 2026 |
| **Report due** | Tuesday 13 October 2026, NLT 2359 |
| **Weight** | counts in the Labs bucket (25% of term grade) |

---

## Why this lab exists

You can read a physical plan, name every `Exchange` in it, and pick the right join strategy from the sizes of the two sides. So consider MIDN A, who has done all of that correctly. Her plan is clean, Catalyst chose the join she would have chosen, and she is not out of memory. Her job takes nine minutes and she cannot say why.

Sounds like a plan-reading problem, right? It is not. Fifteen of her sixteen tasks finished in two seconds. The sixteenth ran for eight and a half minutes, and a stage is not done until its last task is done. Nothing in the plan says this. `explain()` prints the same plan whether the data is perfectly even or catastrophically lopsided, because a plan describes operations and **skew** is a property of the data. The only place skew is visible is the Spark UI, and the only way to find it is to already know the shape you are looking for.

So we will make MIDN A's job happen on purpose, measure it, fix it three different ways, and then discover that one of those three fixes quietly does nothing at this data size for a reason you have to go find in the configuration. The skill attached to all of it is UI forensics: opening a slow job you did not write and naming the one variable to change.

---

## What you need before you start

1. A green `verify_lab08.sh`. Every check, not most of them.
2. Lab 06's pitch fact on this stack. `minio-init` exits 64 if it is absent and the verify script will say so. Lab 08 reads that fact directly, so a broken Lab 07 does not block you.
3. The S3A JARs at `/opt/sd411/jars`. These are provisioned on the VM. If they are missing that is a provisioning failure, and you should escalate rather than fetch them yourself, because the institutional proxy intercepts TLS and Maven resolution will fail with a PKIX error.
4. Your alpha code entered at the top of `scripts/lab08_skew.py`. Nothing you submit counts without it.

---

## Part 0 - Bring the stack up

Download the [lab08 files](lab08.zip) and unzip them into the sd411 directory

```bash
cd lab08
docker compose up -d
bash scripts/verify_lab08.sh
```

Then build the two dimension tables this lab joins against. They are derived from Lab 06's fact, so they do not exist until you run this:

```bash
JARS=/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar

docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --jars $JARS \
  /opt/lab08/scripts/build_skew_dims.py
```

Re-run `verify_lab08.sh`; check 10 should pass now. Every part of the lab runs the same way, one part at a time:

```bash
docker compose exec -e LAB08_HOLD=240 spark-master spark-submit \
  --master spark://spark-master:7077 --jars $JARS \
  /opt/lab08/scripts/lab08_skew.py --part 2
```

`LAB08_HOLD` is how many seconds the script waits before exiting. The driver UI at `http://localhost:4040` exists only while the application is alive, so set it generously when you need to read the UI and to `0` when you are only collecting a time. Results land on the `spark-work` volume; pull them out with:

```bash
docker compose cp spark-master:/opt/spark/work-dir/results ./results
```

---

## Part 1 - Profile the key before you pay for the shuffle

**Record predictions P1 through P6 on the worksheet now, and get them initialed before you run anything.** A prediction written after the measurement is worth zero, and a wrong prediction that was honestly recorded is worth full credit. That is not generosity. The purpose of predicting is to find out where your model of the system is wrong, and you cannot find that out if you look at the answer first.

You have three candidate keys in the pitch fact, all real columns with real distributions:

- `pitch_type`, the Statcast code for what was thrown. Roughly fifteen distinct values, and one of them is a third of all pitches thrown in the league.
- `events`, the outcome of the plate appearance. It is null on most rows, because most pitches do not end a plate appearance.
- `batter`, the player ID. Several hundred distinct values, and some hitters take a lot more pitches than others.

For each key, compute two things. The first is the **key profile**: distinct key count, rows in the hottest key, and the ratio of the hottest key to the median key. The second is the **partition profile**: if you hashed that key into 16 partitions, how many rows land in the fattest partition compared to the median one.

These two ratios are not the same number, and for one of these three keys they are wildly different. Then compute one more thing, the fair share, which is total rows divided by 16. Compare each key's hottest-key count against that fair share. One of your three columns predicts partition skew and the other does not, and saying which, and why, is worth more than any timing in this lab.

---

## Part 2 - Induce the straggler and walk the UI

Now we make MIDN A's job happen. It joins the pitch fact to `dim/events` on the `events` key and aggregates the result. Three things are forced for you in the scaffold, and two of them are deliberately the wrong engineering choice:

- AQE is off, so nothing is quietly fixed behind your back.
- `spark.sql.autoBroadcastJoinThreshold` is `-1`, so a few dozen dimension rows do not simply broadcast away the whole problem.
- The join is a **left outer** join, not an inner join. That one is not arbitrary either, and Part 5.2 is where you find out why it had to be.

Run it, then open `http://localhost:4040` while the hold is running and do the walk from lecture, outside in. Jobs tab first: find the job that took the time. Open it and find the one stage that dominates the wall clock, because there is almost always exactly one and the rest are rounding error. That single decision throws away most of the page. Now open that stage and read the five numbers in the task-duration summary: min, 25th percentile, median, 75th, max.

Then confirm the diagnosis instead of assuming it. Sort the task list by shuffle read size. One task read far more than its peers. Tie that partition back to an actual key value using your Part 1 profile, and write down what that value is. If you cannot name it, you have not finished Part 2.

Record the spill columns too. Spill is the metric students skip, and it is the difference between "this partition was big" and "this partition did not fit in memory, went to local disk mid-computation, and came back."

---

## Part 3 - Salt it, three ways

The classic manual cure is **salting**: break the hot key into several keys so its rows spread across several partitions. `X` becomes `X#0` through `X#N-1`, which hash to N different partitions, and the work that crushed one task now spreads across N. Because this is a join, you also have to replicate the dimension side across all N salt values so a salted probe row still finds its match. Both helpers are provided.

Predict your N on the worksheet first. Then measure at N = 4, 16, and 64.

Two rules. First, before you time anything, assert that the salted result equals the unsalted result exactly, same rows and same numbers. A fast wrong answer is worth nothing, and this is precisely the transform where rows disappear silently. Second, do not assume more salt is better. It is not, and finding where the curve turns is the graded part. When you find it, look back at `SHUFFLE_PARTS` at the top of the scaffold before you write your explanation.

One implementation note that matters more than it looks. The scaffold salts with a hash of stable columns rather than with `rand()`. If Spark recomputes a lost partition, `rand()` draws different salts the second time and rows stop matching. Determinism here is not a style preference, it is correctness under recomputation, and that is the lineage model from Lecture 2 biting you somewhere you did not expect it.

---

## Part 4 - Hand it to AQE, then find out what AQE actually did

**Adaptive Query Execution** revises the plan at each shuffle boundary using the sizes it just measured, and it has been on by default since Spark 3.2. It coalesces small partitions, it converts join strategies at runtime, and it splits skewed partitions. So switch it on with `spark.sql.adaptive.skewJoin.enabled` and the problem should evaporate.

Sounds great, right? Run it at the shipped defaults and time it. Then go look at the straggler task. It did not shrink, and depending on your VM the total time may barely have moved at all.

Reconciling that is the graded work. Open the SQL tab and read the **final plan**, which is the plan that actually ran, not the static output of `explain()`. When those two disagree, the SQL tab is the truth. Find the `AQEShuffleRead` node and read what it claims it did, because "coalesced" and "skewed" are two different claims and only one of them is skew handling. Then go find the two configuration values that decide whether a partition counts as skewed at all, notice that **both** conditions have to hold, and work out which one your data fails. It is not the one most people guess.

Once you know, set the thresholds to values appropriate for this data size, re-run, and confirm from the final plan that the split happened this time. Then answer the question that makes this more than a config exercise: would you ship those threshold values to a production cluster, and why is the shipped default set where it is?

Finally, run part 4c. It takes the same key and runs it as a plain aggregation, with AQE and skew handling both still on. The straggler survives. Work out why, because the reason tells you exactly which skew problems you are still going to have to solve by hand.

---

## Part 5 - Stretch (+5 extra credit)

**5.1.** Re-run the Part 2 job with the broadcast threshold left alone, so the dimension broadcasts. Time it against your best salted run and your best AQE run. Then write two sentences on what that result means about Parts 2 through 4.

**5.2.** Run the same join as an inner join, broadcast still disabled. The skew disappears. Read both plans, find the operator the inner plan has that the left outer plan does not, and name the Catalyst rule responsible. You met that rule in Lab 05 and did not know you would need it again.

---

## The report

A memo to the SD322 standard. PDF, four pages maximum including figures, submitted with your `results/` JSON files.

Structure it as a performance report, not a lab log. State the problem, state your method including the trial protocol and why a median of three, present the measurements in tables a reader can check, then answer the mechanism questions below in prose. Every number in the memo must appear in one of your fingerprinted results files. At least one figure, and figures carry axis labels and units.

If a difference you measured is smaller than the spread across your three trials, say so and do not claim it. Reporting an effect you cannot support is the single fastest way to lose the report points, and it is the same standard the capstone will hold you to in Week 14.

**Mechanism questions.** Answer all five inside the memo body, not in an appendix.

**M1.** One of your three Part 1 keys has a large key skew ratio and a small partition skew ratio. Name it, and give the quantity that actually predicts partition skew. Then state what would have to change about the job for that key to become a skew problem after all.

**M2.** Your Part 2 stage had a median task and a max task. Explain, in terms of how a stage completes, why the stage wall clock is bounded below by the max task and not the mean. Then work out the floor imposed by your worker's core count, and say whether skew gets better or worse as a cluster grows.

**M3.** You chose a salt factor N. Name the cost that falls as N rises and the cost that rises as N rises, be specific about what grows on the dimension side, and explain what caps the useful value of N. Then say how you would pick N for a key you had never profiled.

**M4.** Under AQE defaults, the straggler did not shrink. State the two conditions a partition must meet to be treated as skewed, say which one your data failed and by roughly how much, and attribute any change you did see in total time to the correct mechanism. Then justify the shipped default from the perspective of whoever chose it.

**M5.** You now have three mitigations: salting, AQE skew splitting, and eliminating the shuffle. Rank them for the Part 2 job and defend the ranking. Then say which of them applies to a skewed aggregation, and what you would actually do about the `events` groupBy in a pipeline you owned.

---

## AI usage

Permitted under the course policy and disclosed in `AI_USAGE.md` alongside your submission: what tool, what you asked, and what you did with the answer. AI cannot replace measurement. Every number in your memo comes from your own run, carrying your own alpha code and fingerprint, and you have to be able to defend it out loud without notes.
