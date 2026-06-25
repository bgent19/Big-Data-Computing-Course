# SD411 Lab 1 — Lab Environment Test

**Week 1 (Thu, 20 Aug 2026)**
**Due:** before the start of Lab 2 (Thu, 27 Aug 2026)
**Submission:** electronic, per Part 4 below
**Grace days do not apply to Lab 01.** Per the course policy, no student
proceeds past Week 1 without a working stack. If you cannot get there in
the lab slot, you are expected to finish before our next lab.

---

## What this lab is

You are going to bring up the cluster you will use for the rest of the term:
Spark + MinIO running on your VM under Docker Compose. Then you will run a
verification script and a tiny PySpark job that reads from MinIO, aggregates,
and writes back. If everything works, you are cleared for Lab 2.

## Learning objectives

By the end of this lab you will be able to:

1. Describe what each service in the SD411 stack does and which port it
   listens on.
2. Bring the stack up, tear it down, and inspect a misbehaving container
   using only `docker compose` commands.
3. Run a PySpark job against a MinIO-backed S3A object store and confirm
   the result via the MinIO console.
4. Triage a stack failure: read a verify-script failure, identify which
   container is at fault, and locate the relevant log.

---

## Part 1 — Bring up the stack

### Step 1.1 — Download the files

Open a terminal(CTRL-ALT-t) and navigate to the lab01 directory.

```bash
cd ~/sd411
unzip ~/Downloads/lab01.zip
cd lab01
```

## One-time JAR download

Spark needs the S3A connector (two JARs) to read from MinIO. The natural
way to fetch them is `spark-submit --packages`, which uses Maven over
HTTPS from inside the container. On the USNA network that fails: the
container's Java truststore does not trust the institution's TLS
interception CA, so Maven Central downloads die with `PKIX path
building failed`.

The workaround is to download the JARs on the host instead. The host's
curl uses the system CA bundle, which does include the institutional
root, so the download works.

```bash
bash scripts/download_jars.sh
```

This is a one-time step. The `./jars/` directory is `.gitignore`d so the
data stays local to your machine. The compose file mounts that
directory into the Spark containers at `/opt/spark/extra-jars`, and
Part 3 will tell `spark-submit` to use those JARs directly.

### Step 1.2 — Confirm the lab files are present

```bash
ls -la
# You should see:
#   docker-compose.yml
#   scripts/verify_stack.sh
#   scripts/hello_statcast.py
#   scripts/download_jars.sh
#   data/statcast_sample.csv  # the seed dataset
#   jars/                     # populated by JAR download
```

If `data/statcast_sample.csv` is missing, ask the instructor. The rest of
the lab depends on it being seeded into MinIO.

### Step 1.3 — Bring the stack up

```bash
docker compose up -d
```

What you should observe (over 1–5 min the first time, faster afterwards):

1. Image pulls (`apache/spark:3.5.3-python3`, `minio/minio:...`,
   `minio/mc:...`). First-time download of the Spark image is ~1.2 GB.
2. Four containers created: `sd411-spark-master`, `sd411-spark-worker`,
   `sd411-minio`, `sd411-minio-init`.
3. The first three transition to `running`; the fourth (`minio-init`) is
   a one-shot. It runs to completion and exits 0. **This is expected.**

Check status:

```bash
docker compose ps
```

You should see three services `running` and you may see `sd411-minio-init` as `exited (0)`.

### Step 1.4 — Open the UIs in a browser

Confirm each of these loads in your VM's browser:

| URL | What it is |
| --- | --- |
| <http://localhost:8080> | Spark master UI. Should show 1 worker registered. |
| <http://localhost:8081> | Spark worker UI. |
| <http://localhost:9001> | MinIO console. Log in: `sd411admin` / `sd411password`. |

In the MinIO console, click into the `sd411` bucket → `raw/`. You should
see `statcast_sample.csv`. If not, run the recovery from **Part 1.5**.

### Step 1.5 — Recovery: if `minio-init` failed

If `docker compose ps` shows `sd411-minio-init` with a non-zero exit code:

```bash
docker logs sd411-minio-init        # read the error
# Common cause: data/statcast_sample.csv is missing. Fix that, then:
docker compose up -d --force-recreate minio-init
```

---

## Part 2 — Verify (5 min)

Run the verification script:

```bash
bash scripts/verify_stack.sh
```

You will see twelve named checks, each `[PASS]` / `[FAIL]` / `[WARN]`, and
a summary at the end. **Do not proceed to Part 3 until all checks pass.**

If a check fails, the script prints the next thing to look at, usually a
log file. Read it. The script is built so that failures point at their
own root cause whenever possible.

**Save the verify output.** You will paste it into your submission:

```bash
./scripts/verify_stack.sh > verify_output.txt 2>&1
```

---

## Part 3 — Hello Statcast

You will now run a tiny PySpark job that reads `statcast_sample.csv` from
MinIO, computes pitch counts and average velocity per pitch type, and
writes the result back to MinIO as Parquet.

### Step 3.1 — Stage the script into the work directory

```bash
cp scripts/hello_statcast.py work/hello_statcast.py
```

(The `work/` directory is mounted into the Spark containers as `/opt/work`.)

### Step 3.2 — Make the one required edit

Open `work/hello_statcast.py` in your editor. Find the line:

```python
YOUR_ALPHA = "REPLACE_ME"
```

Replace `REPLACE_ME` with your actual alpha code, e.g. `"m260042"`. This
is the only edit you are required to make. It puts your output under a
path unique to you, which is how the instructor confirms during the lab
check that the script actually ran on your stack.

### Step 3.3 — Run it

```bash
docker exec -it sd411-spark-master spark-submit \
    --jars /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
    /opt/work/hello_statcast.py
```

Expected output (abridged):

```
Spark version:  3.5.3
Master:         spark://spark-master:7077
============================================================
Loaded s3a://sd411/raw/statcast_sample.csv
  Rows:    4,488
  Columns: 119
  First 10 columns / types:
    pitch_type                string
    game_date                 date
    release_speed             double
    ...

Pitch type breakdown:
+----------+----+------------+
|pitch_type|n   |avg_velo_mph|
+----------+----+------------+
|FF        |1233|94.1        |
|SI        |838 |93.2        |
|SL        |596 |85.9        |
|CH        |440 |85.3        |
|FC        |408 |90.2        |
|CU        |370 |79.7        |
|ST        |332 |82.0        |
|FS        |139 |85.7        |
|KC        |68  |81.8        |
|NULL      |37  |NULL        |
|SV        |25  |79.8        |
|PO        |1   |91.5        |
|KN        |1   |86.2        |
+----------+----+------------+

Wrote summary to s3a://sd411/derived/<your_alpha>/pitch_type_summary
Lab 01 hello-Statcast: COMPLETE.
```

Real Statcast data is messy. Expect somewhere around a dozen distinct
`pitch_type` values, including a `NULL` bucket (pitchouts and edge
cases legitimately have no pitch_type). If your breakdown shows only
two or three pitch types, you are probably running the synthetic
fallback rather than real data: flag this to the instructor.

### Step 3.4 — Confirm the write in the MinIO console

Refresh the MinIO console. Navigate to `sd411` → `derived/<your alpha>/`.
You should see `pitch_type_summary/` containing one or more `.parquet`
files plus a `_SUCCESS` marker. Take a screenshot.

### Step 3.5 — Confirm the standalone cluster ran the job

Browse to the Spark master UI at <http://localhost:8080>. Under
**Completed Applications**, you should see `sd411-lab01-hello-statcast`.
If that panel is empty after the script finished, the job ran in local
mode and the worker did nothing. Look for `Master: local[*]` in your
script's output (it should read `spark://spark-master:7077` instead) and
flag the issue.

---

## Stuck? Help protocol

When you've been on the same failure for 20 minutes:

1. **Re-read the prerequisite section** and verify each item.
2. **Run `./scripts/verify_stack.sh` again** and read the output carefully.
   Most failures are explicitly diagnosed there.
3. **Capture three things** before asking for help:
   - The output of `./scripts/verify_stack.sh`
   - The output of `docker compose ps`
   - The last 50 lines of the offending container's logs:
     `docker logs --tail 50 <container-name>`
4. **Ask a classmate** at a similar point in the lab. Discussion of
   approach and Spark UI observations is encouraged. Sharing or copying
   code is not. (See the Collaboration section of the course policy.)
5. **Ask the instructor** in lab, or via the course channel out of lab.
   Bring the three artifacts from step 4.

If a classmate helped you fix it, you cite that in your AI/collaboration
statement (Part 4). That's not a penalty. It's how this course works.

---

## Part 4 — Submit

Submit a single zip file named `sd411_lab01_<alpha>.zip` containing:

| File | What it is |
| --- | --- |
| `report.md` | a 1-page write-up (see template below) |
| `verify_output.txt` | the captured output of `verify_stack.sh` |
| `compose_ps.txt` | the output of `docker compose ps` |
| `spark_ui.png` | screenshot of <http://localhost:8080> showing your completed application |
| `minio_output.png` | screenshot of `sd411/derived/<alpha>/pitch_type_summary/` in the MinIO console |
| `hello_statcast.py` | your edited copy of the script (with your alpha code) |
| `ai_usage.md` | AI usage statement (template below); required even if "none" |

### `report.md` template (~1 page)

```
# SD411 Lab 1 — <Your name, alpha>

## What I built
2–4 sentences describing what's running on your VM and how the four
services connect to each other.

## What broke and how I fixed it
For each non-trivial problem you hit during the lab, write 1–2 sentences:
what you saw, where you looked, and what fixed it. If nothing broke, say
that.

## What I'd do differently next time
1 sentence. Be concrete.

## Spark UI observation
Look at http://localhost:8080 while your hello_statcast.py is running.
What does the "Running Applications" section show? After it finishes,
what does "Completed Applications" show, and what does it tell you about
how Spark scheduled the work?
```

### `ai_usage.md` template

```
# AI usage — SD411 Lab 1

Tool(s) used:    e.g., Claude, ChatGPT, Copilot, GitHub Copilot Chat, or "none"
Used for:        what you asked it about (debugging, explanation, syntax, ...)
Did NOT use for: anything you were uncertain about

Most consequential exchange (paste the prompt and reply, ~10 lines max):
> ...

I can defend every line of what I submitted. Signed: <alpha>
```

If you did not use AI: still submit `ai_usage.md` with `Tool(s) used: none`
and the signed line at the bottom. Required for every assignment in this
course. See the AI policy in the syllabus.

---

## Grading rubric

Lab 01 is graded pass / no-pass. You receive full credit (100% of 1's
weight in the gradebook) for a passing submission, and zero for non-passing.

### Pass requires ALL of:

- [ ] Verify script: 12/12 PASS (warnings on disk space are tolerated)
- [ ] MinIO console screenshot shows `derived/<your alpha>/pitch_type_summary/`
      with at least one `.parquet` file and a `_SUCCESS` marker
- [ ] Spark master UI screenshot shows your `sd411-lab01-hello-statcast`
      application under Completed Applications
- [ ] Your script's output shows `Master: spark://spark-master:7077`
      (not `local[*]`)
- [ ] `report.md` answers all four prompts substantively
- [ ] `ai_usage.md` is present and signed (even if "none")

---

## Glossary of services

| Service | What it does | URL / port |
| --- | --- | --- |
| **spark-master** | Coordinates the standalone Spark cluster. Driver programs connect to it; workers register with it. | `:7077` RPC, `:8080` UI |
| **spark-worker** | Executes tasks. Memory and core count visible in its UI. | `:8081` UI |
| **minio** | S3-compatible object store. We use it like S3 for the rest of the term. | `:9000` API, `:9001` console |
| **minio-init** | One-shot script container. Creates the `sd411` and `landing` buckets, seeds the Statcast sample, exits. Not part of the running stack: it just bootstraps state. | (no UI) |

You will get more comfortable with each of these as the course progresses.
For Lab 01, knowing what's running is enough.

---

*Created by: LT B. Gentile, USN, 2026*
