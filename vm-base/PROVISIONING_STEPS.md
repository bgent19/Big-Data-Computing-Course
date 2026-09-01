# SD411 Machine Provisioning — OS-Agnostic Steps

What has to be true on a machine before it can run the SD411 labs, stated
independently of any operating system, package manager, or distribution. Each
step lists **goal**, **why**, and **done when** — the check that proves the step
succeeded. Implement them however your platform prefers.

Reference implementation in this directory: `provision_ubuntu_vm.sh` — Ubuntu
24.04 on an ordinary network. It implements every step below except step 2,
which is conditional on your network.

All configuration values (`SD411_HOME`, image tags, dataset names, stream
filenames) come from `common.env`. Read them from there rather than hardcoding.

---

## 0. Prerequisites

| Requirement | Value |
|---|---|
| CPU | x86-64 or arm64, 4 cores recommended |
| RAM | 8 GB minimum (Spark + Kafka + MinIO run concurrently) |
| Disk | 40 GB free after OS install |
| Privileges | Administrator/root for steps 1–3 and 6 |
| Network | Outbound HTTPS to the package repo, the container registry, Maven Central, and the dataset sources |

Identify the **login user** — the unprivileged account the student will actually
work in. Several later steps grant it ownership or group membership. Do not
provision into the root account.

---

## 1. Base system packages

**Goal.** Install the OS-level tools the rest of provisioning depends on: a CA
certificate bundle, an HTTPS-capable downloader (`curl` or equivalent), a
signature/keyring tool, Python 3 with virtual-environment and package-installer
support, a JSON processor (`jq`), a socket-listing tool (`ss`/`netstat`), and an
archive extractor (`unzip`).

**Why.** Every later step shells out to one of these. Installing them first
turns a late, confusing failure into an early, obvious one.

**Done when.** `curl`, `python3`, `jq`, and `unzip` are all on `PATH` and
`python3 -m venv --help` exits 0.

---

## 2. Enterprise TLS trust *(conditional — skip on ordinary networks)*

**Goal.** If the network intercepts TLS, install the intercepting root CA into
**every** trust store on the machine, not just the OS one:

1. the OS trust store,
2. Python's `certifi` bundle (used by `requests`, `pip`, and `pybaseball`), and
3. the JVM trust store inside any container that makes TLS calls — either bake
   the CA into a derived image or bind-mount the bundle at run time.

**Why.** These three stores are independent. Fixing only the OS store produces
the classic symptom: `curl` works, Python and Spark still fail with certificate
errors.

**Done when.** An HTTPS fetch of an external host succeeds from the shell, from
Python (`requests.get`), and from inside a container.

**Skip this step entirely** on a normal network — `provision_ubuntu_vm.sh`
omits it. On an intercepting network you must supply this stage yourself; it is
the one part of provisioning that is specific to your institution's network
rather than to the labs.

---

## 3. Container runtime

**Goal.** Install a container engine and a Compose v2–compatible tool, enable
the engine to start at boot, and grant the login user permission to talk to the
engine socket without elevation.

**Why.** All labs run as containers described by Compose files. If the student
needs `sudo` for every `docker` call, bind-mounted files end up root-owned and
later labs break.

**Done when.** As the *login user*, with no elevation: the engine version
prints, `compose version` prints v2.x, and `run --rm hello-world` (or
equivalent) succeeds. Group membership usually requires a logout/login or a
fresh shell session to take effect.

---

## 4. Shared Python environment

**Goal.** Create an isolated Python environment under `${SD411_HOME}/venv` and
install the data-analysis dependencies (`pybaseball`, `pyarrow`, `pandas`) into
it.

**Why.** A shared, path-stable environment means every lab and helper script
references the same interpreter. Isolating it from the system Python avoids
conflicts with OS-managed packages — a hard error on distributions that mark
the system Python as externally managed.

**Done when.** `${SD411_HOME}/venv/bin/python -c "import pandas, pyarrow,
pybaseball"` exits 0.

---

## 5. On-disk layout and lab repository

**Goal.** Create the directory tree the labs assume and place the lab repository
inside it:

```
${SD411_HOME}/          # root of everything SD411 owns
  ├── venv/             # step 4
  ├── data/             # seeded datasets, stream sources
  ├── jars/             # connector JARs (step 6)
  └── repo/             # the lab repository itself
```

**Why.** Compose files and lab scripts refer to these paths absolutely. A single
predictable root also makes the image reproducible and easy to reset.

**Done when.** All four directories exist and the whole tree is owned by the
login user, not root.

---

## 6. Stage connector JARs

**Goal.** Download the Spark connector JARs (S3A/Hadoop-AWS and Spark-Kafka,
matched to the pinned Spark version) into `${SD411_JARS}`, on the host, at
provisioning time.

**Why.** Resolving them at job-submit time via `--packages` requires each
student's Spark container to reach Maven Central over TLS. Staging them once on
the host removes that dependency, guarantees identical versions across the
class, and works offline.

**Version rule.** JAR versions must match the pinned Spark and Hadoop versions
exactly. A mismatch fails at runtime with a `NoSuchMethodError`, not at
download.

**Done when.** The expected JAR files are present in `${SD411_JARS}` with
non-zero size.

---

## 7. Pre-pull pinned container images

**Goal.** Pull every image tag listed in `common.env` — Spark, Hadoop, MinIO,
the MinIO client, and Kafka — so they sit in the local image cache.

**Why.** 25+ students pulling multi-gigabyte images simultaneously on lab day
saturates the network and wastes the class period. Pre-pulling also freezes the
exact image digests into the golden image, so every student runs identical
software.

**Pin by tag, never `latest`.** Fail provisioning loudly on a pull error rather
than deferring the failure to a student mid-lab.

**Done when.** Each pinned tag appears in the local image list.

---

## 8. Seed the primary dataset

**Goal.** Produce the shared season CSV under `${SD411_DATA}`, fetching the real
dataset when the network allows and falling back to a synthetic generator with
the same schema when it does not.

**Why.** Students should never download data during a lab. The synthetic
fallback keeps every lab exercise runnable on a restricted network; only the
values change, not the columns or the code path.

**Done when.** The CSV exists, is non-empty, and its header matches the schema
the labs expect.

---

## 9. Stage the streaming replay source

**Goal.** Fetch the pinned GitHub Archive hour into `${SD411_DATA}/gharchive/`
and convert it once into the replay file `${STREAM_DIR}/${STREAM_FILE}`. Fall
back to deriving the stream from the seed dataset if the archive is
unreachable.

**Why.** The streaming module replays a fixed file into Kafka so every run is
deterministic and reproducible. Generating it at provisioning time — with
network and elevation available — means students only ever read a local file.

**Done when.** The replay file exists, is non-empty, and is owned by the login
user.

---

## 10. Distribute configuration to each lab

**Goal.** Stamp the canonical values from `common.env` into a per-lab
environment file in every lab directory in the repository.

**Why.** Compose reads a `.env` next to its own file. Generating those from one
source of truth means changing an image tag or a path in one place propagates
everywhere, instead of drifting lab by lab.

**Done when.** Every lab directory contains a generated environment file whose
values match `common.env`.

---

## 11. Fix scratch-directory ownership

**Goal.** For any lab that bind-mounts a host scratch directory, create it and
give it to the login user **before** the first container start.

**Why.** A container engine that creates a missing bind-mount source creates it
as root. The student's later writes then fail with permission errors that look
like lab bugs. Prefer named volumes for container scratch where possible; this
step is the safety net for labs not yet migrated.

**Done when.** Every lab scratch directory exists and is owned by the login
user.

---

## 12. Verify, then capture the image

**Goal.** Run the verification pass **as the login user, not as root**, then
tear down cleanly before snapshotting or exporting.

1. Log out and back in so the container-group membership is active.
2. Bring up a lab and run its own verification script; every check must pass.
   Those scripts check the shared plumbing — generated environment file, base
   compose, seeded data, staged JARs — before anything lab-specific, so a
   passing lab is also a passing image.
3. Bring all lab stacks down *including volumes*, so no scratch data or
   seeded object-store state is baked into the image.
4. Snapshot / export / clone.

**Why.** Verifying as root hides exactly the permission problems students will
hit. Capturing with live volumes bakes one machine's scratch state into every
clone.

**Done when.** Verification passes as the unprivileged user, no lab containers
or volumes remain, and the exported image boots and passes verification again
on a fresh clone.

---

## Re-running

Every step above is written to be idempotent: it detects the already-done state
and skips rather than failing. Re-running the whole provisioning flow on a
working machine is a supported repair path, and it is the intended way to apply
a change to `common.env` — bump the value, re-run, re-verify.
