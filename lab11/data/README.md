# lab11 data - the pitch event stream

lab11 does not read a Parquet fact off object storage the way the Module 2 labs
did. Its data is a Kafka topic, `pitches`, seeded once from the same Statcast
season every lab in this course has used. Statcast has always been a stream:
every pitch is an event generated live during a game, and Module 3 is about what
we could do with it the night it happens rather than months later as a batch
file. This lab finally treats it that way.

## Provenance

The stream is drawn deterministically from `${SD411_DATA}/${SEED_CSV}`
(`statcast_2025.csv`, the single-season canonical seed, `SEED_MIN_ROWS=600000`).
No new dataset is downloaded and no `lab06` fact is read, so lab11 has no upstream
lab dependency: a broken lab06 or lab09 cannot cost a student this period.

## How it is built (instructor plumbing, run once per VM image)

`provision/make_pitch_stream.py` draws the first `--rows` pitches carrying a valid
`pitch_type`, lays them on a fixed event-time grid, and writes one JSON object per
line ready for the producer:

```
{"rid": <int>, "pitch_type": "FF", "event_time_ms": <int>}
```

Run it through the stager, which resolves the paths from the stamped `.env`,
creates `${SD411_DATA}/lab11`, applies the analytic defaults, and hands the file
back to the login user. `${SD411_DATA}` is root-owned on the golden image, hence
the `sudo`:

```
sudo ./provision/stage_pitch_stream.sh          # idempotent; FORCE=1 rebuilds
```

That is exactly this invocation, which you can also run by hand:

```
python3 provision/make_pitch_stream.py \
  --seed-csv ${SD411_DATA}/statcast_2025.csv \
  --out      ${SD411_DATA}/lab11/pitch_stream.jsonl \
  --rows 20000 --seed 411 --spacing-ms 180 --window-seconds 60 \
  --late-fraction 0.08 --late-lateness-seconds 90
```

The generator is standard library only, so it needs no venv and no network. It
is a pure function of the seed CSV and those arguments: every VM built from the
same seed produces a byte-identical file (verified, sha256
`20b2477de27e51f04bc035bfec7692785464714bb7ac81cf2adcb15f003eb72f`), which is
what makes the drop count and the oracle gradeable rather than merely different
across students.

`scripts/produce.sh` then creates the single-partition topic and paces the file
into it. The topic survives `docker compose down` (without `-v`) in the
`kafka-data` volume, so it is seeded once and replayed all period.

## The two-phase order (this is the whole design)

The producer emits in two phases, and the ORDER is what makes two watermarks tell
two stories on one stream.

Phase A, the backbone: every on-time pitch, in event-time order, one every
`spacing-ms` (180 ms). Twenty thousand of them span `20000 * 0.18 s = 3600 s`,
about one hour, so about 60 tumbling windows of 60 s. The event-time frontier
climbs smoothly to `T_max`.

Phase B, the late tail: a seeded `late-fraction` (0.08) of the pitches, all with
`event_time = T_max - 90 s`, emitted AFTER the entire backbone. Because the
frontier is already at `T_max` when they arrive, every late pitch has the same
observed lateness, 90 s, no matter how `produce.sh` paces the bytes.

The last 8 records are pinned on-time so the frontier is actually reached (lab10
pinned its final hours for the same reason).

## The numbers you are allowed to know before the lab

These are structural, not measured, so they are safe to publish:

| Quantity | Value | Why |
|---|---|---|
| records produced | 20000 | `--rows` |
| late tail size (phase B) | 1600 | `0.08 * 20000` |
| observed lateness of the tail | 90 s | `--late-lateness-seconds` |
| drop count, watermark 30 s, small batches | 1600 | whole tail is late-by-contract |
| drop count, watermark 180 s | 0 | tail lands inside the open band |
| event-time span | ~3600 s | `20000 * 180 ms` |
| tumbling windows | ~60 | span / 60 s |

The late tail's 1600 is the number Part 1 asks you to predict the tight-watermark
drop from. It is not a coincidence.

## The batch size is part of the drop condition

"Watermark below 90 s drops the tail" is the analytic story, and it is true, but
only once you say what the watermark IS at the moment the tail arrives. Spark
filters late records against a watermark that lags the data by about one
micro-batch of event time, so the real condition is

    (one micro-batch of event time) + allowance  <  the tail's lateness (90 s)

At the lab's normal `maxOffsetsPerTrigger=4000`, one batch spans `4000 * 180 ms`
= 720 s of event time, which swamps the 90 s of lateness: the tail arrives while
the filtering watermark is still 12 minutes behind it, so a 30 s allowance drops
NOTHING. Part 1 therefore runs at `maxOffsetsPerTrigger=200` (36 s per batch):
36 + 30 < 90, and all 1600 go. Measured on the golden VM: 19589 (tight oracle)
minus 17989 (tight sink) = 1600 exactly.

That is not a wart to hide from the class. It is the sharpest available lesson
about what a watermark is: not a property of the data, but of what the engine has
seen and when it got around to looking.

## What is NOT published

The per-`(window, pitch_type)` counts and the peak live state-row count depend on
the seed draw and are baselined on the golden VM, not asserted here. The
closed-window grand total you compute in Part 0 with `oracle.py` is yours to
record; the packet does not hardcode it, because the exactly-once proof is a
comparison of your sink against your own oracle, not against a printed constant.

## Why the exactly-once proof is immune to the late tail

Phase B changes whether the tail is dropped, but never changes any CLOSED
window's count: the tail lands inside the open band within one allowance of the
frontier, never inside a window the oracle counts as closed. `oracle.py` keeps
only closed windows, and the streaming sink in append mode emits only closed
windows, so the two compare on identical ground. That is why Part 2 is a clean
equality and not a fuzzy "about the same."
