# lab09/data — the replay file, and the numbers Part 3 asks you to predict

Nothing in this directory is produced by you. The replay file is staged once on
the golden VM, pre-term, with network, by `vm-base/scripts/stage_stream.sh`
(stage 9 of `provision_student_vm.sh`). Students never download anything.

## What exists, and where

| Thing | Path | Made by |
|---|---|---|
| GitHub Archive hour | `${SD411_DATA}/gharchive/${GH_ARCHIVE_FILE}` | fetched at provision time |
| Replay file | `${STREAM_DIR}/${STREAM_FILE}` | `make_stream.py` |
| Three raw events | `data/sample_events.json` | `make_stream.py`, copied here |

The broker container sees the replay file at `/seed/stream/${STREAM_FILE}` —
`${SD411_DATA}` is bind-mounted read-only at `/seed`. `replay.sh` reads it from
there; it never reaches the host path directly.

Every one of those names comes from the stamped `.env`. If `verify_lab09.sh`
check 10 fails, the replay file was not staged. That is a provisioning failure,
not something you did — tell the instructor.

## Format

One event per line, three tab-separated fields, no literal tab or newline
inside any field:

```
entity_key <TAB> type_key <TAB> compact_json_value
```

- `entity_key` — **high cardinality**: GitHub repo name (`owner/name`), or a
  Statcast `game_pk` under the fallback source.
- `type_key` — **low cardinality**: GitHub event type, or `pitch_type`.
- `value` — the event itself, single-line JSON.

Two key columns is the whole point of Part 3. The same events keyed by each
column land in two identical 4-partition topics and the distributions do not
look alike. High cardinality spreads; low cardinality piles up. That is the
Module 2 skew pathology reappearing in the transport layer.

## Type-key distribution — predict Part 3 from this table

**Measured, not estimated.** This is `make_stream.py --source gharchive` over
the configured hour `GH_ARCHIVE_HOUR=2026-03-04-15`, at `STREAM_EVENTS=200000`.
Re-run `stage_stream.sh` with `FORCE=1` and paste its output here if the hour
in `common.env` ever changes.

### Source: GitHub Archive, hour 2026-03-04-15

158509 events (the hour holds fewer than the 200000 ceiling — well above the
`STREAM_MIN_LINES=60000` floor). 86378 distinct repo keys, 16 distinct types.

| Type | Count | Share |
|---|---:|---:|
| PushEvent | 119200 | 75.20% |
| CreateEvent | 15970 | 10.08% |
| PullRequestEvent | 5934 | 3.74% |
| DeleteEvent | 5919 | 3.73% |
| IssueCommentEvent | 2496 | 1.57% |
| IssuesEvent | 2057 | 1.30% |
| PullRequestReviewCommentEvent | 2034 | 1.28% |
| WatchEvent | 2008 | 1.27% |
| PullRequestReviewEvent | 1813 | 1.14% |
| ForkEvent | 393 | 0.25% |
| all others (6 types) | 685 | 0.43% |

The exact hour shifts these by a couple of points and shifts nothing that
matters. PushEvent's dominance is the lab, and it is stable across every hour
of GitHub's history. Note that its share here is well above the 45 to 50% the
instructor key's pre-measurement table predicted — one type carrying three
quarters of the traffic makes the Part 3 hot partition more lopsided, not less,
so every prediction the lab asks for still lands the same way, harder.

86378 distinct `entity_key` values against 158509 events is enough that four
partitions come out close to even. That contrast is Part 3's whole point.

### Fallback source: Statcast

If the GitHub Archive hour cannot be staged, `stage_stream.sh` falls back to
`make_stream.py --source statcast` over `${SD411_DATA}/${SEED_CSV}`, which needs
no new provisioning at all. The event vocabulary changes and nothing else does:

| GitHub | Statcast | Role |
|---|---|---|
| repo name | `game_pk` | high-cardinality entity key |
| event type | `pitch_type` | low-cardinality, badly skewed type key |

`FF` (four-seam fastball) plays PushEvent's part, at roughly a third of pitches
(estimate, not measured against the seed). The share is smaller than
PushEvent's, so the hot partition is less extreme, but the shape of the answer
and every prediction the lab asks for are unchanged.

## Rebuilding it (instructor)

```bash
bash vm-base/scripts/stage_stream.sh            # idempotent; skips a good file
FORCE=1 bash vm-base/scripts/stage_stream.sh    # rebuild
SOURCE=statcast bash vm-base/scripts/stage_stream.sh
```

`STREAM_EVENTS` sets how many events are written; `STREAM_MIN_LINES` is the
floor `verify_lab09.sh` check 10 enforces. Both live in `common.env`, so change
them there and re-run `vm-base/scripts/sync_env.sh` — never in a lab file.
