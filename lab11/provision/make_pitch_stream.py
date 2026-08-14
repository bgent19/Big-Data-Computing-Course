#!/usr/bin/env python3
"""
make_pitch_stream.py  (instructor plumbing, run once per VM image)

Builds the deterministic event-time stream lab11 replays into Kafka. Standard
library only, so it runs in the pinned Spark image with no pip install and no
TLS-interception exposure (lab09 discipline).

The stream is a pure function of (seed CSV, --rows, --seed, --spacing-ms,
--window-seconds, --late-fraction, --late-lateness-seconds). Given those, every
VM produces byte-identical output, which is what makes the windowed aggregate,
the closed-window oracle, and the drop count gradeable rather than merely
different across students.

Shape of the output (one JSON object per line, ready for produce.sh):
    {"rid": <int>, "pitch_type": "FF", "event_time_ms": <int>}

Two-phase emission ORDER (this is the whole design):
  Phase A, the backbone: every on-time pitch, in event-time order, at a fixed
    grid spacing. The event-time frontier climbs monotonically to T_max.
  Phase B, the late tail: a seeded fraction of pitches whose event_time is set
    to (T_max - late_lateness), emitted AFTER all of phase A. When they arrive
    the frontier is already at T_max, so their observed lateness is exactly
    late_lateness seconds, independent of how produce.sh paces the bytes.

Consequence, and the reason two watermarks tell two stories on ONE stream:
  - A watermark allowance LARGER than late_lateness accepts phase B (0 drops).
  - A watermark allowance SMALLER than late_lateness abandons all of phase B,
    so the dropped-row count equals the phase B size, deterministically.
Neither phase B outcome touches the CLOSED-window total, because phase B's
event-times land inside the open band within the allowance of the frontier,
never inside a closed window. That is why the exactly-once proof (which compares
the closed-window sink to the closed-window oracle) is immune to the late tail.
"""
import argparse
import csv
import json
import random
import sys


def parse_args():
    p = argparse.ArgumentParser(description="Build the lab11 event-time pitch stream.")
    p.add_argument("--seed-csv", required=True, help="path to statcast_2025.csv")
    p.add_argument("--out", required=True, help="output stream file (JSON lines)")
    p.add_argument("--rows", type=int, default=20000)
    p.add_argument("--seed", type=int, default=411)
    p.add_argument("--spacing-ms", type=int, default=180,
                   help="event-time gap between consecutive on-time pitches")
    p.add_argument("--window-seconds", type=int, default=60)
    p.add_argument("--late-fraction", type=float, default=0.08)
    p.add_argument("--late-lateness-seconds", type=int, default=90,
                   help="how far behind the frontier phase B lands, in event time")
    p.add_argument("--base-epoch-ms", type=int, default=1_760_000_000_000,
                   help="event-time origin; any fixed value works, it only shifts the grid")
    return p.parse_args()


def pick_pitch_type_column(header):
    # Statcast documents the column as pitch_type. Fall back defensively so a
    # provisioning run does not die on a header quirk.
    for cand in ("pitch_type", "PitchType", "pitch_name"):
        if cand in header:
            return header.index(cand)
    raise SystemExit("make_pitch_stream: no pitch_type column found in seed header")


def main():
    a = parse_args()
    g = a.spacing_ms

    # ---- draw N valid pitches deterministically (first-N with a pitch_type) --
    rows = []
    with open(a.seed_csv, newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        pt_idx = pick_pitch_type_column(header)
        for rec in reader:
            if len(rec) <= pt_idx:
                continue
            pt = rec[pt_idx].strip()
            if not pt or pt.upper() in ("NA", "NULL"):
                continue
            rows.append(pt)
            if len(rows) >= a.rows:
                break
    if len(rows) < a.rows:
        print("make_pitch_stream: WARN only %d valid pitches (< --rows %d)"
              % (len(rows), a.rows), file=sys.stderr)

    n = len(rows)
    t_max = a.base_epoch_ms + (n - 1) * g          # frontier the backbone reaches

    # ---- choose the seeded late set --------------------------------------------
    # Pin the last PIN indices as on-time so index n-1 is never displaced. If the
    # frontier record itself were late, phase A would never reach t_max and the
    # late tail's lateness would be measured against a frontier that never
    # arrives. lab10 pinned its final hours for the same reason.
    PIN = 8
    rng = random.Random(a.seed)
    late_count = int(round(n * a.late_fraction))
    pool = range(max(0, n - PIN))
    late_idx = set(rng.sample(list(pool), min(late_count, len(pool)))) if late_count else set()

    # ---- emit phase A (backbone, event-time order) then phase B (late tail) ----
    late_event_ms = t_max - a.late_lateness_seconds * 1000
    written_a = written_b = 0
    with open(a.out, "w") as out:
        # Phase A: every non-late pitch at its grid slot, in order.
        for i in range(n):
            if i in late_idx:
                continue
            obj = {"rid": i, "pitch_type": rows[i], "event_time_ms": a.base_epoch_ms + i * g}
            out.write(json.dumps(obj, separators=(",", ":")) + "\n")
            written_a += 1
        # Phase B: the late tail, all pinned late_lateness behind the frontier.
        for i in sorted(late_idx):
            obj = {"rid": i, "pitch_type": rows[i], "event_time_ms": late_event_ms}
            out.write(json.dumps(obj, separators=(",", ":")) + "\n")
            written_b += 1

    # ---- report (goes to data/README.md over the reference block) --------------
    window_ms = a.window_seconds * 1000
    span_s = (t_max - a.base_epoch_ms) / 1000.0
    print("make_pitch_stream: wrote %s" % a.out)
    print("  total records ...... %d  (phase A %d, phase B late tail %d)"
          % (n, written_a, written_b))
    print("  event-time span .... %.0f s  (~%d windows of %ds)"
          % (span_s, (t_max - a.base_epoch_ms) // window_ms + 1, a.window_seconds))
    print("  frontier T_max ..... %d ms" % t_max)
    print("  late tail lands at . %d ms  (%d s behind frontier)"
          % (late_event_ms, a.late_lateness_seconds))
    print("  drop count under a watermark < %ds ... %d (== phase B)"
          % (a.late_lateness_seconds, written_b))
    print("  drop count under a watermark > %ds ... 0"
          % a.late_lateness_seconds)


if __name__ == "__main__":
    main()
