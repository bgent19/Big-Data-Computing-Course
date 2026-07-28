#!/usr/bin/env python3
"""
seed_statcast_multiseason.py — DRAFT multi-season replacement for
seed_statcast.py, written for Lab 08 measurability (see vm-base/SEED_MULTISEASON.md).

Why this exists
---------------
At the single 2025 season (~712K pitches, ~28 MB parquet fact) the Lab 08 skew
job is subsecond and every Part 2-5 wall-clock effect is buried in trial spread.
Enlarging the seed to several seasons gives the join enough real work that the
straggler is measurable without leaning on the lab-scale memory-pressure hack.

Backward compatible
-------------------
If SEED_SEASONS is unset, this behaves exactly like seed_statcast.py: one pull
over SEED_SEASON_START..SEED_SEASON_END. Set SEED_SEASONS to a space-separated
list of years to fetch and concatenate multiple seasons, e.g.:

    SEED_SEASONS="2021 2022 2023 2024 2025"

Each season is fetched separately and APPENDED to the CSV so peak memory stays
at one season, not the whole corpus. The header is written once.

Rollout, floors, and the AQE guardrail are all in vm-base/SEED_MULTISEASON.md.
DO NOT wire this into seed_statcast.sh until it has been run once end to end on
a machine with working pybaseball TLS — the USNA proxy blocks it on the dev box.

Exit codes (same contract as seed_statcast.py):
   0  seed written and passed sanity floors
  64  pybaseball import / fetch failed (caller should try the synthetic fallback)
  65  fetched but failed a sanity floor (too small / missing columns)
"""
import os
import sys

# Single-season fallback window (identical default to seed_statcast.py).
START = os.environ.get("SEED_SEASON_START", "2025-03-27")
END = os.environ.get("SEED_SEASON_END", "2025-09-28")

# Multi-season list. Space-separated years. Empty => single-season fallback.
SEASONS = [s for s in os.environ.get("SEED_SEASONS", "").split() if s.strip()]

# Per-season calendar bounds. Statcast tolerates a wide window; regular season
# plus a margin for spring/postseason is fine and pybaseball drops empty days.
SEASON_START_MMDD = os.environ.get("SEED_SEASON_START_MMDD", "03-01")
SEASON_END_MMDD = os.environ.get("SEED_SEASON_END_MMDD", "11-30")

OUT = os.environ.get("SEED_OUT", "/opt/sd411/data/statcast_2025.csv")
MIN_MB = float(os.environ.get("SEED_MIN_MB", "100"))
MIN_ROWS = int(os.environ.get("SEED_MIN_ROWS", "600000"))

# Columns the labs depend on. `des` is the Lab 3 text corpus; game_pk +
# at_bat_number identify a plate appearance for dedup.
REQUIRED_COLS = ["des", "game_pk", "at_bat_number", "pitch_type", "release_speed"]


def _windows():
    """Yield (start_dt, end_dt) pairs to fetch, one per season or the single
    fallback window."""
    if SEASONS:
        for yr in SEASONS:
            yield f"{yr}-{SEASON_START_MMDD}", f"{yr}-{SEASON_END_MMDD}"
    else:
        yield START, END


def main() -> int:
    try:
        from pybaseball import statcast
    except Exception as exc:  # noqa: BLE001
        print(f"[seed][FAIL] cannot import pybaseball: {exc}", file=sys.stderr)
        return 64

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    # Fresh file; we append per season so one season is the memory ceiling.
    if os.path.exists(OUT):
        os.remove(OUT)

    total_rows = 0
    wrote_header = False
    checked_cols = False

    for start_dt, end_dt in _windows():
        print(f"[seed] fetching Statcast {start_dt} .. {end_dt} "
              f"(several minutes per season)")
        try:
            df = statcast(start_dt=start_dt, end_dt=end_dt)
        except Exception as exc:  # noqa: BLE001
            print(f"[seed][FAIL] statcast({start_dt}..{end_dt}) raised: {exc}",
                  file=sys.stderr)
            print("[seed] likely a TLS-trust issue; confirm REQUESTS_CA_BUNDLE is set",
                  file=sys.stderr)
            return 64

        if df is None or len(df) == 0:
            print(f"[seed][WARN] no rows for {start_dt}..{end_dt}; skipping",
                  file=sys.stderr)
            continue

        if not checked_cols:
            missing = [c for c in REQUIRED_COLS if c not in df.columns]
            if missing:
                print(f"[seed][FAIL] seed missing required columns: {missing}",
                      file=sys.stderr)
                return 65
            checked_cols = True

        df.to_csv(OUT, index=False, mode="a", header=not wrote_header)
        wrote_header = True
        total_rows += len(df)
        print(f"[seed]   +{len(df):,} rows (running total {total_rows:,})")

    if total_rows == 0:
        print("[seed][FAIL] no rows fetched across all windows", file=sys.stderr)
        return 64

    size_mb = os.path.getsize(OUT) / (1024 * 1024)
    print(f"[seed] wrote {OUT}: {total_rows:,} rows, {size_mb:.1f} MB "
          f"({len(SEASONS) or 1} window(s))")

    if size_mb < MIN_MB or total_rows < MIN_ROWS:
        print(f"[seed][FAIL] below floor (need >= {MIN_MB} MB and >= {MIN_ROWS:,} rows)",
              file=sys.stderr)
        return 65

    print("[seed] sanity floors passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
