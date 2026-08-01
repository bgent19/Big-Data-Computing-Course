#!/usr/bin/env python3
r"""
SD411 Lab 09 - instructor plumbing. Students do not run this.

Builds the replay file that scripts/replay.sh streams into Kafka. Run ONCE on
the golden VM during pre-term provisioning, with network available.

Output: ${STREAM_DIR}/${STREAM_FILE}, one event per line, three tab-separated
fields:

    entity_key <TAB> type_key <TAB> compact_json_value

    entity_key   high cardinality  (GitHub repo name, or Statcast game_pk)
    type_key     low cardinality   (GitHub event type, or Statcast pitch_type)
    value        the event itself, single-line JSON

Two key columns is the entire point. Part 3 of the lab has students key the
same events by each column into the same 4-partition topic and measure what
the partition distribution does. High cardinality spreads; low cardinality
piles up. That is the Module 2 skew pathology reappearing in the transport
layer, which is exactly what the Wednesday lecture promised.

Guarantees the replay depends on:
  - no literal tab or newline inside any field (json.dumps escapes both, and
    the two key fields are sanitized)
  - stops reading the source as soon as it has enough events, so the size of
    the GitHub Archive hour file does not matter

Standard library only. No pip install anywhere in this lab.

Usage:
    python3 make_stream.py --source gharchive \
        --input /opt/sd411/data/gharchive/2026-03-04-15.json.gz \
        --out /opt/sd411/data/stream/gh_events.tsv --events 200000

    python3 make_stream.py --source statcast \
        --input /opt/sd411/data/statcast_2025.csv \
        --out /opt/sd411/data/stream/gh_events.tsv --events 200000
"""

import argparse
import csv
import gzip
import io
import json
import os
import sys

SANITIZE = str.maketrans({"\t": " ", "\n": " ", "\r": " "})


def clean(value, fallback):
    """One-line, tab-free key. Empty keys become the fallback, never blank."""
    if value is None:
        return fallback
    text = str(value).translate(SANITIZE).strip()
    return text if text else fallback


def open_maybe_gzip(path):
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def from_gharchive(path, limit, sample_out):
    """
    GitHub Archive hourly files are newline-delimited JSON, one event per line.
    We trim each event down to the fields the lab actually reads. The full
    event is 2 to 20 KB; the trimmed record is around 200 bytes, which keeps
    the replay file small enough to live in the repo-adjacent data directory
    and fast enough to push at the paced rate.
    """
    samples = []
    with open_maybe_gzip(path) as handle:
        for line in handle:
            if len(samples) >= 3 and limit <= 0:
                break
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            if len(samples) < 3:
                samples.append(event)

            repo = clean((event.get("repo") or {}).get("name"), "unknown/unknown")
            etype = clean(event.get("type"), "UnknownEvent")
            actor = clean((event.get("actor") or {}).get("login"), "unknown")
            value = {
                "id": clean(event.get("id"), "0"),
                "type": etype,
                "actor": actor,
                "repo": repo,
                "created_at": clean(event.get("created_at"), "1970-01-01T00:00:00Z"),
                "public": bool(event.get("public", True)),
                "payload_bytes": len(json.dumps(event.get("payload") or {})),
            }
            yield repo, etype, value
            limit -= 1
            if limit <= 0 and len(samples) >= 3:
                break

    if sample_out and samples:
        with open(sample_out, "w", encoding="utf-8") as out:
            json.dump(samples, out, indent=2, sort_keys=True)


def from_statcast(path, limit, sample_out):
    """
    Fallback source. Every pitch already is an event: it happened at a moment,
    it belongs to a game, and it has a type. The lab's vocabulary changes and
    nothing else does. game_pk plays the role of the repo name (high
    cardinality, roughly even), pitch_type plays the role of the event type
    (low cardinality, badly skewed toward FF).
    """
    samples = []
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if limit <= 0:
                break
            game = clean(row.get("game_pk"), "0")
            ptype = clean(row.get("pitch_type"), "UN")
            value = {
                "id": clean(row.get("pitch_id") or row.get("sv_id"), game + "-0"),
                "type": ptype,
                "actor": clean(row.get("pitcher"), "unknown"),
                "repo": game,
                "created_at": clean(row.get("game_date"), "1970-01-01") + "T00:00:00Z",
                "public": True,
                "payload_bytes": len(json.dumps(row)),
            }
            if len(samples) < 3:
                samples.append(value)
            yield game, ptype, value
            limit -= 1

    if sample_out and samples:
        with open(sample_out, "w", encoding="utf-8") as out:
            json.dump(samples, out, indent=2, sort_keys=True)


def main():
    parser = argparse.ArgumentParser(description="Build the lab09 Kafka replay file.")
    parser.add_argument("--source", choices=["gharchive", "statcast"], default="gharchive")
    parser.add_argument("--input", required=True, help="Source file (.json.gz or .csv)")
    parser.add_argument("--out", required=True, help="Destination TSV")
    parser.add_argument("--events", type=int, default=int(os.environ.get("STREAM_EVENTS", "200000")))
    parser.add_argument("--sample", default=None,
                        help="Optional path for 3 pretty-printed raw events (Part 1 schema reading)")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        sys.stderr.write("FATAL: source not found: %s\n" % args.input)
        return 64

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    sample_out = args.sample or os.path.join(os.path.dirname(os.path.abspath(args.out)),
                                             "sample_events.json")

    if args.source == "gharchive":
        stream = from_gharchive(args.input, args.events, sample_out)
    else:
        stream = from_statcast(args.input, args.events, sample_out)

    written = 0
    types = {}
    entities = set()
    with open(args.out, "w", encoding="utf-8") as out:
        for entity, etype, value in stream:
            out.write("%s\t%s\t%s\n" % (entity, etype,
                                        json.dumps(value, separators=(",", ":"), ensure_ascii=True)))
            written += 1
            types[etype] = types.get(etype, 0) + 1
            if len(entities) < 500000:
                entities.add(entity)

    if written == 0:
        sys.stderr.write("FATAL: wrote 0 events. Check --source against the input file.\n")
        return 65

    top = sorted(types.items(), key=lambda kv: -kv[1])[:8]
    print("wrote %d events to %s" % (written, args.out))
    print("distinct entity keys: %d" % len(entities))
    print("distinct type keys:   %d" % len(types))
    print("top type keys (this is the skew the lab measures):")
    for name, count in top:
        print("  %-32s %8d  %5.1f%%" % (name, count, 100.0 * count / written))
    print("sample events: %s" % sample_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
