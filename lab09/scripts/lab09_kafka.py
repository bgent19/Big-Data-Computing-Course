#!/usr/bin/env python3
"""
SD411 Lab 09 - Kafka, End to End                     STUDENT SCAFFOLD

Run this from the lab09/ directory ON THE VM (not inside a container):

    python3 scripts/lab09_kafka.py --part 1

Why this one runs on the host when every other lab submitted through
spark-submit: this lab has no Spark job. The instruments are the Kafka CLI
tools, which live inside the broker image, and the harness below is a thin
wrapper that runs them with `docker compose exec` and turns their output into
tables you can put in a memo. The stretch part goes back to spark-submit.

Standard library only. Nothing to install.

THE RULE THAT MATTERS: every measurement in this lab is preceded by a written
prediction on the WORKSHEET. Call predict() before you call the measurement.
It timestamps your answer into lab09_predictions.log, which is part of your
submission. A missing prediction scores zero methodology credit for that part.
A wrong prediction scores full credit. A prediction written after the number
appears scores zero, and the log makes that visible.
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time

# --------------------------------------------------------------------------
# Configuration. Everything resolves from the stamped .env so this file
# hardcodes no port, tag, or path.
# --------------------------------------------------------------------------
LAB_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = "/opt/kafka/bin"
PRED_LOG = os.path.join(LAB_DIR, "lab09_predictions.log")


def load_env():
    env = {}
    path = os.path.join(LAB_DIR, ".env")
    if not os.path.exists(path):
        sys.stderr.write("FATAL: lab09/.env not found. Run vm-base/scripts/sync_env.sh.\n")
        sys.exit(64)
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


ENV = load_env()
BOOTSTRAP = ENV.get("KAFKA_BOOTSTRAP", "kafka:9092")


# --------------------------------------------------------------------------
# Plumbing. You should not need to change anything in this section.
# --------------------------------------------------------------------------
def run(args, capture=True, check=False, timeout=600):
    """Run a command in the lab directory and return (rc, stdout, stderr)."""
    proc = subprocess.run(
        args, cwd=LAB_DIR, timeout=timeout,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True,
    )
    if check and proc.returncode != 0:
        sys.stderr.write("command failed: %s\n%s\n" % (" ".join(args), proc.stderr or ""))
        sys.exit(proc.returncode)
    return proc.returncode, (proc.stdout or ""), (proc.stderr or "")


def kafka_cli(script, *args, **kwargs):
    """Run one of the broker's bundled CLI tools inside the kafka container."""
    cmd = ["docker", "compose", "exec", "-T", "kafka", "%s/%s" % (BIN, script),
           "--bootstrap-server", BOOTSTRAP] + [str(a) for a in args]
    return run(cmd, **kwargs)


def predict(part, question):
    """
    Ask for a prediction, then write it to the prediction log with a
    timestamp. Do this BEFORE the measurement it belongs to.
    """
    print("\n  PREDICT [%s]  %s" % (part, question))
    answer = input("  your prediction: ").strip()
    while not answer:
        answer = input("  a prediction is required. your prediction: ").strip()
    with open(PRED_LOG, "a", encoding="utf-8") as log:
        log.write("%s\t%s\t%s\t%s\n" % (time.strftime("%Y-%m-%dT%H:%M:%S"), part, question, answer))
    return answer


def timed(label, fn, trials=3):
    """
    Median-of-3 timer, same harness as lab03 onward. Returns (median, samples).
    Report the median, never a single run, and never the mean.
    """
    samples = []
    for i in range(trials):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
        print("    %s trial %d: %.2fs" % (label, i + 1, samples[-1]))
    median = statistics.median(samples)
    print("    %s MEDIAN: %.2fs" % (label, median))
    return median, samples


# --------------------------------------------------------------------------
# Kafka operations
# --------------------------------------------------------------------------
def create_topic(name, partitions, configs=None, replication=1):
    args = ["--create", "--if-not-exists", "--topic", name,
            "--partitions", partitions, "--replication-factor", replication]
    for key, value in (configs or {}).items():
        args += ["--config", "%s=%s" % (key, value)]
    rc, out, err = kafka_cli("kafka-topics.sh", *args)
    if rc != 0:
        sys.stderr.write(err)
    print("  topic %s: %d partitions%s" % (name, partitions,
          (" " + json.dumps(configs)) if configs else ""))
    return rc == 0


def delete_topic(name):
    kafka_cli("kafka-topics.sh", "--delete", "--topic", name)


def describe_topic(name):
    rc, out, _ = kafka_cli("kafka-topics.sh", "--describe", "--topic", name)
    print(out.rstrip())
    return out


def _offsets_at(topic, when):
    """Parse kafka-get-offsets.sh output (topic:partition:offset) into a dict."""
    rc, out, err = kafka_cli("kafka-get-offsets.sh", "--topic", topic, "--time", when)
    if rc != 0:
        sys.stderr.write(err)
        return {}
    offsets = {}
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[1].isdigit():
            offsets[int(parts[1])] = int(parts[2])
    return offsets


def end_offsets(topic):
    """
    Per-partition END offset: the offset the NEXT record will be given. On a
    topic that has never been compacted or expired, that equals the record
    count for that partition, which makes this the cheapest possible
    partition-distribution instrument. Returns {partition: offset}.
    """
    return _offsets_at(topic, "latest")


def earliest_offsets(topic):
    """
    Per-partition EARLIEST readable offset: the oldest record the broker will
    still hand you. It is 0 until retention deletes something, and then it
    advances. Part 5 is the difference between this and end_offsets(): when
    they are equal, every record has expired and a read returns nothing.
    Returns {partition: offset}.
    """
    return _offsets_at(topic, "earliest")


def partition_profile(topic, label=""):
    """
    Print the per-partition record counts, each partition's share, and the
    max/min ratio. That ratio is the same skew number you computed in lab08,
    measured on a different substrate.

    Counts come from the end offsets, so this is only a record count on a
    topic that has expired nothing. On the Part 5 retention topic it reports
    positions, not survivors; use earliest_offsets() there.
    """
    counts = end_offsets(topic)
    if not counts:
        print("  no offsets for %s" % topic)
        return counts, 0.0
    total = sum(counts.values()) or 1
    biggest = max(counts.values())
    smallest = min(counts.values())
    ratio = float(biggest) / smallest if smallest else float("inf")
    print("\n  partition profile: %s %s" % (topic, label))
    print("    %-10s %10s %8s" % ("partition", "records", "share"))
    for part in sorted(counts):
        print("    %-10d %10d %7.1f%%" % (part, counts[part], 100.0 * counts[part] / total))
    print("    %-10s %10d" % ("total", total))
    print("    max/min ratio: %s" % ("inf" if smallest == 0 else "%.2fx" % ratio))
    return counts, ratio


def replay(topic, keyfield="entity", total=60000, rate=1000):
    """Stream events into a topic at a paced rate. See scripts/replay.sh."""
    cmd = ["docker", "compose", "exec", "-T", "kafka", "sh",
           "/opt/lab09/scripts/replay.sh", topic, keyfield, str(total), str(rate)]
    rc, out, err = run(cmd, timeout=1800)
    if rc != 0:
        sys.stderr.write(err)
    for line in out.splitlines():
        if line.startswith("replay:"):
            print("  " + line)
    return rc == 0


def consume(topic, max_messages=10, group=None, partition=None,
            from_beginning=True, offset=None, timeout_ms=15000):
    """
    Read records and return them as a list of dicts with key, partition, and
    offset. Uses the console consumer, so it prints exactly what the broker
    hands over and nothing more.
    """
    args = ["--topic", topic, "--max-messages", str(max_messages),
            "--timeout-ms", str(timeout_ms),
            "--property", "print.key=true",
            "--property", "print.partition=true",
            "--property", "print.offset=true",
            "--property", "key.separator=|"]
    if partition is not None:
        args += ["--partition", str(partition), "--offset", str(offset if offset is not None else "earliest")]
    elif from_beginning:
        args += ["--from-beginning"]
    if group:
        args += ["--group", group]

    rc, out, err = kafka_cli("kafka-console-consumer.sh", *args, timeout=timeout_ms / 1000.0 + 30)
    # The console consumer separates EVERY field with key.separator, so a line
    # reads: Partition:0|Offset:12|<key>|<value>. Split with maxsplit=3 so a
    # separator character inside the JSON value cannot shift the columns.
    records = []
    for line in out.splitlines():
        fields = dict(re.findall(r"(Partition|Offset):(\d+)", line))
        parts = line.split("|", 3)
        records.append({
            "partition": int(fields.get("Partition", -1)),
            "offset": int(fields.get("Offset", -1)),
            "key": parts[2] if len(parts) == 4 else None,
            "value": parts[3] if len(parts) == 4 else line,
            "raw": line,
        })
    return records


def group_describe(group):
    """
    Parse `kafka-consumer-groups.sh --describe` into rows. The columns that
    matter: PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, CONSUMER-ID.
    """
    rc, out, err = kafka_cli("kafka-consumer-groups.sh", "--describe", "--group", group)
    print(out.rstrip() or err.rstrip())
    rows = []
    for line in out.splitlines():
        fields = line.split()
        if len(fields) >= 6 and fields[2].isdigit():
            rows.append({
                "topic": fields[1],
                "partition": int(fields[2]),
                "current": fields[3],
                "end": fields[4],
                "lag": fields[5],
                "consumer": fields[6] if len(fields) > 6 else "-",
            })
    return rows


def group_state(group):
    """Return the coordinator's one-word state for a group, or '' if unknown."""
    rc, out, err = kafka_cli("kafka-consumer-groups.sh", "--describe",
                             "--group", group, "--state")
    found = re.search(r"\b(Empty|Dead|Stable|PreparingRebalance|CompletingRebalance)\b",
                      out + err)
    return found.group(1) if found else ""


def wait_for_group_empty(group, timeout=180):
    """
    Block until a group has no live members, and say so while waiting.

    You need this after stop_consumer(). `docker rm -f` is a SIGKILL, so the
    member never sends a LeaveGroup and the coordinator keeps it in the group
    until it misses heartbeats for session.timeout.ms (~45s at 3.x). Until
    that happens the group is still Stable, and anything that requires an
    inactive group -- --reset-offsets, --delete -- refuses with a message
    about the group not being inactive. That refusal is the Part 4 timing
    lesson showing up again in Part 5, not a broken harness.

    Killing several members at once costs more than one session timeout: the
    group walks PreparingRebalance -> CompletingRebalance -> Empty and takes
    around 90 seconds to get there. Watch the states print. That is the
    coordinator working, not a hang.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = group_state(group)
        if state in ("Empty", "Dead", ""):
            print("  group %s is %s after %.0fs" % (group, state or "gone",
                                                    timeout - (deadline - time.time())))
            return True
        print("    group %s is %s, waiting out session.timeout.ms..." % (group, state))
        time.sleep(5)
    print("  WARNING: group %s still has a live member after %ds" % (group, timeout))
    return False


def group_reset(group, topic, to="earliest"):
    """Rewind a group's committed offsets. The group must have no live member."""
    rc, out, err = kafka_cli("kafka-consumer-groups.sh", "--group", group,
                             "--topic", topic, "--reset-offsets", "--to-" + to, "--execute")
    print(out.rstrip() or err.rstrip())
    return rc == 0


def start_consumer(label, topic, group, max_messages=1000000, from_beginning=True):
    """
    Start a detached console consumer as its own container so it can be killed
    later. This is how you create a two-member group and then take one member
    out from under it.

    from_beginning defaults True so a NEW group starts at offset 0 and reads
    the topic it was pointed at. Without it the console consumer's
    auto.offset.reset is `latest`, the group starts at the tail, and every
    number in Part 4 comes back zero because there is nothing behind the
    consumer to read. Remember that --from-beginning is ignored once the group
    has a committed offset, so restarting a member resumes; it does not
    re-read.
    """
    name = "lab09-%s" % label
    run(["docker", "rm", "-f", name])
    cmd = ["docker", "compose", "run", "-d", "--no-deps", "--name", name, "kafka",
           "%s/kafka-console-consumer.sh" % BIN,
           "--bootstrap-server", BOOTSTRAP, "--topic", topic, "--group", group,
           "--max-messages", str(max_messages)] + \
          (["--from-beginning"] if from_beginning else [])
    rc, out, err = run(cmd)
    if rc != 0:
        sys.stderr.write(err)
    print("  consumer %s started in group %s" % (name, group))
    return name


def stop_consumer(label):
    name = label if label.startswith("lab09-") else "lab09-%s" % label
    run(["docker", "rm", "-f", name])
    print("  consumer %s killed" % name)


# ==========================================================================
# PART 1 - The log and the event schema                            (10 pts)
# ==========================================================================
def part1():
    """
    A topic with exactly one partition is the whole log abstraction with the
    distribution taken out, which makes it the right place to start.

    TODO 1.1  Create topic `gh.events.p1` with 1 partition.
    TODO 1.2  Replay 5000 events into it, keyed by entity, at 500/s.
    TODO 1.3  Print the end offsets. Before you do, predict() the value of the
              end offset for partition 0 and say why.
    TODO 1.4  Consume the first 5 records from the beginning and print them.
              Then consume 5 more from the beginning AGAIN in a second call.
              Predict first: does the second read return the same records?
    TODO 1.5  Open data/sample_events.json. In the WORKSHEET, name the field
              that carries event time, the field you would use as a key for
              per-repository ordering, and one field that is state rather than
              an event.
    """
    print("\n== Part 1: the log ==")
    # TODO: your code here
    raise NotImplementedError("Part 1 not implemented")


# ==========================================================================
# PART 2 - Partitions and ordering                                 (15 pts)
# ==========================================================================
def part2():
    """
    Partitioning buys throughput and costs ordering. Measure the cost.

    TODO 2.1  Create topic `gh.events.p4` with 4 partitions.
    TODO 2.2  Replay 20000 events with keyfield="none" at 1000/s.
    TODO 2.3  predict(): with no key, how will 20000 records distribute across
              4 partitions? Give a max/min ratio, not a hand wave.
              Then call partition_profile().
    TODO 2.4  predict(): a whole-topic read of 20 records off a 4-partition
              topic. How many partitions will those 20 records come from?
              Then consume 20 records with from_beginning=True and record the
              (partition, offset) pairs in the order they arrive.
              Expect a surprise: they arrive from ONE partition, in a
              contiguous run. The consumer fetches in per-partition batches
              and drains what it was handed before it switches. Which
              partition it starts on is not promised to you either.
    TODO 2.5  Read 20 records from partition 0 (partition=0,
              offset="earliest"), then 20 from partition 1. BOTH come back
              numbered 0 through 19, and they are different records. Print the
              first record of each so you can see that offset 0 of partition 0
              and offset 0 of partition 1 name two unrelated events.
              That pair of readings, not the bouncing you might have expected
              in 2.4, is the proof that an offset means nothing without a
              partition.
    TODO 2.6  In the WORKSHEET, state the exact ordering guarantee Kafka gives
              and the exact one it does not, using your two readings as the
              evidence.
    """
    print("\n== Part 2: partitions and ordering ==")
    # TODO: your code here
    raise NotImplementedError("Part 2 not implemented")


# ==========================================================================
# PART 3 - Keys and partition distribution                         (20 pts)
# ==========================================================================
def part3():
    """
    Same events, same topic shape, two different keys. This is lab08's skew
    problem wearing a different uniform.

    TODO 3.1  Create `gh.keyed.entity` and `gh.keyed.type`, 4 partitions each.
    TODO 3.2  predict() the max/min partition ratio for EACH topic before you
              produce anything. You have the type distribution in the output
              of make_stream.py, printed in data/README.md, so this is an
              arithmetic prediction and not a guess.
    TODO 3.3  Replay 60000 events into gh.keyed.entity with keyfield="entity".
              Replay 60000 events into gh.keyed.type with keyfield="type".
    TODO 3.4  partition_profile() both. Record both ratios.
    TODO 3.5  Consume 10 records from gh.keyed.type on partition 0 and record
              the keys. Then answer two DIFFERENT questions, because they have
              different answers and swapping them is the classic mistake:
                (a) Are all the records on this partition the same key?
                    Report what you actually observed. Whether one partition
                    happens to draw one key or seven is a fact about this
                    hour's hash values, not a guarantee.
                (b) Is every record with a given key on this one partition?
                    THIS is the guarantee, and it is the one the ordering
                    contract rests on.
              Say which of the two Kafka promises you, and why the other one
              cannot be promised.
    TODO 3.6  In the WORKSHEET: you have to key by something that guarantees
              per-repository ordering, and you have to avoid the skew you just
              measured. State whether both are achievable at once here, and
              what you would do if they were not.
    """
    print("\n== Part 3: keys and distribution ==")
    # TODO: your code here
    raise NotImplementedError("Part 3 not implemented")


# ==========================================================================
# PART 4 - Consumer groups, offsets, lag, and rebalance            (25 pts)
# ==========================================================================
def part4():
    """
    The part that pays for the whole architecture: consumers that read at
    their own pace, crash, and come back where they left off.

    Work against gh.keyed.entity (4 partitions) from Part 3.

    TODO 4.1  predict(): you start ONE consumer in group `g-solo`. How many
              partitions does it own?
              start_consumer("solo", "gh.keyed.entity", "g-solo"), wait a few
              seconds, then group_describe("g-solo"). Confirm.
    TODO 4.2  predict(): you add a SECOND consumer to the same group. How many
              partitions does each own now, and what happens to the offsets
              already committed?
              Start it, wait, describe again.
    TODO 4.3  predict(): you start a THIRD, FOURTH, and FIFTH member of the
              same group on a 4-partition topic. What does the fifth one do?
              Start them, describe, and read the CONSUMER-ID column carefully.
    TODO 4.4  Kill ONE member with stop_consumer(). Describe the group
              immediately, then in a loop every 10 seconds until the
              assignment changes. Record how many seconds it took and which
              member picked up the orphaned partitions. Sampling in a loop is
              the point: two samples 60 seconds apart can only tell you the
              answer is somewhere under 60.
    TODO 4.5  Lag. A live console consumer is far faster than a paced replay,
              so lag against a running group reads 0 and stays 0 -- there is
              nothing to see. Build a real backlog instead:
                (a) stop_consumer() every member, then
                    wait_for_group_empty("g-solo"). Committed offsets survive;
                    only the members are gone.
                (b) replay 20000 more events into the topic.
                (c) group_describe(): LAG is now the whole backlog, because
                    nothing has moved the committed offset.
                (d) start_consumer() one member again, wait, describe again,
                    and watch LAG fall back to 0.
              Record the LAG column at (c) and (d). Explain, in one sentence,
              what LAG is the difference of, and give its units.
    TODO 4.6  A brand new group on the same topic. predict() first: does
              `g-second` start at offset 0, or at the offset g-solo has
              reached? Read 20 records with
              consume(topic, max_messages=20, group="g-second") -- that is a
              foreground read, so it commits and exits cleanly instead of
              racing you to the tail -- then group_describe() BOTH groups and
              compare CURRENT-OFFSET. g-second sits at 0 on the partitions its
              bounded read never reached and just past 0 on the one it did,
              with lag equal to the whole topic. g-solo is at the tail. Same
              topic, same records, two independent positions.
    TODO 4.7  Stop every consumer you started, then wait_for_group_empty() on
              g-solo. Part 5 opens with an offset reset, and a reset refuses
              while any member is still live.
    """
    print("\n== Part 4: consumer groups ==")
    # TODO: your code here
    raise NotImplementedError("Part 4 not implemented")


# ==========================================================================
# PART 5 - Replay and retention                                    (10 pts)
# ==========================================================================
def part5():
    """
    Read-without-delete is the property the rest of Module 3 leans on. Prove
    it, then find its edge.

    TODO 5.1  Call wait_for_group_empty("g-solo") first -- you killed those
              members with SIGKILL and the coordinator needs to time them out.
              Then group_reset("g-solo", "gh.keyed.entity", to="earliest") and
              describe the group. What are the CURRENT-OFFSET and LAG values
              now?
    TODO 5.2  predict(): re-consume 5 records with group="g-solo". You are
              rewound to 0, so compare them against the first records of the
              topic. Consume 5 AGAIN in a second call and note that the second
              call returns the NEXT five, not the same five: the first read
              committed. A commit moved a pointer; it deleted nothing.
    TODO 5.3  Create topic `gh.short` with 1 partition and configs
              {"retention.ms": 30000, "segment.ms": 10000}. Replay 5000
              events into it at rate=100, deliberately slow. The produce has
              to take longer than the retention window, or every record lands
              inside one segment and the sweep takes all of them at once.
    TODO 5.4  Record end_offsets("gh.short") AND earliest_offsets("gh.short")
              the moment the replay returns, and consume 5 records from
              partition 0 at offset="earliest". The earliest offset is already
              well above 0: the front of the log expired while the back of it
              was still being written. Those 5 records are the survivors.
              predict() what both offsets will be in 30 seconds.
    TODO 5.5  Sleep 30 seconds. Print both again and try the same read.
              Nothing is producing any more, so the sweep catches up: the
              earliest offset climbs until it meets the end offset and the
              read comes back []. Kafka is not broken and neither is the
              harness — every record expired.
              The end offset is the same number in both readings. Say why
              that is the correct behavior, in terms of what an offset names.
    """
    print("\n== Part 5: replay and retention ==")
    # TODO: your code here
    raise NotImplementedError("Part 5 not implemented")


# ==========================================================================
# PART 6 - STRETCH: the log as a bounded table                     (+5 pts)
# ==========================================================================
def part6():
    """
    Optional. Requires the spark profile:
        docker compose --profile spark up -d

    Kafka computes nothing. Spark does. This is the seam the next three weeks
    live on, and a batch read is the gentlest possible way to cross it,
    because with startingOffsets earliest and endingOffsets latest the stream
    is just a bounded table.

    Submit instead: scripts/lab09_batch_read.py, run with spark-submit and the
    connector JARs from ${SD411_JARS}. See README Part 6 for the exact
    invocation. Report the row count, the per-partition count from Spark, and
    whether Spark's per-partition counts match your end_offsets() from Part 3.
    """
    print("\n== Part 6: stretch ==")
    raise NotImplementedError("Part 6 is optional")


PARTS = {1: part1, 2: part2, 3: part3, 4: part4, 5: part5, 6: part6}


def main():
    parser = argparse.ArgumentParser(description="SD411 Lab 09 harness")
    parser.add_argument("--part", type=int, required=True, choices=sorted(PARTS))
    args = parser.parse_args()
    print("bootstrap: %s   lab dir: %s" % (BOOTSTRAP, LAB_DIR))
    PARTS[args.part]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
