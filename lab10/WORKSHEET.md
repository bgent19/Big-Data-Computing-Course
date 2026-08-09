# Lab 10 Worksheet - Windowing on Wikipedia Pageviews

Name: ________________________  Alpha: ______________  Date: ______________

**How this is graded.** Every block marked PREDICT has an instructor initial
line. Get it initialled before you run that part. A prediction recorded after
the measurement scores zero, and a wrong prediction recorded first scores full
marks. Show the arithmetic where the prediction is a number you can derive.

---

## Part 0 - ground truth and shard forensics

### PREDICT (before running part0)

**P0.1** The feed is 168 hourly files, one week, English Wikipedia at 50+ views
per hour. Estimate the total number of rows, to the nearest order of magnitude,
and say what you based the estimate on.

```
estimate: ______________________
basis:
```

**P0.2** Shard `shard-0037.parquet` is the 38th file. What do you expect the
minimum and maximum `ts` in that file to be, and what is the span between them
in hours?

```
min_ts: ____________  max_ts: ____________  span (hours): ______
reasoning:
```

Instructor initials: __________

### MEASURE

**P0.3** Fill in from the summary row and the per-shard table.

| quantity | measured |
|---|---|
| total rows | |
| total views | |
| min(ts) | |
| max(ts) | |
| distinct pages | |
| span of shard 0000 (hours) | |
| span of shard 0001 (hours) | |
| span of shard 0037 (hours) | |
| span of shard 0166 (hours) | |

**P0.4** Was P0.2 right? If the span of shard 0037 is not zero, say in one
sentence what that implies about the order in which this feed delivers event
time. Then explain why the first few shards have smaller spans than every other
shard, and what that fact alone tells you about the *direction* of the disorder.

```


```

---

## Part 1 - tumbling windows and the resolution sweep

### PREDICT (before running part1)

**P1.1** The feed covers 168 hours beginning at hour 00 UTC. How many windows
will each tumbling size emit? Derive them, do not guess.

| size | predicted windows | derivation |
|---|---:|---|
| 1 hour | | |
| 3 hours | | |
| 6 hours | | |
| 24 hours | | |

**P1.2** English Wikipedia traffic has a strong daily cycle. At which of those
four window sizes do you expect that cycle to stop being visible in the bar
chart, and why?

```


```

Instructor initials: __________

### MEASURE

**P1.3** Measured window counts, and the final state row count for each run.

| size | windows emitted | final state rows | matched P1.1? |
|---|---:|---:|---|
| 1 hour | | | |
| 3 hours | | | |
| 6 hours | | | |
| 24 hours | | | |

**P1.4** The article you chose for the burstiness sweep, and why it beat the
site's most-viewed page on the statistic you ranked by.

```
page: ______________________________
statistic used: ____________________
peak hour views: __________  median hour views: __________
```

**P1.5** Compare your article's spike across the two charts. Read the tallest
bar and a typical bar off each one.

| chart | peak bar | typical bar | peak / typical |
|---|---:|---:|---:|
| 1 hour | | | |
| 24 hours | | | |

Then answer all three. (a) Is the spike still obvious at 24 hours? (b) What
could you tell from the 1-hour chart that the 24-hour chart can no longer tell
you? (c) State the general rule in one sentence, in terms of window size and the
duration of the event you are trying to observe.

```


```

---

## Part 2 - sliding windows and the multiplication

### PREDICT (before running part2)

**P2.1** Window size 24 hours, slide 3 hours.

| quantity | predicted | derivation |
|---|---:|---|
| windows each event lands in | | |
| total windows emitted | | |
| sum(views) over all windows / batch total | | |

**P2.2** Now add `domain` as a second grouping key. Predict the final state row
count and show the two factors you multiplied.

```
predicted state rows: __________ = __________ x __________
```

Instructor initials: __________

### MEASURE

**P2.3** Measured.

| quantity | predicted | measured |
|---|---:|---:|
| total windows emitted | | |
| sum ratio | | |
| state rows, global key | | |
| state rows, keyed by domain | | |

**P2.4** One of the three predictions in P2.1 is almost certainly wrong, and it
is the window count. Reconcile it. Write the inequality that a window start `s`
must satisfy for the window `[s, s+24h)` to touch a 168-hour feed, then count
the multiples of 3 hours that satisfy it.

```


```

**P2.5** The windows at each end of the range are partly outside the data. What
is a dashboard doing wrong if it plots those alongside the rest without saying
so, and roughly how wrong is the first one?

```


```

---

## Part 3 - session windows

### PREDICT (before running part3)

**P3.1** You are about to run a session-window aggregation over the stream, with
no watermark anywhere in the query. Will it start? If you think it will not,
name the reason before you see the message.

```


```

**P3.2** With a 3-hour gap and a 500-view floor, how many sessions do you expect
the site's front page to produce over the week? How many do you expect a news
spike article to produce?

```
front page: ______     news spike article: ______
reasoning:
```

Instructor initials: __________

### MEASURE

**P3.3** The exception from the streaming attempt, verbatim. Type and first
line at minimum.

```


```

**P3.4** Batch session results.

| quantity | measured |
|---|---:|
| total sessions | |
| distinct pages | |
| mean sessions per page | |
| longest session, hours | |
| page with the most sessions | |

Then the estimate you were asked not to run: distinct pages with the 500-view
filter, distinct pages without it, and the resulting multiplier on the number
of open sessions the state store would hold.

```
with filter: __________  without: __________  multiplier: ______x
```

---

## Part 4 - state forensics and the reordering proof

### PREDICT (before running part4)

**P4.1** In Part 0 you measured how far out of order this feed delivers event
time. The 1-hour tumbling streaming query is about to be compared row by row
against the batch oracle. How many hourly windows will disagree, and why?

```
predicted disagreements: ______
reasoning:
```

**P4.2** Over the run, in how many batches do you expect the state row count to
go down?

```
predicted: ______   reasoning:
```

Instructor initials: __________

### MEASURE

**P4.3** Measured.

| quantity | predicted | measured |
|---|---:|---:|
| disagreements vs oracle | | |
| final state rows | | |
| batches run | | |
| batches where state decreased | | |

**P4.4** Extrapolate the final state row count for this same query run
continuously, showing the arithmetic.

| scenario | state rows | arithmetic |
|---|---:|---|
| one week, global key (measured) | | |
| one year, global key | | |
| one year, keyed by page | | |

**P4.5** The closing question. Nothing in your query ever released a window's
state, and the reason is that nothing in your query is capable of deciding a
window is finished. In one sentence: what single piece of information would let
the engine make that decision?

```


```

---

## Friction log

Copy the completed table from README.md here, or reference it. Escalations you
logged count in your favour, not against you.

---

## Oral spot-check preparation

At the opening of Lab 11 you will be asked one of these. Prepare all four.

1. Walk me through how a state row count gets from an executor to the number
   printed in your terminal.
2. Your sliding window emitted more windows than 8 times the tumbling count.
   Show me on the whiteboard why.
3. Your streaming answer matched the batch answer exactly on a feed you proved
   was out of order. Explain why that had to be true.
4. Point at the batch in your state curve where the engine could first have
   released a window, and tell me what it was missing.
