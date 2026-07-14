# lab02 data directory

Mounted read-only into the mc container at `/data` and into the namenode
container at `/labdata` (a different path there: the apache/hadoop image's
entrypoint force-chmods `/data` on every start, which crash-loops the
container if anything is bind-mounted at that path).

## What is NOT here: the big file

lab02 does not ship or generate a big file. The "big file" for E1-E3 is the
shared, provisioned season seed:

    ${SD411_DATA}/${SEED_CSV}   ->   /opt/sd411/data/statcast_2025.csv

mounted read-only into the containers at `/seed`. Full 2025 regular season,
roughly 700-800K pitches, at least 100 MB. Every lab in the course measures
against those same bytes, which is the only reason cross-lab numbers can be
compared at all. The floors (`SEED_MIN_MB`, `SEED_MIN_ROWS`) live in
`common.env` and are enforced by `gen_data.sh` and `verify_lab02.sh`.

If the seed is missing, the VM provisioner did not complete. Re-run it rather
than substituting a smaller file: a lab01-sized single-game sample will make
every timing in Part 2 pure noise.

Instructor regeneration, if the seed must be rebuilt by hand. USNA's TLS
interception breaks pybaseball's bundled certifi store, so point requests at
the system CA bundle (`SYSTEM_CA_BUNDLE` in `common.env`):

    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt python - <<'PY'
    from pybaseball import statcast
    statcast('2025-03-27', '2025-09-28').to_csv('statcast_2025.csv', index=False)
    PY

## What IS here

`small/` -- 500 tiny CSV files sliced off the head of the seed by
`./scripts/gen_data.sh`. Roughly 2 KB each, a couple of MB in total. This is
the small-files corpus for E2 and E3, and it is deliberately a rounding error
next to the seed: that contrast is the entire point of E2.

Generated locally and git-ignored. Never commit it.
