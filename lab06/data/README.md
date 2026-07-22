# Lab 6 data — nothing to download

This lab reads the shared course seed. There is no per-lab `data/` payload and
no download step.

- Seed object: `${SEED_CSV}` (default `statcast_2025.csv`), the 2025 regular
  season, staged on the VM at `${SD411_DATA}` (default `/opt/sd411/data`).
- `minio-init` copies it into `s3a://${S3_BUCKET}/raw/` at stack-up and exits
  **64** if it is absent. Exit 64 means the VM was not provisioned, not that
  your stack is broken. Tell your instructor; do not try to re-pull it.
- Sanity floors live in `common.env`: `SEED_MIN_MB=100`,
  `SEED_MIN_ROWS=600000`. Verify check 10 enforces the size floor and Part 0
  enforces the row floor.

## What Part 0 builds from it

Part 0 reads the seed CSV and writes a Parquet fact table to
`s3a://${S3_BUCKET}/fact/pitches`, adding a typed `game_date` and a derived
`game_month`. That object is the input for Parts A, B, and C, and for Lab 7's
join experiments next week. Do not rename the path.

## Instructor note: re-staging the seed

If the shared seed needs regenerating, it is produced host-side, not in a
container, because the institutional TLS interception breaks pybaseball's
certificate validation inside the venv:

```bash
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt   # system bundle has the USNA root
python3 - <<'EOF'
from pybaseball import statcast
statcast(start_dt="2025-03-27", end_dt="2025-09-28") \
    .to_csv("/opt/sd411/data/statcast_2025.csv", index=False)
EOF
```

Dates come from `SEED_SEASON_START` / `SEED_SEASON_END` in `common.env`. Keep
those three in sync: if the season window changes, the size and row floors
should be re-checked. `scripts/fix_trust_stores.sh` is the durable fix if the
`REQUESTS_CA_BUNDLE` export is not enough on a given VM.
