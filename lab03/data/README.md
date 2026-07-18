# Lab 3 seed data - provisioning notes

Reconciled to the shared-provisioning model. The seed is NO LONGER dropped into
each lab's local `data/` directory. It is provisioned ONCE on the student VM at
the path `common.env` calls `${SD411_DATA}` (default `/opt/sd411/data`), and
every lab's `minio-init` mounts that shared location read-only and copies the
CSV into MinIO. This lab therefore ships with no local seed file and no local
`data/statcast_2025.csv`.

Instructor, before the term: generate the seed and place it at
`${SD411_DATA}/statcast_2025.csv` on the golden VM image (or wherever the
provisioner stages it). Full 2025 regular season via pybaseball:

```python
# pip install pybaseball pandas
from pybaseball import statcast
df = statcast(start_dt="2025-03-27", end_dt="2025-09-28")   # matches SEED_SEASON_* in common.env
print(len(df), "rows,", len(df.columns), "columns")          # expect ~700K x ~119
df.to_csv("statcast_2025.csv", index=False)
```

Sanity floors are enforced by `common.env` and the verify script:
`SEED_MIN_MB=100`, `SEED_MIN_ROWS=600000`. The columns `pitch_type`,
`release_speed`, and `game_date` must be present (the lab's queries and the
Part C focus columns depend on them).

If the full season is too heavy for the VM image, a half season works; keep it
above the 100 MB floor so verify check 10 passes and timings stay meaningful.
Because the seed is fetched host-side and staged (not pulled from inside a
container), the USNA TLS-interception issue that breaks in-container pybaseball
does not apply to this step.
