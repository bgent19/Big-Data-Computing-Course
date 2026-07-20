# lab05 data

There is no per-lab data directory contents any more, and nothing here is
copied by hand.

The season seed (`statcast_2025.csv`) is provisioned once on the student VM at
`/opt/sd411/data/` and mounted read-only into `minio-init` as `/seed` by the
base compose. On first bring-up `minio-init` copies it to
`s3a://sd411/raw/statcast_2025.csv`; if a warm MinIO volume from lab03/lab04
already holds the object, the copy is skipped.

The Parquet copy at `sd411/parquet/statcast_2025/` is created in Part 0 by
`scripts/make_parquet.py` if lab03's copy is gone.

Instructor regeneration of the seed (run once, on the VM, not per lab):

    python -c "from pybaseball import statcast; statcast('2025-03-27','2025-09-28').to_csv('/opt/sd411/data/statcast_2025.csv', index=False)"

pybaseball needs REQUESTS_CA_BUNDLE pointed at the system CA bundle behind the
institutional proxy; see vm-base/scripts/fix_trust_stores.sh.

Never commit data files.
