#!/usr/bin/env python3
# =============================================================================
# SD411 Lab 3 -- inspect_parquet.py (Part C: the metadata autopsy)
#
# Runs INSIDE spark-master. pyarrow does NOT ship with
# apache/spark:3.5.3-python3 -- run this once first if it's missing
# (verify_lab03.sh check 11 flags it):
#   docker compose exec spark-master pip3 install pyarrow
#
# First pull one Parquet part-file out of MinIO into ./work :
#
#   docker compose --profile tools run --rm mc
#   # inside the mc shell (writes into the shared spark-work volume):
#   mc ls local/sd411/lab03/<alpha>/parquet_snappy/
#   # mc does not glob wildcards in `cp`; use the exact filename from `ls`
#   mc cp local/sd411/lab03/<alpha>/parquet_snappy/<exact-filename> /work/
#   exit
#
# Then:
#
#   docker compose exec spark-master python3 \
#       /opt/lab03/scripts/inspect_parquet.py part-00000-*.parquet
#   (the exec lands in the spark-work working dir, where the pulled file sits)
#
# Everything this prints is read from the file FOOTER. No row data is
# decoded. That is the point: this is the information a reader consults
# before deciding which bytes to fetch at all.
# =============================================================================

import sys
import glob
import pyarrow.parquet as pq

FOCUS_COLUMNS = ["pitch_type", "release_speed"]

def fmt_mb(b):
    return f"{b / (1024 * 1024):.2f} MB"

def main():
    if len(sys.argv) < 2:
        print(__doc__ or "usage: inspect_parquet.py <file.parquet>")
        sys.exit(2)
    matches = glob.glob(sys.argv[1])
    if not matches:
        print(f"No file matches {sys.argv[1]}. Did the mc cp succeed?")
        sys.exit(1)
    path = matches[0]

    pf = pq.ParquetFile(path)
    md = pf.metadata

    print("=" * 70)
    print(f"FILE: {path}")
    print("=" * 70)
    print(f"format version:       {md.format_version}")
    print(f"created by:           {md.created_by}")
    print(f"total rows:           {md.num_rows:,}")
    print(f"columns:              {md.num_columns}")
    print(f"row groups:           {md.num_row_groups}")
    print(f"footer metadata size: {md.serialized_size:,} bytes")
    print()

    # Per row group: row counts and the focus columns' chunk-level detail.
    for rg_i in range(md.num_row_groups):
        rg = md.row_group(rg_i)
        print(f"--- row group {rg_i}: {rg.num_rows:,} rows, "
              f"{fmt_mb(rg.total_byte_size)} uncompressed ---")
        for col_i in range(rg.num_columns):
            col = rg.column(col_i)
            name = col.path_in_schema
            if name not in FOCUS_COLUMNS:
                continue
            stats = col.statistics
            print(f"  column: {name}")
            print(f"    physical type:     {col.physical_type}")
            print(f"    encodings:         {col.encodings}")
            print(f"    compression:       {col.compression}")
            print(f"    compressed size:   {fmt_mb(col.total_compressed_size)}")
            print(f"    uncompressed size: {fmt_mb(col.total_uncompressed_size)}")
            if stats is not None and stats.has_min_max:
                print(f"    min / max:         {stats.min} / {stats.max}")
                print(f"    null count:        {stats.null_count}")
            else:
                print( "    statistics:        (none recorded)")
        print()

    print("Answer these in your report (Part C):")
    print(" C1. How many row groups, and how does that bound the maximum")
    print("     read parallelism for THIS file? (Week 3 Day 1 notes.)")
    print(" C2. For pitch_type: which encodings appear, and why does the")
    print("     compressed size make sense given ~15 distinct values?")
    print(" C3. For release_speed: state the min/max of one row group, then")
    print("     state exactly which of your Part B queries can use it to")
    print("     skip the entire row group, and under what condition.")
    print(" C4. The footer is N bytes (printed above). Explain why a query")
    print("     engine reads it FIRST, and what it would cost to get the")
    print("     same information from a CSV.")

if __name__ == "__main__":
    main()
