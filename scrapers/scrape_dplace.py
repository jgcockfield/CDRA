#!/usr/bin/env python3
"""
D-PLACE scraper — EA029 (Agriculture: major crop type)

Downloads data from the D-PLACE GitHub repository and saves to CSV.
Streams data.csv line-by-line so memory usage stays low regardless of file size.

Output: dplace_EA029.csv
"""

import csv
import io
import os
import sys
import requests

GITHUB_BASE = "https://raw.githubusercontent.com/D-PLACE/dplace-data/master/datasets/EA"
SOCIETIES_URL = f"{GITHUB_BASE}/societies.csv"
CODES_URL     = f"{GITHUB_BASE}/codes.csv"
DATA_URL      = f"{GITHUB_BASE}/data.csv"

TARGET_VAR  = sys.argv[1].upper() if len(sys.argv) > 1 else "EA029"
OUTPUT_FILE = os.path.join("outputs", "dplace", f"dplace_{TARGET_VAR}.csv")


def fetch_small(url: str) -> list[dict]:
    """Download a small CSV fully into memory and return as list of dicts."""
    print(f"  Fetching {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return list(csv.DictReader(io.StringIO(r.text)))


def stream_filter_data(url: str, var_id: str) -> list[dict]:
    """Stream data.csv and return only rows matching var_id."""
    print(f"  Streaming {url} (filtering for {var_id}) ...")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()

    lines = r.iter_lines(decode_unicode=True)
    header_line = next(lines)
    headers = next(csv.reader([header_line]))
    print(f"  data.csv columns: {headers}")

    matched = []
    reader_headers = headers  # keep for DictReader-style parsing

    for raw_line in lines:
        if not raw_line:
            continue
        row_values = next(csv.reader([raw_line]))
        row = dict(zip(reader_headers, row_values))
        if row.get("var_id") == var_id:
            matched.append(row)

    return matched


def main():
    print("=== D-PLACE EA029 scraper ===\n")

    # 1. Load societies lookup  (keyed by society id)
    print("Loading societies ...")
    societies_raw = fetch_small(SOCIETIES_URL)
    # Discover the id column name (could be 'id' or 'Society_id')
    soc_id_col = "id" if "id" in societies_raw[0] else societies_raw[0].keys().__iter__().__next__()
    societies = {row[soc_id_col]: row for row in societies_raw}
    print(f"  {len(societies)} societies loaded. ID column = '{soc_id_col}'")
    print(f"  Society columns: {list(societies_raw[0].keys())}\n")

    # 2. Load codes lookup for EA029 (code value -> human-readable label)
    print("Loading codes ...")
    codes_raw = fetch_small(CODES_URL)
    codes = {
        row["code"]: row.get("name") or row.get("description", "")
        for row in codes_raw
        if row.get("var_id") == TARGET_VAR
    }
    print(f"  {len(codes)} codes for {TARGET_VAR}: {codes}\n")

    # 3. Stream data.csv, keep only EA029 rows
    print("Streaming data ...")
    ea029_rows = stream_filter_data(DATA_URL, TARGET_VAR)
    print(f"  {len(ea029_rows)} rows found for {TARGET_VAR}\n")

    if not ea029_rows:
        print("ERROR: No EA029 rows found. Check the var_id column name in data.csv.")
        sys.exit(1)

    # 4. Detect the society-id column in data.csv
    data_soc_col = None
    for candidate in ("soc_id", "Society_id", "society_id", "id"):
        if candidate in ea029_rows[0]:
            data_soc_col = candidate
            break
    if data_soc_col is None:
        print(f"ERROR: Cannot find society ID column in data.csv. Columns: {list(ea029_rows[0].keys())}")
        sys.exit(1)
    print(f"  data.csv society ID column = '{data_soc_col}'")

    # 5. Write output CSV
    code_col  = f"{TARGET_VAR}_code"
    label_col = f"{TARGET_VAR}_label"
    out_cols = [
        "society_id",
        "society_name",
        "region",
        "lat",
        "lon",
        "focal_year",
        code_col,
        label_col,
        "comment",
        "references",
    ]

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_cols)
        writer.writeheader()

        for row in ea029_rows:
            sid  = row.get(data_soc_col, "")
            soc  = societies.get(sid, {})
            code = row.get("code", "")

            writer.writerow({
                "society_id":   sid,
                "society_name": soc.get("pref_name_for_society", ""),
                "region":       soc.get("region", ""),
                "lat":          soc.get("Lat", ""),
                "lon":          soc.get("Long", ""),
                "focal_year":   soc.get("main_focal_year", ""),
                code_col:       code,
                label_col:      codes.get(code, ""),
                "comment":      row.get("comment", ""),
                "references":   row.get("references", ""),
            })

    print(f"\nDone. Saved {len(ea029_rows)} rows to {OUTPUT_FILE}")

    # 6. Quick sanity preview
    print("\nFirst 5 rows:")
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= 6:
                break
            print(" ", line.rstrip())


if __name__ == "__main__":
    main()
