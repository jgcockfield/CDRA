#!/usr/bin/env python3
"""
D-PLACE SCCS scraper

Downloads a single SCCS variable from the D-PLACE GitHub repository,
joins society metadata and code labels, and saves to CSV.
Streams data.csv line-by-line to keep memory usage low.

Usage:
    python scrapers/scrape_sccs.py SCCS61

Output: outputs/sccs/sccs_<VARID>.csv
"""

import csv
import io
import os
import sys
import requests

GITHUB_BASE  = "https://raw.githubusercontent.com/D-PLACE/dplace-data/master/datasets/SCCS"
SOCIETIES_URL = f"{GITHUB_BASE}/societies.csv"
CODES_URL     = f"{GITHUB_BASE}/codes.csv"
DATA_URL      = f"{GITHUB_BASE}/data.csv"

TARGET_VAR  = sys.argv[1].upper() if len(sys.argv) > 1 else "SCCS61"
OUTPUT_FILE = os.path.join("outputs", "sccs", f"sccs_{TARGET_VAR}.csv")


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
    for raw_line in lines:
        if not raw_line:
            continue
        row_values = next(csv.reader([raw_line]))
        row = dict(zip(headers, row_values))
        if row.get("var_id") == var_id:
            matched.append(row)

    return matched


def main():
    print(f"=== D-PLACE SCCS scraper — {TARGET_VAR} ===\n")

    # 1. Load societies lookup (keyed by society id)
    print("Loading societies ...")
    societies_raw = fetch_small(SOCIETIES_URL)
    soc_id_col = "id" if "id" in societies_raw[0] else next(iter(societies_raw[0]))
    societies = {row[soc_id_col]: row for row in societies_raw}
    print(f"  {len(societies)} societies loaded. ID column = '{soc_id_col}'")
    print(f"  Society columns: {list(societies_raw[0].keys())}\n")

    # 2. Load codes lookup for TARGET_VAR (code value -> human-readable label)
    print("Loading codes ...")
    codes_raw = fetch_small(CODES_URL)
    codes = {
        row["code"]: row.get("name") or row.get("description", "")
        for row in codes_raw
        if row.get("var_id") == TARGET_VAR
    }
    print(f"  {len(codes)} codes for {TARGET_VAR}: {codes}\n")

    # 3. Stream data.csv, keep only TARGET_VAR rows
    print("Streaming data ...")
    data_rows = stream_filter_data(DATA_URL, TARGET_VAR)
    print(f"  {len(data_rows)} rows found for {TARGET_VAR}\n")

    if not data_rows:
        print(f"ERROR: No rows found for {TARGET_VAR}. Check the var_id in data.csv.")
        sys.exit(1)

    # 4. Detect the society-id column in data.csv
    data_soc_col = None
    for candidate in ("soc_id", "Society_id", "society_id", "id"):
        if candidate in data_rows[0]:
            data_soc_col = candidate
            break
    if data_soc_col is None:
        print(f"ERROR: Cannot find society ID column. Columns: {list(data_rows[0].keys())}")
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

        for row in data_rows:
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

    print(f"\nDone. Saved {len(data_rows)} rows to {OUTPUT_FILE}")

    # 6. Quick sanity preview
    print("\nFirst 5 rows:")
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= 6:
                break
            print(" ", line.rstrip())


if __name__ == "__main__":
    main()
