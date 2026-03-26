#!/usr/bin/env python3
"""
PubMed DRD4 scraper

Searches PubMed for all DRD4 literature, fetches metadata and abstracts via
NCBI Entrez E-utilities, and attempts full article text retrieval via the PMC
Open Access API. Paywalled articles are flagged in the CSV.

Output: outputs/pubmed/pubmed_drd4_<timestamp>.csv
"""

import csv
import datetime
import os
import sys
import time
import xml.etree.ElementTree as ET

import requests

# ── Configuration ──────────────────────────────────────────────────────────────
SEARCH_TERM = "DRD4[All Fields]"
BATCH_SIZE  = 200
DELAY       = 0.34          # seconds between requests (NCBI: max 3/sec without key)
OUTPUT_DIR  = os.path.join("outputs", "pubmed")
EUTILS      = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

OUT_COLS = [
    "pmid", "pmcid", "title", "authors", "journal",
    "pub_date", "doi", "abstract", "full_text", "open_access",
]


# ── HTTP helper ────────────────────────────────────────────────────────────────
def get(url: str, params: dict) -> requests.Response:
    """Rate-limited GET."""
    time.sleep(DELAY)
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r


# ── Search ─────────────────────────────────────────────────────────────────────
def esearch_all(term: str) -> list[str]:
    """Return all PMIDs matching term, paginating in batches."""
    r = get(f"{EUTILS}/esearch.fcgi", {
        "db": "pubmed", "term": term, "retmax": 0, "retmode": "json",
    })
    total = int(r.json()["esearchresult"]["count"])
    print(f"  Total results: {total:,}")

    pmids: list[str] = []
    while len(pmids) < total:
        r = get(f"{EUTILS}/esearch.fcgi", {
            "db": "pubmed", "term": term,
            "retmax": BATCH_SIZE, "retstart": len(pmids),
            "retmode": "json",
        })
        batch = r.json()["esearchresult"]["idlist"]
        if not batch:
            break
        pmids.extend(batch)
        print(f"  Retrieved {len(pmids):,}/{total:,} PMIDs")

    return pmids


# ── PubMed metadata fetch + parse ──────────────────────────────────────────────
def _all_text(el: ET.Element | None) -> str:
    """Concatenate all text within an element, including sub-element tails."""
    return "".join(el.itertext()).strip() if el is not None else ""


def efetch_pubmed_xml(pmids: list[str]) -> ET.Element:
    """Fetch PubMed XML for a batch of PMIDs."""
    r = get(f"{EUTILS}/efetch.fcgi", {
        "db": "pubmed",
        "id": ",".join(pmids),
        "rettype": "xml",
        "retmode": "xml",
    })
    return ET.fromstring(r.content)


def parse_pubmed_xml(root: ET.Element) -> list[dict]:
    """Parse a PubmedArticleSet element into a list of record dicts."""
    records = []

    for article in root.findall(".//PubmedArticle"):
        rec = {col: "" for col in OUT_COLS}

        mc = article.find("MedlineCitation")
        if mc is None:
            continue

        rec["pmid"] = mc.findtext("PMID", "")

        art = mc.find("Article")
        if art is None:
            records.append(rec)
            continue

        # Title
        rec["title"] = _all_text(art.find("ArticleTitle"))

        # Authors
        authors = []
        for author in art.findall(".//Author"):
            last = author.findtext("LastName", "")
            fore = author.findtext("ForeName", "")
            if last:
                authors.append(f"{last} {fore}".strip())
            else:
                collective = author.findtext("CollectiveName", "")
                if collective:
                    authors.append(collective)
        rec["authors"] = "; ".join(authors)

        # Journal
        rec["journal"] = art.findtext(".//Journal/Title", "")

        # Publication date
        pub_date_el = art.find(".//PubDate")
        if pub_date_el is not None:
            parts = filter(None, [
                pub_date_el.findtext("Year", ""),
                pub_date_el.findtext("Month", ""),
                pub_date_el.findtext("Day", ""),
            ])
            rec["pub_date"] = " ".join(parts)

        # Abstract (structured abstracts have labelled sections)
        abstract_parts = []
        for ab in art.findall(".//AbstractText"):
            label = ab.get("Label", "")
            text  = _all_text(ab)
            abstract_parts.append(f"{label}: {text}" if label else text)
        rec["abstract"] = " ".join(abstract_parts)

        # DOI and PMCID
        for aid in article.findall(".//ArticleId"):
            id_type = aid.get("IdType", "")
            if id_type == "doi":
                rec["doi"]   = aid.text or ""
            elif id_type == "pmc":
                rec["pmcid"] = aid.text or ""   # e.g. "PMC1234567"

        records.append(rec)

    return records


# ── PMC full-text fetch ────────────────────────────────────────────────────────
def fetch_pmc_fulltext(pmcid: str) -> tuple[str, bool]:
    """
    Attempt to retrieve full article text from PMC Open Access.
    Returns (text, is_open_access). Non-OA articles return ("paywalled", False).
    """
    numeric_id = pmcid.replace("PMC", "").strip()

    try:
        r = get(f"{EUTILS}/efetch.fcgi", {
            "db": "pmc", "id": numeric_id,
            "rettype": "xml", "retmode": "xml",
        })
    except requests.HTTPError:
        return "paywalled", False

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError:
        return "paywalled", False

    # PMC returns an <ERROR> root element for non-OA or missing articles
    if root.tag in ("ERROR", "error") or root.find(".//error") is not None:
        return "paywalled", False

    body = root.find(".//body")
    if body is None:
        return "paywalled", False

    paragraphs = [_all_text(p) for p in body.iter("p") if _all_text(p)]
    if not paragraphs:
        return "paywalled", False

    return " ".join(paragraphs), True


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    timestamp   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"pubmed_drd4_{timestamp}.csv")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=== PubMed DRD4 scraper ===")
    print(f"Search:  {SEARCH_TERM!r}")
    print(f"Output:  {output_file}\n")

    # 1. Collect all PMIDs
    print("Searching PubMed ...")
    pmids = esearch_all(SEARCH_TERM)
    print(f"  Done — {len(pmids):,} PMIDs\n")

    # 2. Fetch + parse metadata in batches
    all_records: list[dict] = []
    total_batches = (len(pmids) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(pmids), BATCH_SIZE):
        batch     = pmids[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"Fetching metadata batch {batch_num}/{total_batches} "
              f"({len(batch)} articles) ...")
        try:
            xml_root = efetch_pubmed_xml(batch)
            records  = parse_pubmed_xml(xml_root)
            all_records.extend(records)
            print(f"  Parsed {len(records)} records  (running total: {len(all_records):,})")
        except Exception as e:
            print(f"  ERROR on batch {batch_num}: {e}", file=sys.stderr)

    # 3. Full-text retrieval via PMC OA
    pmc_articles = [r for r in all_records if r["pmcid"]]
    print(f"\nFetching full text for {len(pmc_articles):,} articles with PMCID "
          f"({len(all_records) - len(pmc_articles):,} will be marked paywalled) ...")

    oa_count = 0
    for idx, rec in enumerate(all_records, 1):
        if not rec["pmcid"]:
            rec["full_text"]   = "paywalled"
            rec["open_access"] = "False"
            continue

        text, is_oa = fetch_pmc_fulltext(rec["pmcid"])
        rec["full_text"]   = text
        rec["open_access"] = str(is_oa)
        if is_oa:
            oa_count += 1

        done = sum(1 for r in all_records[:idx] if r["pmcid"])
        if done % 200 == 0:
            print(f"  {done:,}/{len(pmc_articles):,} PMC fetches done  "
                  f"(open access so far: {oa_count:,})")

    print(f"  PMC articles: {len(pmc_articles):,}  |  Open access: {oa_count:,}")

    # 4. Write CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUT_COLS)
        writer.writeheader()
        writer.writerows(all_records)

    print(f"\nDone. Saved {len(all_records):,} records to {output_file}")

    # 5. Preview
    print("\nFirst 3 records:")
    for rec in all_records[:3]:
        print(f"  PMID={rec['pmid']}  PMCID={rec['pmcid'] or '—'}  "
              f"OA={rec['open_access']}")
        print(f"  {rec['title'][:90]}")
        print()


if __name__ == "__main__":
    main()
