"""
Script 1: Enrich existing eCFR rows
====================================
What this does:
  - This was really for the beginning stages, now it's a bit outdated
  - Reads the existing database CSV
  - Finds rows whose source_url points to ecfr.gov (these are federal CFR documents)
  - Calls the eCFR versions API to get the true amendment date for each document
  - Fills in two columns:
      latest_amendment_date : the most recent date this section or part was amended per the API
      version_flag          : "Check" if amended after VERSION_CHECK_AFTER, else blank

Section-level vs part-level:
  - If the URL points to a specific section (e.g. §122.23), we fetch that section's
    own amendment history and use the most recent SUBSTANTIVE change date.
  - If the URL points to a whole part (e.g. Part 412), we use the part's
    latest_amendment_date from the API meta field.
  - Substantive=False versions (typo fixes, renumbering) are intentionally ignored —
    they don't represent real regulatory changes.

IMPORTANT — what latest_amendment_date means here:
  For section-level rows: the date that specific section last had a substantive change.
  For part-level rows: the date anything in that part last changed.
  effective_date is left untouched — set that manually based on your own review.

How to run:
  pip install requests
  python3 script1_enrich.py

Output:
  Overwrites the input CSV with updated columns.
  All existing data is preserved exactly as-is.
"""

import csv
import re
import time
import requests

# ── Configuration ──────────────────────────────────────────────────────────────

INPUT_CSV  = "batch_1_included_updated_discover_additional.csv"
OUTPUT_CSV = "batch_1_included_updated_discover_additional.csv"

# Rows amended after this date will be flagged for review.
# Set this to whenever you last manually reviewed these documents.
VERSION_CHECK_AFTER = "2026-02-26"

ECFR_VERSIONS_URL = "https://www.ecfr.gov/api/versioner/v1/versions/title-{title}.json"

# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_ecfr_url(url):
    """
    Parse an eCFR source URL into its component parts.

    Returns a dict with any of: title, part, section — whatever is present.

    Examples:
      .../title-40/part-122/section-122.23  → {title:40, part:122, section:122.23}
      .../title-40/part-412                 → {title:40, part:412}
    """
    result = {}
    title   = re.search(r'title-(\d+)', url)
    part    = re.search(r'part-(\w+)', url)
    section = re.search(r'section-([\d.]+)', url)
    if title:   result['title']   = title.group(1)
    if part:    result['part']    = part.group(1)
    if section: result['section'] = section.group(1)
    return result


def fetch_latest_amendment_date(parsed):
    """
    Call the eCFR versions endpoint and return the most recent amendment date
    for this document, based on whether it's a section or part-level URL.

    For sections: returns the most recent substantive amendment date.
    For parts:    returns the meta.latest_amendment_date.
    Returns '' on failure or if no date found.

    The versions endpoint returns every historical version of the document,
    each with an amendment_date and a substantive flag. We filter for
    substantive=True because non-substantive edits (formatting, renumbering)
    don't represent real policy changes.
    """
    title = parsed.get('title')
    if not title:
        return ''

    params = {}
    if parsed.get('part'):    params['part']    = parsed['part']
    if parsed.get('section'): params['section'] = parsed['section']

    url = ECFR_VERSIONS_URL.format(title=title) #plugs in our title to get all the information undermeath

    try:
        #calls the api given the accurate section and parameters 
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        #print(f"data from the last api call response: {data}")
    except requests.RequestException as e:
        print(f"    WARNING: API call failed: {e}")
        return ''

    has_section = 'section' in parsed

    if has_section:
        # Find the most recent substantive, non-removed amendment.
        # max() on YYYY-MM-DD strings works correctly — lexicographic = chronological.
        substantive_dates = [
            v['amendment_date'] #when the
            for v in data.get('content_versions', [])
            if v.get('substantive') and not v.get('removed')
        ]
        return max(substantive_dates) if substantive_dates else '' #if no substantive edits then we do not record the date.
    else:
        # Part-level: the API meta gives us the latest date across all sections.
        return data.get('meta', {}).get('latest_amendment_date', '')


# ── Main logic ─────────────────────────────────────────────────────────────────

def main():
    # Step 1: Read existing CSV
    print(f"Reading {INPUT_CSV}...")
    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames)

    print(f"  Loaded {len(rows)} rows.")

    ecfr_rows = [r for r in rows if 'ecfr.gov' in r.get('source_url', '')]
    print(f"  Found {len(ecfr_rows)} eCFR rows to enrich.")

    # Step 2: Add new columns if not already present.
    for col in ['latest_amendment_date', 'version_flag', 'parentage']:
        if col not in fieldnames:
            fieldnames.append(col)

    for row in rows:
        row.setdefault('latest_amendment_date', '')
        row.setdefault('version_flag', '')

    # Step 3: For each eCFR row, fetch the section/part-level amendment date
    print("  Fetching latest amendment dates (one call per row)...")
    enriched = 0
    flagged  = 0

    for row in ecfr_rows:
        url    = row['source_url']
        parsed = parse_ecfr_url(url)
        pol_id = row['POL_id']

        print(f"  {pol_id} | title={parsed.get('title')} "
              f"part={parsed.get('part')} section={parsed.get('section', '—')}")

        latest = fetch_latest_amendment_date(parsed)
        row['latest_amendment_date'] = latest

        # Flag if the latest amendment date is newer than our threshold
        if latest and latest > VERSION_CHECK_AFTER:
            row['version_flag'] = 'Check'
            flagged += 1
        else:
            row['version_flag'] = ''

        enriched += 1
        time.sleep(0.5)  # polite delay between API calls

    print(f"\n  Enriched {enriched} rows.")
    print(f"  Flagged {flagged} rows for version review (amended after {VERSION_CHECK_AFTER}).")

    # Step 5: Write updated CSV
    print(f"\nWriting output to {OUTPUT_CSV}...")
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done. {OUTPUT_CSV} updated successfully.")


if __name__ == "__main__":
    main()