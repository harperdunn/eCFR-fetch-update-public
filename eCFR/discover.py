"""
Script 2: Discover new CAFO-relevant CFR documents
====================================================
What this does:
  - Searches the eCFR API for CAFO-related terms across all CFR titles
  - Deduplicates results across all keyword searches
  - Filters out documents already in your database (by source_url match)
  - Appends genuinely new documents as "Needs Review" rows for a reviewer to confirm
  - Auto-assigns POL_ids continuing from the highest existing number

What it does NOT do:
  - It does not mark anything as "Included" — that requires human judgment
  - It does not fill in Summary, related_docs, or effective_date — those are manual, could automate them in the future
  - It does not overwrite any existing rows

How to run:
  pip install requests
  python3 discover.py

Output:
  Appends new rows to the bottom of INPUT_CSV to create OUTPUT_CSV.
  Prints a summary of what was found and added.
"""

import csv
import re
import time
import datetime
import requests
from urllib.parse import quote

# ── Configuration ──────────────────────────────────────────────────────────────

INPUT_CSV      = "data/batch_1_included_enriched.csv"
OUTPUT_CSV     = "batch_1_included_demo.csv"  # (usually) same file — we append in place
#Make sure this is deleted if you're no longer updating a file that was already created by discover (aka adding keywords), otherwise it will not return anything.
LAST_RUN_FILE  = "discover_last_run.txt"  # auto-updated each run. 

# Keywords to search across all CFR titles.
#potential additions: 
KEYWORDS = [
    "CAFO", #strong matches with "concentrated animal feeding operation"
    "feedlot",
    "manure",
    "factory farm",
    "animal waste",
    "land application",
    "nutrient management"
]

RESULTS_PER_SEARCH = 20  # top N results per keyword (filtered to relevant titles client-side)
# 1 API Call per Keyword (4 right now)

ECFR_SEARCH_URL = "https://www.ecfr.gov/api/search/v1/results" #endpoint

# ── Helpers ────────────────────────────────────────────────────────────────────

def get_highest_pol_number(rows):
    """
    Find the highest numeric POL_id in existing rows.

    We parse the number out of strings like 'POL-105' → 105.
    New IDs will start from highest + 1.

    We never backfill gaps (e.g. POL-008 is missing) because those gaps
    may be intentional and backfilling risks collisions with documents
    referenced elsewhere.
    """
    highest = 0
    for row in rows:
        pol_id = row.get('POL_id', '')
        match = re.search(r'POL-(\d+)', pol_id)
        if match:
            num = int(match.group(1))
            if num > highest:
                highest = num
    return highest


def build_source_url(title_num, hierarchy):
    """
    Construct a canonical eCFR URL from search result hierarchy data.

    The search API returns a 'hierarchy' dict like:
      {"title": "40", "part": "122", "section": "122.23"}

    We build: https://www.ecfr.gov/current/title-40/part-122/section-122.23

    If section is missing (part-level result), we stop at the part.
    If part is missing (title-level result), we stop at the title.

    This matches the URL pattern already used in the database.
    """
    base = f"https://www.ecfr.gov/current/title-{title_num}"
    part = hierarchy.get('part')
    section = hierarchy.get('section')
    appendix = hierarchy.get('appendix')

    if part and section:
        return f"{base}/part-{part}/section-{section}"
    elif part and appendix:
        return f"{base}/part-{part}/appendix-{quote(appendix)}"
    elif part:
        return f"{base}/part-{part}"
    else:
        return base


def search_ecfr(keyword, last_run_date):
    """
    Call the eCFR search API for a single keyword across all CFR titles.
    Only returns sections modified after last_run_date, so each run is
    incremental rather than a full re-scan.

    Returns a list of result dicts, each containing:
      - 'title': document title string
      - 'source_url': constructed eCFR URL
      - 'title_num': CFR title number (int)
      - 'agency': issuing agency name 
      - 'hierarchy': raw hierarchy dict from API (for debugging)

    If the API call fails, we print a warning and return an empty list
    rather than crashing — so one bad search doesn't stop the whole run.
    """
    params = {
        "query":               keyword,
        "per_page":            RESULTS_PER_SEARCH,
        "last_modified_after": last_run_date,
    }

    try:
        response = requests.get(ECFR_SEARCH_URL, params=params, timeout=15) #api call
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"  WARNING: Search failed for '{keyword}': {e}")
        return []

    results = []
    for item in data.get('results', []):
        hierarchy = item.get('hierarchy', {})

        title_num = int(hierarchy['title']) if hierarchy.get('title', '').isdigit() else None
        url = build_source_url(title_num, hierarchy)

        # Build title in format: "40 CFR § 412.45: Effluent limitations..."
        hierarchy_headings = item.get('hierarchy_headings', {})
        headings = item.get('headings', {})
        section_num = (hierarchy_headings.get('section') or '').strip()
        appendix_id = (hierarchy_headings.get('appendix') or '').strip()
        part_num = (hierarchy_headings.get('part') or '').strip()
        #take out the html tags if keywords are in the section/part/appendix name
        section_name = re.sub(r'<[^>]+>', '', (headings.get('section') or '').strip()).strip()
        appendix_name = re.sub(r'<[^>]+>', '', (headings.get('appendix') or '').strip()).strip()
        part_name = re.sub(r'<[^>]+>', '', (headings.get('part') or '').strip()).strip()
        cfr_prefix = f"{title_num} CFR" if title_num else "CFR"
        if section_num:
            doc_title = f"{cfr_prefix} § {section_num}: {section_name}" if section_name else f"{cfr_prefix} § {section_num}"
        elif appendix_id:
            doc_title = f"{cfr_prefix} {appendix_id}: {appendix_name}" if appendix_name else f"{cfr_prefix} {appendix_id}"
        elif part_num:
            doc_title = f"{cfr_prefix} Part {part_num}: {part_name}" if part_name else f"{cfr_prefix} Part {part_num}"
        else:
            doc_title = cfr_prefix

        # Strip HTML tags from the excerpt (API returns <strong>, <span>, etc.)
        raw_excerpt = item.get('full_text_excerpt', '')
        excerpt = re.sub(r'<[^>]+>', '', raw_excerpt).strip()

        # Build parentage: walk all CFR hierarchy levels in order, include any
        # that are present. Stops before section/appendix (already in title col).
        HIERARCHY_LEVELS = [
            'title', 'subtitle', 'chapter', 'subchapter',
            'part', 'subpart', 'subject_group',
        ]
        parentage_parts = [
            re.sub(r'<[^>]+>', '', (hierarchy_headings.get(level) or '').strip()).strip()
            for level in HIERARCHY_LEVELS
            if (hierarchy_headings.get(level) or '').strip()
        ]
        parentage = ' > '.join(parentage_parts)

        results.append({
            'title':      doc_title,
            'source_url': url,
            'title_num':  title_num,
            'agency':     re.sub(r'<[^>]+>', '', (headings.get('chapter') or '').strip()).strip(),
            'excerpt':    excerpt,
            'parentage':  parentage,
            'hierarchy':  hierarchy,
        })

    return results


# ── Main logic ─────────────────────────────────────────────────────────────────

def main():
    # Step 0: Load last run date from file, default to 2000-01-01 for first run
    today = datetime.date.today().isoformat()
    try:
        with open(LAST_RUN_FILE) as f:
            last_run_date = f.read().strip()
        print(f"Last run date: {last_run_date} (searching for changes since then)")
    except FileNotFoundError:
        last_run_date = "2000-01-01"
        print(f"No last run file found — doing full scan from {last_run_date}")

    # Step 1: Read existing CSV
    print(f"Reading {INPUT_CSV}...")
    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames)

    print(f"  Loaded {len(rows)} existing rows.")

    if 'full_text_excerpt' not in fieldnames:
        fieldnames.append('full_text_excerpt')
    if 'parentage' not in fieldnames:
        fieldnames.append('parentage')

    # Step 2: Build a set of existing URLs for fast deduplication.
    # it doesn't slow down as the database grows. With a list it would be O(n).
    existing_urls = {r['source_url'].strip() for r in rows}
    print(f"  Tracking {len(existing_urls)} existing source URLs for deduplication.")

    # Step 3: Find the next available POL_id number
    highest = get_highest_pol_number(rows)
    next_pol_num = highest + 1
    print(f"  Highest existing POL_id: POL-{highest:03d}. New IDs start at POL-{next_pol_num:03d}.")

    # Step 4: Run all searches and collect unique new results.
    # One API call per keyword — results are filtered to TITLES client-side.
    # We use a dict keyed by source_url to deduplicate across keyword searches.
    # (Same section can appear in "CAFO" search AND "manure" search — we only keep it once.)
    discovered = {}  # {source_url: {title, source_url, title_num, agency}}

    for call_num, keyword in enumerate(KEYWORDS, start=1):
        print(f"  [{call_num}/{len(KEYWORDS)}] Searching for '{keyword}'...")

        results = search_ecfr(keyword, last_run_date)
        new_this_call = 0

        for result in results:
            url = result['source_url']

            # Skip if already in the database
            if url in existing_urls:
                continue

            # Skip if already discovered in a previous search
            if url in discovered:
                continue

            discovered[url] = {
                'title':      result['title'],
                'source_url': url,
                'title_num':  result['title_num'],
                'agency':     result['agency'],
                'excerpt':    result['excerpt'],
                'parentage':  result['parentage'],
            }
            new_this_call += 1

        print(f"    → {len(results)} results, {new_this_call} new unique finds.")

        # Small delay between API calls — good practice to avoid hammering the server.
        time.sleep(0.5)

    print(f"\n  Total unique new documents found: {len(discovered)}")

    if not discovered:
        print("  Nothing new to add. Your database is already up to date for these searches.")
        return

    # Step 5: Build new rows for each discovered document
    new_rows = []
    for url, doc in discovered.items():
        pol_id = f"POL-{next_pol_num:03d}"
        next_pol_num += 1

        # Build the new row with all columns.
        # Fields we can fill deterministically from the API or hardcoded logic:
        new_row = {col: '' for col in fieldnames}  # start with all columns blank
        new_row.update({
            'POL_id':             pol_id,
            'record_status':      'Needs Review',
            'exclusion_reason':   '',
            'title':              doc['title'],
            'source_url':         doc['source_url'],
            'pipeline_type':      '1.0',
            'jurisdiction_level': 'Federal',
            'document_type':      'Regulation',
            'issuing_agency':     doc['agency'],
            'binding_authority':  'Binding', #all eCFR regulations are binding
            'binding_notes':      '',
            'policy_status':      '',
            'effective_date':     '',
            'expiration_date':    '',
            'related_docs':       '',
            'full_text_excerpt':  doc['excerpt'],
            'parentage':          doc['parentage'],
            'Summary':            '',
        })
        new_rows.append(new_row)

    # Step 6: Append new rows to the CSV
    print(f"Appending {len(new_rows)} new rows to {OUTPUT_CSV}...")
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)       # all existing rows first
        writer.writerows(new_rows)   # new discoveries at the bottom

    print(f"Done. {OUTPUT_CSV} now has {len(rows) + len(new_rows)} rows.")
    print(f"\nNew rows added (all marked 'Needs Review' for a reviewer to confirm):")
    for r in new_rows:
        print(f"  {r['POL_id']} | {r['title'][:70]}")

    # Save today's date so the next run only looks for changes since now
    with open(LAST_RUN_FILE, 'w') as f:
        f.write(today)
    print(f"\nUpdated {LAST_RUN_FILE} to {today}.")


if __name__ == "__main__":
    main()