"""
Script 3: Version Monitor
==========================
What this does:
  - Reads the existing database CSV
  - For every row whose source_url points to ecfr.gov, calls the eCFR versions
    API to get the current amendment date for that specific section or part
  - Compares that against the effective_date already stored in the CSV
  - If the API shows a newer substantive amendment, it:
      1. Updates effective_date in the CSV to the new date
      2. Sets version_flag to "Check" so Juliette knows to review it
      3. Logs the change in version_report.txt

What it does NOT do:
  - It does not tell you what specifically changed — a human still needs to
    read the amended section and assess whether it affects your research.
  - It does not touch non-eCFR rows (state, local documents have no API).
  - It does not clear existing "Check" flags — only a reviewer should do that
    once they have confirmed whether their specific section was affected.

How to run:
  pip install requests
  python3 monitor.py

Output:
  - Updates OUTPUT_CSV with new dates and flags where changes found from INPUT_CSV
  - Writes version_report.txt summarising what changed and what to check
  - Prints a summary to the terminal

Intended schedule:
  Run weekly (or after any known eCFR update) to stay current.
"""

import csv
import re
import time
import requests
from datetime import date

# ── Configuration ──────────────────────────────────────────────────────────────

INPUT_CSV   = "needs_review_docs.csv"
OUTPUT_CSV  = "needs_review_docs_monitored.csv"
REPORT_FILE = "version_report_needs_review_docs.txt"

ECFR_VERSIONS_URL = "https://www.ecfr.gov/api/versioner/v1/versions/title-{title}.json"

# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_ecfr_url(url):
    """
    Parse an eCFR source URL into title, part, section, and appendix components.
    Returns a dict with whichever components are present in the URL.

    Handles URLs built by discover.py's build_source_url, including appendix URLs like:
      https://www.ecfr.gov/current/title-40/part-412/appendix-Appendix%20B%20to%20Part%20412
    """
    result = {}
    title    = re.search(r'title-(\d+)', url)
    part     = re.search(r'part-(\w+)', url)
    section  = re.search(r'section-([\d.]+)', url)
    appendix = re.search(r'/appendix-(.+)', url)
    if title:    result['title']    = title.group(1)
    if part:     result['part']     = part.group(1)
    if section:  result['section']  = section.group(1)
    if appendix: result['appendix'] = appendix.group(1)
    return result


def fetch_current_amendment_date(parsed):
    """
    Fetch the current amendment date for a specific section or part.

    For sections/appendices: returns the most recent substantive amendment date.
    For parts:    returns meta.latest_amendment_date.
    Returns None on API failure (so caller can abort safely).
    Returns '' if the request succeeded but no date was found.

    We return None vs '' deliberately:
      None  = API call failed — don't update anything, data may be stale
      ''    = API succeeded but section has no dated versions yet
    """
    title = parsed.get('title')
    if not title:
        return ''

    params = {}
    if parsed.get('part'):     params['part']     = parsed['part']
    if parsed.get('section'):  params['section']  = parsed['section']
    if parsed.get('appendix'): params['appendix'] = parsed['appendix']

    url = ECFR_VERSIONS_URL.format(title=title)

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"    WARNING: API call failed: {e}")
        return None  # signals caller to skip this row, not overwrite with blank

    # For all granularities, filter content_versions for substantive changes.
    # Section/appendix queries return only that item's versions; part-level
    # queries return all sections in the part — either way this gives the
    # latest date where something substantively changed.
    substantive_dates = [
        v['amendment_date']
        for v in data.get('content_versions', [])
        if v.get('substantive') and not v.get('removed')
    ]
    return max(substantive_dates) if substantive_dates else ''


def write_report(report_lines, filepath):
    """
    Write the version report to a text file, overwriting any previous report.
    The report reflects THIS run only — not a cumulative history.
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
        f.write('\n')


# ── Main logic ─────────────────────────────────────────────────────────────────

def main():
    # Step 1: Read the existing CSV
    print(f"Reading {INPUT_CSV}...")
    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames)

    print(f"  Loaded {len(rows)} rows.")

    # Step 2: Ensure required columns exist
    for col in ['latest_amendment_date', 'version_flag', 'full_text_excerpt', 'nntage']:
        if col not in fieldnames:
            fieldnames.append(col)
    for row in rows:
        row.setdefault('latest_amendment_date', '')
        row.setdefault('version_flag', '')

    # Step 3: Check each eCFR row against the current API
    changes  = []
    checked  = 0
    skipped  = 0  # rows where API call failed

    ecfr_rows = [r for r in rows if 'ecfr.gov' in r.get('source_url', '')]
    print(f"  Checking {len(ecfr_rows)} eCFR rows against current API...")

    for row in ecfr_rows:
        url    = row['source_url']
        parsed = parse_ecfr_url(url)
        pol_id = row['POL_id']

        print(f"  {pol_id} | title={parsed.get('title')} "
              f"part={parsed.get('part')} section={parsed.get('section', '—')}")

        current_date = fetch_current_amendment_date(parsed)

        if current_date is None:
            # API failure — skip this row entirely, don't overwrite stored data
            print(f"    Skipped — API call failed, stored date preserved.")
            skipped += 1
            time.sleep(0.5)
            continue

        stored_date = row.get('latest_amendment_date', '').strip()
        checked += 1

        # A real change is detected when the API date is newer than stored.
        # String comparison is safe — YYYY-MM-DD is lexicographically ordered.
        if current_date and current_date > stored_date:
            changes.append({
                'pol_id':       pol_id,
                'title':        row['title'],
                'stored_date':  stored_date if stored_date else '(never recorded)',
                'current_date': current_date,
                'is_granular':  'section' in parsed or 'appendix' in parsed,
            })
            row['latest_amendment_date'] = current_date
            row['version_flag']          = 'Check'
            print(f"    CHANGED: {stored_date or '(none)'} → {current_date}")
        else:
            print(f"    No change: {stored_date}")

        time.sleep(0.5)

    print(f"\n  Checked: {checked} | Skipped (API failures): {skipped} | Changes: {len(changes)}")

    # Step 4: Build the report
    today = date.today().strftime('%Y-%m-%d')
    report_lines = [
        f"VERSION MONITOR REPORT — {today}",
        f"{'=' * 45}",
        f"eCFR rows checked  : {checked}",
        f"API failures       : {skipped}",
        f"Changes detected   : {len(changes)}",
        "",
    ]

    if not changes:
        report_lines.append("No changes detected. All stored dates match the current eCFR API.")
    else:
        report_lines.append("The following documents need review:")
        report_lines.append("")
        for c in changes:
            precision = "section/appendix-level" if c['is_granular'] else "part-level"
            report_lines.extend([
                f"  {c['pol_id']} | {c['title']}",
                f"    Precision   : {precision}",
                f"    Last stored : {c['stored_date']}",
                f"    Now shows   : {c['current_date']}",
                f"    Action      : Read the amended text and confirm whether",
                f"                  your specific subsection was affected.",
                f"                  Clear version_flag in CSV once confirmed.",
                "",
            ])

        report_lines.extend([
            "─" * 45,
            "NOTE: Section/appendix-level dates mean that specific item changed.",
            "Part-level dates mean something in the part changed — verify",
            "which section was affected before updating related_docs.",
        ])

    # Step 5: Write report file
    write_report(report_lines, REPORT_FILE)
    print(f"  Report written to {REPORT_FILE}.")

    # Step 6: Write updated CSV — only if there were changes
    if changes:
        print(f"Writing updated CSV to {OUTPUT_CSV}...")
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {len(changes)} row(s) updated and flagged for review.")
    else:
        print("  No changes — CSV not rewritten.")

    # Step 7: Print report to terminal
    print()
    for line in report_lines:
        print(line)


if __name__ == "__main__":
    main()