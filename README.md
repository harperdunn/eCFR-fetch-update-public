# NARN — eCFR Data Scripts

Scripts for discovering, enriching, and monitoring CAFO-relevant federal regulations from the [eCFR](https://www.ecfr.gov/) (Electronic Code of Federal Regulations).

---

## Overview

These scripts maintain a CSV database of CFR documents relevant to CAFOs (Concentrated Animal Feeding Operations).

**Ongoing workflow (run weekly):**
```
discover.py  →  monitor.py
```

| Script | Purpose |
|---|---|
| `discover.py` | Searches the eCFR API for new CAFO-relevant regulations and appends them to the database |
| `monitor.py` | Checks existing database rows against the current eCFR API and flags any that have been amended |
| `enrich.py` | One-time setup script used for the initial batch — fills in `latest_amendment_date` for existing rows |

---

## Setup

```bash
pip install requests
```

---

## Scripts

### `discover.py`

Searches the eCFR API across a set of CAFO-related keywords and appends genuinely new documents to the database as `Needs Review` rows.

- Deduplicates results across keyword searches and against existing database URLs
- Auto-assigns `POL_id` values continuing from the highest existing number
- Only returns documents modified after the last run date (incremental)
- Does **not** mark anything as "Included" — that requires human review

**Keywords searched:** `CAFO`, `feedlot`, `manure`, `factory farm`, `animal waste`, `land application`, `nutrient management`

```bash
cd eCFR
python3 discover.py
```

**Output:** Appends new rows to `batch_1_included_ultimate_broad_update.csv`. Updates `discover_last_run.txt` with today's date for the next incremental run.

---

### `monitor.py`

Checks each eCFR row in the database against the current API and flags documents that have been amended since the stored date.

- Compares stored `latest_amendment_date` against the live API date
- Updates `latest_amendment_date` and sets `version_flag` to `Check` for any changes
- Writes a `version_report.txt` summarizing what changed
- Only rewrites the CSV if changes were actually detected
- Does **not** clear existing `Check` flags — that's done manually after human review

```bash
cd eCFR
python3 monitor.py
```

**Output:** `needs_review_docs_monitored.csv` and `version_report_needs_review_docs.txt`.

Intended to run weekly or after any known eCFR update.

---

## Database Columns

| Column | Description |
|---|---|
| `POL_id` | Unique identifier (`POL-001`, `POL-002`, ...) |
| `record_status` | `Needs Review`, `Included`, or `Excluded` |
| `title` | Document title (e.g., `40 CFR § 122.23: Concentrated animal feeding operations`) |
| `source_url` | eCFR URL for the specific section or part |
| `issuing_agency` | Agency name from the CFR chapter heading |
| `binding_authority` | `Binding` for all eCFR regulations |
| `effective_date` | Set manually after human review |
| `latest_amendment_date` | Most recent substantive amendment date from the API |
| `version_flag` | `Check` if amended more recently than last review; cleared manually |
| `full_text_excerpt` | Text excerpt from the search result |
| `parentage` | CFR hierarchy path (title > chapter > subchapter > part) |
| `Summary` | Filled in manually |
