# Market Data Pipeline

A pipeline that fetches recent AI research papers from the [OpenAlex](https://openalex.org) API and stores them in a [Neon](https://neon.tech) PostgreSQL database for dashboard and analysis use.

## Project Structure

```
market-data-pipeline/
├── pipeline.py             # End-to-end pipeline: fetch → create table → load → test
├── data_test.py            # Standalone runner for data-quality tests only
├── db.py                   # Database connection helper (reads credentials from .env)
├── sql/
│   ├── create_papers_table.sql   # DDL: papers table definition and indexes
│   ├── insert_papers.sql         # DML: bulk insert with duplicate handling
│   └── data_tests.sql            # SQL data-quality test queries
├── requirements.txt
└── .env                    # Database credentials (gitignored)
```

## Setup

**1. Create and activate the virtual environment**

```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

**2. Install dependencies**

```powershell
pip install -r requirements.txt
```

**3. Configure credentials**

Create a `.env` file in the project root:

```
DB_PASSWORD=your_neon_password
```

The database connects to the Neon PostgreSQL instance defined in `db.py`. The `.env` file is gitignored and excluded from Cursor's AI context via `.cursorignore`.

## Usage

### Run the full pipeline

Fetches AI papers published in the last 3 days from OpenAlex, creates the `papers` table if it does not exist, bulk-inserts the results (duplicates skipped), and runs all data-quality tests.

```powershell
python pipeline.py
```

### Run data-quality tests only

Connects to the database and runs the six SQL tests against the existing `papers` table without fetching or loading any data.

```powershell
python data_test.py
```

## Pipeline Steps

| Step | What it does |
|---|---|
| **1. Fetch** | Looks up the OpenAlex AI taxonomy IDs, then paginates through all works published in the last 3 days that belong to the Artificial Intelligence subfield or field. |
| **2. Create table** | Runs `sql/create_papers_table.sql` with `CREATE TABLE IF NOT EXISTS` — safe to re-run. |
| **3. Load** | Maps each raw API response to the flat `papers` schema and bulk-inserts rows. Duplicate `openalex_id` values are silently skipped via `ON CONFLICT DO NOTHING`. |
| **4. Test** | Runs the six queries in `sql/data_tests.sql` and prints a PASS/FAIL table. Exits with code `1` if any test fails. |

## Data-Quality Tests

| Test | What it checks |
|---|---|
| NOT NULL / required fields | `openalex_id`, `title`, `publication_year`, and `type` are never NULL |
| Primary key uniqueness | No duplicate `openalex_id` values exist |
| Date / year consistency | `EXTRACT(YEAR FROM publication_date)` matches `publication_year` |
| Numeric range sanity | No negative `cited_by_count`, `fwci`, or `author_count`; `citation_percentile` in \[0, 1\] |
| Top-percentile consistency | Every paper in the top 1% is also in the top 10% |
| Pipeline freshness | At least one row was ingested in the last 24 hours |

## Database Schema

All data is stored in a single flat `papers` table in Neon PostgreSQL. Key fields:

| Category | Fields |
|---|---|
| Identity | `openalex_id` (PK), `doi`, `title` |
| Bibliographic | `publication_date`, `publication_year`, `language`, `type` |
| Venue | `journal_name`, `journal_issn` |
| Open Access | `is_oa`, `oa_status` |
| Citation Metrics | `cited_by_count`, `fwci`, `citation_percentile`, `is_in_top_1_percent`, `is_in_top_10_percent` |
| Collaboration | `author_count`, `countries_distinct_count`, `institutions_distinct_count` |
| Topic | `primary_topic`, `primary_subfield`, `primary_field` |
| First Author | `first_author_name`, `first_author_id` |
| Flags | `is_retracted` |
| Pipeline | `ingested_at` |

Indexes are created on `publication_date`, `primary_field`, and `cited_by_count` for efficient dashboard queries.

## Dependencies

- [pyalex](https://github.com/J535D165/pyalex) — OpenAlex API client
- [psycopg2-binary](https://pypi.org/project/psycopg2-binary/) — PostgreSQL adapter
- [python-dotenv](https://pypi.org/project/python-dotenv/) — loads `.env` credentials
- [streamlit](https://streamlit.io) — dashboard framework
