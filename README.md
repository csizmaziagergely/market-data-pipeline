# Market Data Pipeline

A pipeline that fetches recent AI research papers from the [OpenAlex](https://openalex.org) API and stores them in a [Neon](https://neon.tech) PostgreSQL database for dashboard and analysis use.

## Project Structure

```
market-data-pipeline/
├── fetch_ai_papers.py      # Fetch recent AI papers from OpenAlex and save to temp/
├── process_papers.py       # Load a JSON file into the database (create table + insert)
├── db.py                   # Database connection helper (reads credentials from .env)
├── create_table.py         # Standalone script to create the papers table and indexes
├── load_papers.py          # Standalone script to load the latest temp/ JSON into the DB
├── sql/
│   ├── create_papers_table.sql   # DDL: papers table definition and indexes
│   └── insert_papers.sql         # DML: bulk insert with duplicate handling
├── temp/                   # Output directory for fetched JSON files (gitignored)
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

### 1. Fetch papers

Fetches AI papers published in the last 3 days from OpenAlex and saves them to `temp/<timestamp>.json`:

```powershell
python fetch_ai_papers.py
```

### 2. Process and load into the database

Takes a JSON file as input, creates the `papers` table if it does not exist, and inserts the data. Duplicate papers (matched by OpenAlex ID) are automatically skipped.

```powershell
python process_papers.py temp\<timestamp>.json
```

Example:

```powershell
python process_papers.py temp\20260325_150341.json
```

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
