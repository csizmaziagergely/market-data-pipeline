"""
Loads AI papers from the most recent temp/<timestamp>.json file into the
`papers` table in Neon.

- Picks the latest file in temp/ automatically.
- Maps the nested OpenAlex JSON to the flat schema.
- Uses a bulk INSERT ... ON CONFLICT (openalex_id) DO NOTHING so re-runs
  are safe and duplicates are silently skipped.

SQL is defined in sql/insert_papers.sql.
"""

import json
from pathlib import Path

import psycopg2.extras

from db import get_connection

TEMP_DIR = Path(__file__).parent / "temp"
SQL_FILE = Path(__file__).parent / "sql" / "insert_papers.sql"


def latest_json_file() -> Path:
    files = sorted(TEMP_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No JSON files found in {TEMP_DIR}")
    return files[-1]


def extract_short_id(url: str | None) -> str | None:
    """Turn 'https://openalex.org/W123' into 'W123'."""
    if not url:
        return None
    return url.rstrip("/").rsplit("/", 1)[-1]


def map_paper(p: dict) -> tuple:
    oa = p.get("open_access") or {}
    loc = p.get("primary_location") or {}
    source = loc.get("source") or {}
    cnp = p.get("citation_normalized_percentile") or {}
    pt = p.get("primary_topic") or {}
    subfield = pt.get("subfield") or {}
    field = pt.get("field") or {}

    first_author_name = None
    first_author_id = None
    authorships = p.get("authorships") or []
    for a in authorships:
        if a.get("author_position") == "first":
            author = a.get("author") or {}
            first_author_name = author.get("display_name")
            first_author_id = extract_short_id(author.get("id"))
            break

    return (
        extract_short_id(p.get("id")),                          # openalex_id
        ((p.get("doi") or "").replace("https://doi.org/", "")) or None,  # doi
        p.get("title") or p.get("display_name"),                # title
        p.get("publication_date"),                              # publication_date
        p.get("publication_year"),                              # publication_year
        p.get("language"),                                      # language
        p.get("type"),                                          # type
        source.get("display_name"),                             # journal_name
        source.get("issn_l"),                                   # journal_issn
        oa.get("is_oa"),                                        # is_oa
        oa.get("oa_status"),                                    # oa_status
        p.get("cited_by_count"),                                # cited_by_count
        p.get("fwci"),                                          # fwci
        cnp.get("value"),                                       # citation_percentile
        cnp.get("is_in_top_1_percent"),                         # is_in_top_1_percent
        cnp.get("is_in_top_10_percent"),                        # is_in_top_10_percent
        p.get("referenced_works_count"),                        # referenced_works_count
        len(authorships),                                       # author_count
        p.get("countries_distinct_count"),                      # countries_distinct_count
        p.get("institutions_distinct_count"),                   # institutions_distinct_count
        pt.get("display_name"),                                 # primary_topic
        subfield.get("display_name"),                           # primary_subfield
        field.get("display_name"),                              # primary_field
        first_author_name,                                      # first_author_name
        first_author_id,                                        # first_author_id
        p.get("is_retracted"),                                  # is_retracted
    )


def main() -> None:
    json_path = latest_json_file()
    print(f"[load] reading {json_path.name} ...")

    with open(json_path, encoding="utf-8") as fh:
        data = json.load(fh)

    papers = data.get("papers", [])
    print(f"[load] {len(papers)} papers found in file")

    rows = [map_paper(p) for p in papers]
    insert_sql = SQL_FILE.read_text(encoding="utf-8")

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=500)
                print(f"[load] insert complete — {cur.rowcount} new rows added "
                      f"({len(rows) - cur.rowcount} duplicates skipped)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
