"""
Ingest AI papers from a JSON file into the Neon `papers` table.

Usage:
    python process_papers.py <path_to_json>

Steps performed:
    1. Validate the JSON file path.
    2. Ensure the `papers` table and indexes exist (CREATE IF NOT EXISTS).
    3. Load and map papers from the JSON file.
    4. Bulk-insert rows, skipping any duplicates (ON CONFLICT DO NOTHING).
"""

import argparse
import json
from pathlib import Path

import psycopg2.extras

from db import get_connection

SQL_DIR = Path(__file__).parent / "sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest AI papers from a JSON file into the Neon papers table."
    )
    parser.add_argument(
        "json_file",
        type=Path,
        help="Path to the OpenAlex JSON file produced by fetch_ai_papers.py",
    )
    return parser.parse_args()


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
        extract_short_id(p.get("id")),
        ((p.get("doi") or "").replace("https://doi.org/", "")) or None,
        p.get("title") or p.get("display_name"),
        p.get("publication_date"),
        p.get("publication_year"),
        p.get("language"),
        p.get("type"),
        source.get("display_name"),
        source.get("issn_l"),
        oa.get("is_oa"),
        oa.get("oa_status"),
        p.get("cited_by_count"),
        p.get("fwci"),
        cnp.get("value"),
        cnp.get("is_in_top_1_percent"),
        cnp.get("is_in_top_10_percent"),
        p.get("referenced_works_count"),
        len(authorships),
        p.get("countries_distinct_count"),
        p.get("institutions_distinct_count"),
        pt.get("display_name"),
        subfield.get("display_name"),
        field.get("display_name"),
        first_author_name,
        first_author_id,
        p.get("is_retracted"),
    )


def main() -> None:
    args = parse_args()
    json_path: Path = args.json_file.resolve()

    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")
    if json_path.suffix.lower() != ".json":
        raise ValueError(f"Expected a .json file, got: {json_path.name}")

    print(f"[ingest] file     : {json_path.name}")

    with open(json_path, encoding="utf-8") as fh:
        data = json.load(fh)

    papers = data.get("papers", [])
    print(f"[ingest] papers   : {len(papers)}")

    rows = [map_paper(p) for p in papers]

    create_sql = (SQL_DIR / "create_papers_table.sql").read_text(encoding="utf-8")
    insert_sql = (SQL_DIR / "insert_papers.sql").read_text(encoding="utf-8")

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                print("[ingest] table    : ready")

                psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=500)
                inserted = cur.rowcount
                skipped = len(rows) - inserted
                print(f"[ingest] inserted : {inserted}")
                print(f"[ingest] skipped  : {skipped} (duplicates)")
    finally:
        conn.close()

    print("[ingest] done")


if __name__ == "__main__":
    main()
