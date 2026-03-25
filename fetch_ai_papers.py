"""
Fetches recent AI research papers from the OpenAlex API and saves them to a
timestamped JSON file in the temp/ directory.

Steps:
  1. Search OpenAlex Topics for "artificial intelligence".
  2. Extract unique subfield/field IDs whose display_name is "Artificial Intelligence".
  3. Filter Works from the last three days directly on the API using those IDs.
  4. Paginate through all results and save every field to temp/<timestamp>.json.
"""

import json
import os
from datetime import date, timedelta, datetime

from pyalex import Topics, Works


TEMP_DIR = os.path.join(os.path.dirname(__file__), "temp")


def find_ai_taxonomy_ids() -> dict[str, list[str]]:
    """
    Search Topics for 'artificial intelligence' and collect the unique IDs of
    every subfield and field whose display_name is 'Artificial Intelligence'.

    Returns a dict with keys 'subfield' and 'field', each holding a list of
    short numeric IDs (e.g. ['1702']) ready to use in a Works filter.
    """
    results = Topics().search("artificial intelligence").get()
    if not results:
        raise RuntimeError("No topics found for 'artificial intelligence'.")

    ids: dict[str, set[str]] = {"subfield": set(), "field": set()}

    for topic in results:
        for key in ("subfield", "field"):
            node = topic.get(key) or {}
            if node.get("display_name", "").lower() == "artificial intelligence":
                # full URL: https://openalex.org/subfields/1702  ->  short id: 1702
                full_id: str = node.get("id", "")
                short_id = full_id.rsplit("/", 1)[-1]
                ids[key].add(short_id)

    for key, found in ids.items():
        if found:
            print(f"[taxonomy] AI {key} IDs: {sorted(found)}")

    return {k: sorted(v) for k, v in ids.items()}


def fetch_recent_papers(taxonomy_ids: dict[str, list[str]], days: int = 3) -> list[dict]:
    """Fetch all works published in the last `days` days filtered on the API
    by topics.subfield.id and/or topics.field.id."""
    from_date = (date.today() - timedelta(days=days)).isoformat()

    filters: dict = {"from_publication_date": from_date}

    subfield_ids = taxonomy_ids.get("subfield", [])
    field_ids = taxonomy_ids.get("field", [])

    if subfield_ids:
        filters["topics.subfield.id"] = "|".join(subfield_ids)
    if field_ids:
        filters["topics.field.id"] = "|".join(field_ids)

    if len(filters) == 1:
        raise RuntimeError("No AI subfield or field IDs found to filter on.")

    print(f"[fetch] works from {from_date} with API filters: {filters}")

    papers: list[dict] = []
    pager = Works().filter(**filters).paginate(per_page=200)

    for page_num, page in enumerate(pager, start=1):
        papers.extend(page)
        print(f"  page {page_num:>3}  -  {len(papers)} papers so far")

    return papers


def save_results(papers: list[dict], taxonomy_ids: dict[str, list[str]]) -> str:
    """Persist results to temp/<timestamp>.json and return the file path."""
    os.makedirs(TEMP_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(TEMP_DIR, f"{timestamp}.json")

    payload = {
        "generated_at": datetime.now().isoformat(),
        "taxonomy_ids": taxonomy_ids,
        "paper_count": len(papers),
        "papers": papers,
    }

    with open(filename, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    return filename


def main() -> None:
    taxonomy_ids = find_ai_taxonomy_ids()
    papers = fetch_recent_papers(taxonomy_ids)

    if not papers:
        print("[result] No papers found for the selected date range.")
        return

    output_path = save_results(papers, taxonomy_ids)
    print(f"[done] {len(papers)} papers saved to {output_path}")


if __name__ == "__main__":
    main()
