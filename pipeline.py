"""
End-to-end AI papers pipeline.

Steps
-----
1. fetch  - query the OpenAlex API for recent AI papers (no temp file written)
2. create - ensure the `papers` table and indexes exist
3. load   - bulk-insert fetched papers, duplicates silently skipped
4. test   - run data-quality assertions against the loaded rows

Run the full pipeline:
    python pipeline.py
"""

import re
import sys
from datetime import date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import requests.exceptions
from pyalex import Topics, Works
from pyalex.api import QueryError

from db import get_connection

SQL_DIR = Path(__file__).parent / "sql"


class Pipeline:

    # ── public interface ──────────────────────────────────────────────────

    def fetch(self) -> list[dict]:
        """Query the OpenAlex API and return raw paper dicts (last 3 days)."""
        taxonomy_ids = self._find_ai_taxonomy_ids()
        return self._fetch_recent_papers(taxonomy_ids)

    def create_table(self, conn: psycopg2.extensions.connection) -> None:
        """Create the papers table and indexes if they do not already exist."""
        sql = (SQL_DIR / "create_papers_table.sql").read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(sql)

    def load(
        self,
        conn: psycopg2.extensions.connection,
        papers: list[dict],
    ) -> tuple[int, int]:
        """Map and bulk-insert papers. Returns (inserted, skipped)."""
        rows = [self._map_paper(p) for p in papers]
        insert_sql = (SQL_DIR / "insert_papers.sql").read_text(encoding="utf-8")
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=500)
            inserted = cur.rowcount
        skipped = len(rows) - inserted
        return inserted, skipped

    def test(
        self,
        conn: psycopg2.extensions.connection,
    ) -> list[tuple[str, bool, str]]:
        """Run all data-quality tests. Returns list of (name, passed, detail)."""
        tests = self._load_tests()
        results: list[tuple[str, bool, str]] = []
        with conn.cursor() as cur:
            for name, sql in tests:
                passed, detail = self._run_test(cur, name, sql)
                results.append((name, passed, detail))
        return results

    def run(self) -> None:
        """Fetch → create table → load → test in a single connection."""
        print(f"\n{'=' * 60}")
        print("  AI Papers Pipeline")
        print(f"{'=' * 60}\n")

        # Step 1: Fetch from API
        print("[1/4] Fetching papers from OpenAlex API ...")
        try:
            papers = self.fetch()
        except QueryError as exc:
            print(f"      ERROR: API query rejected — {exc}")
            sys.exit(1)
        except requests.exceptions.RequestException as exc:
            print(f"      ERROR: API request failed — {exc}")
            sys.exit(1)
        except RuntimeError as exc:
            print(f"      ERROR: {exc}")
            sys.exit(1)

        if not papers:
            print("      No papers found. Exiting.")
            return
        print(f"      {len(papers)} papers fetched.\n")

        # Open DB connection
        try:
            conn = get_connection()
        except EnvironmentError as exc:
            print(f"      ERROR: {exc}")
            sys.exit(1)
        except psycopg2.OperationalError as exc:
            print(f"      ERROR: Could not connect to the database — {exc}")
            sys.exit(1)

        try:
            # Step 2: Create table
            print("[2/4] Ensuring database table exists ...")
            try:
                with conn:
                    self.create_table(conn)
            except FileNotFoundError as exc:
                print(f"      ERROR: SQL file not found — {exc}")
                sys.exit(1)
            except psycopg2.Error as exc:
                print(f"      ERROR: Failed to create table — {exc}")
                sys.exit(1)
            print("      Table ready.\n")

            # Step 3: Load papers
            print("[3/4] Loading papers into database ...")
            try:
                with conn:
                    inserted, skipped = self.load(conn, papers)
            except FileNotFoundError as exc:
                print(f"      ERROR: SQL file not found — {exc}")
                sys.exit(1)
            except psycopg2.Error as exc:
                print(f"      ERROR: Failed to load papers — {exc}")
                sys.exit(1)
            print(f"      Inserted : {inserted}")
            print(f"      Skipped  : {skipped} (duplicates)\n")

            # Step 4: Data-quality tests
            print("[4/4] Running data-quality tests ...")
            try:
                with conn:
                    results = self.test(conn)
            except FileNotFoundError as exc:
                print(f"      ERROR: SQL file not found — {exc}")
                sys.exit(1)
            except psycopg2.Error as exc:
                print(f"      ERROR: Failed to run data-quality tests — {exc}")
                sys.exit(1)

            self._print_test_results(results)

            if any(not passed for _, passed, _ in results):
                sys.exit(1)
        finally:
            conn.close()

    # ── private: API fetching ─────────────────────────────────────────────

    def _find_ai_taxonomy_ids(self) -> dict[str, list[str]]:
        results = Topics().search("artificial intelligence").get()
        if not results:
            raise RuntimeError("No topics found for 'artificial intelligence'.")

        ids: dict[str, set[str]] = {"subfield": set(), "field": set()}
        for topic in results:
            for key in ("subfield", "field"):
                node = topic.get(key) or {}
                if node.get("display_name", "").lower() == "artificial intelligence":
                    full_id: str = node.get("id", "")
                    short_id = full_id.rsplit("/", 1)[-1]
                    ids[key].add(short_id)

        for key, found in ids.items():
            if found:
                print(f"      AI {key} IDs: {sorted(found)}")

        return {k: sorted(v) for k, v in ids.items()}

    def _fetch_recent_papers(
        self,
        taxonomy_ids: dict[str, list[str]],
    ) -> list[dict]:
        from_date = (date.today() - timedelta(days=3)).isoformat()
        filters: dict = {"from_publication_date": from_date}

        subfield_ids = taxonomy_ids.get("subfield", [])
        field_ids = taxonomy_ids.get("field", [])

        if subfield_ids:
            filters["topics.subfield.id"] = "|".join(subfield_ids)
        if field_ids:
            filters["topics.field.id"] = "|".join(field_ids)

        if len(filters) == 1:
            raise RuntimeError("No AI subfield or field IDs found to filter on.")

        print(f"      From date : {from_date}")

        papers: list[dict] = []
        pager = Works().filter(**filters).paginate(per_page=200)
        for page_num, page in enumerate(pager, start=1):
            papers.extend(page)
            print(f"      Page {page_num:>3}  -  {len(papers)} papers so far")

        return papers

    # ── private: mapping ──────────────────────────────────────────────────

    @staticmethod
    def _extract_short_id(url: str | None) -> str | None:
        if not url:
            return None
        return url.rstrip("/").rsplit("/", 1)[-1]

    def _map_paper(self, p: dict) -> tuple:
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
                first_author_id = self._extract_short_id(author.get("id"))
                break

        return (
            self._extract_short_id(p.get("id")),
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

    # ── private: data tests ───────────────────────────────────────────────

    @staticmethod
    def _load_tests() -> list[tuple[str, str]]:
        raw = (SQL_DIR / "data_tests.sql").read_text(encoding="utf-8")
        pattern = re.compile(
            r"--\s*(Test \d+:[^\n]+)\n(.*?)(?=\n--\s*Test \d+:|$)",
            re.DOTALL,
        )
        return [(m.group(1).strip(), m.group(2).strip()) for m in pattern.finditer(raw)]

    @staticmethod
    def _run_test(cur, name: str, sql: str) -> tuple[bool, str]:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        if "occurrences" in cols:
            passed = len(rows) == 0
            detail = (
                "no duplicate rows"
                if passed
                else f"{len(rows)} duplicate openalex_id(s) found"
            )
            return passed, detail

        if not rows:
            return False, "query returned no rows (unexpected)"

        row = rows[0]
        failures = {
            cols[i]: val
            for i, val in enumerate(row)
            if val is not None and val != 0
        }
        passed = len(failures) == 0
        detail = "all checks passed" if passed else str(failures)
        return passed, detail

    @staticmethod
    def _print_test_results(results: list[tuple[str, bool, str]]) -> None:
        print()
        print(f"{'Test':<45} {'Result':<8} Detail")
        print("-" * 90)
        for name, passed, detail in results:
            status = "PASS" if passed else "FAIL"
            print(f"{name:<45} {status:<8} {detail}")
        print()
        failures = [name for name, passed, _ in results if not passed]
        if failures:
            print(f"{len(failures)} test(s) failed.")
        else:
            print(f"All {len(results)} tests passed.")


if __name__ == "__main__":
    Pipeline().run()
