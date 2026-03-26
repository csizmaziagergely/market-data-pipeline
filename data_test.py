"""
Runs data-quality tests against the `papers` table.

Each test executes one of the named SQL statements from sql/data_tests.sql
and asserts the expected result. A summary is printed at the end; the process
exits with code 1 if any test fails.
"""

import re
import sys
from pathlib import Path

from db import get_connection

SQL_FILE = Path(__file__).parent / "sql" / "data_tests.sql"


def _load_tests(path: Path) -> list[tuple[str, str]]:
    """
    Parse sql/data_tests.sql into (name, sql) pairs.

    Each block starts with a leading comment `-- Test N: <name>` and ends
    just before the next such comment (or EOF).
    """
    raw = path.read_text(encoding="utf-8")
    pattern = re.compile(r"--\s*(Test \d+:[^\n]+)\n(.*?)(?=\n--\s*Test \d+:|$)", re.DOTALL)
    return [(m.group(1).strip(), m.group(2).strip()) for m in pattern.finditer(raw)]


def _run_test(cur, name: str, sql: str) -> tuple[bool, str]:
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    # Test 2 (duplicates): must return no rows
    if "occurrences" in cols:
        passed = len(rows) == 0
        detail = "no duplicate rows" if passed else f"{len(rows)} duplicate openalex_id(s) found"
        return passed, detail

    # All other tests return a single summary row; every value must be 0
    row = rows[0]
    failures = {cols[i]: val for i, val in enumerate(row) if val is not None and val != 0}
    passed = len(failures) == 0
    detail = "all checks passed" if passed else str(failures)
    return passed, detail


def main() -> None:
    tests = _load_tests(SQL_FILE)
    if not tests:
        print("No tests found in SQL file.")
        sys.exit(1)

    conn = get_connection()
    results: list[tuple[str, bool, str]] = []

    try:
        with conn.cursor() as cur:
            for name, sql in tests:
                passed, detail = _run_test(cur, name, sql)
                results.append((name, passed, detail))
    finally:
        conn.close()

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
        sys.exit(1)
    else:
        print(f"All {len(results)} tests passed.")


if __name__ == "__main__":
    main()
