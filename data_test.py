"""
Standalone runner for data-quality tests against the `papers` table.

Delegates all logic to Pipeline. Exit code is 1 if any test fails.
"""

import sys

from db import get_connection
from pipeline import Pipeline


def main() -> None:
    conn = get_connection()
    pipeline = Pipeline()
    try:
        results = pipeline.test(conn)
    finally:
        conn.close()

    pipeline._print_test_results(results)

    if any(not passed for _, passed, _ in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
