"""
Creates the `papers` table (and supporting indexes) in the Neon database.
Safe to run multiple times — the SQL uses CREATE TABLE IF NOT EXISTS and
CREATE INDEX IF NOT EXISTS throughout.

SQL is defined in sql/create_papers_table.sql.
"""

from pathlib import Path

from db import get_connection

SQL_FILE = Path(__file__).parent / "sql" / "create_papers_table.sql"


def main() -> None:
    sql = SQL_FILE.read_text(encoding="utf-8")

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        print(f"Executed {SQL_FILE.name} successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
