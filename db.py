"""
Database connection helper.

Loads DB_PASSWORD from the .env file and builds a psycopg2 connection
to the Neon PostgreSQL instance.
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

_HOST = "ep-lucky-grass-ala15qxa-pooler.c-3.eu-central-1.aws.neon.tech"
_USER = "neondb_owner"
_DBNAME = "neondb"


def get_connection() -> psycopg2.extensions.connection:
    """Return a new psycopg2 connection using credentials from the environment."""
    password = os.environ.get("DB_PASSWORD")
    if not password:
        raise EnvironmentError("DB_PASSWORD is not set. Check your .env file.")

    return psycopg2.connect(
        host=_HOST,
        user=_USER,
        password=password,
        dbname=_DBNAME,
        sslmode="require",
    )


if __name__ == "__main__":
    conn = get_connection()
    print("Connection successful.")
    conn.close()
