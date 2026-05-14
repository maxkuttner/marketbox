"""Apply pending SQL migrations from ./migrations/ in filename order."""

import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()

_required = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
_missing = [k for k in _required if not os.environ.get(k)]
if _missing:
    sys.exit(f"Missing env vars: {', '.join(_missing)}")

DSN = (
    f"host={os.environ['DB_HOST']} port={os.environ['DB_PORT']} "
    f"dbname={os.environ['DB_NAME']} user={os.environ['DB_USER']} "
    f"password={os.environ['DB_PASSWORD']}"
)

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS _schema_migrations (
    filename   TEXT        PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT now()
);
"""


def main() -> None:
    with psycopg.connect(DSN) as conn:
        conn.execute(_BOOTSTRAP)
        conn.commit()

        applied: set[str] = {
            row[0] for row in conn.execute("SELECT filename FROM _schema_migrations")
        }

        pending = sorted(
            f for f in MIGRATIONS_DIR.glob("*.sql") if f.name not in applied
        )

        if not pending:
            print("migrations: nothing to apply")
            return

        for path in pending:
            print(f"  applying {path.name} ...", end=" ", flush=True)
            conn.execute(path.read_text())
            conn.execute(
                "INSERT INTO _schema_migrations (filename) VALUES (%s)", (path.name,)
            )
            conn.commit()
            print("ok")

        print(f"migrations: applied {len(pending)} file(s)")


if __name__ == "__main__":
    main()
