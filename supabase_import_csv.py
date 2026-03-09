import csv
import os
import re
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

ENV_PATH = Path(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env.supabase")
CSV_PATH = Path(r"C:\Users\sayyi\Downloads\Real Karbon Timesheets.csv")


def die(msg: str, code: int = 2):
    print(msg)
    raise SystemExit(code)


def safe_ident(name: str) -> str:
    if not name:
        die("Empty table name")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        die(f"Unsafe table name: {name}. Use letters/numbers/underscore only.")
    return name


def main():
    if not ENV_PATH.exists():
        die(f"Missing env file: {ENV_PATH}")
    if not CSV_PATH.exists():
        die(f"Missing CSV file: {CSV_PATH}")

    cfg = dotenv_values(str(ENV_PATH))
    db_url = (cfg.get("DATABASE_URL") or "").strip()
    table = safe_ident((cfg.get("TARGET_TABLE") or "").strip() or "karbon_timesheets_raw")

    # Prefer split PG* fields if present (avoids URL-encoding issues)
    pghost = (cfg.get("PGHOST") or "").strip()
    pgport = (cfg.get("PGPORT") or "").strip() or "5432"
    pgdb = (cfg.get("PGDATABASE") or "").strip() or "postgres"
    pguser = (cfg.get("PGUSER") or "").strip()
    pgpass = (cfg.get("PGPASSWORD") or "").strip()
    pgssl = (cfg.get("PGSSLMODE") or "").strip() or "require"

    conn_kwargs = None
    if pghost and pguser and pgpass:
        conn_kwargs = {
            "host": pghost,
            "port": int(pgport),
            "dbname": pgdb,
            "user": pguser,
            "password": pgpass,
            "sslmode": pgssl,
        }

    print("Connecting to Supabase Postgres…")
    try:
        if conn_kwargs:
            conn = psycopg2.connect(**conn_kwargs)
        else:
            if not db_url:
                die("DATABASE_URL not set in .env.supabase (and PGHOST/PGUSER/PGPASSWORD not set)")
            conn = psycopg2.connect(db_url)
    except Exception as e:
        die(
            "Failed to connect to Supabase Postgres. If your password contains special characters, use PGHOST/PGUSER/PGPASSWORD fields in .env.supabase.\n"
            f"Error: {e}"
        )

    conn.autocommit = False

    with conn, conn.cursor() as cur:
        # Ensure schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS public")

        # Read header
        # CSV sometimes comes as Windows-1252 / latin-1; use a tolerant decode.
        with CSV_PATH.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            header = next(reader)

        # Sanitize column names
        cols = []
        seen = set()
        for h in header:
            h0 = (h or "").strip()
            if not h0:
                h0 = "col"
            h1 = re.sub(r"[^A-Za-z0-9_]+", "_", h0)
            if re.match(r"^[0-9]", h1):
                h1 = "c_" + h1
            if not h1:
                h1 = "col"
            base = h1
            i = 2
            while h1.lower() in seen:
                h1 = f"{base}_{i}"
                i += 1
            seen.add(h1.lower())
            cols.append(h1)

        # Create table: store everything as text initially (safe raw landing)
        col_sql = ",\n  ".join([f'"{c}" text' for c in cols])
        cur.execute(f'CREATE TABLE IF NOT EXISTS public."{table}" (\n  {col_sql}\n)')

        # Truncate (optional) — comment this out if you want append-only
        cur.execute(f'TRUNCATE TABLE public."{table}"')

        print(f"Importing CSV into public.{table} …")

        # Use COPY for speed
        with CSV_PATH.open("r", encoding="utf-8", errors="replace", newline="") as f:
            cur.copy_expert(
                sql=f'COPY public."{table}" ("' + '","'.join(cols) + '") FROM STDIN WITH (FORMAT csv, HEADER true)',
                file=f,
            )

        # Count
        cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
        n = cur.fetchone()[0]
        conn.commit()

    print(f"Done. Rows loaded: {n}")


if __name__ == "__main__":
    main()
