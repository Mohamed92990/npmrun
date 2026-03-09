from collections import Counter
from datetime import datetime

import psycopg2
from dotenv import dotenv_values

ENV = r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env.supabase"
VIEW = "public.karbon_timesheets_typed"


def connect():
    cfg = dotenv_values(ENV)
    return psycopg2.connect(
        host=cfg["PGHOST"],
        port=int(cfg.get("PGPORT", "5432")),
        dbname=cfg.get("PGDATABASE", "postgres"),
        user=cfg["PGUSER"],
        password=cfg["PGPASSWORD"],
        sslmode=cfg.get("PGSSLMODE", "require"),
    )


def top_distinct(cur, col, n=20):
    cur.execute(f"select {col}, count(*) c from {VIEW} where {col} is not null group by {col} order by c desc limit %s", (n,))
    return cur.fetchall()


def main():
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"select count(*) from {VIEW}")
        total = cur.fetchone()[0]
        cur.execute(f"select min(date_ts), max(date_ts) from {VIEW}")
        mn, mx = cur.fetchone()
        print("rows", total)
        print("date range", mn, "->", mx)

        print("\nTop clients:")
        for v,c in top_distinct(cur, "client", 25):
            print(f"- {v}: {c}")

        print("\nTop team members:")
        for v,c in top_distinct(cur, "team_member", 25):
            print(f"- {v}: {c}")

        print("\nTop roles:")
        for v,c in top_distinct(cur, "role", 25):
            print(f"- {v}: {c}")

        print("\nTop task types:")
        for v,c in top_distinct(cur, "task_type", 25):
            print(f"- {v}: {c}")

        print("\nTop works:")
        for v,c in top_distinct(cur, "work", 25):
            print(f"- {v}: {c}")


if __name__ == "__main__":
    main()
