from pathlib import Path

import psycopg2
from dotenv import dotenv_values

ENV_PATH = Path(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env.supabase")
SQL_PATH = Path(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\supabase_create_view.sql")


def main():
    cfg = dotenv_values(str(ENV_PATH))
    conn = psycopg2.connect(
        host=cfg["PGHOST"],
        port=int(cfg.get("PGPORT", "5432")),
        dbname=cfg.get("PGDATABASE", "postgres"),
        user=cfg["PGUSER"],
        password=cfg["PGPASSWORD"],
        sslmode=cfg.get("PGSSLMODE", "require"),
    )

    sql = SQL_PATH.read_text(encoding="utf-8")
    with conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

    print("Applied SQL:", SQL_PATH)


if __name__ == "__main__":
    main()
