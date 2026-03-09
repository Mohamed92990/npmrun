from dotenv import dotenv_values
import psycopg2

cfg = dotenv_values(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env.supabase")

def main():
    table = cfg.get("TARGET_TABLE", "karbon_timesheets_raw")
    conn = psycopg2.connect(
        host=cfg["PGHOST"],
        port=int(cfg.get("PGPORT", "5432")),
        dbname=cfg.get("PGDATABASE", "postgres"),
        user=cfg["PGUSER"],
        password=cfg["PGPASSWORD"],
        sslmode=cfg.get("PGSSLMODE", "require"),
    )

    with conn, conn.cursor() as cur:
        cur.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_schema='public' and table_name=%s
            order by ordinal_position
            """,
            (table,),
        )
        cols = cur.fetchall()
        print("table", table)
        print("cols", len(cols))
        for c, t in cols:
            print(f"- {c}: {t}")

        cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
        print("rows", cur.fetchone()[0])

        cur.execute(f'SELECT * FROM public."{table}" LIMIT 1')
        row = cur.fetchone()
        print("sample columns", len(row))


if __name__ == "__main__":
    main()
