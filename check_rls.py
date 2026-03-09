import psycopg2
from dotenv import dotenv_values

cfg = dotenv_values(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env.supabase")

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
        select n.nspname as schema,
               c.relname as table,
               c.relrowsecurity as rls_enabled,
               c.relforcerowsecurity as rls_forced
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
          and c.relname = 'karbon_timesheets_raw'
        """
    )
    print(cur.fetchall())
