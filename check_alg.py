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
    cur.execute("""
        select client, count(*) c
        from public.karbon_timesheets_typed
        where client ilike %s
        group by client
        order by c desc
        limit 25
    """, ("%ALG%",))
    for row in cur.fetchall():
        print(row)
