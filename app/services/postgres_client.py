from __future__ import annotations

import os
from dataclasses import dataclass

import psycopg2
from dotenv import dotenv_values


@dataclass
class PgConnInfo:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str = "require"


def load_pg_conn_info(env_path: str) -> PgConnInfo:
    cfg = {
        **dotenv_values(env_path),
        **os.environ,
    }
    host = (cfg.get("PGHOST") or "").strip()
    user = (cfg.get("PGUSER") or "").strip()
    password = (cfg.get("PGPASSWORD") or "").strip()

    if not (host and user and password):
        raise RuntimeError("Missing PGHOST/PGUSER/PGPASSWORD in environment variables or .env.supabase")

    return PgConnInfo(
        host=host,
        port=int((cfg.get("PGPORT") or "5432").strip()),
        dbname=(cfg.get("PGDATABASE") or "postgres").strip(),
        user=user,
        password=password,
        sslmode=(cfg.get("PGSSLMODE") or "require").strip(),
    )


class PostgresClient:
    def __init__(self, conn: PgConnInfo):
        self.conninfo = conn

    def connect(self):
        return psycopg2.connect(
            host=self.conninfo.host,
            port=self.conninfo.port,
            dbname=self.conninfo.dbname,
            user=self.conninfo.user,
            password=self.conninfo.password,
            sslmode=self.conninfo.sslmode,
        )
