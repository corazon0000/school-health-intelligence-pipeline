import os
import json
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
import duckdb

PG = {
    "host":     "192.168.50.131",
    "port":     5432,
    "db":       "kp",
    "user":     "postgres",
    "password": "postgres",
}

LAKE_DIR     = "lake"
DATA_DIR     = os.path.join(LAKE_DIR, "data")
CATALOG_PATH = os.path.join(LAKE_DIR, "metadata.ducklake")
OUT_DIR      = "out"
RUN_HISTORY  = os.path.join(OUT_DIR, "run_history.csv")

QUERIES = {
    "kp_mstr_school": """
        SELECT
            id, degree_id, name, address, phone_number, email,
            has_package, school_code, school_type,
            active_date, expired_date, active_curriculumn,
            npsn, onboard, is_ambasador, is_delete
        FROM public.kp_mstr_school
        ORDER BY active_date ASC
    """,

    "kp_user_counts": """
        SELECT
            school_id,
            user_type_id,
            COUNT(*)                                            AS user_count,
            SUM(CASE WHEN active = true  THEN 1 ELSE 0 END)    AS active_count,
            SUM(CASE WHEN dummy  = true  THEN 1 ELSE 0 END)    AS dummy_count,
            MAX(last_active)                                    AS last_active_max
        FROM public.kp_user
        WHERE school_id IS NOT NULL AND school_id > 0
        GROUP BY school_id, user_type_id
    """,

    "kp_user_last_active_agg": """
        SELECT
            u.school_id,
            COUNT(DISTINCT la.user_id)  AS users_tracked,
            MAX(la.last_active)         AS school_last_active,
            MIN(la.last_active)         AS school_first_active
        FROM public.kp_user_last_active la
        JOIN public.kp_user u ON la.user_id = u.id
        WHERE u.school_id IS NOT NULL AND u.school_id > 0
          AND u.dummy = false
        GROUP BY u.school_id
    """,

    "kp_user_study_duration_agg": """
        SELECT
            u.school_id,
            COUNT(DISTINCT s.user_id)   AS users_with_study,
            SUM(s.duration)             AS total_duration_seconds,
            AVG(s.duration)             AS avg_duration_seconds,
            MAX(s.created_at)           AS last_study_date
        FROM public.kp_user_study_duration s
        JOIN public.kp_user u ON s.user_id = u.id
        WHERE u.school_id IS NOT NULL AND u.school_id > 0
          AND u.dummy = false
        GROUP BY u.school_id
    """,
}

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUT_DIR,  exist_ok=True)

def get_engine():
    url = (
        f"postgresql+psycopg2://{PG['user']}:{PG['password']}"
        f"@{PG['host']}:{PG['port']}/{PG['db']}"
    )
    return create_engine(url, pool_pre_ping=True)

def append_run_history(row: dict):
    df = pd.DataFrame([row])
    write_header = not os.path.exists(RUN_HISTORY)
    df.to_csv(RUN_HISTORY, mode="a", header=write_header, index=False, encoding="utf-8")

def write_parquet(name: str, df: pd.DataFrame) -> str:
    table_dir = os.path.join(DATA_DIR, name)
    os.makedirs(table_dir, exist_ok=True)
    path = os.path.join(table_dir, "raw.parquet")
    df.to_parquet(path, index=False)
    return path

def setup_ducklake() -> duckdb.DuckDBPyConnection:
    os.makedirs(LAKE_DIR, exist_ok=True)
    con = duckdb.connect(CATALOG_PATH)
    try:
        con.execute("INSTALL ducklake;")
        con.execute("LOAD ducklake;")
        print("  [ducklake] extension loaded [OK]")
    except Exception:
        print("  [ducklake] extension not available - using standard DuckDB VIEW")
    return con

def register_view(con, name: str):
    path = os.path.join(DATA_DIR, name, "raw.parquet").replace("\\", "/")
    if not os.path.exists(path):
        print(f"  [catalog] SKIP {name} - file missing")
        return
    con.execute(
        f'CREATE OR REPLACE VIEW "{name}" AS SELECT * FROM read_parquet(\'{path}\')'
    )
    print(f"  [catalog] view '{name}' registered [OK]")

def main():
    ensure_dirs()

    print("\n" + "=" * 50)
    print("  Script 1 - Extract to DuckLake")
    print("=" * 50)

    print("\n[1] Connecting to Postgres...")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(f"    OK - {PG['host']}:{PG['port']}/{PG['db']}\n")

    print("[2] Setup DuckLake catalog...")
    con = setup_ducklake()
    print()

    run_time = datetime.now(tz=timezone.utc).isoformat()
    print(f"[3] Extracting - {run_time}\n")

    for name, sql in QUERIES.items():
        print(f"  -- {name}")
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)

            path = write_parquet(name, df)
            register_view(con, name)

            print(f"     rows    : {len(df):,}")
            print(f"     saved   : {path}\n")

            append_run_history({
                "run_time": run_time,
                "table":    name,
                "rows":     len(df),
                "status":   "ok",
                "error":    "",
            })

        except Exception as e:
            print(f"     ERROR: {e}\n")
            append_run_history({
                "run_time": run_time,
                "table":    name,
                "rows":     0,
                "status":   "error",
                "error":    str(e),
            })

    con.close()

    print("=" * 50)
    print("  COMPLETED - Raw data stored in lake/")
    print("=" * 50 + "\n")

if __name__ == "__main__":
    main()