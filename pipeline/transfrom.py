import os
import json
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import duckdb

CATALOG_PATH    = os.path.join("lake", "metadata.ducklake")
OUT_DIR         = "out"
MART_CSV        = os.path.join(OUT_DIR, "mart_school_health.csv")
ALERTS_CSV      = os.path.join(OUT_DIR, "alerts.csv")
DIGEST_JSON     = os.path.join(OUT_DIR, "alerts_digest.json")
REPORT_JSON     = os.path.join(OUT_DIR, "transform_report.json")

NOW = pd.Timestamp.now(tz="UTC")

DUMMY_KEYWORDS = [
    "testing", "dummy", "uji coba", "trial", "percobaan",
    "akun testing", "akun trial", "sekolah testing",
    "kelas pintar dummy", "test school",
]

def load_raw(con) -> dict:
    print("[1] Loading raw data from DuckLake...")
    tables = {}
    for name in ["kp_mstr_school", "kp_user_counts",
                 "kp_user_last_active_agg", "kp_user_study_duration_agg"]:
        df = con.sql(f'SELECT * FROM "{name}"').df()
        tables[name] = df
        print(f"    {name}: {len(df):,} rows")
    print()
    return tables

def clean_school(df: pd.DataFrame) -> tuple:
    report = {"original": len(df), "dropped": {}}
    df = df.copy()

    df = df[df["is_delete"] == False].copy()

    df["active_date"]  = pd.to_datetime(df["active_date"],  errors="coerce", utc=True)
    df["expired_date"] = pd.to_datetime(df["expired_date"], errors="coerce", utc=True)

    mask_dummy = df["name"].str.lower().str.strip().apply(
        lambda n: any(kw in n for kw in DUMMY_KEYWORDS)
    )
    report["dropped"]["nama_dummy"] = int(mask_dummy.sum())
    df = df[~mask_dummy].copy()

    mask_terbalik = (
        df["expired_date"].notna() & df["active_date"].notna() &
        (df["expired_date"] < df["active_date"])
    )
    report["dropped"]["kontrak_terbalik"] = int(mask_terbalik.sum())
    df = df[~mask_terbalik].copy()

    contract_days = (df["expired_date"] - df["active_date"]).dt.days
    mask_zero = contract_days <= 0
    report["dropped"]["durasi_zero"] = int(mask_zero.sum())
    df = df[~mask_zero].copy()

    df["npsn_duplicate_flag"] = (
        df["npsn"].notna() & df.duplicated(subset=["npsn"], keep=False)
    )

    report["clean"] = len(df)
    return df, report

def clean_user_counts(df: pd.DataFrame) -> tuple:
    report = {"original": len(df), "dropped": {}, "fixed": {}}
    df = df.copy()

    mask_zero = df["user_count"] == 0
    report["dropped"]["user_count_zero"] = int(mask_zero.sum())
    df = df[~mask_zero].copy()

    mask_over = df["dummy_count"] > df["user_count"]
    report["fixed"]["dummy_clamped"] = int(mask_over.sum())
    df.loc[mask_over, "dummy_count"] = df.loc[mask_over, "user_count"]

    df["real_users"] = (df["active_count"] - df["dummy_count"]).clip(lower=0)

    report["clean"] = len(df)
    return df, report

def clean_last_active(df: pd.DataFrame) -> tuple:
    report = {"original": len(df), "dropped": {}}
    df = df.copy()

    df["school_last_active"]  = pd.to_datetime(df["school_last_active"],  errors="coerce", utc=True)
    df["school_first_active"] = pd.to_datetime(df["school_first_active"], errors="coerce", utc=True)

    mask_future = df["school_last_active"].notna() & (df["school_last_active"] > NOW)
    report["dropped"]["future_date"] = int(mask_future.sum())
    df = df[~mask_future].copy()

    report["clean"] = len(df)
    return df, report

def clean_study(df: pd.DataFrame) -> tuple:
    report = {"original": len(df), "dropped": {}}
    df = df.copy()

    mask_zero = df["total_duration_seconds"] <= 0
    report["dropped"]["duration_zero"] = int(mask_zero.sum())
    df = df[~mask_zero].copy()

    df["last_study_date"] = pd.to_datetime(df["last_study_date"], errors="coerce", utc=True)
    df["study_hours"]     = (df["total_duration_seconds"] / 3600).round(4)

    report["clean"] = len(df)
    return df, report

def build_user_summary(df_users: pd.DataFrame) -> pd.DataFrame:
    pivot = df_users.groupby("school_id").apply(lambda g: pd.Series({
        "total_users":      g["user_count"].sum(),
        "total_real_users": g["real_users"].sum(),
        "total_dummy":      g["dummy_count"].sum(),
        "dummy_pct":        round(100 * g["dummy_count"].sum() / g["user_count"].sum(), 1)
                            if g["user_count"].sum() > 0 else 0,
        "real_siswa":       g.loc[g["user_type_id"] == 1, "real_users"].sum(),
        "total_siswa":      g.loc[g["user_type_id"] == 1, "user_count"].sum(),
        "real_guru":        g.loc[g["user_type_id"].isin([4, 5]), "real_users"].sum(),
        "total_guru":       g.loc[g["user_type_id"].isin([4, 5]), "user_count"].sum(),
    }), include_groups=False).reset_index()

    return pivot

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["contract_status"] = np.where(df["expired_date"] >= NOW, "aktif", "expired")
    df["days_to_expire"] = (df["expired_date"] - NOW).dt.days
    df["days_since_last_active"] = (NOW - df["school_last_active"]).dt.days
    df["days_since_last_study"] = (NOW - df["last_study_date"]).dt.days
    df["customer_age_days"] = (NOW - df["active_date"]).dt.days
    df["acquisition_year"]  = df["active_date"].dt.year
    df["acquisition_month"] = df["active_date"].dt.tz_localize(None).dt.to_period("M").astype(str)

    degree_map = {1: "SD", 2: "SMP", 3: "SMA/SMK", 4: "Lainnya"}
    df["degree"] = df["degree_id"].map(degree_map).fillna("Lainnya")
    df["study_hours"] = df["study_hours"].fillna(0)
    df["users_ever_login"] = df["users_tracked"].fillna(0).astype(int)

    def engagement_tier(row):
        if row["study_hours"] > 0:
            return "High"
        elif (row["users_ever_login"] > 0
              and pd.notna(row["days_since_last_active"])
              and row["days_since_last_active"] <= 90):
            return "Medium"
        elif row["users_ever_login"] > 0:
            return "Low"
        else:
            return "Ghost"

    df["engagement_tier"] = df.apply(engagement_tier, axis=1)

    df["is_ghost_school"] = (
        (df["contract_status"] == "aktif") &
        (df["users_ever_login"] == 0) &
        (df["study_hours"] == 0)
    )

    def churn_risk(row):
        if row["contract_status"] == "expired" and row["has_package"]:
            return "Critical"
        elif (row["contract_status"] == "aktif"
              and row["days_to_expire"] <= 30
              and row["engagement_tier"] in ("Ghost", "Low")):
            return "High"
        elif (row["contract_status"] == "aktif"
              and (row["days_to_expire"] <= 90 or row["engagement_tier"] == "Ghost")):
            return "Medium"
        else:
            return "Low"

    df["churn_risk"] = df.apply(churn_risk, axis=1)

    def school_size(real_siswa):
        if real_siswa >= 500:   return "Large (500+)"
        elif real_siswa >= 100: return "Medium (100-499)"
        elif real_siswa >= 10:  return "Small (10-99)"
        elif real_siswa > 0:    return "Micro (<10)"
        else:                   return "No Students"

    df["school_size"] = df["users_ever_login"].apply(school_size)

    df["renewal_risk_flag"] = (
        (df["contract_status"] == "aktif") &
        (df["days_to_expire"] <= 90)
    )

    df["mart_updated_at"] = NOW.isoformat()

    return df

def generate_alerts(df: pd.DataFrame) -> pd.DataFrame:
    alerts = []
    COLS = ["school_id", "school_name", "degree", "school_type",
            "days_to_expire", "engagement_tier", "total_real_users"]

    a1 = df[(df["contract_status"] == "aktif") & (df["days_to_expire"] <= 30)][COLS].copy()
    a1["alert_type"]    = "RENEWAL_URGENT"
    a1["severity"]      = "HIGH"
    a1["alert_message"] = a1["days_to_expire"].apply(lambda d: f"Kontrak habis dalam {d} hari")
    alerts.append(a1)

    a2 = df[(df["contract_status"] == "aktif") & (df["days_to_expire"] > 30) & (df["days_to_expire"] <= 90)][COLS].copy()
    a2["alert_type"]    = "RENEWAL_WARNING"
    a2["severity"]      = "MEDIUM"
    a2["alert_message"] = a2["days_to_expire"].apply(lambda d: f"Kontrak habis dalam {d} hari")
    alerts.append(a2)

    a3 = df[df["is_ghost_school"] == True][COLS].copy()
    a3["alert_type"]    = "GHOST_SCHOOL"
    a3["severity"]      = "MEDIUM"
    a3["alert_message"] = "Sekolah aktif tapi tidak ada aktivitas siswa"
    alerts.append(a3)

    a4 = df[df["churn_risk"] == "Critical"][COLS].copy()
    a4["alert_type"]    = "CHURN_CRITICAL"
    a4["severity"]      = "CRITICAL"
    a4["alert_message"] = "Sekolah expired — pernah punya package, belum perpanjang"
    alerts.append(a4)

    if not alerts:
        return pd.DataFrame()

    result = pd.concat(alerts, ignore_index=True)
    result["generated_at"] = NOW.isoformat()
    return result

def build_digest(alerts: pd.DataFrame, mart: pd.DataFrame) -> dict:
    aktif   = (mart["contract_status"] == "aktif").sum()
    expired = (mart["contract_status"] == "expired").sum()
    ghost   = mart["is_ghost_school"].sum()

    digest = {
        "generated_at": NOW.isoformat(),
        "summary": {
            "total_schools":   len(mart),
            "aktif":           int(aktif),
            "expired":         int(expired),
            "ghost_schools":   int(ghost),
            "total_real_users": int(mart["total_real_users"].fillna(0).sum()),
            "users_ever_login": int(mart["users_ever_login"].sum()),
        },
        "alerts_count": {
            "CRITICAL": int((alerts["severity"] == "CRITICAL").sum()) if len(alerts) else 0,
            "HIGH":     int((alerts["severity"] == "HIGH").sum())     if len(alerts) else 0,
            "MEDIUM":   int((alerts["severity"] == "MEDIUM").sum())   if len(alerts) else 0,
        },
        "renewal_pipeline": {
            "expire_30_days": int((mart["days_to_expire"].between(0, 30)).sum()),
            "expire_60_days": int((mart["days_to_expire"].between(0, 60)).sum()),
            "expire_90_days": int((mart["days_to_expire"].between(0, 90)).sum()),
        },
        "engagement": {
            tier: int((mart["engagement_tier"] == tier).sum())
            for tier in ["High", "Medium", "Low", "Ghost"]
        },
        "churn_risk": {
            risk: int((mart["churn_risk"] == risk).sum())
            for risk in ["Critical", "High", "Medium", "Low"]
        },
        "top_alerts": json.loads(alerts.head(10).to_json(orient="records", force_ascii=False)) if len(alerts) else [],
    }
    return digest

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    run_time = datetime.now(tz=timezone.utc).isoformat()

    print("\n" + "=" * 55)
    print("  Script 2 — Transform, Clean, Mart, Alerts")
    print("=" * 55 + "\n")

    con    = duckdb.connect(CATALOG_PATH)
    tables = load_raw(con)
    con.close()

    print("[2] Cleaning...")
    df_school, r_school = clean_school(tables["kp_mstr_school"])
    df_users,  r_users  = clean_user_counts(tables["kp_user_counts"])
    df_last,   r_last   = clean_last_active(tables["kp_user_last_active_agg"])
    df_study,  r_study  = clean_study(tables["kp_user_study_duration_agg"])

    print(f"    kp_mstr_school          : {r_school['original']:,} → {r_school['clean']:,} rows")
    print(f"    kp_user_counts          : {r_users['original']:,} → {r_users['clean']:,} rows")
    print(f"    kp_user_last_active_agg : {r_last['original']:,} → {r_last['clean']:,} rows")
    print(f"    kp_user_study_duration  : {r_study['original']:,} → {r_study['clean']:,} rows\n")

    print("[3] Building user summary...")
    df_user_summary = build_user_summary(df_users)
    print(f"    {len(df_user_summary):,} schools with user data\n")

    print("[4] Joining tables...")
    mart = df_school.rename(columns={"id": "school_id", "name": "school_name"})
    mart = mart.merge(df_user_summary, on="school_id", how="left")
    mart = mart.merge(
        df_last[["school_id", "school_last_active", "school_first_active", "users_tracked"]],
        on="school_id", how="left"
    )
    mart = mart.merge(
        df_study[["school_id", "users_with_study", "study_hours",
                  "total_duration_seconds", "last_study_date"]],
        on="school_id", how="left"
    )
    print(f"    Mart: {len(mart):,} schools\n")

    print("[5] Computing metrics...")
    mart = compute_metrics(mart)

    print("[6] Generating alerts...")
    alerts = generate_alerts(mart)
    digest = build_digest(alerts, mart)

    print("[7] Saving output...")
    mart_out = mart.copy()
    for col in mart_out.select_dtypes(include=["datetimetz", "datetime64[ns, UTC]"]).columns:
        mart_out[col] = mart_out[col].astype(str)
    
    mart_out.to_csv(MART_CSV, index=False, encoding="utf-8-sig")
    if len(alerts):
        alerts.to_csv(ALERTS_CSV, index=False, encoding="utf-8-sig")

    with open(DIGEST_JSON, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2, default=str)

    report = {
        "run_time": run_time,
        "cleaning": {
            "kp_mstr_school":             r_school,
            "kp_user_counts":             r_users,
            "kp_user_last_active_agg":     r_last,
            "kp_user_study_duration_agg": r_study,
        },
        "mart_rows": len(mart),
        "alerts_rows": len(alerts),
    }
    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print("\n" + "=" * 55)
    print("  COMPLETED")
    print("=" * 55 + "\n")

if __name__ == "__main__":
    main()
