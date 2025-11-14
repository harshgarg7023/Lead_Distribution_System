import os
import pandas as pd
from datetime import datetime, timedelta
from difflib import SequenceMatcher  # similarity

# ---------------- CONFIG ----------------
LEADS_FILE = "Sample leads__Probus _ carInfo.xlsx"
POSP_FILE = "posp_updated.xlsx"

LEADS_SHEET_NAME = "Sheet1"   # change to "Sheet2" if your leads are there
POSP_SHEET_NAME = "Sheet2"

LEADS_MASTER_FILE = "leads_master.xlsx"   # track processed leads
MATCHES_FILE = "lead_posp_matches_city_state_only.xlsx"  # store all matches
POSP_LOAD_FILE = "posp_load.xlsx"         # 🆕 track daily load per POSP

MAX_MATCHES_PER_LEAD = 1      # 1 POSP per lead

SCORING_CONFIG = {
    "exact_city_base": 10,
    "fuzzy_city_90": 8,
    "fuzzy_city_70": 5,
    "fuzzy_city_50": 3,
    "recency_7": 5,
    "recency_30": 3,
    "performance_weight": 1.0,   # multiply performance_score by this
    "min_score": 8,              # MIN_SCORE
    "active_days_window": 30,    # last N days for last_biz_date
    "daily_posp_cap": 15,        # max leads per POSP per day
}
# ----------------------------------------


def simple_similarity(a, b):
    """String similarity in %, using SequenceMatcher (better than char-overlap)."""
    if pd.isna(a) or pd.isna(b):
        return 0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio() * 100


def assert_required_columns(df, required_cols, df_name="DataFrame"):
    """Ensure required columns exist; raise clear error if missing."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}")


# ---- LOAD LEADS MASTER (PROCESSED LEADS) ----
def load_leads_master():
    """Load leads_master.xlsx if exists, else empty DataFrame."""
    if os.path.exists(LEADS_MASTER_FILE):
        df = pd.read_excel(LEADS_MASTER_FILE)
        df.columns = df.columns.str.strip().str.lower()
        if "lead_key" not in df.columns and "leadid" in df.columns and "registrationno" in df.columns:
            df["lead_key"] = (
                df["leadid"].astype(str).fillna("") + "_" +
                df["registrationno"].astype(str).fillna("")
            )
        return df
    return pd.DataFrame(columns=["lead_key", "leadid", "registrationno"])


# ---- LOAD / INIT POSP LOAD (LEADS PER DAY) ----
def load_posp_load(today_date):
    """
    Load POSP load file (posp_load.xlsx).
    Columns: posp_id, assigned_count_today, last_reset_date
    Reset counts if last_reset_date != today.
    """
    if os.path.exists(POSP_LOAD_FILE):
        df = pd.read_excel(POSP_LOAD_FILE)
        df.columns = df.columns.str.strip().str.lower()
        if "posp_id" not in df.columns:
            df["posp_id"] = None
        if "assigned_count_today" not in df.columns:
            df["assigned_count_today"] = 0
        if "last_reset_date" not in df.columns:
            df["last_reset_date"] = today_date
    else:
        df = pd.DataFrame(columns=["posp_id", "assigned_count_today", "last_reset_date"])

    # Normalize types
    if not df.empty:
        df["assigned_count_today"] = pd.to_numeric(
            df["assigned_count_today"], errors="coerce"
        ).fillna(0).astype(int)
        df["last_reset_date"] = pd.to_datetime(
            df["last_reset_date"], errors="coerce"
        ).dt.date.fillna(today_date)

        # Reset counts if date changed
        mask_reset = df["last_reset_date"] != today_date
        df.loc[mask_reset, "assigned_count_today"] = 0
        df.loc[mask_reset, "last_reset_date"] = today_date
    return df


def build_load_map(posp_load_df):
    """
    Build a dict: key = str(posp_id), value = assigned_count_today.
    """
    load_map = {}
    for _, row in posp_load_df.iterrows():
        posp_id = row.get("posp_id")
        if pd.isna(posp_id):
            continue
        key = str(posp_id)
        load_map[key] = int(row.get("assigned_count_today", 0))
    return load_map


# ---- MAIN MATCHING FUNCTION ----
def match_leads_with_posp(leads, posps, posp_load_map):
    """Match each lead to the best possible POSP partners."""
    today = datetime.now()
    days_window = SCORING_CONFIG["active_days_window"]
    min_score = SCORING_CONFIG["min_score"]
    daily_cap = SCORING_CONFIG["daily_posp_cap"]
    perf_weight = SCORING_CONFIG["performance_weight"]

    # Ensure valid date format
    posps["last_biz_date"] = pd.to_datetime(
        posps["last_biz_date"], errors="coerce", dayfirst=True
    )

    # Filter active partners (last N days window)
    posps = posps[posps["last_biz_date"] >= today - timedelta(days=days_window)].copy()

    # Normalise performance_score (if column missing, treat as 0)
    if "performance_score" in posps.columns:
        posps["performance_score"] = pd.to_numeric(
            posps["performance_score"], errors="coerce"
        ).fillna(0)
    else:
        posps["performance_score"] = 0

    # Filter POSPs with app installed if column exists
    if "app_installed" in posps.columns:
        col = posps["app_installed"]
        if col.dtype == "O":  # strings like "YES"/"NO"
            posps["app_installed_bool"] = col.astype(str).str.lower().isin(
                ["yes", "y", "true", "1"]
            )
        else:
            posps["app_installed_bool"] = col.astype(int) == 1
        posps = posps[posps["app_installed_bool"] == True].copy()
    else:
        posps["app_installed_bool"] = True  # assume all installed for now

    matched_results = []

    for _, lead in leads.iterrows():
        lead_city = lead.get("regcity", "")
        lead_state = lead.get("regstate", "")
        lead_phone = lead.get("leadid", "")
        lead_regno = lead.get("registrationno", "")

        lead_city_norm = str(lead_city).strip().lower()
        lead_state_norm = str(lead_state).strip().lower()

        exact_posp_scores = []
        fuzzy_posp_scores = []
        seen_posp_ids = set()   # avoid scoring same POSP multiple times for this lead

        for _, posp in posps.iterrows():
            posp_id = posp["user_id"]
            posp_key = str(posp_id)

            # avoid duplicate rows for same POSP in this lead
            if posp_id in seen_posp_ids:
                continue

            assigned_count_today = posp_load_map.get(posp_key, 0)

            # respect daily cap
            if assigned_count_today >= daily_cap:
                seen_posp_ids.add(posp_id)
                continue

            posp_city = posp.get("city_name", "")
            posp_state = posp.get("state_name", "")
            perf_score_raw = posp.get("performance_score", 0)
            perf_score = perf_score_raw * perf_weight

            posp_city_norm = str(posp_city).strip().lower()
            posp_state_norm = str(posp_state).strip().lower()

            # State must match
            if not lead_state_norm or not posp_state_norm or lead_state_norm != posp_state_norm:
                seen_posp_ids.add(posp_id)
                continue

            # Recency score
            last_biz_date = posp["last_biz_date"]
            days_since_last = (
                (today - last_biz_date).days
                if pd.notna(last_biz_date)
                else 999
            )
            recency_score = 0
            if days_since_last <= 7:
                recency_score = SCORING_CONFIG["recency_7"]
            elif days_since_last <= 30:
                recency_score = SCORING_CONFIG["recency_30"]

            # Exact city match
            if lead_city_norm and posp_city_norm and lead_city_norm == posp_city_norm:
                base_geo_score = SCORING_CONFIG["exact_city_base"]
                total_score = base_geo_score + recency_score + perf_score

                exact_posp_scores.append({
                    "lead_phone": lead_phone,
                    "lead_regno": lead_regno,
                    "lead_city": lead_city,
                    "lead_state": lead_state,
                    "posp_id": posp_id,
                    "posp_name": posp.get("user_name", ""),
                    "posp_city": posp_city,
                    "posp_state": posp_state,
                    "total_score": total_score,
                    "last_biz_date": last_biz_date,
                    "performance_score": perf_score_raw,
                    "days_since_last": days_since_last,
                    "match_type": "exact_city",
                    "similarity": 100.0,
                    "assigned_count_today": assigned_count_today,
                })
            else:
                # Fuzzy city similarity
                city_similarity = simple_similarity(lead_city, posp_city)

                if city_similarity < 50:
                    seen_posp_ids.add(posp_id)
                    continue

                if city_similarity >= 90:
                    base_geo_score = SCORING_CONFIG["fuzzy_city_90"]
                elif city_similarity >= 70:
                    base_geo_score = SCORING_CONFIG["fuzzy_city_70"]
                else:  # 50–69
                    base_geo_score = SCORING_CONFIG["fuzzy_city_50"]

                total_score = base_geo_score + recency_score + perf_score

                fuzzy_posp_scores.append({
                    "lead_phone": lead_phone,
                    "lead_regno": lead_regno,
                    "lead_city": lead_city,
                    "lead_state": lead_state,
                    "posp_id": posp_id,
                    "posp_name": posp.get("user_name", ""),
                    "posp_city": posp_city,
                    "posp_state": posp_state,
                    "total_score": total_score,
                    "last_biz_date": last_biz_date,
                    "performance_score": perf_score_raw,
                    "days_since_last": days_since_last,
                    "match_type": "fuzzy_city",
                    "similarity": city_similarity,
                    "assigned_count_today": assigned_count_today,
                })

            seen_posp_ids.add(posp_id)

        # Prefer exact matches over fuzzy
        if exact_posp_scores:
            posp_scores = exact_posp_scores
        else:
            posp_scores = fuzzy_posp_scores

        # Case 1: no eligible POSP at all
        if not posp_scores:
            matched_results.append({
                "lead_phone": lead_phone,
                "lead_regno": lead_regno,
                "lead_city": lead_city,
                "lead_state": lead_state,
                "posp_id": None,
                "posp_name": None,
                "posp_city": None,
                "posp_state": None,
                "total_score": 0,
                "last_biz_date": None,
                "performance_score": None,
                "days_since_last": None,
                "match_type": "none",
                "similarity": 0,
                "assigned_count_today": None,
                "assigned_status": "not_assigned",
            })
            continue

        # Sort by score, recency, then fewer assigned leads
        best_matches = sorted(
            posp_scores,
            key=lambda x: (x["total_score"], x["last_biz_date"], -x["assigned_count_today"]),
            reverse=True
        )[:MAX_MATCHES_PER_LEAD]

        # Apply MIN_SCORE filter
        best_matches = [m for m in best_matches if m["total_score"] >= min_score]

        # Case 2: candidates exist but all below MIN_SCORE
        if not best_matches:
            matched_results.append({
                "lead_phone": lead_phone,
                "lead_regno": lead_regno,
                "lead_city": lead_city,
                "lead_state": lead_state,
                "posp_id": None,
                "posp_name": None,
                "posp_city": None,
                "posp_state": None,
                "total_score": 0,
                "last_biz_date": None,
                "performance_score": None,
                "days_since_last": None,
                "match_type": "none",
                "similarity": 0,
                "assigned_count_today": None,
                "assigned_status": "not_assigned",
            })
        else:
            for m in best_matches:
                m["assigned_status"] = "assigned"
            matched_results.extend(best_matches)

    if not matched_results:
        return pd.DataFrame()
    return pd.DataFrame(matched_results)


# ---- RUN PIPELINE ----
def run_pipeline():
    print(f"[{datetime.now()}] Running lead-to-POSP matching...")

    # Load raw data
    leads_df = pd.read_excel(LEADS_FILE, sheet_name=LEADS_SHEET_NAME)
    posp_df = pd.read_excel(POSP_FILE, sheet_name=POSP_SHEET_NAME)

    leads_df.columns = leads_df.columns.str.strip().str.lower()
    posp_df.columns = posp_df.columns.str.strip().str.lower()

    # Safety: required columns
    assert_required_columns(
        leads_df,
        ["leadid", "registrationno", "regcity", "regstate"],
        df_name="Leads sheet"
    )
    assert_required_columns(
        posp_df,
        ["user_id", "city_name", "state_name", "last_biz_date"],
        df_name="POSP sheet"
    )

    # Remove duplicate leads (same leadid + registrationno)
    leads_df = leads_df.drop_duplicates(subset=["leadid", "registrationno"]).copy()

    # Compute lead_key for this run
    leads_df["lead_key"] = (
        leads_df["leadid"].astype(str).fillna("") + "_" +
        leads_df["registrationno"].astype(str).fillna("")
    )

    # Load leads_master and filter NEW leads only
    leads_master_df = load_leads_master()
    existing_keys = set(leads_master_df["lead_key"].astype(str).tolist()) if not leads_master_df.empty else set()
    new_leads_df = leads_df[~leads_df["lead_key"].isin(existing_keys)].copy()

    print(f"Total leads in sheet   : {len(leads_df)}")
    print(f"Already processed leads: {len(existing_keys)}")
    print(f"New leads to process   : {len(new_leads_df)}")

    if new_leads_df.empty:
        print("No new leads. Exiting.")
        return pd.DataFrame()

    # Remove duplicate POSP rows by user_id
    posp_df = posp_df.drop_duplicates(subset=["user_id"]).copy()

    # Load / normalize POSP load and build map
    today_date = datetime.now().date()
    posp_load_df = load_posp_load(today_date)
    posp_load_map = build_load_map(posp_load_df)

    # Match ONLY new leads
    matched_df = match_leads_with_posp(new_leads_df, posp_df, posp_load_map)

    if not matched_df.empty:
        matched_df = matched_df.drop_duplicates(
            subset=["lead_phone", "lead_regno", "posp_id", "assigned_status"]
        ).copy()

    # Update POSP load for assigned leads
    if not matched_df.empty:
        assigned_df = matched_df[matched_df["assigned_status"] == "assigned"].copy()
        if not assigned_df.empty:
            for _, row in assigned_df.iterrows():
                posp_id = row.get("posp_id")
                if pd.isna(posp_id):
                    continue
                key = str(posp_id)

                # Find or create row in posp_load_df
                mask = posp_load_df["posp_id"].astype(str) == key
                if not mask.any():
                    new_row = {
                        "posp_id": posp_id,
                        "assigned_count_today": 1,
                        "last_reset_date": today_date,
                    }
                    posp_load_df = pd.concat([posp_load_df, pd.DataFrame([new_row])], ignore_index=True)
                else:
                    idx = posp_load_df[mask].index[0]
                    posp_load_df.loc[idx, "assigned_count_today"] = int(
                        posp_load_df.loc[idx, "assigned_count_today"]
                    ) + 1
                    posp_load_df.loc[idx, "last_reset_date"] = today_date

            posp_load_df.to_excel(POSP_LOAD_FILE, index=False)
            print(f"📁 {POSP_LOAD_FILE} updated with new assigned counts.")

    # Append matches (assigned + not_assigned) to MATCHES_FILE
    if not matched_df.empty:
        if os.path.exists(MATCHES_FILE):
            existing_matches = pd.read_excel(MATCHES_FILE)
            existing_matches.columns = existing_matches.columns.str.strip().str.lower()
            combined = pd.concat([existing_matches, matched_df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["lead_phone", "lead_regno", "posp_id", "assigned_status"]
            )
        else:
            combined = matched_df

        combined.to_excel(MATCHES_FILE, index=False)
        print(f"\n📁 Results appended to {MATCHES_FILE}")
    else:
        print("\nNo results created for new leads.")

    # Update leads_master so these leads are not processed again
    master_new = new_leads_df[["lead_key", "leadid", "registrationno"]].copy()
    leads_master_all = pd.concat([leads_master_df, master_new], ignore_index=True)
    leads_master_all = leads_master_all.drop_duplicates(subset=["lead_key"])
    leads_master_all.to_excel(LEADS_MASTER_FILE, index=False)
    print(f"📁 {LEADS_MASTER_FILE} updated with processed leads")

    print("\n✅ Matching Completed! Sample of new matches (assigned & not_assigned):")
    print(matched_df.head())

    return matched_df


if __name__ == "__main__":
    matched = run_pipeline()
