import os
import pandas as pd
from datetime import datetime, timedelta
from difflib import SequenceMatcher  # similarity

# ---------------- CONFIG ----------------
LEADS_FILE = "Sample leads__Probus _ carInfo.xlsx"
POSP_FILE = "posp_updated.xlsx"

LEADS_SHEET_NAME = "Sheet1"   # change to "Sheet2" if your leads are there
POSP_SHEET_NAME = "Sheet2"

MAX_MATCHES_PER_LEAD = 1      # 1 POSP per lead

LEADS_MASTER_FILE = "leads_master.xlsx"   # 🆕 track processed leads
MATCHES_FILE = "lead_posp_matches_city_state_only.xlsx"  # store all matches
# ----------------------------------------


# ---- BETTER SIMILARITY SCORING (using SequenceMatcher) ----
def simple_similarity(a, b):
    """String similarity in %, using SequenceMatcher (better than char-overlap)."""
    if pd.isna(a) or pd.isna(b):
        return 0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio() * 100


# ---- LOAD LEADS MASTER (PROCESSED LEADS) ----
def load_leads_master():
    """Load leads_master.xlsx if exists, else empty DataFrame."""
    if os.path.exists(LEADS_MASTER_FILE):
        df = pd.read_excel(LEADS_MASTER_FILE)
        df.columns = df.columns.str.strip().str.lower()
        # ensure lead_key exists
        if "lead_key" not in df.columns and "leadid" in df.columns and "registrationno" in df.columns:
            df["lead_key"] = (
                df["leadid"].astype(str).fillna("") + "_" +
                df["registrationno"].astype(str).fillna("")
            )
        return df
    # empty master
    return pd.DataFrame(columns=["lead_key", "leadid", "registrationno"])


# ---- MAIN MATCHING FUNCTION ----
def match_leads_with_posp(leads, posps):
    """Match each lead to the best possible POSP partners."""
    today = datetime.now()

    # Ensure valid date format
    posps["last_biz_date"] = pd.to_datetime(
        posps["last_biz_date"], errors="coerce", dayfirst=True
    )

    # Filter active partners (last 30 days)
    posps = posps[posps["last_biz_date"] >= today - timedelta(days=30)].copy()

    # Filter POSPs with app installed if column exists
    if "app_installed" in posps.columns:
        col = posps["app_installed"]
        # normalise to boolean
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
            if posp_id in seen_posp_ids:
                continue  # skip duplicate POSP rows

            posp_city = posp.get("city_name", "")
            posp_state = posp.get("state_name", "")

            posp_city_norm = str(posp_city).strip().lower()
            posp_state_norm = str(posp_state).strip().lower()

            # 🚫 State must match
            if not lead_state_norm or not posp_state_norm or lead_state_norm != posp_state_norm:
                seen_posp_ids.add(posp_id)
                continue

            # --- recency part (common for exact & fuzzy) ---
            last_biz_date = posp["last_biz_date"]
            days_since_last = (
                (today - last_biz_date).days
                if pd.notna(last_biz_date)
                else 999
            )
            recency_score = 0
            if days_since_last <= 7:
                recency_score = 5
            elif days_since_last <= 30:
                recency_score = 3

            # --- exact city match case ---
            if lead_city_norm and posp_city_norm and lead_city_norm == posp_city_norm:
                # Strong base score for exact city
                total_score = 10 + recency_score

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
                    "last_biz_date": last_biz_date,  # for tie-breaker
                })
            else:
                # --- fuzzy city similarity (only used if no exact matches exist) ---
                city_similarity = simple_similarity(lead_city, posp_city)

                # ignore totally unrelated cities
                if city_similarity < 50:
                    seen_posp_ids.add(posp_id)
                    continue

                base_score = 0
                if city_similarity >= 90:
                    base_score = 8
                elif city_similarity >= 70:
                    base_score = 5
                elif city_similarity >= 50:
                    base_score = 3

                total_score = base_score + recency_score

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
                    "last_biz_date": last_biz_date,  # for tie-breaker
                })

            seen_posp_ids.add(posp_id)

        # 🔁 If we have exact city matches, ignore all fuzzy ones
        if exact_posp_scores:
            posp_scores = exact_posp_scores
        else:
            posp_scores = fuzzy_posp_scores

        if not posp_scores:
            continue

        # ✅ Sort by score, then by last_biz_date (most recent first)
        best_matches = sorted(
            posp_scores,
            key=lambda x: (x["total_score"], x["last_biz_date"]),
            reverse=True
        )[:MAX_MATCHES_PER_LEAD]

        matched_results.extend(best_matches)

    # Convert to DataFrame
    if not matched_results:
        return pd.DataFrame()

    df = pd.DataFrame(matched_results)
    return df


# ---- RUN PIPELINE ----
def run_pipeline():
    print(f"[{datetime.now()}] Running lead-to-POSP matching...")

    # Step 1: Load raw data
    leads_df = pd.read_excel(LEADS_FILE, sheet_name=LEADS_SHEET_NAME)
    posp_df = pd.read_excel(POSP_FILE, sheet_name=POSP_SHEET_NAME)

    # Step 2: Clean columns
    leads_df.columns = leads_df.columns.str.strip().str.lower()
    posp_df.columns = posp_df.columns.str.strip().str.lower()

    # Remove duplicate leads (same leadid + registrationno)
    if "leadid" in leads_df.columns and "registrationno" in leads_df.columns:
        leads_df = leads_df.drop_duplicates(subset=["leadid", "registrationno"]).copy()

    # 🆕 Compute lead_key for this run
    if "leadid" in leads_df.columns and "registrationno" in leads_df.columns:
        leads_df["lead_key"] = (
            leads_df["leadid"].astype(str).fillna("") + "_" +
            leads_df["registrationno"].astype(str).fillna("")
        )
    else:
        # fallback: use index if key columns missing
        leads_df["lead_key"] = leads_df.index.astype(str)

    # 🆕 Load leads_master and filter NEW leads only
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
    if "user_id" in posp_df.columns:
        posp_df = posp_df.drop_duplicates(subset=["user_id"]).copy()

    # Step 3: Match ONLY new leads
    matched_df = match_leads_with_posp(new_leads_df, posp_df)

    # Remove duplicate (lead, posp) pairs in final result (extra safety)
    if not matched_df.empty:
        matched_df = matched_df.drop_duplicates(
            subset=["lead_phone", "lead_regno", "posp_id"]
        ).copy()

    # Step 4: Append matches to MATCHES_FILE
    if not matched_df.empty:
        if os.path.exists(MATCHES_FILE):
            existing_matches = pd.read_excel(MATCHES_FILE)
            existing_matches.columns = existing_matches.columns.str.strip().str.lower()
            combined = pd.concat([existing_matches, matched_df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["lead_phone", "lead_regno", "posp_id"]
            )
        else:
            combined = matched_df

        combined.to_excel(MATCHES_FILE, index=False)
        print(f"\n📁 Results appended to {MATCHES_FILE}")
    else:
        print("\nNo matches created for new leads.")

    # Step 5: Update leads_master so these leads are not processed again
    master_new = new_leads_df[["lead_key", "leadid", "registrationno"]].copy()
    leads_master_all = pd.concat([leads_master_df, master_new], ignore_index=True)
    leads_master_all = leads_master_all.drop_duplicates(subset=["lead_key"])
    leads_master_all.to_excel(LEADS_MASTER_FILE, index=False)
    print(f"📁 {LEADS_MASTER_FILE} updated with processed leads")

    print("\n✅ Matching Completed! Sample of new matches:")
    print(matched_df.head())

    return matched_df


if __name__ == "__main__":
    matched = run_pipeline()
