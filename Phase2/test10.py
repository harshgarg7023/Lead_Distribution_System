import os
import pandas as pd
from datetime import datetime, timedelta
from difflib import SequenceMatcher  # similarity

# ---------------- CONFIG ----------------
LEADS_FILE = "leads__Probus _ CarInfo.xlsx"   # 👈 updated to your new file
POSP_FILE = "posp_updated.xlsx"

LEADS_SHEET_NAME = "Sheet1"
POSP_SHEET_NAME = "Sheet2"

MAX_MATCHES_PER_LEAD = 1      # 1 POSP per lead

LEADS_MASTER_FILE = "leads_master.xlsx"   # track processed leads
MATCHES_FILE = "Processing Results.xlsx"  # store all matches

MIN_SCORE = 8  # minimum total_score required to assign a lead
# ----------------------------------------


# ---- BETTER SIMILARITY SCORING (using SequenceMatcher) ----
def simple_similarity(a, b):
    """String similarity in %, using SequenceMatcher (better than char-overlap)."""
    if pd.isna(a) or pd.isna(b):
        return 0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio() * 100


# ---- PINCODE NORMALIZATION & MATCH LEVEL ----
def normalize_pincode(pin):
    """Return 6-digit pincode string or '' if invalid."""
    if pd.isna(pin):
        return ""
    s = "".join(ch for ch in str(pin) if ch.isdigit())  # keep only digits
    if len(s) != 6:
        return ""
    return s


def pincode_match_level(lead_pin, posp_pin):
    """
    Returns (level, prefix_len) based on how strongly pincodes match.
    level: '6', '5', '3', or 'none'
    prefix_len: 6, 5, 3, or 0
    """
    lp = normalize_pincode(lead_pin)
    pp = normalize_pincode(posp_pin)
    if not lp or not pp:
        return "none", 0

    if lp == pp:
        return "6", 6
    if lp[:5] == pp[:5]:
        return "5", 5
    if lp[:3] == pp[:3]:
        return "3", 3

    return "none", 0


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

        # 🔹 Lead pincode (we renamed 'pincode_x000d_' → 'pincode' in run_pipeline)
        lead_pincode = lead.get("pincode") or ""

        lead_city_norm = str(lead_city).strip().lower()
        lead_state_norm = str(lead_state).strip().lower()

        # Three tiers: pincode > exact city > fuzzy city
        pin_posp_scores = []
        exact_posp_scores = []
        fuzzy_posp_scores = []

        seen_posp_ids = set()   # avoid scoring same POSP multiple times for this lead

        for _, posp in posps.iterrows():
            posp_id = posp["user_id"]
            if posp_id in seen_posp_ids:
                continue  # skip duplicate POSP rows

            posp_city = posp.get("city_name", "")
            posp_state = posp.get("state_name", "")

            # POSP pincode (column is 'pincode' in your file)
            posp_pincode = posp.get("pincode") or ""

            perf_score = posp.get("performance_score", 0)

            posp_city_norm = str(posp_city).strip().lower()
            posp_state_norm = str(posp_state).strip().lower()

            # --- recency part ---
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

            # 🥇 1) PINCODE PRIORITY (graded: 6-digit, 5-digit, 3-digit)
            level, prefix_len = pincode_match_level(lead_pincode, posp_pincode)

            if level != "none":
                if level == "6":
                    base_geo_score = 18   # exact pincode – strongest
                    match_type = "pincode_6"
                elif level == "5":
                    base_geo_score = 14   # very close
                    match_type = "pincode_5"
                else:  # "3"
                    base_geo_score = 10   # same sorting district
                    match_type = "pincode_3"

                total_score = base_geo_score + recency_score + perf_score

                pin_posp_scores.append({
                    "lead_phone": lead_phone,
                    "lead_regno": lead_regno,
                    "lead_city": lead_city,
                    "lead_state": lead_state,
                    "lead_pincode": lead_pincode,
                    "posp_id": posp_id,
                    "posp_name": posp.get("user_name", ""),
                    "posp_city": posp_city,
                    "posp_state": posp_state,
                    "posp_pincode": posp_pincode,
                    "total_score": total_score,
                    "last_biz_date": last_biz_date,
                    "performance_score": perf_score,
                    "days_since_last": days_since_last,
                    "match_type": match_type,
                    "similarity": 100.0,  # for pincode prefix
                })
                seen_posp_ids.add(posp_id)
                # If we already matched by pincode, we don't downgrade to city/state
                continue

            # 🥈 2) CITY/STATE LOGIC (only if no pincode prefix match)
            # 🚫 State must match here
            if not lead_state_norm or not posp_state_norm or lead_state_norm != posp_state_norm:
                seen_posp_ids.add(posp_id)
                continue

            # --- exact city match case ---
            if lead_city_norm and posp_city_norm and lead_city_norm == posp_city_norm:
                base_geo_score = 10
                total_score = base_geo_score + recency_score + perf_score

                exact_posp_scores.append({
                    "lead_phone": lead_phone,
                    "lead_regno": lead_regno,
                    "lead_city": lead_city,
                    "lead_state": lead_state,
                    "lead_pincode": lead_pincode,
                    "posp_id": posp_id,
                    "posp_name": posp.get("user_name", ""),
                    "posp_city": posp_city,
                    "posp_state": posp_state,
                    "posp_pincode": posp_pincode,
                    "total_score": total_score,
                    "last_biz_date": last_biz_date,
                    "performance_score": perf_score,
                    "days_since_last": days_since_last,
                    "match_type": "exact_city",
                    "similarity": 100.0,
                })
            else:
                # --- fuzzy city similarity (only used if no exact matches exist) ---
                city_similarity = simple_similarity(lead_city, posp_city)

                # ignore totally unrelated cities
                if city_similarity < 50:
                    seen_posp_ids.add(posp_id)
                    continue

                if city_similarity >= 90:
                    base_geo_score = 8
                elif city_similarity >= 70:
                    base_geo_score = 5
                else:  # 50–69
                    base_geo_score = 3

                total_score = base_geo_score + recency_score + perf_score

                fuzzy_posp_scores.append({
                    "lead_phone": lead_phone,
                    "lead_regno": lead_regno,
                    "lead_city": lead_city,
                    "lead_state": lead_state,
                    "lead_pincode": lead_pincode,
                    "posp_id": posp_id,
                    "posp_name": posp.get("user_name", ""),
                    "posp_city": posp_city,
                    "posp_state": posp_state,
                    "posp_pincode": posp_pincode,
                    "total_score": total_score,
                    "last_biz_date": last_biz_date,
                    "performance_score": perf_score,
                    "days_since_last": days_since_last,
                    "match_type": "fuzzy_city",
                    "similarity": city_similarity,
                })

            seen_posp_ids.add(posp_id)

        # 🔁 PRIORITY: pincode > exact_city > fuzzy_city
        if pin_posp_scores:
            posp_scores = pin_posp_scores
        elif exact_posp_scores:
            posp_scores = exact_posp_scores
        else:
            posp_scores = fuzzy_posp_scores

        # 👉 Case 1: no eligible POSP at all
        if not posp_scores:
            matched_results.append({
                "lead_phone": lead_phone,
                "lead_regno": lead_regno,
                "lead_city": lead_city,
                "lead_state": lead_state,
                "lead_pincode": lead_pincode,
                "posp_id": None,
                "posp_name": None,
                "posp_city": None,
                "posp_state": None,
                "posp_pincode": None,
                "total_score": 0,
                "last_biz_date": None,
                "performance_score": None,
                "days_since_last": None,
                "match_type": "none",
                "similarity": 0,
                "assigned_status": "not_assigned",
            })
            continue

        # ✅ Sort by score, then by last_biz_date (most recent first)
        best_matches = sorted(
            posp_scores,
            key=lambda x: (x["total_score"], x["last_biz_date"]),
            reverse=True
        )[:MAX_MATCHES_PER_LEAD]

        # Apply MIN_SCORE filter
        best_matches = [m for m in best_matches if m["total_score"] >= MIN_SCORE]

        # 👉 Case 2: we had candidates, but all below MIN_SCORE
        if not best_matches:
            matched_results.append({
                "lead_phone": lead_phone,
                "lead_regno": lead_regno,
                "lead_city": lead_city,
                "lead_state": lead_state,
                "lead_pincode": lead_pincode,
                "posp_id": None,
                "posp_name": None,
                "posp_city": None,
                "posp_state": None,
                "posp_pincode": None,
                "total_score": 0,
                "last_biz_date": None,
                "performance_score": None,
                "days_since_last": None,
                "match_type": "none",
                "similarity": 0,
                "assigned_status": "not_assigned",
            })
        else:
            # valid matches → mark as assigned
            for m in best_matches:
                m["assigned_status"] = "assigned"
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

    # 🔧 Fix weird pincode column name in leads: 'pincode_x000d_' → 'pincode'
    if "pincode_x000d_" in leads_df.columns and "pincode" not in leads_df.columns:
        leads_df = leads_df.rename(columns={"pincode_x000d_": "pincode"})

    # Remove duplicate leads (same leadid + registrationno)
    if "leadid" in leads_df.columns and "registrationno" in leads_df.columns:
        leads_df = leads_df.drop_duplicates(subset=["leadid", "registrationno"]).copy()

    # Compute lead_key for this run
    if "leadid" in leads_df.columns and "registrationno" in leads_df.columns:
        leads_df["lead_key"] = (
            leads_df["leadid"].astype(str).fillna("") + "_" +
            leads_df["registrationno"].astype(str).fillna("")
        )
    else:
        leads_df["lead_key"] = leads_df.index.astype(str)

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
    if "user_id" in posp_df.columns:
        posp_df = posp_df.drop_duplicates(subset=["user_id"]).copy()

    # Step 3: Match ONLY new leads
    matched_df = match_leads_with_posp(new_leads_df, posp_df)

    # Remove duplicate (lead, posp) pairs in final result
    if not matched_df.empty:
        matched_df = matched_df.drop_duplicates(
            subset=["lead_phone", "lead_regno", "posp_id", "assigned_status"]
        ).copy()

    # Step 4: Append matches (assigned + not_assigned) to MATCHES_FILE
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

    # Step 5: Update leads_master so these leads are not processed again
    master_new = new_leads_df[["lead_key", "leadid", "registrationno"]].copy()
    leads_master_all = pd.concat([leads_master_df, master_new], ignore_index=True)
    leads_master_all = leads_master_all.drop_duplicates(subset=["lead_key"])
    #leads_master_all.to_excel(LEADS_MASTER_FILE, index=False)
    print(f"📁 {LEADS_MASTER_FILE} updated with processed leads")

    print("\n✅ Matching Completed! Sample of new matches (assigned & not_assigned):")
    print(matched_df.head())

    return matched_df


if __name__ == "__main__":
    matched = run_pipeline()
