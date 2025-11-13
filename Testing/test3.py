import pandas as pd
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
LEADS_FILE = "Sample leads__Probus _ carInfo.xlsx"
POSP_FILE = "posp_updated.xlsx"

LEADS_SHEET_NAME = "Sheet1"   # change to "Sheet2" if your leads are there
POSP_SHEET_NAME = "Sheet2"
# ----------------------------------------


# ---- BASIC SIMILARITY SCORING ----
def simple_similarity(a, b):
    """Basic text similarity scoring (used for city match)."""
    if pd.isna(a) or pd.isna(b):
        return 0
    a, b = str(a).lower(), str(b).lower()
    matches = sum(1 for ch in a if ch in b)
    return (matches / max(len(a), len(b))) * 100


def compute_geo_score(lead_city, lead_state, posp_city, posp_state):
    """
    Compute score based on city/state.
    - Hard rule: state must match (case-insensitive), otherwise score = 0
    - City similarity adds score
    """
    if not lead_state or not posp_state:
        return 0

    ls = str(lead_state).strip().lower()
    ps = str(posp_state).strip().lower()

    # 🚫 Don't match across different states
    if ls != ps:
        return 0

    score = 0
    city_similarity = simple_similarity(str(lead_city), str(posp_city))

    # City similarity scoring
    if city_similarity >= 90:
        score += 8
    elif city_similarity >= 70:
        score += 5
    elif city_similarity >= 50:
        score += 3

    return score


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

        for _, posp in posps.iterrows():
            posp_city = posp.get("city_name", "")
            posp_state = posp.get("state_name", "")

            posp_city_norm = str(posp_city).strip().lower()
            posp_state_norm = str(posp_state).strip().lower()

            # 🚫 State must match
            if not lead_state_norm or not posp_state_norm or lead_state_norm != posp_state_norm:
                continue

            # --- recency part (common for exact & fuzzy) ---
            days_since_last = (
                (today - posp["last_biz_date"]).days
                if pd.notna(posp["last_biz_date"])
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
                    "posp_id": posp["user_id"],
                    "posp_name": posp.get("user_name", ""),
                    "posp_city": posp_city,
                    "posp_state": posp_state,
                    "total_score": total_score
                })
            else:
                # --- fuzzy city similarity (only used if no exact matches exist) ---
                city_similarity = simple_similarity(lead_city, posp_city)

                # ignore totally unrelated cities
                if city_similarity < 50:
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
                    "posp_id": posp["user_id"],
                    "posp_name": posp.get("user_name", ""),
                    "posp_city": posp_city,
                    "posp_state": posp_state,
                    "total_score": total_score
                })

        # 🔁 If we have exact city matches, ignore all fuzzy ones
        if exact_posp_scores:
            posp_scores = exact_posp_scores
        else:
            posp_scores = fuzzy_posp_scores

        if not posp_scores:
            continue

        # Pick top 3 matches
        best_matches = sorted(
            posp_scores, key=lambda x: x["total_score"], reverse=True
        )[:3]
        matched_results.extend(best_matches)

    return pd.DataFrame(matched_results)



# ---- RUN PIPELINE ----
def run_pipeline():
    print(f"[{datetime.now()}] Running lead-to-POSP matching...")

    # Step 1: Load data
    leads_df = pd.read_excel(LEADS_FILE, sheet_name=LEADS_SHEET_NAME)
    posp_df = pd.read_excel(POSP_FILE, sheet_name=POSP_SHEET_NAME)

    # Step 2: Clean data
    leads_df.columns = leads_df.columns.str.strip().str.lower()
    posp_df.columns = posp_df.columns.str.strip().str.lower()

    # 🆕 Remove duplicate POSP rows by user_id (prevents same POSP repeated)
    if "user_id" in posp_df.columns:
        posp_df = posp_df.drop_duplicates(subset=["user_id"]).copy()

    # Step 3: Match
    matched_df = match_leads_with_posp(leads_df, posp_df)

    # Step 4: Display result
    print("\n✅ Matching Completed! Top Matches:")
    print(matched_df.head())

    # Optional: save result to Excel
    matched_df.to_excel("lead_posp_matches_city_state_only.xlsx", index=False)
    print("\n📁 Results saved to lead_posp_matches_city_state_only.xlsx")

    return matched_df


if __name__ == "__main__":
    matched = run_pipeline()
