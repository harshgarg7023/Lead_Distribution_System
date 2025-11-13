import pandas as pd
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
LEADS_FILE = "Sample leads__Probus _ carInfo.xlsx"
POSP_FILE = "posp_updated.xlsx"
# ----------------------------------------

posp_df = pd.read_excel(POSP_FILE)

# ---- BASIC SIMILARITY SCORING ----
def simple_similarity(a, b):
    """Basic text similarity scoring (used for city/state match)."""
    if pd.isna(a) or pd.isna(b):
        return 0
    a, b = a.lower(), b.lower()
    matches = sum(1 for ch in a if ch in b)
    return (matches / max(len(a), len(b))) * 100


def compute_geo_score(lead_city, lead_state, posp_city, posp_state):
    """Compute score based on city/state similarity."""
    score = 0
    city_similarity = simple_similarity(str(lead_city), str(posp_city))
    state_similarity = simple_similarity(str(lead_state), str(posp_state))

    if city_similarity >= 90:
        score += 5
    elif city_similarity >= 70:
        score += 3

    if state_similarity >= 90:
        score += 3
    elif state_similarity >= 70:
        score += 2

    return score


# ---- MAIN MATCHING FUNCTION ----
def match_leads_with_posp(leads, posps):
    """Match each lead to the best possible POSP partners."""
    today = datetime.now()

    # Ensure valid date format
    posps["last_biz_date"] = pd.to_datetime(posps["last_biz_date"], errors="coerce", dayfirst=True)

    # Filter active partners (last 30 days)
    posps = posps[posps["last_biz_date"] >= today - timedelta(days=30)].copy()

    matched_results = []

    for _, lead in leads.iterrows():
        lead_city = lead.get("regcity", "")
        lead_state = lead.get("regstate", "")
        lead_phone = lead.get("leadid", "")
        lead_regno = lead.get("registrationno", "")

        posp_scores = []

        for _, posp in posps.iterrows():
            score = compute_geo_score(lead_city, lead_state, posp.get("city_name", ""), posp.get("state_name", ""))

            # Add activity recency score
            days_since_last = (today - posp["last_biz_date"]).days if pd.notna(posp["last_biz_date"]) else 999
            if days_since_last <= 7:
                score += 3
            elif days_since_last <= 30:
                score += 2

            posp_scores.append({
                "lead_phone": lead_phone,
                "lead_regno": lead_regno,
                "lead_city": lead_city,
                "lead_state": lead_state,
                "posp_id": posp["user_id"],
                "posp_name": posp.get("user_name", ""),
                "posp_city": posp.get("city_name", ""),
                "posp_state": posp.get("state_name", ""),
                "total_score": score
            })

        # Pick top 3 matches
        best_matches = sorted(posp_scores, key=lambda x: x["total_score"], reverse=True)[:3]
        matched_results.extend(best_matches)

    return pd.DataFrame(matched_results)


# ---- RUN PIPELINE ----
def run_pipeline():
    print(f"[{datetime.now()}] Running lead-to-POSP matching...")

    # Step 1: Load data
    leads_df = pd.read_excel(LEADS_FILE, sheet_name="Sheet1")
    posp_df = pd.read_excel(POSP_FILE, sheet_name="Sheet2")  # ✅ Changed from read_csv to read_excel

    # Step 2: Clean data
    leads_df.columns = leads_df.columns.str.strip().str.lower()
    posp_df.columns = posp_df.columns.str.strip().str.lower()

    # Step 3: Match
    matched_df = match_leads_with_posp(leads_df, posp_df)

    # Step 4: Display result
    print("\n✅ Matching Completed! Top Matches:")
    print(matched_df)  # Show only first few results
    return matched_df



if __name__ == "__main__":
    matched = run_pipeline()
