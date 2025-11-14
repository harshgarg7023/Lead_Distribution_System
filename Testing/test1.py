import os
from datetime import datetime, timedelta

import pandas as pd

# ---------------- CONFIG ----------------
LEADS_FILE = "Sample leads__Probus _ carInfo.xlsx"
LEADS_SHEET_NAME = "Sheet1"   # 👈 you said leads are in Sheet2

POSP_FILE = "posp_updated.xlsx"
POSP_SHEET_NAME = "Sheet2"

LEADS_MASTER_FILE = "leads_master.xlsx"
ASSIGNMENTS_FILE = "lead_assignments.xlsx"

TOP_N_PARTNERS = 3
# ----------------------------------------


def load_excel_if_exists(path: str) -> pd.DataFrame:
    """Load Excel if exists, else return empty DataFrame."""
    if os.path.exists(path):
        return pd.read_excel(path)
    return pd.DataFrame()


# ---------- DATA LOADING ----------

def load_leads() -> pd.DataFrame:
    """Load raw leads from CarInfo Excel (Google Sheet export)."""
    leads_df = pd.read_excel(LEADS_FILE, sheet_name=LEADS_SHEET_NAME)
    leads_df.columns = leads_df.columns.str.strip().str.lower()

    # 🔁 Map your actual columns here
    # In your sample file we saw columns like: leadid, registrationno, regcity, regstate, pincode?
    # Adjust as per actual file.
    # For key + matching we’ll need at least: phone/leadid, vehicle no, address/city/state, pincode.

    # Normalize column names
    col_map = {
        "leadid": "leadid",
        "registrationno": "registrationno",
        "regcity": "regcity",
        "regstate": "regstate",
        "pincode": "pincode",  # if present
        "address": "address"   # if present
    }
    # Only keep columns that actually exist
    mapped_cols = {k: v for k, v in col_map.items() if k in leads_df.columns}
    leads_df = leads_df.rename(columns=mapped_cols)

    # Create a lead_key to avoid duplicate processing
    leads_df["leadid"] = leads_df.get("leadid", "").astype(str)
    leads_df["registrationno"] = leads_df.get("registrationno", "").astype(str)

    leads_df["lead_key"] = (
        leads_df["leadid"].fillna("") + "_" + leads_df["registrationno"].fillna("")
    )

    # Try to ensure pincode column exists for matching – if not, you can derive or add later
    if "pincode" not in leads_df.columns:
        leads_df["pincode"] = ""  # placeholder

    return leads_df


def load_posps() -> pd.DataFrame:
    """Load partner (POSP) data from Excel."""
    posp_df = pd.read_excel(POSP_FILE, sheet_name=POSP_SHEET_NAME)
    posp_df.columns = posp_df.columns.str.strip().str.lower()

    # We saw columns like: user_id, user_name, pincode, last_biz_date, city_name, state_name
    # Make sure these exist
    required_cols = ["user_id", "user_name", "pincode", "last_biz_date"]
    for c in required_cols:
        if c not in posp_df.columns:
            raise ValueError(f"Required column '{c}' missing in POSP sheet")

    # Convert dates
    posp_df["last_biz_date"] = pd.to_datetime(
        posp_df["last_biz_date"],
        errors="coerce",
        dayfirst=True
    )

    # Ensure pincode is string
    posp_df["pincode"] = posp_df["pincode"].astype(str).str.strip()

    # If you have a column to mark app installed, map it here
    # For now, assume all are installed or add logic like:
    # posp_df["app_installed"] = True
    if "app_installed" not in posp_df.columns:
        posp_df["app_installed"] = True  # 👈 change later if you have real data

    return posp_df


# ---------- MATCHING LOGIC ----------

def score_partner_for_lead(lead_row, posp_row, today: datetime) -> int:
    """Compute a simple score for (lead, posp) pair."""
    score = 0

    lead_pin = str(lead_row.get("pincode", "")).strip()
    posp_pin = str(posp_row.get("pincode", "")).strip()

    # pincode match
    if lead_pin and lead_pin == posp_pin:
        score += 10

    # recency score
    last_biz_date = posp_row.get("last_biz_date")
    if pd.notna(last_biz_date):
        days_since_last = (today - last_biz_date).days
        if days_since_last <= 7:
            score += 5
        elif days_since_last <= 30:
            score += 3

    # (Optional) You can add city/state/address similarity here later

    return score


def match_new_leads(leads_new: pd.DataFrame, posp_df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame of assignments: each row = one (lead, partner)."""
    today = datetime.now()

    # Filter eligible POSPs
    cutoff = today - timedelta(days=30)
    eligible_posp = posp_df[
        (posp_df["app_installed"] == True) &
        (posp_df["last_biz_date"] >= cutoff)
    ].copy()

    assignments = []

    for _, lead in leads_new.iterrows():
        # If pincode is empty, you might want to skip or handle differently
        lead_pin = str(lead.get("pincode", "")).strip()

        candidates = eligible_posp
        if lead_pin:
            candidates = candidates[candidates["pincode"] == lead_pin]

        # If no same-pincode POSP, you could keep candidates as all eligible or skip
        if candidates.empty:
            # For now, skip this lead
            continue

        # Compute scores
        scored = []
        for _, posp in candidates.iterrows():
            s = score_partner_for_lead(lead, posp, today)
            if s > 0:
                scored.append((posp, s))

        if not scored:
            continue

        # Sort by score desc, take top N
        scored_sorted = sorted(scored, key=lambda x: x[1], reverse=True)[:TOP_N_PARTNERS]

        for posp, score in scored_sorted:
            assignments.append({
                "lead_key": lead["lead_key"],
                "leadid": lead.get("leadid", ""),
                "registrationno": lead.get("registrationno", ""),
                "lead_pincode": lead.get("pincode", ""),
                "lead_city": lead.get("regcity", ""),
                "lead_state": lead.get("regstate", ""),
                "posp_id": posp["user_id"],
                "posp_name": posp["user_name"],
                "posp_pincode": posp["pincode"],
                "posp_city": posp.get("city_name", ""),
                "posp_state": posp.get("state_name", ""),
                "score": score,
                "assigned_at": today
            })

    if not assignments:
        return pd.DataFrame()

    return pd.DataFrame(assignments)


# ---------- PIPELINE ----------

def run_pipeline():
    print(f"[{datetime.now()}] Starting lead distribution...")

    # 1. Load data
    leads_df = load_leads()
    posp_df = load_posps()
    leads_master_df = load_excel_if_exists(LEADS_MASTER_FILE)
    assignments_existing_df = load_excel_if_exists(ASSIGNMENTS_FILE)

    # Normalize master columns
    if not leads_master_df.empty:
        leads_master_df.columns = leads_master_df.columns.str.strip().str.lower()
        if "lead_key" not in leads_master_df.columns:
            # If older version without lead_key, create a fallback
            leads_master_df["lead_key"] = (
                leads_master_df.get("leadid", "").astype(str) + "_" +
                leads_master_df.get("registrationno", "").astype(str)
            )

    # 2. Identify new leads (not in master)
    if leads_master_df.empty:
        existing_keys = set()
    else:
        existing_keys = set(leads_master_df["lead_key"].astype(str).tolist())

    leads_df["lead_key"] = leads_df["lead_key"].astype(str)
    new_leads_df = leads_df[~leads_df["lead_key"].isin(existing_keys)].copy()

    print(f"Total leads in sheet: {len(leads_df)}")
    print(f"Already processed leads: {len(existing_keys)}")
    print(f"New leads to process: {len(new_leads_df)}")

    if new_leads_df.empty:
        print("No new leads. Exiting.")
        return

    # 3. Match new leads with POSPs
    assignments_df = match_new_leads(new_leads_df, posp_df)

    if assignments_df.empty:
        print("No assignments created (no matching POSPs).")
    else:
        print(f"Assignments created: {len(assignments_df)}")
        print(assignments_df.head())

        # 4. Save / append assignments
        if assignments_existing_df.empty:
            assignments_df.to_excel(ASSIGNMENTS_FILE, index=False)
        else:
            assignments_all = pd.concat(
                [assignments_existing_df, assignments_df], ignore_index=True
            )
            assignments_all.to_excel(ASSIGNMENTS_FILE, index=False)

    # 5. Update leads_master
    # Mark new leads as processed/assigned
    new_leads_df["status"] = "ASSIGNED"
    new_leads_df["processed_at"] = datetime.now()

    if leads_master_df.empty:
        leads_master_all = new_leads_df
    else:
        leads_master_all = pd.concat([leads_master_df, new_leads_df], ignore_index=True)

    leads_master_all.to_excel(LEADS_MASTER_FILE, index=False)

    print("✅ Pipeline completed. Master & assignments updated.")


if __name__ == "__main__":
    run_pipeline()
