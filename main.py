import requests
import json
import os
import re
from datetime import datetime, timedelta, date
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Variables d'environnement ---
PHANTOM_API_KEY  = os.environ["PHANTOM_API_KEY"]
PHANTOM_AGENT_ID = os.environ["PHANTOM_AGENT_ID"]
APOLLO_API_KEY   = os.environ["APOLLO_API_KEY"]
GOOGLE_SHEET_ID  = os.environ["GOOGLE_SHEET_ID"]
SHEET_TAB        = os.environ.get("SHEET_TAB_NAME", "Leads Pipeline")
RECENCY_DAYS     = int(os.environ.get("RECENCY_DAYS", "90"))
CREDS_JSON       = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

APOLLO_HEADERS = {"X-Api-Key": APOLLO_API_KEY, "Content-Type": "application/json"}

# --- ICP Bonnes Manières ---
ICP_TITLES = [
    "directeur commercial", "directrice commerciale",
    "chief commercial officer", "cco",
    "head of sales", "head of revenue",
    "directeur du développement commercial", "directrice du développement commercial",
    "sales director", "director of sales",
    "vp sales", "vp commercial", "vice president sales",
]

ICP_INDUSTRIES_INCLUDE = [
    "web design", "digital marketing", "seo", "search engine optimization",
    "online media", "internet", "marketing and advertising",
    "computer & network security", "cybersecurity", "information security",
    "network security", "cyber security",
    "public relations", "media relations", "communications", "pr agency",
    "software", "computer software", "saas", "enterprise software", "application software",
]

ICP_INDUSTRIES_EXCLUDE = [
    "staffing and recruiting", "outsourcing/offshoring",
    "managed services", "it services",
    "management consulting", "business consulting", "consulting",
    "professional training & coaching", "e-learning", "training",
    "human resources", "hr", "coaching",
]

MARKETING_TITLES_NEGATIVE = [
    "chief marketing", "cmo", "vp marketing", "head of marketing",
    "directeur marketing", "directrice marketing", "marketing director",
    "marketing manager", "responsable marketing",
]

ICP_SIZE_MIN = 10
ICP_SIZE_MAX = 50


# ============================================================
# ÉTAPE 1 — Source A : Phantombuster
# ============================================================
def parse_duration_in_role(duration_str):
    if not duration_str:
        return None, ""
    s = duration_str.lower().replace("in role", "").strip()
    years, months = 0, 0
    y = re.search(r"(\d+)\s*year", s)
    m = re.search(r"(\d+)\s*month", s)
    if y:
        years = int(y.group(1))
    if m:
        months = int(m.group(1))
    total_days = years * 365 + months * 30
    if total_days == 0:
        return None, ""
    estimated_date = (datetime.now() - timedelta(days=total_days)).date()
    return total_days, estimated_date.isoformat()


def fetch_phantombuster():
    r = requests.get(
        "https://api.phantombuster.com/api/v2/agents/fetch-output",
        params={"id": PHANTOM_AGENT_ID},
        headers={"X-Phantombuster-Key": PHANTOM_API_KEY}
    )
    data = r.json()
    raw = json.loads(data.get("output", "[]"))

    profiles = []
    skipped = 0
    for p in raw:
        duration_str = p.get("durationInRole", "") or p.get("jobChangeDate", "")
        days_ago, estimated_date = parse_duration_in_role(duration_str)
        if days_ago is None or days_ago <= RECENCY_DAYS:
            profiles.append({
                "first_name":      p.get("firstName", ""),
                "last_name":       p.get("lastName", ""),
                "title":           p.get("jobTitle", ""),
                "company_name":    p.get("company", ""),
                "linkedin_url":    (p.get("linkedinUrl") or "").lower().strip("/"),
                "job_change_date": estimated_date,
                "email":           "",
                "company_size":    "",
                "industry":        "",
                "location":        "",
                "source":          "phantombuster",
            })
        else:
            skipped += 1

    print(f"[Source A] Phantombuster : {len(raw)} bruts | {len(profiles)} dans la fenêtre {RECENCY_DAYS}j | {skipped} ignorés")
    return profiles


# ============================================================
# ÉTAPE 2 — Source B : Apollo Search
# ============================================================
def fetch_apollo_search():
    payload = {
        "person_titles": [
            "directeur commercial", "directrice commerciale",
            "chief commercial officer", "head of sales",
            "sales director", "director of sales",
            "vp sales", "directeur du développement commercial"
        ],
        "organization_industry_tag_ids": [
            "computer software", "internet", "online media",
            "marketing and advertising", "computer & network security",
            "public relations and communications", "web design"
        ],
        "organization_num_employees_ranges": ["10,50"],
        "person_locations": ["France", "Belgium", "Switzerland"],
        "changed_jobs_within_days": RECENCY_DAYS,
        "per_page": 50,
        "page": 1,
    }
    r = requests.post(
        "https://api.apollo.io/v1/mixed_people/search",
        headers=APOLLO_HEADERS,
        json=payload
    )
    raw = r.json().get("people", [])

    profiles = []
    for p in raw:
        org = p.get("organization") or {}
        profiles.append({
            "first_name":      p.get("first_name", ""),
            "last_name":       p.get("last_name", ""),
            "title":           p.get("title", ""),
            "company_name":    org.get("name", ""),
            "linkedin_url":    (p.get("linkedin_url") or "").lower().strip("/"),
            "job_change_date": "",
            "email":           p.get("email", ""),
            "company_size":    org.get("estimated_num_employees", ""),
            "industry":        org.get("industry", ""),
            "location":        (p.get("city") or "") + ", " + (p.get("country") or ""),
            "source":          "apollo_search",
        })

    print(f"[Source B] Apollo Search : {len(profiles)} profils")
    return profiles


# ============================================================
# ÉTAPE 3 — Fusion & déduplication
# ============================================================
def merge_profiles(phantom_profiles, apollo_profiles):
    merged = {}

    for p in phantom_profiles:
        key = p["linkedin_url"] or f"{p['first_name'].lower()}_{p['last_name'].lower()}_{p['company_name'].lower()}"
        merged[key] = p

    apollo_only = 0
    duplicates = 0
    for p in apollo_profiles:
        key = p["linkedin_url"] or f"{p['first_name'].lower()}_{p['last_name'].lower()}_{p['company_name'].lower()}"
        if key in merged:
            for field in ["email", "company_size", "industry", "location"]:
                if not merged[key].get(field) and p.get(field):
                    merged[key][field] = p[field]
            merged[key]["source"] = "phantombuster + apollo"
            duplicates += 1
        else:
            merged[key] = p
            apollo_only += 1

    all_profiles = list(merged.values())
    print(f"[Fusion] {len(all_profiles)} uniques | {len(phantom_profiles)} PB | {apollo_only} Apollo seul | {duplicates} doublons fusionnés")
    return all_profiles


# ============================================================
# ÉTAPE 4 — Enrichissement Apollo (si données manquantes)
# ============================================================
def enrich_profiles(profiles):
    credits = 0
    for p in profiles:
        if p.get("industry") and p.get("company_size") and p.get("email"):
            continue
        payload = {
            "first_name":             p.get("first_name", ""),
            "last_name":              p.get("last_name", ""),
            "organization_name":      p.get("company_name", ""),
            "linkedin_url":           p.get("linkedin_url", ""),
            "reveal_personal_emails": False,
            "reveal_phone_number":    False,
        }
        try:
            r = requests.post(
                "https://api.apollo.io/v1/people/match",
                headers=APOLLO_HEADERS,
                json=payload
            )
            person = r.json().get("person") or {}
            org = person.get("organization") or {}
            if not p.get("email"):
                p["email"] = person.get("email", "")
            if not p.get("industry"):
                p["industry"] = org.get("industry", "")
            if not p.get("company_size"):
                p["company_size"] = org.get("estimated_num_employees", "")
            if not p.get("location"):
                p["location"] = (person.get("city") or "") + ", " + (person.get("country") or "")
            if not p.get("title"):
                p["title"] = person.get("title", "")
            credits += 1
        except Exception as e:
            print(f"Enrichissement échoué pour {p.get('first_name')} {p.get('last_name')} : {e}")

    print(f"[Enrichissement] Crédits Apollo utilisés : ~{credits} / 2500 mensuels")
    return profiles


# ============================================================
# ÉTAPE 5 — Scoring ICP
# ============================================================
def score_lead(lead):
    score = 0
    reasons = []
    title_lower = (lead.get("title") or "").lower()
    industry_lower = (lead.get("industry") or "").lower()
    team_titles = [t.lower() for t in lead.get("team_titles", [])]

    if not any(t in title_lower for t in ICP_TITLES):
        lead["score"] = 0
        lead["priority"] = "Hors cible — mauvaise fonction"
        lead["score_detail"] = "Fonction non cible, lead écarté"
        return lead

    score += 3
    reasons.append("Fonction cible +3")

    if any(i in industry_lower for i in ICP_INDUSTRIES_EXCLUDE):
        lead["score"] = 0
        lead["priority"] = "Hors cible — secteur exclu"
        lead["score_detail"] = f"Secteur exclu : {lead.get('industry', '')}"
        return lead

    if any(i in industry_lower for i in ICP_INDUSTRIES_INCLUDE):
        score += 2
        reasons.append("Secteur cible +2")
    else:
        reasons.append("Secteur non identifié +0")

    size = lead.get("company_size")
    if size:
        try:
            s = int(str(size).replace(",", "").split("-")[0])
            if ICP_SIZE_MIN <= s <= ICP_SIZE_MAX:
                score += 2
                reasons.append(f"Taille cible ({s} empl.) +2")
            else:
                reasons.append(f"Taille hors cible ({s} empl.) +0")
        except:
            pass

    has_marketing = any(m in t for m in MARKETING_TITLES_NEGATIVE for t in team_titles)
    if not has_marketing:
        score += 2
        reasons.append("Pas de marketing dédié +2")
    else:
        score -= 1
        reasons.append("Marketing dans l'équipe -1")

    jcd = lead.get("job_change_date", "")
    if jcd:
        try:
            days_ago = (date.today() - date.fromisoformat(jcd[:10])).days
            if days_ago <= 30:
                score += 1
                reasons.append(f"Signal très frais ({days_ago}j) +1")
            else:
                reasons.append(f"Signal frais ({days_ago}j) +0")
        except:
            pass

    score = max(0, score)
    if score >= 7:
        priority = "P1 — À contacter cette semaine"
    elif score >= 5:
        priority = "P2 — À contacter ce mois"
    elif score >= 3:
        priority = "P3 — À surveiller"
    else:
        priority = "Hors cible"

    lead["score"] = score
    lead["priority"] = priority
    lead["score_detail"] = " | ".join(reasons)
    return lead


# ============================================================
# ÉTAPE 6 — Google Sheets
# ============================================================
def push_to_sheets(qualified):
    creds = service_account.Credentials.from_service_account_info(
        CREDS_JSON, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)
    today = date.today().isoformat()

    rows = []
    for l in qualified:
        days_in_role = ""
        if l.get("job_change_date"):
            try:
                d = (date.today() - date.fromisoformat(l["job_change_date"][:10])).days
                days_in_role = f"{d}j dans le poste"
            except:
                pass

        angle = (
            f"Nouveau {l.get('title', '')} chez {l.get('company_name', '')} ({days_in_role}) — "
            f"structure commerciale en construction, moment idéal pour poser les bases avec Bonnes Manières."
        )

        rows.append([
            today,
            l.get("first_name", ""),
            l.get("last_name", ""),
            l.get("title", ""),
            l.get("company_name", ""),
            l.get("industry", ""),
            str(l.get("company_size", "")),
            l.get("location", ""),
            l.get("email", ""),
            l.get("linkedin_url", ""),
            str(l.get("score", "")) + "/10",
            l.get("priority", ""),
            l.get("job_change_date", ""),
            angle,
            l.get("score_detail", ""),
            l.get("source", ""),
        ])

    if rows:
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{SHEET_TAB}!A:P",
            valueInputOption="USER_ENTERED",
            body={"values": rows}
        ).execute()
        print(f"✅ {len(rows)} leads ajoutés au Google Sheet.")
    else:
        print("ℹ️ Aucun lead qualifié aujourd'hui.")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print(f"\n=== ROUTINE BONNES MANIÈRES — {date.today()} ===\n")

    phantom_profiles = fetch_phantombuster()
    apollo_profiles  = fetch_apollo_search()
    all_profiles     = merge_profiles(phantom_profiles, apollo_profiles)
    enriched         = enrich_profiles(all_profiles)
    scored           = [score_lead(l) for l in enriched]
    qualified        = [l for l in scored if l.get("score", 0) >= 5]

    print(f"\nLeads qualifiés (score ≥ 5) : {len(qualified)}")

    if qualified:
        print("\nTOP 3 LEADS DU JOUR :")
        for i, l in enumerate(sorted(qualified, key=lambda x: x["score"], reverse=True)[:3], 1):
            print(f"{i}. {l.get('first_name')} {l.get('last_name')} — {l.get('title')} chez {l.get('company_name')} | Score: {l.get('score')}/10 | {l.get('priority')}")
            print(f"   → {l.get('score_detail')}")

    push_to_sheets(qualified)
    print("\n=== FIN DE ROUTINE ===")
