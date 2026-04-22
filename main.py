import requests
import json
import os
import re
import csv
import io
from datetime import datetime, timedelta, date
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Configuration via GitHub Secrets ---
PHANTOM_API_KEY  = os.environ["PHANTOM_API_KEY"]
PHANTOM_AGENT_ID = os.environ["PHANTOM_AGENT_ID"]
APOLLO_API_KEY   = os.environ["APOLLO_API_KEY"]
GOOGLE_SHEET_ID  = os.environ["GOOGLE_SHEET_ID"]
CREDS_JSON       = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
SHEET_TAB        = "Leads Pipeline"
RECENCY_DAYS     = 90

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
    "technology", "information technology", "développement de logiciels",
    "technologie", "services et conseil aux entreprises", "services de publicité",
    "produits logiciels", "services de données",
]

ICP_INDUSTRIES_EXCLUDE = [
    "staffing and recruiting", "outsourcing/offshoring",
    "managed services", "it services",
    "management consulting", "business consulting", "consulting",
    "professional training & coaching", "e-learning", "training",
    "human resources", "hr", "coaching",
    "enseignement", "administration", "services législatifs",
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
    # Récupérer les infos de l'agent pour trouver l'URL du fichier résultat
    r = requests.get(
        "https://api.phantombuster.com/api/v2/agents/fetch",
        params={"id": PHANTOM_AGENT_ID},
        headers={"X-Phantombuster-Key": PHANTOM_API_KEY}
    )
    agent_info = r.json()
    print(f"[Debug PB] Agent status: {agent_info.get('agent', {}).get('lastEndMessage', 'N/A')}")

    # Essayer de récupérer le fichier résultat CSV via S3
    s3_folder = agent_info.get("agent", {}).get("s3Folder", "")
    org_s3     = agent_info.get("agent", {}).get("orgS3Folder", "")
    result_url = ""

    if s3_folder:
        result_url = f"https://phantombuster.s3.amazonaws.com/{s3_folder}/result.csv"
    elif org_s3:
        result_url = f"https://phantombuster.s3.amazonaws.com/{org_s3}/result.csv"

    raw_profiles = []

    # Tenter de télécharger le CSV depuis S3
    if result_url:
        try:
            csv_r = requests.get(result_url, timeout=30)
            if csv_r.status_code == 200:
                reader = csv.DictReader(io.StringIO(csv_r.text))
                raw_profiles = list(reader)
                print(f"[Debug PB] CSV S3 téléchargé : {len(raw_profiles)} lignes")
        except Exception as e:
            print(f"[Debug PB] Échec S3 : {e}")

    # Fallback : fetch-output (resultObject ou output)
    if not raw_profiles:
        r2 = requests.get(
            "https://api.phantombuster.com/api/v2/agents/fetch-output",
            params={"id": PHANTOM_AGENT_ID},
            headers={"X-Phantombuster-Key": PHANTOM_API_KEY}
        )
        data = r2.json()
        print(f"[Debug PB] fetch-output keys: {list(data.keys())}")

        # Essayer resultObject d'abord
        result_obj = data.get("resultObject", "")
        if result_obj:
            try:
                raw_profiles = json.loads(result_obj)
                print(f"[Debug PB] resultObject parsé : {len(raw_profiles)} profils")
            except:
                # Peut-être que c'est du CSV dans resultObject
                try:
                    reader = csv.DictReader(io.StringIO(result_obj))
                    raw_profiles = list(reader)
                    print(f"[Debug PB] resultObject CSV : {len(raw_profiles)} profils")
                except Exception as e:
                    print(f"[Debug PB] Échec parsing resultObject : {e}")

        # Fallback sur output
        if not raw_profiles:
            output = data.get("output", "[]")
            try:
                raw_profiles = json.loads(output)
                print(f"[Debug PB] output JSON : {len(raw_profiles)} profils")
            except:
                print(f"[Debug PB] output non parsable (longueur: {len(output)})")

    profiles = []
    skipped = 0
    for p in raw_profiles:
        # Gérer les deux formats : JSON (camelCase) et CSV (camelCase aussi)
        duration_str = p.get("durationInRole", "") or p.get("jobChangeDate", "")
        days_ago, estimated_date = parse_duration_in_role(duration_str)

        # LinkedIn URL : préférer linkedInProfileUrl (standard) sur profileUrl (Sales Nav)
        linkedin_url = (
            p.get("linkedInProfileUrl") or
            p.get("defaultProfileUrl") or
            p.get("linkedinUrl") or ""
        ).lower().strip("/")

        if days_ago is None or days_ago <= RECENCY_DAYS:
            profiles.append({
                "first_name":      p.get("firstName", ""),
                "last_name":       p.get("lastName", ""),
                "title":           p.get("title", "") or p.get("jobTitle", ""),
                "company_name":    p.get("companyName", "") or p.get("company", ""),
                "linkedin_url":    linkedin_url,
                "job_change_date": estimated_date,
                "email":           "",
                "company_size":    "",
                "industry":        p.get("industry", ""),
                "location":        p.get("location", ""),
                "source":          "phantombuster",
            })
        else:
            skipped += 1

    print(f"[Source A] Phantombuster : {len(raw_profiles)} bruts | {len(profiles)} dans la fenêtre {RECENCY_DAYS}j | {skipped} ignorés")
    return profiles


# ============================================================
# ÉTAPE 2 — Enrichissement Apollo (si données manquantes)
# ============================================================
def enrich_profiles(profiles):
    credits = 0
    for p in profiles:
        if p.get("email") and p.get("company_size"):
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
            if not p.get("company_size"):
                p["company_size"] = org.get("estimated_num_employees", "")
            if not p.get("industry"):
                p["industry"] = org.get("industry", "")
            if not p.get("location"):
                p["location"] = (person.get("city") or "") + ", " + (person.get("country") or "")
            credits += 1
        except Exception as e:
            print(f"Enrichissement échoué pour {p.get('first_name')} {p.get('last_name')} : {e}")

    print(f"[Enrichissement] Crédits Apollo utilisés : ~{credits} / 5000 mensuels")
    return profiles


# ============================================================
# ÉTAPE 3 — Scoring ICP
# ============================================================
def score_lead(lead):
    score = 0
    reasons = []
    title_lower = (lead.get("title") or "").lower()
    industry_lower = (lead.get("industry") or "").lower()

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
        reasons.append(f"Secteur non identifié ({lead.get('industry', 'vide')}) +0")

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
    else:
        reasons.append("Taille inconnue +0")

    # Pas de marketing dédié détecté → +2 par défaut (on n'a pas les titres d'équipe)
    score += 2
    reasons.append("Pas de marketing dédié détecté +2")

    jcd = lead.get("job_change_date", "")
    if jcd:
        try:
            days_ago = (date.today() - date.fromisoformat(jcd[:10])).days
            if days_ago <= 30:
                score += 1
                reasons.append(f"Signal très frais ({days_ago}j) +1")
            else:
                reasons.append(f"Signal ({days_ago}j) +0")
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
# ÉTAPE 4 — Google Sheets
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
            f"Nouveau·elle {l.get('title', '')} chez {l.get('company_name', '')} ({days_in_role}) — "
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

    service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{SHEET_TAB}!A:P",
        valueInputOption="USER_ENTERED",
        body={"values": rows}
    ).execute()
    print(f"✅ {len(rows)} leads ajoutés au Google Sheet.")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print(f"\n=== ROUTINE BONNES MANIÈRES — {date.today()} ===\n")

    profiles  = fetch_phantombuster()
    enriched  = enrich_profiles(profiles)
    scored    = [score_lead(l) for l in enriched]
    qualified = [l for l in scored if l.get("score", 0) >= 5]

    print(f"\nLeads qualifiés (score ≥ 5) : {len(qualified)}")

    if len(qualified) > 0:
        print("\nTOP 3 LEADS DU JOUR :")
        for i, l in enumerate(sorted(qualified, key=lambda x: x["score"], reverse=True)[:3], 1):
            print(f"{i}. {l.get('first_name')} {l.get('last_name')} — {l.get('title')} chez {l.get('company_name')} | Score: {l.get('score')}/10 | {l.get('priority')}")
            print(f"   → {l.get('score_detail')}")
        push_to_sheets(qualified)
    else:
        print("ℹ️ Aucun lead qualifié aujourd'hui — fin de routine.")
        # Afficher les 3 premiers profils scorés pour debug
        if scored:
            print("\n[Debug] Aperçu des 3 premiers profils scorés :")
            for l in scored[:3]:
                print(f"  - {l.get('first_name')} {l.get('last_name')} | {l.get('title')} | Score: {l.get('score')} | {l.get('score_detail')}")

    print("\n=== FIN DE ROUTINE ===")
