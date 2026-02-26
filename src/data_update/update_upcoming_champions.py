# src/data_update/update_upcoming_champions.py

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from datetime import datetime, timedelta
import unicodedata
import os
import sys
import importlib.util

# === CONFIG ===
API_KEY = "691ccc74c6d55850f0b5c836ec0b10f2"
BASE_URL = "https://v3.football.api-sports.io"
LEAGUE_ID = 2  # Champions League
HEADERS = {"x-apisports-key": API_KEY}
DEFAULT_TIMEOUT = 10

def create_session(retries=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = create_session()

#
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

loader_spec = importlib.util.spec_from_file_location("project_loader", os.path.join(PROJECT_ROOT, "project_loader.py"))
if loader_spec is None or loader_spec.loader is None:
    raise ImportError("Impossibile caricare project_loader.py")
project_loader = importlib.util.module_from_spec(loader_spec)
loader_spec.loader.exec_module(project_loader)
load_project_module = project_loader.load_project_module
PROJECT_ROOT = project_loader.PROJECT_ROOT

season_config = load_project_module("season_config", "season_config.py")
STAGIONE_CORRENTE = season_config.STAGIONE_CORRENTE

SEASON = STAGIONE_CORRENTE  # stagione corrente (2025/2026)
# === CONFIG ===


OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_champions.csv")

# Mapping per normalizzazione
TEAM_NAME_MAPPING = {
    "bayern munchen": "bayern munich",
    "borussia monchengladbach": "borussia m'gladbach",
    "borussia mönchengladbach": "borussia m'gladbach",
    "man united": "manchester united",
    "man city": "manchester city",
    "psg": "paris saint germain",
    # ...aggiungi altri mapping se necessario...
}

def normalize(s, mapping=None):
    s = str(s).strip().lower()
    s = unicodedata.normalize('NFKD', s).encode('ascii', errors='ignore').decode()
    if mapping:
        return mapping.get(s, s)
    return s

def normalize_df(df, team_cols=None):
    if team_cols:
        for col in team_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: normalize(x, TEAM_NAME_MAPPING))
    return df

def check_api_connection():
    url = f"{BASE_URL}/status"
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        print("✅ Connessione API-FOOTBALL OK")
        return True
    except requests.RequestException as e:
        print(f"❌ Connessione API-FOOTBALL non disponibile: {e}")
        return False

if not check_api_connection():
    print("⛔ Interruzione aggiornamento: API non raggiungibile.")
    sys.exit(1)

# Calcolo intervallo date (oggi → +30 giorni)
date_from = datetime.today().strftime("%Y-%m-%d")
date_to = (datetime.today() + timedelta(days=30)).strftime("%Y-%m-%d")

# === API REQUEST ===
url = f"{BASE_URL}/fixtures"
params = {
    "league": LEAGUE_ID,
    "season": SEASON,
    "from": date_from,
    "to": date_to,
    "status": "NS"
}

response = SESSION.get(url, headers=HEADERS, params=params, timeout=DEFAULT_TIMEOUT)

# === PARSE + SAVE ===
if response.status_code == 200:
    matches = response.json()["response"]
    print(f"\n✅ Partite Champions trovate: {len(matches)}")

    rows = []
    for match in matches:
        fixture = match["fixture"]
        teams = match["teams"]
        rows.append({
            "match_id": fixture["id"],
            "date": fixture["date"],
            "home_team": teams["home"]["name"],
            "away_team": teams["away"]["name"]
        })

    df = pd.DataFrame(rows)
    # Normalizza squadre
    df = normalize_df(df, team_cols=["home_team", "away_team"])
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Salvate {len(df)} partite Champions normalizzate in {OUTPUT_PATH}")

else:
    print(f"\n❌ Errore API: {response.status_code}")
    print(response.text)