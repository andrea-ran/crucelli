# src/data_update/update_upcoming.py

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import os
import unicodedata
from datetime import datetime
import sys
import importlib.util

# Configurazione API
API_KEY = "691ccc74c6d55850f0b5c836ec0b10f2"
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

# Configurazione campionati e stagioni
LEAGUES = {
    "Saudi Professional League": 307,
    "HNL": 210,
    "Ligue 1": 61,
    "Bundesliga": 78,
    "Premier League": 39,
    "Serie A": 135,
    "Eredivisie": 88,
    "Liga Portugal": 94,
    "Premiership": 179,      # Scozia
    "LaLiga": 140,
    "Super Lig": 203
}

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

SEASON = STAGIONE_CORRENTE

# Percorso file output
UPCOMING_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_matches.csv")

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
LEAGUE_NAME_MAPPING = {
    "premiership": "scottish premiership",
    "pro league": "saudi pro league",
    # ...aggiungi altri mapping se necessario...
}

def normalize(s, mapping=None):
    s = str(s).strip().lower()
    s = unicodedata.normalize('NFKD', s).encode('ascii', errors='ignore').decode()
    if mapping:
        return mapping.get(s, s)
    return s

def normalize_df(df, team_cols=None, league_cols=None):
    if team_cols:
        for col in team_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: normalize(x, TEAM_NAME_MAPPING))
    if league_cols:
        for col in league_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: normalize(x, LEAGUE_NAME_MAPPING))
    return df

def fetch_upcoming_matches():
    all_matches = []
    for league_name, league_id in LEAGUES.items():
        print(f"📥 Recupero prossime partite: {league_name} ({league_id})...")
        url = f"https://v3.football.api-sports.io/fixtures?league={league_id}&season={SEASON}&next=20"
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        for match in data.get("response", []):
            all_matches.append({
                "match_id": match["fixture"]["id"],
                "date": match["fixture"]["date"],
                "league_id": match["league"]["id"],
                "league_name": match["league"]["name"],
                "season": match["league"]["season"],
                "home_team": match["teams"]["home"]["name"],
                "away_team": match["teams"]["away"]["name"],
                "home_score": match["goals"]["home"],
                "away_score": match["goals"]["away"],
                "status": match["fixture"]["status"]["short"]
            })
    return pd.DataFrame(all_matches)

def check_api_connection():
    url = "https://v3.football.api-sports.io/status"
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        print("✅ Connessione API-FOOTBALL OK")
        return True
    except requests.RequestException as e:
        print(f"❌ Connessione API-FOOTBALL non disponibile: {e}")
        return False

if __name__ == "__main__":
    if not check_api_connection():
        print("⛔ Interruzione aggiornamento: API non raggiungibile.")
        sys.exit(1)
    df = fetch_upcoming_matches()
    # Normalizza squadre e lega
    df = normalize_df(df, team_cols=["home_team", "away_team"], league_cols=["league_name"])
    os.makedirs(os.path.dirname(UPCOMING_PATH), exist_ok=True)
    df.to_csv(UPCOMING_PATH, index=False)
    print("✅ File upcoming_matches.csv aggiornato e normalizzato!")
