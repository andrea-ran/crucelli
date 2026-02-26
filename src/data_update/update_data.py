import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import os
import json
import datetime
import unicodedata
import sys
import importlib.util

# Configurazione API
API_KEY = "691ccc74c6d55850f0b5c836ec0b10f2"
HEADERS = {"x-apisports-key": API_KEY}

# Configurazione campionati
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
# Configurazione stagioni da analizzare

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
STAGIONE_PRECEDENTE = season_config.STAGIONE_PRECEDENTE
STAGIONE_PENULTIMA = season_config.STAGIONE_PENULTIMA
STAGIONE_TERZULTIMA = season_config.STAGIONE_TERZULTIMA

SEASONS = [STAGIONE_TERZULTIMA, STAGIONE_PENULTIMA, STAGIONE_PRECEDENTE, STAGIONE_CORRENTE]

# Percorsi file
LAST_UPDATE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "last_update.json")
MATCHES_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "all_matches_current.csv")
STATS_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_current.csv")
UPCOMING_MATCHES_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_matches.csv")

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

# Session globale con retry e timeout
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

# Controlla se c'è connessione Internet
def check_connection():
    try:
        SESSION.get("https://www.google.com", timeout=3)
        return True
    except requests.RequestException:
        return False

# Controlla ultima data di aggiornamento
def get_last_update():
    if not os.path.exists(LAST_UPDATE_PATH):
        return None
    with open(LAST_UPDATE_PATH, "r") as file:
        return json.load(file).get("last_update")

# Aggiorna la data dell'ultimo aggiornamento
def update_last_update():
    os.makedirs(os.path.dirname(LAST_UPDATE_PATH), exist_ok=True)
    with open(LAST_UPDATE_PATH, "w") as file:
        json.dump({"last_update": str(datetime.date.today())}, file)

# Funzione per determinare la giornata corrente
def get_current_matchday(league_id, season):
    url = f"https://v3.football.api-sports.io/standings?league={league_id}&season={season}"
    try:
        resp = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        standings = data.get("response", [])
        if standings:
            standings_data = standings[0].get("league", {}).get("standings", [[]])[0]
            if standings_data:
                return standings_data[0].get("all", {}).get("played")  # Numero di partite giocate
    except requests.RequestException as e:
        print(f"⚠️ Impossibile determinare la giornata corrente per la lega {league_id}, stagione {season}: {e}")
    return None

# Scarica e aggiorna all_matches.csv
def update_matches():
    print("\n🔄 Aggiornamento partite in corso...")
    all_matches = []
    for league_name, league_id in LEAGUES.items():
        season = STAGIONE_CORRENTE
        print(f"📥 Recupero partite: {league_name} ({league_id}), stagione {season}...")
        url = f"https://v3.football.api-sports.io/fixtures?league={league_id}&season={season}"
        try:
            resp = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"⚠️ Errore fetching matches for {league_name} ({league_id}): {e}")
            continue

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

    df = pd.DataFrame(all_matches)
    # Normalizza squadre e leghe
    df = normalize_df(df, team_cols=["home_team", "away_team"], league_cols=["league_name"])
    os.makedirs(os.path.dirname(MATCHES_PATH), exist_ok=True)
    df.to_csv(MATCHES_PATH, index=False)
    print("✅ all_matches_current.csv aggiornato e normalizzato!")

# Scarica e aggiorna team_stats.csv
def update_team_stats():
    print("\n🔄 Aggiornamento statistiche squadre in corso...")
    all_stats = []
    for league_name, league_id in LEAGUES.items():
        season = STAGIONE_CORRENTE
        print(f"📥 Recupero statistiche squadre: {league_name} ({league_id}), stagione {season}...")
        url = f"https://v3.football.api-sports.io/standings?league={league_id}&season={season}"
        try:
            resp = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"⚠️ Errore fetching team stats for {league_name} ({league_id}): {e}")
            continue

        resp_list = data.get("response", [])
        if not resp_list:
            print(f"⚠️ Nessun dato per {league_name} {season}, salto...")
            continue
        for team in resp_list[0]["league"]["standings"][0]:
            all_stats.append({
                "team_id": team["team"]["id"],
                "team_name": team["team"]["name"],
                "league_id": league_id,
                "league_name": league_name,
                "season": season,
                "rank": team["rank"],
                "points": team["points"],
                "played": team["all"]["played"],
                "won": team["all"]["win"],
                "draw": team["all"]["draw"],
                "lost": team["all"]["lose"],
                "goals_for": team["all"]["goals"]["for"],
                "goals_against": team["all"]["goals"]["against"],
                "goal_diff": team["goalsDiff"]
            })

    df = pd.DataFrame(all_stats)

    # Ottieni la giornata corrente per ogni lega
    print("\n🔄 Recupero della giornata corrente...")
    current_matchdays = {}
    for league_name, league_id in LEAGUES.items():
        current_matchdays[league_name] = get_current_matchday(league_id, SEASONS[-1])  # Usa la stagione più recente
    df["current_matchday"] = df["league_name"].map(current_matchdays)

    # Normalizza squadre e leghe
    df = normalize_df(df, team_cols=["team_name"], league_cols=["league_name"])

    # Salva i dati aggiornati
    os.makedirs(os.path.dirname(STATS_PATH), exist_ok=True)
    df.to_csv(STATS_PATH, index=False)
    print(f"✅ team_stats_current.csv aggiornato e normalizzato!")

# Main
if __name__ == "__main__":
    if not check_connection():
        print("❌ Nessuna connessione a Internet. Riprova più tardi.")
    elif not check_api_connection():
        print("⛔ Interruzione aggiornamento: API non raggiungibile.")
        sys.exit(1)
    else:
        last_update = get_last_update()
        today = str(datetime.date.today())
        if last_update != today:
            try:
                update_matches()
                update_team_stats()
            except KeyboardInterrupt:
                print("\n⛔ Aggiornamento interrotto dall'utente.")
                sys.exit(1)
            update_last_update()
            print("✅ Dati aggiornati con successo!")
        else:
            print("✅ I dati sono già aggiornati alla data odierna.")