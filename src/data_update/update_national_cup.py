import requests
import pandas as pd
import os
import importlib.util

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

loader_spec = importlib.util.spec_from_file_location("project_loader", os.path.join(PROJECT_ROOT, "project_loader.py"))
if loader_spec is None or loader_spec.loader is None:
    raise ImportError("Impossibile caricare project_loader.py")
project_loader = importlib.util.module_from_spec(loader_spec)
loader_spec.loader.exec_module(project_loader)
load_project_module = project_loader.load_project_module
PROJECT_ROOT = project_loader.PROJECT_ROOT

season_config = load_project_module("season_config", "season_config.py")
STAGIONE_PRECEDENTE = season_config.STAGIONE_PRECEDENTE

# Configurazione
API_KEY = "691ccc74c6d55850f0b5c836ec0b10f2"
HEADERS = {"x-apisports-key": API_KEY}
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "coppa_nazionale.csv")

# Dizionario: nome coppa nazionale -> id lega API-SPORTS
NATIONAL_CUPS = {
    "Coppa Italia": 137,           # Italia
    "FA Cup": 45,                  # Inghilterra
    "Copa del Rey": 143,           # Spagna
    "DFB Pokal": 81,               # Germania
    "Coupe de France": 66,         # Francia
    "Taca de Portugal": 97,        # Portogallo
    "KNVB Beker": 89,              # Olanda
    "Croatian Cup": 213,           # Croazia
    "King Cup": 308,               # Arabia Saudita
    "Turkish Cup": 206,            # Turchia
    "Scottish Cup": 181            # Scozia
}

winners = []

for cup_name, league_id in NATIONAL_CUPS.items():
    url = "https://v3.football.api-sports.io/fixtures"
    params = {
        "league": league_id,
        "season": STAGIONE_PRECEDENTE
    }
    response = requests.get(url, headers=HEADERS, params=params)
    data = response.json()
    try:
        # Cerca la finale
        finali = [
            f for f in data["response"]
            if "final" in f["league"]["round"].lower()
        ]
        if not finali:
            print(f"⚠️ Finale non trovata per {cup_name}")
            continue
        finale = finali[0]
        if finale["teams"]["home"]["winner"]:
            team_name = finale["teams"]["home"]["name"]
        else:
            team_name = finale["teams"]["away"]["name"]
        winners.append({"team_name": team_name, "season": STAGIONE_PRECEDENTE, "cup": cup_name})
        print(f"🏆 {cup_name} {STAGIONE_PRECEDENTE}: {team_name}")
    except Exception as e:
        print(f"⚠️ Errore per {cup_name}: {e}")

# Salva su CSV
df = pd.DataFrame(winners)
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False)
print(f"\n✅ File coppa_nazionale.csv aggiornato in {OUTPUT_PATH}")