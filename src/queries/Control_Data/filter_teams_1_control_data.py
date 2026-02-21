import requests
from datetime import datetime
import pandas as pd

# Configurazione API
API_URL = "https://v3.football.api-sports.io/fixtures"
API_KEY = "691ccc74c6d55850f0b5c836ec0b10f2"  # Sostituisci con la tua chiave API
LEAGUE_ID = 78  # ID della Bundesliga (esempio)
SEASON = 2024  # Stagione corrente

# Percorso del file team_stats
STATS_PATH = "/Users/andrea/Desktop/crucelli/data/raw/team_stats.csv"

# Carica i dati
df_stats = pd.read_csv(STATS_PATH)

# Normalizza i nomi dei campionati
def normalize(s):
    return str(s).strip().lower()

df_stats["league_name"] = df_stats["league_name"].apply(normalize)

# Stagioni da analizzare
seasons = [2024, 2023, 2022]

# Analizza le prime 5 classificate per ogni stagione
for season in seasons:
    print(f"\n🔍 Analisi per la stagione {season}:")
    df_season = df_stats[df_stats["season"] == season]
    leagues = df_season["league_name"].unique()

    for league in leagues:
        # Filtra le prime 5 classificate
        df_league = df_season[(df_season["league_name"] == league) & (df_season["rank"] <= 5)]

        print(f"\n🏆 Campionato: {league.capitalize()} (Prime 5 classificate)")
        print(df_league[["team_name", "rank", "points"]].sort_values("rank").to_string(index=False))

def get_current_matchday():
    headers = {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": API_KEY
    }
    params = {
        "league": LEAGUE_ID,
        "season": SEASON
    }
    try:
        response = requests.get("https://v3.football.api-sports.io/standings", headers=headers, params=params)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            standings = data.get("response", [])
            if standings:
                # Estrai il numero di partite giocate dalla prima squadra
                league_info = standings[0].get("league", {})
                standings_data = league_info.get("standings", [[]])[0]  # Primo gruppo di standings
                if standings_data:
                    current_matchday = standings_data[0].get("all", {}).get("played")  # Numero di partite giocate
                    return current_matchday
                else:
                    print("Nessuna informazione disponibile nelle standings.")
                    return None
            else:
                print("Nessuna informazione disponibile nella risposta.")
                return None
        else:
            print(f"Errore API: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Errore di connessione: {e}")
        return None

# Esegui la funzione
current_matchday = get_current_matchday()
if current_matchday:
    print(f"Giornata corrente: {current_matchday}")
else:
    print("Impossibile determinare la giornata corrente.")