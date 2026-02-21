import pandas as pd
import os
import requests
from datetime import datetime

API_KEY = "691ccc74c6d55850f0b5c836ec0b10f2"
HEADERS = {"x-apisports-key": API_KEY}

# Percorsi output filtri
FILTER_PATHS = [
    "data/processed/selected_teams_filter1.csv",
    "data/processed/selected_teams_filter2.csv",
    "data/processed/selected_teams_filter3.csv",
    "data/processed/selected_teams_filter4.csv",
    "data/processed/selected_teams_filter5.csv"
]
UPCOMING_PATH = "data/raw/upcoming_matches.csv"
OUTPUT_PATH = "data/processed/bet.csv"

# Unione squadre selezionate da tutti i filtri
selected_teams = set()
# Dizionario squadra -> lista filtri
team_to_filters = {}
for idx, path in enumerate(FILTER_PATHS, 1):
    if os.path.exists(path):
        df = pd.read_csv(path)
        if "team_name" in df.columns:
            teams = df["team_name"].str.strip().str.lower()
        elif "team_id" in df.columns:
            teams = df["team_id"].astype(str)
        else:
            teams = []
        for team in teams:
            selected_teams.add(team)
            if team not in team_to_filters:
                team_to_filters[team] = []
            team_to_filters[team].append(str(idx))

# Carica partite in programma
upcoming = pd.read_csv(UPCOMING_PATH)


# Trova il prossimo incontro per ogni squadra selezionata
from collections import defaultdict

# Prepara dizionario squadra -> lista match
team_next_match = {}
upcoming["date"] = pd.to_datetime(upcoming["date"])

for team in sorted(selected_teams, key=str.lower):
    # Trova tutti i match futuri dove la squadra è home o away
    team_matches = upcoming[(upcoming["home_team"].str.strip().str.lower() == team) |
                            (upcoming["away_team"].str.strip().str.lower() == team)].copy()
    if not team_matches.empty:
        # Prendi il match con la data più vicina
        next_match = team_matches.sort_values("date").iloc[0]
        # Crea le colonne mastermind
        filtro_cols = {}
        for i in range(1, 6):
            filtro_cols[f"F{i}"] = 'x' if str(i) in team_to_filters.get(team, []) else ''
        team_next_match[team] = {
            "squadra selezionata": next_match["home_team"] if next_match["home_team"].strip().lower() == team else next_match["away_team"],
            "campionato": next_match["league_name"],
            "squadra in casa": next_match["home_team"],
            "squadra fuori casa": next_match["away_team"],
            "data": pd.to_datetime(next_match["date"]).strftime("%d/%m/%y ore %H:%M"),
            **filtro_cols
        }

# Output finale
if team_next_match:
    df_out = pd.DataFrame(team_next_match.values())
    # Ordina per data (convertendo la colonna in datetime per ordinare correttamente)
    df_out['data_sort'] = pd.to_datetime(df_out['data'], format="%d/%m/%y ore %H:%M")
    # Evidenzia le partite della giornata odierna
    oggi = datetime.now().strftime("%d/%m/%y")
    df_out['oggi'] = df_out['data'].apply(lambda x: 'OGGI' if x.startswith(oggi) else '')
    df_out = df_out.sort_values('data_sort').drop(columns=['data_sort'])
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Merge completato. File salvato in {OUTPUT_PATH}\n")
    print(df_out)
else:
    print("Nessun match trovato per le squadre selezionate.")