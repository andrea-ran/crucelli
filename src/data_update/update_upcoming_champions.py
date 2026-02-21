# src/data_update/update_upcoming_champions.py

import requests
import pandas as pd
from datetime import datetime, timedelta
import unicodedata
import os

# === CONFIG ===
API_KEY = "691ccc74c6d55850f0b5c836ec0b10f2"
BASE_URL = "https://v3.football.api-sports.io"
LEAGUE_ID = 2  # Champions League

#

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from season_config import STAGIONE_CORRENTE

SEASON = STAGIONE_CORRENTE  # stagione corrente (2025/2026)
# === CONFIG ===


OUTPUT_PATH = "data/raw/upcoming_champions.csv"

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

# Calcolo intervallo date (oggi → +30 giorni)
date_from = datetime.today().strftime("%Y-%m-%d")
date_to = (datetime.today() + timedelta(days=30)).strftime("%Y-%m-%d")

# === API REQUEST ===
url = f"{BASE_URL}/fixtures"
headers = {
    "x-apisports-key": API_KEY
}
params = {
    "league": LEAGUE_ID,
    "season": SEASON,
    "from": date_from,
    "to": date_to,
    "status": "NS"
}

response = requests.get(url, headers=headers, params=params)

# === PARSE + SAVE ===
if response.status_code == 200:
    matches = response.json()["response"]
    print(f"\n✅ Partite trovate: {len(matches)}")

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
    print(f"✅ Salvate {len(df)} partite normalizzate in {OUTPUT_PATH}")

else:
    print(f"\n❌ Errore API: {response.status_code}")
    print(response.text)