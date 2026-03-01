import os
import math
from datetime import datetime, timezone, timedelta
import unicodedata
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.synonyms import normalize_team_name

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STORICO_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "storico.csv")
API_KEY = os.getenv("API_FOOTBALL_KEY", "691ccc74c6d55850f0b5c836ec0b10f2")
HEADERS = {"x-apisports-key": API_KEY} if API_KEY else {}
DEFAULT_TIMEOUT = 10
FINISHED_STATUSES = {"FT", "AET", "PEN"}
MATCH_DURATION_MINUTES = 100  # base duration used to estimate remaining time


def normalize_text(value):
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode()
    return text


def create_session(retries=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = create_session()


def fetch_fixture_result(match_id):
    if not HEADERS:
        return None
    url = f"https://v3.football.api-sports.io/fixtures?id={match_id}"
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        payload = response.json().get("response", [])
        if not payload:
            return None
        match = payload[0]
        home_team = match["teams"]["home"]["name"]
        away_team = match["teams"]["away"]["name"]
        home_score = match["goals"]["home"]
        away_score = match["goals"]["away"]
        fixture_info = match["fixture"]
        status_info = fixture_info["status"]
        status_short = status_info["short"]
        elapsed = status_info.get("elapsed")
        timestamp = fixture_info.get("timestamp")

        winner = ""
        if home_score is not None and away_score is not None:
            if home_score > away_score:
                winner = home_team
            elif away_score > home_score:
                winner = away_team
            else:
                winner = "pareggio"

        return {
            "status": status_short,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "winner": winner,
            "elapsed": elapsed,
            "timestamp": timestamp,
        }
    except requests.RequestException:
        return None


def fetch_selected_team_odd(match_id, selected_team, home_team, away_team):
    if not HEADERS:
        return ""
    url = f"https://v3.football.api-sports.io/odds?fixture={match_id}"
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        payload = response.json().get("response", [])
        if not payload:
            return ""

        selected_norm = normalize_text(selected_team)
        home_norm = normalize_text(home_team)
        away_norm = normalize_text(away_team)
        pick_side = ""
        if selected_norm == home_norm:
            pick_side = "home"
        elif selected_norm == away_norm:
            pick_side = "away"

        if not pick_side:
            return ""

        for market_container in payload:
            bookmakers = market_container.get("bookmakers", [])
            for bookmaker in bookmakers:
                for bet in bookmaker.get("bets", []):
                    values = bet.get("values", [])
                    if not values:
                        continue
                    home_odd = ""
                    away_odd = ""
                    for item in values:
                        raw_value = str(item.get("value", "")).strip()
                        odd = str(item.get("odd", "")).strip()
                        norm_value = normalize_text(raw_value)
                        if norm_value in {"home", "1"} or norm_value == home_norm:
                            home_odd = odd
                        elif norm_value in {"away", "2"} or norm_value == away_norm:
                            away_odd = odd

                    if pick_side == "home" and home_odd:
                        return home_odd
                    if pick_side == "away" and away_odd:
                        return away_odd
        return ""
    except requests.RequestException:
        return ""


def update_storico_results():
    # Forza la rimozione delle ore dalla colonna data
    if not os.path.exists(STORICO_PATH):
        print(f"Nessun file storico trovato: {STORICO_PATH}")
        return

    storico_df = pd.read_csv(STORICO_PATH)
    if "data" in storico_df.columns:
        storico_df["data"] = storico_df["data"].astype(str).str.split(" ore").str[0].str.strip()

    if storico_df.empty or "match_id" not in storico_df.columns:
        print("Storico vuoto o privo di match_id, nessun aggiornamento necessario.")
        return

    string_cols = [
        "hs",
        "as",
        "vincitore",
        "esito_pick",
        "quota",
    ]

    for col in string_cols:
        if col not in storico_df.columns:
            storico_df[col] = ""
        if storico_df[col].dtype != "string":
            storico_df[col] = storico_df[col].astype("string")

    unique_match_ids = (
        storico_df["match_id"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    fixture_cache = {}
    odds_cache = {}

    for match_id in unique_match_ids:
        fixture_cache[match_id] = fetch_fixture_result(match_id)

    for idx in storico_df.index:
        match_id = str(storico_df.at[idx, "match_id"]).strip()
        if not match_id:
            continue

        fixture_data = fixture_cache.get(match_id)
        if not fixture_data:
            continue

        # Normalize data column to show only the calendar date
        data_value = str(storico_df.at[idx, "data"]).strip()
        if data_value:
            try:
                parsed = datetime.strptime(data_value, "%d/%m/%y ore %H:%M")
                storico_df.at[idx, "data"] = parsed.strftime("%d/%m/%y")
            except ValueError:
                if " ore" in data_value:
                    storico_df.at[idx, "data"] = data_value.split(" ore")[0].strip()

        home_score = fixture_data.get("home_score")
        away_score = fixture_data.get("away_score")
        # Forza sempre la conversione a int se non None
        storico_df.at[idx, "hs"] = "" if home_score is None else str(int(home_score))
        storico_df.at[idx, "as"] = "" if away_score is None else str(int(away_score))

        status = fixture_data.get("status", "")
        if status in FINISHED_STATUSES:
            winner = fixture_data.get("winner", "")
            storico_df.at[idx, "vincitore"] = winner
            selected_team = storico_df.at[idx, "squadra selezionata"] if "squadra selezionata" in storico_df.columns else ""

            selected_norm = normalize_team_name(selected_team)
            winner_norm = normalize_team_name(winner) if winner else ""

            if winner_norm == "pareggio":
                storico_df.at[idx, "esito_pick"] = "PERSA"
            elif winner_norm and selected_norm == winner_norm:
                storico_df.at[idx, "esito_pick"] = "VINTA"
            elif winner_norm:
                storico_df.at[idx, "esito_pick"] = "PERSA"

            cache_key = f"{match_id}|{selected_norm}"
            if cache_key not in odds_cache:
                odds_cache[cache_key] = fetch_selected_team_odd(
                    match_id=match_id,
                    selected_team=selected_team,
                    home_team=fixture_data.get("home_team", ""),
                    away_team=fixture_data.get("away_team", ""),
                )

            odd_value = odds_cache.get(cache_key, "")
            if odd_value:
                storico_df.at[idx, "quota"] = odd_value

        # Rimuovi colonne non più usate
        for col in ["status_partita", "aggiornato_il", "home_score", "away_score"]:
            if col in storico_df.columns:
                storico_df = storico_df.drop(columns=[col])

    # Scrivi solo le colonne desiderate, in ordine pulito
    colonne_finali = [
        "match_id",
        "data",
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
        "hs",
        "as",
        "vincitore",
        "esito_pick",
        "quota",
    ]
    colonne_presenti = [c for c in colonne_finali if c in storico_df.columns]
    storico_df = storico_df[colonne_presenti]
    storico_df.to_csv(STORICO_PATH, index=False)
    print(f"✅ Storico risultati aggiornato: {STORICO_PATH}")


# --- POST-PROCESSING: Mantieni solo la riga con quota più alta per match_id tra squadre selezionate che si affrontano ---
import numpy as np
storico_path = STORICO_PATH if 'STORICO_PATH' in globals() else 'data/processed/storico.csv'
df = pd.read_csv(storico_path)

# Funzione per scegliere la riga con quota più alta o segnare n.p.
def pick_max_quota(group):
    # Se una sola riga, restituisci così com'è
    if len(group) == 1:
        row = group.iloc[0].copy()
        if pd.isna(row['quota']) or str(row['quota']).strip() == '':
            row['esito_pick'] = 'n.p.'
        return row
    # Se più righe, scegli quella con quota massima (se entrambe vuote, n.p.)
    group = group.copy()
    group['quota_num'] = pd.to_numeric(group['quota'], errors='coerce')
    if group['quota_num'].isnull().all():
        # Nessuna quota disponibile
        row = group.iloc[0].copy()
        row['esito_pick'] = 'n.p.'
        return row
    else:
        idx_max = group['quota_num'].idxmax()
        return group.loc[idx_max].drop('quota_num')

# Applica la funzione per ogni match_id
new_df = df.groupby('match_id', as_index=False).apply(pick_max_quota)
# Se il groupby crea un multiindex, resetta
if isinstance(new_df.index, pd.MultiIndex):
    new_df = new_df.reset_index(drop=True)
# Salva il risultato
new_df.to_csv(storico_path, index=False)
print(f"✅ Post-processing: mantenute solo le righe con quota più alta per match_id in {storico_path}")


# --- Filtra solo partite concluse (con hs e as valorizzati e non vuoti/nulli) ---
mask_giocata = (~new_df['hs'].isnull()) & (~new_df['as'].isnull()) & (new_df['hs'].astype(str).str.strip() != '') & (new_df['as'].astype(str).str.strip() != '')
new_df = new_df[mask_giocata].copy()
# Riordina per data dopo il filtro
try:
    new_df['data_sort'] = pd.to_datetime(new_df['data'], format="%d/%m/%y")
    new_df = new_df.sort_values('data_sort', ascending=False).drop(columns=['data_sort'])
except Exception as e:
    print(f"[WARN] Ordinamento per data fallito: {e}")
new_df.to_csv(storico_path, index=False)
print(f"✅ Storico filtrato: solo partite concluse in {storico_path}")


if __name__ == "__main__":
    update_storico_results()
