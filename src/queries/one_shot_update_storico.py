import os
import sys
import unicodedata

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from api_config import API_KEY, HEADERS

STORICO_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "storico.csv")
F1_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "selezione_regola_1.csv")

DEFAULT_TIMEOUT = 10


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


def normalize_text(value):
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode()
    return text


def fetch_fixture_odds(match_id, home_team, away_team):
    if not HEADERS:
        return {"home": "", "away": ""}
    url = f"https://v3.football.api-sports.io/odds?fixture={match_id}"
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        payload = response.json().get("response", [])
        if not payload:
            return {"home": "", "away": ""}

        home_norm = normalize_text(home_team)
        away_norm = normalize_text(away_team)
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

                    if home_odd or away_odd:
                        return {"home": home_odd, "away": away_odd}
        return {"home": "", "away": ""}
    except requests.RequestException:
        return {"home": "", "away": ""}


def pick_selected_team(home_team, away_team, selected_set, odds):
    home_norm = normalize_text(home_team)
    away_norm = normalize_text(away_team)
    home_in = home_norm in selected_set
    away_in = away_norm in selected_set

    if home_in and not away_in:
        return home_team
    if away_in and not home_in:
        return away_team
    if home_in and away_in:
        try:
            home_odd = float(odds.get("home", "")) if odds.get("home", "") != "" else None
        except ValueError:
            home_odd = None
        try:
            away_odd = float(odds.get("away", "")) if odds.get("away", "") != "" else None
        except ValueError:
            away_odd = None

        if home_odd is not None and away_odd is not None:
            return home_team if home_odd >= away_odd else away_team
        if home_odd is not None:
            return home_team
        if away_odd is not None:
            return away_team

    return ""


def main():
    if not API_KEY:
        print("API_FOOTBALL_KEY non impostata. Esempio: export API_FOOTBALL_KEY=la_tua_chiave")
        return

    if not os.path.exists(STORICO_PATH):
        print(f"Nessuno storico trovato: {STORICO_PATH}")
        return
    if not os.path.exists(F1_PATH):
        print(f"File filtri non trovato: {F1_PATH}")
        return

    storico_df = pd.read_csv(STORICO_PATH)
    if storico_df.empty or "match_id" not in storico_df.columns:
        print("Storico vuoto o privo di match_id, nessuna modifica eseguita.")
        return

    filtri_df = pd.read_csv(F1_PATH)
    if "squadra" not in filtri_df.columns:
        print("Colonna squadra non trovata nel file filtri.")
        return

    selected_set = set(filtri_df["squadra"].astype(str).apply(normalize_text).tolist())

    if "squadra selezionata" not in storico_df.columns:
        storico_df["squadra selezionata"] = ""

    odds_cache = {}

    updated = 0
    for idx in storico_df.index:
        if str(storico_df.at[idx, "squadra selezionata"]).strip():
            continue

        match_id = str(storico_df.at[idx, "match_id"]).strip()
        if not match_id:
            continue

        home_team = str(storico_df.at[idx, "squadra in casa"]).strip()
        away_team = str(storico_df.at[idx, "squadra fuori casa"]).strip()
        if not home_team or not away_team:
            continue

        if match_id not in odds_cache:
            odds_cache[match_id] = fetch_fixture_odds(match_id, home_team, away_team)
        odds = odds_cache.get(match_id, {"home": "", "away": ""})

        selected_team = pick_selected_team(home_team, away_team, selected_set, odds)
        if not selected_team:
            continue

        storico_df.at[idx, "squadra selezionata"] = selected_team
        updated += 1

    try:
        storico_df["data_sort"] = pd.to_datetime(storico_df["data"], format="%d/%m/%y")
        storico_df = storico_df.sort_values("data_sort", ascending=False).drop(columns=["data_sort"])
    except Exception:
        pass

    storico_df.to_csv(STORICO_PATH, index=False)
    print(f"✅ Storico aggiornato: {STORICO_PATH}")
    print(f"Righe aggiornate: {updated}")


if __name__ == "__main__":
    main()
