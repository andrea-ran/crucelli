import os
from datetime import datetime
import unicodedata

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

STORICO_PATH = os.path.join("data", "processed", "storico.csv")
API_KEY = os.getenv("API_FOOTBALL_KEY", "691ccc74c6d55850f0b5c836ec0b10f2")
HEADERS = {"x-apisports-key": API_KEY} if API_KEY else {}
DEFAULT_TIMEOUT = 10
FINISHED_STATUSES = {"FT", "AET", "PEN"}


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
        status_short = match["fixture"]["status"]["short"]

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
    if not os.path.exists(STORICO_PATH):
        print(f"Nessun file storico trovato: {STORICO_PATH}")
        return

    storico_df = pd.read_csv(STORICO_PATH)
    if storico_df.empty or "match_id" not in storico_df.columns:
        print("Storico vuoto o privo di match_id, nessun aggiornamento necessario.")
        return

    for col in ["status_partita", "home_score", "away_score", "vincitore", "esito_pick", "quota_pick_api", "aggiornato_il"]:
        if col not in storico_df.columns:
            storico_df[col] = ""

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
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for match_id in unique_match_ids:
        fixture_cache[match_id] = fetch_fixture_result(match_id)

    for idx in storico_df.index:
        match_id = str(storico_df.at[idx, "match_id"]).strip()
        if not match_id:
            continue

        fixture_data = fixture_cache.get(match_id)
        if not fixture_data:
            continue

        status = fixture_data.get("status", "")
        storico_df.at[idx, "status_partita"] = status

        home_score = fixture_data.get("home_score")
        away_score = fixture_data.get("away_score")
        storico_df.at[idx, "home_score"] = "" if home_score is None else home_score
        storico_df.at[idx, "away_score"] = "" if away_score is None else away_score

        if status in FINISHED_STATUSES:
            winner = fixture_data.get("winner", "")
            storico_df.at[idx, "vincitore"] = winner
            selected_team = storico_df.at[idx, "squadra selezionata"] if "squadra selezionata" in storico_df.columns else ""
            selected_norm = normalize_text(selected_team)
            winner_norm = normalize_text(winner) if winner else ""

            if winner_norm == "pareggio":
                storico_df.at[idx, "esito_pick"] = "PAREGGIO"
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
                storico_df.at[idx, "quota_pick_api"] = odd_value

        storico_df.at[idx, "aggiornato_il"] = now_str

    storico_df.to_csv(STORICO_PATH, index=False)
    print(f"✅ Storico risultati aggiornato: {STORICO_PATH}")


if __name__ == "__main__":
    update_storico_results()
