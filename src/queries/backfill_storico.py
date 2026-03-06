import os
import sys
from datetime import datetime
import unicodedata

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from api_config import API_KEY, HEADERS

F1_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "selezione_regola_1.csv")
MATCHES_CURRENT_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "all_matches_current.csv")
MATCHES_ARCHIVE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "all_matches_archive.csv")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "storico_retro.csv")
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


def pick_selected_odd(selected_team, home_team, away_team, odds):
    selected_norm = normalize_text(selected_team)
    home_norm = normalize_text(home_team)
    away_norm = normalize_text(away_team)
    if selected_norm == home_norm:
        return odds.get("home", "")
    if selected_norm == away_norm:
        return odds.get("away", "")
    return ""


def pick_lowest_odd_team(home_team, away_team, odds):
    home_odd_raw = str(odds.get("home", "")).strip()
    away_odd_raw = str(odds.get("away", "")).strip()
    try:
        home_odd = float(home_odd_raw) if home_odd_raw else None
    except ValueError:
        home_odd = None
    try:
        away_odd = float(away_odd_raw) if away_odd_raw else None
    except ValueError:
        away_odd = None

    if home_odd is not None and away_odd is not None:
        return home_team if home_odd <= away_odd else away_team
    if home_odd is not None:
        return home_team
    if away_odd is not None:
        return away_team
    return ""


def load_selected_set():
    if not os.path.exists(F1_PATH):
        raise FileNotFoundError(f"File non trovato: {F1_PATH}")
    df = pd.read_csv(F1_PATH)
    if "squadra" not in df.columns and "team_name" in df.columns:
        df = df.rename(columns={"team_name": "squadra"})
    if "squadra" not in df.columns:
        raise ValueError("Colonna 'squadra' non trovata in selezione_regola_1.csv")
    return set(df["squadra"].astype(str).apply(normalize_text).tolist())


def load_matches():
    frames = []
    if os.path.exists(MATCHES_CURRENT_PATH):
        frames.append(pd.read_csv(MATCHES_CURRENT_PATH))
    if os.path.exists(MATCHES_ARCHIVE_PATH):
        frames.append(pd.read_csv(MATCHES_ARCHIVE_PATH))
    if not frames:
        raise FileNotFoundError("Nessun file match trovato in data/raw")
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["match_id"])
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    return df


def main():
    selected_set = load_selected_set()
    matches_df = load_matches()

    now_utc = pd.Timestamp.now(tz="UTC")
    start_date = pd.Timestamp(year=now_utc.year, month=1, day=1, tz="UTC")
    end_date = now_utc

    matches_df = matches_df[matches_df["date"].notna()].copy()
    matches_df = matches_df[(matches_df["date"] >= start_date) & (matches_df["date"] <= end_date)]

    matches_df["home_norm"] = matches_df["home_team"].astype(str).apply(normalize_text)
    matches_df["away_norm"] = matches_df["away_team"].astype(str).apply(normalize_text)

    is_home = matches_df["home_norm"].isin(selected_set)
    is_away = matches_df["away_norm"].isin(selected_set)
    matches_df = matches_df[is_home | is_away].copy()

    def pick_selected(row):
        home_in = row["home_norm"] in selected_set
        away_in = row["away_norm"] in selected_set
        if home_in and not away_in:
            return row["home_team"]
        if away_in and not home_in:
            return row["away_team"]
        return ""

    matches_df["squadra selezionata"] = matches_df.apply(pick_selected, axis=1)
    matches_df["SC"] = matches_df.apply(
        lambda r: "SI" if r["home_norm"] in selected_set and r["away_norm"] in selected_set else "",
        axis=1,
    )
    matches_df["data"] = matches_df["date"].dt.strftime("%d/%m/%y")

    matches_df["quota"] = ""
    if API_KEY:
        odds_cache = {}
        for idx, row in matches_df.iterrows():
            match_id = str(row.get("match_id", "")).strip()
            home_team = str(row.get("home_team", "")).strip()
            away_team = str(row.get("away_team", "")).strip()
            if not match_id or not home_team or not away_team:
                continue

            if match_id not in odds_cache:
                odds_cache[match_id] = fetch_fixture_odds(match_id, home_team, away_team)
            odds = odds_cache.get(match_id, {"home": "", "away": ""})

            selected_team = str(row.get("squadra selezionata", "")).strip()
            if not selected_team and str(row.get("SC", "")).strip().upper() == "SI":
                preferred = pick_lowest_odd_team(home_team, away_team, odds)
                if preferred:
                    matches_df.at[idx, "squadra selezionata"] = preferred
                    selected_team = preferred

            if selected_team:
                matches_df.at[idx, "quota"] = pick_selected_odd(selected_team, home_team, away_team, odds)
    else:
        print("API_FOOTBALL_KEY non impostata: quote non disponibili.")

    out_cols = [
        "match_id",
        "data",
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
        "quota",
        "SC",
        "league_name",
        "season",
    ]
    rename_map = {
        "home_team": "squadra in casa",
        "away_team": "squadra fuori casa",
    }
    output_df = matches_df.rename(columns=rename_map)
    output_df["data_sort"] = pd.to_datetime(output_df["data"], format="%d/%m/%y", errors="coerce")
    output_df = output_df.sort_values("data_sort", ascending=False).drop(columns=["data_sort"])
    output_df = output_df[out_cols]
    output_df = output_df[output_df["quota"].astype(str).str.strip() != ""]

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    output_df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Storico retro creato: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
