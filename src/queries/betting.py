import argparse
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

# Usa solo l'output di regola_1 (F1). Se manca, esegui regola_1.py per generarlo.
F1_PATH = "data/processed/selezione_regola_1.csv"
UPCOMING_PATH = "data/raw/upcoming_matches.csv"
OUTPUT_PATH = "data/processed/bet.csv"
STORICO_PATH = "data/processed/storico.csv"
DEFAULT_TIMEOUT = 10

PARSER = argparse.ArgumentParser(description="Genera bet.csv e aggiorna storico.csv")
PARSER.add_argument(
    "--backfill-quotes",
    action="store_true",
    help="Prova a riempire le quote mancanti in storico.csv",
)
ARGS = PARSER.parse_args()


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


def pick_selected_odd(selected_team, home_team, away_team, odds):
    selected_norm = normalize_text(selected_team)
    home_norm = normalize_text(home_team)
    away_norm = normalize_text(away_team)
    if selected_norm == home_norm:
        return odds.get("home", "")
    if selected_norm == away_norm:
        return odds.get("away", "")
    return ""


def apply_direct_match_selection(df_out):
    if df_out.empty or "SC" not in df_out.columns:
        return df_out
    if not API_KEY:
        print("API_FOOTBALL_KEY non impostata: scontri diretti non risolti per quota.")
        return df_out

    odds_cache = {}
    for idx, row in df_out.iterrows():
        if str(row.get("SC", "")).strip().upper() != "SI":
            continue

        match_id = str(row.get("match_id", "")).strip()
        home_team = str(row.get("squadra in casa", "")).strip()
        away_team = str(row.get("squadra fuori casa", "")).strip()
        if not match_id or not home_team or not away_team:
            continue

        if match_id not in odds_cache:
            odds_cache[match_id] = fetch_fixture_odds(match_id, home_team, away_team)
        odds = odds_cache.get(match_id, {"home": "", "away": ""})
        preferred = pick_lowest_odd_team(home_team, away_team, odds)
        if preferred:
            df_out.at[idx, "squadra selezionata"] = preferred
    return df_out


def apply_selected_odds(df_out):
    if df_out.empty:
        return df_out
    if not API_KEY:
        print("API_FOOTBALL_KEY non impostata: quote non disponibili.")
        if "quota" not in df_out.columns:
            df_out["quota"] = ""
        return df_out

    odds_cache = {}
    if "quota" not in df_out.columns:
        df_out["quota"] = ""

    for idx, row in df_out.iterrows():
        match_id = str(row.get("match_id", "")).strip()
        home_team = str(row.get("squadra in casa", "")).strip()
        away_team = str(row.get("squadra fuori casa", "")).strip()
        selected_team = str(row.get("squadra selezionata", "")).strip()
        if not match_id or not home_team or not away_team or not selected_team:
            continue

        if match_id not in odds_cache:
            odds_cache[match_id] = fetch_fixture_odds(match_id, home_team, away_team)
        odds = odds_cache.get(match_id, {"home": "", "away": ""})
        df_out.at[idx, "quota"] = pick_selected_odd(selected_team, home_team, away_team, odds)

    return df_out


def backfill_missing_quotes():
    if not API_KEY:
        print("API_FOOTBALL_KEY non impostata: backfill quote non eseguito.")
        return
    if not os.path.exists(STORICO_PATH):
        print(f"Nessuno storico trovato: {STORICO_PATH}")
        return

    storico_df = pd.read_csv(STORICO_PATH)
    if storico_df.empty or "match_id" not in storico_df.columns:
        print("Storico vuoto o privo di match_id, nessun backfill eseguito.")
        return

    if "quota" not in storico_df.columns:
        storico_df["quota"] = ""
    storico_df["quota"] = storico_df["quota"].astype("string").fillna("")

    missing_mask = storico_df["quota"].astype(str).str.strip() == ""
    if not missing_mask.any():
        print("Nessuna quota mancante da riempire.")
        return

    odds_cache = {}
    updated = 0
    for idx, row in storico_df[missing_mask].iterrows():
        match_id = str(row.get("match_id", "")).strip()
        home_team = str(row.get("squadra in casa", "")).strip()
        away_team = str(row.get("squadra fuori casa", "")).strip()
        selected_team = str(row.get("squadra selezionata", "")).strip()
        if not match_id or not home_team or not away_team or not selected_team:
            continue

        if match_id not in odds_cache:
            odds_cache[match_id] = fetch_fixture_odds(match_id, home_team, away_team)
        odds = odds_cache.get(match_id, {"home": "", "away": ""})
        selected_odd = pick_selected_odd(selected_team, home_team, away_team, odds)
        if selected_odd:
            storico_df.at[idx, "quota"] = selected_odd
            updated += 1

    if updated:
        storico_df.to_csv(STORICO_PATH, index=False)
    print(f"Quote riempite: {updated}")


def append_and_update_storico(df_bet):
    storico_min_cols = [
        "match_id",
        "data",
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
        "quota",
    ]

    if os.path.exists(STORICO_PATH):
        storico_df = pd.read_csv(STORICO_PATH)
    else:
        storico_df = pd.DataFrame(columns=storico_min_cols)

    if "data" in storico_df.columns:
        storico_df["data"] = storico_df["data"].astype(str).str.split(" ore").str[0].str.strip()

    for col in storico_min_cols:
        if col not in storico_df.columns:
            storico_df[col] = ""
    for col in list(storico_df.columns):
        if col not in storico_min_cols:
            storico_df = storico_df.drop(columns=[col])

    if "quota" in storico_df.columns:
        storico_df["quota"] = storico_df["quota"].astype("string").fillna("")

    if not df_bet.empty:
        df_all = df_bet.copy()
        df_all["data"] = df_all["data"].astype(str).str.split(" ore").str[0].str.strip()
        df_all_min = df_all[[
            "match_id",
            "data",
            "squadra selezionata",
            "squadra in casa",
            "squadra fuori casa",
            "quota",
        ]].copy()

        storico_df["_key"] = storico_df["match_id"].astype(str).str.strip()
        df_all_min["_key"] = df_all_min["match_id"].astype(str).str.strip()

        selected_map = df_all_min.set_index("_key")["squadra selezionata"]
        selected_map = selected_map.dropna().astype(str).str.strip()
        selected_map = selected_map[selected_map != ""]
        if not selected_map.empty:
            missing_mask = (
                storico_df["_key"].isin(selected_map.index)
                & (storico_df["squadra selezionata"].astype(str).str.strip() == "")
            )
            storico_df.loc[missing_mask, "squadra selezionata"] = (
                storico_df.loc[missing_mask, "_key"].map(selected_map)
            )

        quota_map = df_all_min.set_index("_key")["quota"]
        quota_map = quota_map.dropna().astype(str).str.strip()
        quota_map = quota_map[quota_map != ""]
        if not quota_map.empty:
            missing_quota_mask = (
                storico_df["_key"].isin(quota_map.index)
                & (storico_df["quota"].astype(str).str.strip() == "")
            )
            storico_df.loc[missing_quota_mask, "quota"] = (
                storico_df.loc[missing_quota_mask, "_key"].map(quota_map)
            )

        existing_keys = set(storico_df["_key"].tolist())
        df_new = df_all_min[~df_all_min["_key"].isin(existing_keys)].copy()
        if not df_new.empty:
            storico_df = pd.concat([storico_df, df_new], ignore_index=True)
        storico_df = storico_df.drop(columns=["_key"], errors="ignore")

    try:
        storico_df["data_sort"] = pd.to_datetime(storico_df["data"], format="%d/%m/%y")
        storico_df = storico_df.sort_values("data_sort", ascending=False).drop(columns=["data_sort"])
    except Exception:
        pass

    os.makedirs(os.path.dirname(STORICO_PATH), exist_ok=True)
    storico_df.to_csv(STORICO_PATH, index=False)


if not os.path.exists(F1_PATH):
    print(f"{F1_PATH} non trovato. Genera il file eseguendo `src/queries/regola_1.py` e riprova.")
    sys.exit(1)

df_agg = pd.read_csv(F1_PATH)
if "team_name" in df_agg.columns and "squadra" not in df_agg.columns:
    df_agg = df_agg.rename(columns={"team_name": "squadra"})
if "league_name" in df_agg.columns and "lega" not in df_agg.columns:
    df_agg = df_agg.rename(columns={"league_name": "lega"})
print(f"Squadre filtrate caricate da: {F1_PATH}")

if not os.path.exists(UPCOMING_PATH):
    raise FileNotFoundError(f"File non trovato: {UPCOMING_PATH}")
df_upcoming = pd.read_csv(UPCOMING_PATH)
df_upcoming["date"] = pd.to_datetime(df_upcoming["date"], utc=True)

now_utc = pd.Timestamp.now(tz="UTC")
today_utc = now_utc.normalize()
if "status" in df_upcoming.columns:
    allowed_statuses = {"NS", "TBD"}
    df_upcoming = df_upcoming[
        (df_upcoming["date"].dt.normalize() >= today_utc) &
        (df_upcoming["status"].astype(str).str.upper().isin(allowed_statuses))
    ].copy()
else:
    df_upcoming = df_upcoming[df_upcoming["date"].dt.normalize() >= today_utc].copy()

from collections import OrderedDict

selected_teams = set(df_agg["squadra"].astype(str).str.strip().str.lower())
team_next_match = OrderedDict()
for _, row in df_agg.iterrows():
    squadra = row["squadra"].strip().lower()
    filtri = row["filtri"] if "filtri" in row else ""
    lega = row["lega"] if "lega" in row else ""
    rank_2025 = row["2025"] if "2025" in row else ""
    rank_2024 = row["2024"] if "2024" in row else ""
    team_matches = df_upcoming[(df_upcoming["home_team"].str.strip().str.lower() == squadra) |
                               (df_upcoming["away_team"].str.strip().str.lower() == squadra)].copy()
    if not team_matches.empty:
        next_match = team_matches.sort_values("date").iloc[0]
        scontro_diretto = "SI" if {
            next_match["home_team"].strip().lower(),
            next_match["away_team"].strip().lower(),
        }.issubset(selected_teams) else ""
        team_next_match[squadra] = {
            "match_id": next_match["match_id"],
            "squadra selezionata": next_match["home_team"] if next_match["home_team"].strip().lower() == squadra else next_match["away_team"],
            "squadra in casa": next_match["home_team"],
            "squadra fuori casa": next_match["away_team"],
            "data": next_match["date"].strftime("%d/%m/%y ore %H:%M"),
            "SC": scontro_diretto,
            "lega": lega,
            "2025": rank_2025,
            "2024": rank_2024,
            "filtri": filtri,
        }

if team_next_match:
    df_out = pd.DataFrame(team_next_match.values())
    df_out["squadre_set"] = df_out.apply(
        lambda r: frozenset([r["squadra in casa"].strip().lower(), r["squadra fuori casa"].strip().lower()]),
        axis=1,
    )
    df_out = df_out.drop_duplicates(subset=["squadre_set", "data"])
    df_out = df_out.drop(columns=["squadre_set"])
    df_out["data_sort"] = pd.to_datetime(df_out["data"], format="%d/%m/%y ore %H:%M")
    today_date = datetime.now().date()
    df_out = df_out[df_out["data_sort"].dt.date <= today_date].copy()
    oggi = datetime.now().strftime("%d/%m/%y")
    df_out["oggi"] = df_out["data"].apply(lambda x: "OGGI" if x.startswith(oggi) else "")
    df_out = df_out.sort_values("data_sort").drop(columns=["data_sort"])

    df_out = apply_direct_match_selection(df_out)
    df_out = apply_selected_odds(df_out)

    append_and_update_storico(df_out.copy())

    df_out = df_out[df_out["oggi"] == "OGGI"].copy()
    df_out = df_out.drop(columns=["match_id"], errors="ignore")
    colonne_finali = [
        "squadra selezionata",
        "quota",
        "squadra in casa",
        "squadra fuori casa",
        "data",
        "oggi",
        "SC",
        "lega",
        "2025",
        "2024",
        "filtri",
    ]
    colonne_presenti = [c for c in colonne_finali if c in df_out.columns]
    altre_colonne = [c for c in df_out.columns if c not in colonne_presenti]
    df_out = df_out[colonne_presenti + altre_colonne]
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"Partite di oggi esportate in bet.csv: {len(df_out)}")
    print(f"✅ Merge completato. File salvato in {OUTPUT_PATH}\n")
    print(f"✅ Storico aggiornato in {STORICO_PATH}\n")
else:
    append_and_update_storico(pd.DataFrame())
    print("Nessuna partita trovata per le squadre selezionate.")

if ARGS.backfill_quotes:
    backfill_missing_quotes()
