import pandas as pd
import os
import sys
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata

# Usa solo l'output di regola_1 (F1). Se manca, esegui regola_1.py per generarlo.
F1_PATH = "data/processed/selezione_regola_1.csv"
UPCOMING_PATH = "data/raw/upcoming_matches.csv"
OUTPUT_PATH = "data/processed/bet.csv"
STORICO_PATH = "data/processed/storico.csv"

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


def append_and_update_storico(df_bet):
    storico_cols = [
        "match_id",
        "data",
        "campionato",
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
        "2025",
        "2024",
        "F1",
        "F2",
        "F3",
        "F4",
        "status_partita",
        "home_score",
        "away_score",
        "vincitore",
        "esito_pick",
        "quota_pick_api",
        "aggiornato_il",
    ]

    if os.path.exists(STORICO_PATH):
        storico_df = pd.read_csv(STORICO_PATH)
    else:
        storico_df = pd.DataFrame(columns=storico_cols)

    for col in storico_cols:
        if col not in storico_df.columns:
            storico_df[col] = ""

    if not df_bet.empty and "oggi" in df_bet.columns:
        df_oggi = df_bet[df_bet["oggi"] == "OGGI"].copy()
        if not df_oggi.empty:
            df_oggi = df_oggi.rename(columns={"quota": "quota_pick_api"})
            df_oggi["status_partita"] = ""
            df_oggi["home_score"] = ""
            df_oggi["away_score"] = ""
            df_oggi["vincitore"] = ""
            df_oggi["esito_pick"] = ""
            df_oggi["quota_pick_api"] = ""
            df_oggi["aggiornato_il"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            df_oggi = df_oggi[storico_cols]

            storico_df["_key"] = (
                storico_df["match_id"].astype(str).str.strip() + "|" +
                storico_df["squadra selezionata"].astype(str).str.strip().str.lower()
            )
            df_oggi["_key"] = (
                df_oggi["match_id"].astype(str).str.strip() + "|" +
                df_oggi["squadra selezionata"].astype(str).str.strip().str.lower()
            )
            existing_keys = set(storico_df["_key"].tolist())
            df_new = df_oggi[~df_oggi["_key"].isin(existing_keys)].copy()
            if not df_new.empty:
                storico_df = pd.concat([storico_df, df_new], ignore_index=True)
            storico_df = storico_df.drop(columns=["_key"], errors="ignore")

    if not storico_df.empty:
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
                selected_team = storico_df.at[idx, "squadra selezionata"]
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

    os.makedirs(os.path.dirname(STORICO_PATH), exist_ok=True)
    storico_df.to_csv(STORICO_PATH, index=False)

# Carica squadre selezionate da F1; se mancante, prova a generarlo eseguendo lo script `regola_1.py`
if not os.path.exists(F1_PATH):
    print(f"{F1_PATH} non trovato. Genera il file eseguendo `src/queries/regola_1.py` e riprova.")
    sys.exit(1)

df_agg = pd.read_csv(F1_PATH)
if "team_name" in df_agg.columns and "squadra" not in df_agg.columns:
    df_agg = df_agg.rename(columns={"team_name": "squadra"})
if "league_name" in df_agg.columns and "lega" not in df_agg.columns:
    df_agg = df_agg.rename(columns={"league_name": "lega"})
print(f"Squadre filtrate caricate da: {F1_PATH}")

# Carica partite in programma
if not os.path.exists(UPCOMING_PATH):
    raise FileNotFoundError(f"File non trovato: {UPCOMING_PATH}")
df_upcoming = pd.read_csv(UPCOMING_PATH)
df_upcoming["date"] = pd.to_datetime(df_upcoming["date"], utc=True)

# Mantieni solo partite future e non giocate
now_utc = pd.Timestamp.now(tz="UTC")
if "status" in df_upcoming.columns:
    allowed_statuses = {"NS", "TBD"}
    df_upcoming = df_upcoming[
        (df_upcoming["date"] >= now_utc) &
        (df_upcoming["status"].astype(str).str.upper().isin(allowed_statuses))
    ].copy()
else:
    df_upcoming = df_upcoming[df_upcoming["date"] >= now_utc].copy()

# Trova il prossimo incontro per ogni squadra selezionata
team_next_match = {}
for _, row in df_agg.iterrows():
    squadra = row["squadra"].strip().lower()
    filtri = row["filtri"]
    # Trova tutti i match futuri dove la squadra è home o away
    team_matches = df_upcoming[(df_upcoming["home_team"].str.strip().str.lower() == squadra) |
                               (df_upcoming["away_team"].str.strip().str.lower() == squadra)].copy()
    if not team_matches.empty:
        # Prendi il match con la data più vicina
        next_match = team_matches.sort_values("date").iloc[0]
        # Crea le colonne indicatori filtri
        filtro_cols = {}
        for f in ["F1", "F2", "F3", "F4"]:
            filtro_cols[f] = 'x' if f in filtri.split(',') else ''
        team_next_match[squadra] = {
            "match_id": next_match["match_id"],
            "squadra selezionata": next_match["home_team"] if next_match["home_team"].strip().lower() == squadra else next_match["away_team"],
            "campionato": next_match["league_name"],
            "squadra in casa": next_match["home_team"],
            "squadra fuori casa": next_match["away_team"],
            "data": next_match["date"].strftime("%d/%m/%y ore %H:%M"),
            "2025": row["2025"],
            "2024": row["2024"],
            **filtro_cols
        }

# Output finale
if team_next_match:
    df_out = pd.DataFrame(team_next_match.values())
    # Ordina per data (convertendo la colonna in datetime per ordinare correttamente)
    df_out['data_sort'] = pd.to_datetime(df_out['data'], format="%d/%m/%y ore %H:%M")
    oggi = datetime.now().strftime("%d/%m/%y")
    df_out['oggi'] = df_out['data'].apply(lambda x: 'OGGI' if x.startswith(oggi) else '')
    df_out = df_out.sort_values('data_sort').drop(columns=['data_sort'])

    append_and_update_storico(df_out.copy())

    df_out = df_out.drop(columns=['match_id'], errors='ignore')
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Merge completato. File salvato in {OUTPUT_PATH}\n")
    print(f"✅ Storico aggiornato in {STORICO_PATH}\n")
else:
    append_and_update_storico(pd.DataFrame())
    print("Nessuna partita trovata per le squadre selezionate.")
