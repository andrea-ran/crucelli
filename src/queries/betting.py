import pandas as pd
import numpy as np
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

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.synonyms import normalize_team_name


def should_skip_storico_update():
    return "--skip-storico-update" in sys.argv


def update_storico_results():
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

        for col in ["status_partita", "aggiornato_il", "home_score", "away_score"]:
            if col in storico_df.columns:
                storico_df = storico_df.drop(columns=[col])

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
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
        "hs",
        "as",
        "vincitore",
        "esito_pick",
        "quota_pick_api",
    ]

    if os.path.exists(STORICO_PATH):
        storico_df = pd.read_csv(STORICO_PATH)
    else:
        storico_df = pd.DataFrame(columns=storico_cols)

    storico_cols = [
        "match_id",
        "data",
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
        "hs",
        "as",
        "vincitore",
        "esito_pick",
        "quota_pick_api",
    ]

    if os.path.exists(STORICO_PATH):
        storico_df = pd.read_csv(STORICO_PATH)
    else:
        storico_df = pd.DataFrame(columns=storico_cols)

    # Rimuovi orario dalla colonna data in tutto lo storico
    if "data" in storico_df.columns:
        storico_df["data"] = storico_df["data"].astype(str).str.split(" ore").str[0].str.strip()

    if os.path.exists(STORICO_PATH):
        storico_df = pd.read_csv(STORICO_PATH)
    else:
        storico_df = pd.DataFrame(columns=storico_cols)

    # Filtra lo storico: mantieni solo le partite dove 'squadra selezionata' è tra quelle selezionate dalla regola
    if not df_bet.empty and "squadra selezionata" in df_bet.columns:
        squadre_selezionate = set(df_bet["squadra selezionata"].astype(str).str.strip().str.lower())
        storico_df = storico_df[storico_df["squadra selezionata"].astype(str).str.strip().str.lower().isin(squadre_selezionate)].copy()

    # Assicura che tutte le colonne richieste siano presenti
    for col in storico_cols:
        if col not in storico_df.columns:
            storico_df[col] = ""
    # Rimuovi tutte le colonne non richieste
    for col in list(storico_df.columns):
        if col not in storico_cols:
            storico_df = storico_df.drop(columns=[col])

    if not df_bet.empty and "oggi" in df_bet.columns:
        df_oggi = df_bet[df_bet["oggi"] == "OGGI"].copy()
        if not df_oggi.empty:
            df_oggi = df_oggi.rename(columns={"quota": "quota_pick_api"})
            df_oggi["hs"] = ""
            df_oggi["as"] = ""
            df_oggi["vincitore"] = ""
            df_oggi["esito_pick"] = ""
            df_oggi["quota_pick_api"] = ""
            # NON modificare la colonna data qui: lasciamo l'orario per bet.csv
            # La rimozione dell'orario avviene solo su storico_df più sotto
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

    # Nessun aggiornamento di risultati/quote qui: solo struttura e popolamento
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

from collections import OrderedDict
# Trova il prossimo incontro per ogni squadra selezionata e aggiungi colonne filtro
team_next_match = OrderedDict()
for _, row in df_agg.iterrows():
    squadra = row["squadra"].strip().lower()
    filtri = row["filtri"] if "filtri" in row else ""
    lega = row["lega"] if "lega" in row else ""
    rank_2025 = row["2025"] if "2025" in row else ""
    rank_2024 = row["2024"] if "2024" in row else ""
    # Trova tutti i match futuri dove la squadra è home o away
    team_matches = df_upcoming[(df_upcoming["home_team"].str.strip().str.lower() == squadra) |
                               (df_upcoming["away_team"].str.strip().str.lower() == squadra)].copy()
    if not team_matches.empty:
        # Prendi il match con la data più vicina
        next_match = team_matches.sort_values("date").iloc[0]
        # Inserisci tutte le colonne filtro richieste
        team_next_match[squadra] = {
            "match_id": next_match["match_id"],
            "squadra selezionata": next_match["home_team"] if next_match["home_team"].strip().lower() == squadra else next_match["away_team"],
            "squadra in casa": next_match["home_team"],
            "squadra fuori casa": next_match["away_team"],
            "data": next_match["date"].strftime("%d/%m/%y ore %H:%M"),
            "lega": lega,
            "2025": rank_2025,
            "2024": rank_2024,
            "filtri": filtri,
        }

# Output finale
if team_next_match:
    df_out = pd.DataFrame(team_next_match.values())
    # Rimuovi incontri doppi tra squadre selezionate (indipendentemente da casa/trasferta)
    df_out['squadre_set'] = df_out.apply(lambda r: frozenset([r['squadra in casa'].strip().lower(), r['squadra fuori casa'].strip().lower()]), axis=1)
    df_out = df_out.drop_duplicates(subset=['squadre_set', 'data'])
    df_out = df_out.drop(columns=['squadre_set'])
    # Ordina per data (convertendo la colonna in datetime per ordinare correttamente)
    df_out['data_sort'] = pd.to_datetime(df_out['data'], format="%d/%m/%y ore %H:%M")
    oggi = datetime.now().strftime("%d/%m/%y")
    df_out['oggi'] = df_out['data'].apply(lambda x: 'OGGI' if x.startswith(oggi) else '')
    df_out = df_out.sort_values('data_sort').drop(columns=['data_sort'])

    # NON modificare la colonna data qui: lasciamo l'orario per bet.csv
    append_and_update_storico(df_out.copy())

    df_out = df_out.drop(columns=['match_id'], errors='ignore')
    # Ordina le colonne in modo leggibile: squadra selezionata, squadra in casa, squadra fuori casa, data, oggi, lega, 2025, 2024, filtri
    colonne_finali = [
        "squadra selezionata", "squadra in casa", "squadra fuori casa", "data", "oggi", "lega", "2025", "2024", "filtri"
    ]
    colonne_presenti = [c for c in colonne_finali if c in df_out.columns]
    altre_colonne = [c for c in df_out.columns if c not in colonne_presenti]
    df_out = df_out[colonne_presenti + altre_colonne]
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Merge completato. File salvato in {OUTPUT_PATH}\n")
    print(f"✅ Storico aggiornato in {STORICO_PATH}\n")
    if not should_skip_storico_update():
        update_storico_results()
else:
    append_and_update_storico(pd.DataFrame())
    print("Nessuna partita trovata per le squadre selezionate.")
    if not should_skip_storico_update():
        update_storico_results()
