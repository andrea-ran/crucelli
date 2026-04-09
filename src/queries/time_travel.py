import argparse
import json
import os
import sys
from datetime import datetime
from collections import defaultdict
import unicodedata

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from api_config import API_KEY, HEADERS
from src.synonyms import normalize_league_name, normalize_team_name
import season_config

ARCHIVE_STATS_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_archive.csv")
CURRENT_STATS_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_current.csv")
COPPA_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "coppa_nazionale.csv")
CHAMPIONS_SLOTS_PATH = os.path.join(PROJECT_ROOT, "champions_slots.json")

MATCHES_CURRENT_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "all_matches_current.csv")
MATCHES_ARCHIVE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "all_matches_archive.csv")

OUTPUT_SELECTION_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "selezione_regola_1_time_travel.csv")
OUTPUT_DAY_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "bet_time_travel.csv")

FINISHED_STATUSES = {"FT", "AET", "PEN"}
DEFAULT_TIMEOUT = 10
ODDS_COLUMNS = ["quota_1", "quota_x", "quota_2"]


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


def parse_as_of_date(value):
    if not value:
        return None
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError("Formato data non valido per --as-of-date")


def normalize_text(value):
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode()
    return text

def safe_normalize_league_name(value):
    if value is None:
        return ""
    return normalize_league_name(str(value))


def _extract_match_odds(values, home_norm, away_norm):
    odds = {"home": "", "draw": "", "away": ""}
    for item in values:
        raw_value = str(item.get("value", "")).strip()
        odd = str(item.get("odd", "")).strip()
        norm_value = normalize_text(raw_value)
        if norm_value in {"home", "1"} or norm_value == home_norm:
            odds["home"] = odd
        elif norm_value in {"away", "2"} or norm_value == away_norm:
            odds["away"] = odd
        elif norm_value in {"draw", "x", "tie", "pareggio"}:
            odds["draw"] = odd
    return odds


def fetch_fixture_odds(match_id, home_team, away_team):
    if not HEADERS:
        return {"home": "", "draw": "", "away": ""}
    url = f"https://v3.football.api-sports.io/odds?fixture={match_id}"
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        payload = response.json().get("response", [])
        if not payload:
            return {"home": "", "draw": "", "away": ""}

        home_norm = normalize_text(home_team)
        away_norm = normalize_text(away_team)
        for market_container in payload:
            bookmakers = market_container.get("bookmakers", [])
            for bookmaker in bookmakers:
                for bet in bookmaker.get("bets", []):
                    values = bet.get("values", [])
                    if not values:
                        continue
                    bet_name = normalize_text(bet.get("name", ""))
                    if "1x2" not in bet_name and "match winner" not in bet_name and "full time result" not in bet_name and "fulltime result" not in bet_name:
                        continue
                    odds = _extract_match_odds(values, home_norm, away_norm)
                    if odds["home"] or odds["away"] or odds["draw"]:
                        return odds

        for market_container in payload:
            bookmakers = market_container.get("bookmakers", [])
            for bookmaker in bookmakers:
                bets = bookmaker.get("bets", [])
                if not bets:
                    continue
                values = bets[0].get("values", [])
                if not values:
                    continue
                odds = _extract_match_odds(values, home_norm, away_norm)
                if odds["home"] or odds["away"] or odds["draw"]:
                    return odds
        return {"home": "", "draw": "", "away": ""}
    except requests.RequestException:
        return {"home": "", "draw": "", "away": ""}


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


def apply_odds(df_out):
    if df_out.empty:
        return df_out
    if not API_KEY:
        if "quota" not in df_out.columns:
            df_out["quota"] = ""
        for col in ODDS_COLUMNS:
            if col not in df_out.columns:
                df_out[col] = ""
        return df_out

    odds_cache = {}
    if "quota" not in df_out.columns:
        df_out["quota"] = ""
    for col in ODDS_COLUMNS:
        if col not in df_out.columns:
            df_out[col] = ""

    for idx, row in df_out.iterrows():
        match_id = str(row.get("match_id", "")).strip()
        home_team = str(row.get("squadra in casa", "")).strip()
        away_team = str(row.get("squadra fuori casa", "")).strip()
        if not match_id or not home_team or not away_team:
            continue

        if match_id not in odds_cache:
            odds_cache[match_id] = fetch_fixture_odds(match_id, home_team, away_team)
        odds = odds_cache.get(match_id, {"home": "", "draw": "", "away": ""})
        df_out.at[idx, "quota_1"] = odds.get("home", "")
        df_out.at[idx, "quota_x"] = odds.get("draw", "")
        df_out.at[idx, "quota_2"] = odds.get("away", "")

        selected_team = str(row.get("squadra selezionata", "")).strip()
        if selected_team:
            df_out.at[idx, "quota"] = pick_selected_odd(selected_team, home_team, away_team, odds)

    return df_out


def load_matches():
    frames = []
    if os.path.exists(MATCHES_ARCHIVE_PATH):
        frames.append(pd.read_csv(MATCHES_ARCHIVE_PATH))
    if os.path.exists(MATCHES_CURRENT_PATH):
        frames.append(pd.read_csv(MATCHES_CURRENT_PATH))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    return df


def compute_standings_as_of(matches_df, season, as_of_date):
    if matches_df.empty:
        return pd.DataFrame()

    df = matches_df.copy()
    df = df[df["season"] == season].copy()
    if df.empty:
        return pd.DataFrame()

    df = df.dropna(subset=["date"]).copy()
    as_of_utc = pd.Timestamp(as_of_date, tz="UTC")
    df = df[df["date"].dt.normalize() <= as_of_utc].copy()
    if "status" in df.columns:
        df = df[df["status"].astype(str).str.upper().isin(FINISHED_STATUSES)].copy()

    if df.empty:
        return pd.DataFrame()

    records = []
    for _, row in df.iterrows():
        home = str(row.get("home_team", "")).strip()
        away = str(row.get("away_team", "")).strip()
        if not home or not away:
            continue
        try:
            home_score = int(float(row.get("home_score", 0)))
        except (TypeError, ValueError):
            home_score = 0
        try:
            away_score = int(float(row.get("away_score", 0)))
        except (TypeError, ValueError):
            away_score = 0

        if home_score > away_score:
            home_pts, away_pts = 3, 0
            home_w, away_w = 1, 0
            home_d, away_d = 0, 0
            home_l, away_l = 0, 1
        elif home_score < away_score:
            home_pts, away_pts = 0, 3
            home_w, away_w = 0, 1
            home_d, away_d = 0, 0
            home_l, away_l = 1, 0
        else:
            home_pts, away_pts = 1, 1
            home_w, away_w = 0, 0
            home_d, away_d = 1, 1
            home_l, away_l = 0, 0

        league_id = row.get("league_id", "")
        league_name = row.get("league_name", "")

        records.append({
            "team_name": home,
            "league_id": league_id,
            "league_name": league_name,
            "season": season,
            "points": home_pts,
            "played": 1,
            "won": home_w,
            "draw": home_d,
            "lost": home_l,
            "goals_for": home_score,
            "goals_against": away_score,
        })
        records.append({
            "team_name": away,
            "league_id": league_id,
            "league_name": league_name,
            "season": season,
            "points": away_pts,
            "played": 1,
            "won": away_w,
            "draw": away_d,
            "lost": away_l,
            "goals_for": away_score,
            "goals_against": home_score,
        })

    stats = pd.DataFrame(records)
    if stats.empty:
        return pd.DataFrame()

    agg = stats.groupby(["team_name", "league_id", "league_name", "season"], as_index=False).sum()
    agg["goal_diff"] = agg["goals_for"] - agg["goals_against"]
    agg["matches"] = agg["played"]

    agg = agg.sort_values(
        by=["league_id", "league_name", "points", "goal_diff", "goals_for", "team_name"],
        ascending=[True, True, False, False, False, True],
    ).copy()
    agg["rank"] = agg.groupby(["league_id", "league_name"], sort=False).cumcount() + 1
    agg["current_matchday"] = agg.groupby(["league_id", "league_name"], sort=False)["played"].transform("max")
    agg["team_id"] = ""
    return agg


def is_selected_match_last_home(upcoming_df, matches_df, season, league_norm, team_norm):
    if upcoming_df.empty or matches_df.empty:
        return False
    df_up = upcoming_df[upcoming_df["season"] == season].copy()
    if df_up.empty:
        return False
    df_up = df_up.dropna(subset=["date"]).copy()
    if df_up.empty:
        return False
    df_up = df_up[df_up["league_name"].apply(safe_normalize_league_name) == league_norm].copy()
    if df_up.empty:
        return False
    df_up["home_norm"] = df_up["home_team"].astype(str).apply(normalize_team_name)
    df_up = df_up[df_up["home_norm"] == team_norm].copy()
    if df_up.empty:
        return False

    df = matches_df[matches_df["season"] == season].copy()
    df = df.dropna(subset=["date"]).copy()
    df = df[df["league_name"].apply(safe_normalize_league_name) == league_norm].copy()
    if df.empty:
        return False
    df["home_norm"] = df["home_team"].astype(str).apply(normalize_team_name)
    df_home = df[df["home_norm"] == team_norm].copy()
    if df_home.empty:
        return False
    last_home_date = df_home["date"].max()
    if pd.isna(last_home_date):
        return False

    return (df_up["date"] == last_home_date).any()


def remove_runaway_leaders(df_all, filtri_per_squadra, upcoming_df, matches_df, season):
    df_season = df_all[df_all["season"] == season].copy()
    if df_season.empty:
        return

    leagues = (
        df_season["league_name"]
        .dropna()
        .astype(str)
        .apply(safe_normalize_league_name)
        .unique()
    )

    for league_norm in leagues:
        df_league = df_season[df_season["league_name"].apply(safe_normalize_league_name) == league_norm].copy()
        if len(df_league) < 2:
            continue
        df_sorted = df_league.sort_values("rank")
        prima = df_sorted.iloc[0]
        seconda = df_sorted.iloc[1]
        try:
            gap = float(prima["points"]) - float(seconda["points"])
        except (TypeError, ValueError):
            continue
        if gap < 14:
            continue

        team = str(prima["team_name"])
        team_norm = normalize_team_name(team)
        if is_selected_match_last_home(upcoming_df, matches_df, season, league_norm, team_norm):
            continue

        to_remove = [k for k in filtri_per_squadra.keys() if normalize_team_name(k) == team_norm]
        for key in to_remove:
            filtri_per_squadra.pop(key, None)


def remove_mid_gap_teams(df_all, filtri_per_squadra, season, champions_slots):
    df_season = df_all[df_all["season"] == season].copy()
    if df_season.empty:
        return

    leagues = (
        df_season["league_name"]
        .dropna()
        .astype(str)
        .apply(safe_normalize_league_name)
        .unique()
    )

    for league_norm in leagues:
        slot_champions = 4
        for league_key in champions_slots.keys():
            if normalize_league_name(league_key) == league_norm:
                slot_champions = champions_slots[league_key]
                break

        df_league = df_season[df_season["league_name"].apply(safe_normalize_league_name) == league_norm].copy()
        df_league = df_league.sort_values("rank")
        if len(df_league) <= slot_champions:
            continue

        prima = df_league.iloc[0]
        prima_punti = prima.get("points")
        first_non_champions = df_league.iloc[slot_champions]
        non_champions_punti = first_non_champions.get("points")
        if pd.isnull(prima_punti) or pd.isnull(non_champions_punti):
            continue

        df_champions = df_league.head(slot_champions).copy()
        for _, row in df_champions.iterrows():
            try:
                punti_team = float(row["points"])
            except (TypeError, ValueError):
                continue
            distacco_dalla_prima = float(prima_punti) - punti_team
            margine_su_non_champions = punti_team - float(non_champions_punti)
            if distacco_dalla_prima >= 8 and margine_su_non_champions >= 10:
                team_norm = normalize_team_name(str(row["team_name"]))
                to_remove = [k for k in filtri_per_squadra.keys() if normalize_team_name(k) == team_norm]
                for key in to_remove:
                    filtri_per_squadra.pop(key, None)


# --- Filtri copiati da regola_1.py ---

def filtro_1(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def get_champions_zone(df_local, stagione, slots):
        df_season = df_local[df_local["season"] == stagione].copy()
        df_season["league_name"] = df_season["league_name"].apply(safe_normalize_league_name)
        result = []
        for league, slot in slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)

    def get_near_champions_zone(df_local, stagione, slots, max_gap_points=3):
        df_season = df_local[df_local["season"] == stagione].copy()
        df_season["league_name"] = df_season["league_name"].apply(safe_normalize_league_name)
        result = []
        for league, slot in slots.items():
            league_norm = normalize_league_name(league)
            df_league = df_season[df_season["league_name"] == league_norm].sort_values("rank")
            if df_league.empty or len(df_league) < slot:
                continue
            soglia_champions = df_league.iloc[slot - 1]["points"]
            squadre_vicine = df_league[df_league["points"] >= (soglia_champions - max_gap_points)]["team_name"].tolist()
            result.extend(squadre_vicine)
        return set(result)

    def get_coppa_winners(df_coppa_local, stagione):
        return set(df_coppa_local[df_coppa_local["season"] == stagione]["team_name"].tolist())

    zone_corrente = get_champions_zone(df, stagione_corrente, champions_slots)
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots_prev)
    zone_vicina_precedente = get_near_champions_zone(df, stagione_precedente, champions_slots_prev, max_gap_points=3)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    ammesse_stagione_precedente = qualificate_precedente.union(zone_vicina_precedente)
    squadre_filtrate = [team for team in zone_corrente if team in ammesse_stagione_precedente]
    return squadre_filtrate


def filtro_2(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def get_champions_zone(df_local, stagione, slots):
        df_season = df_local[df_local["season"] == stagione].copy()
        df_season["team_name"] = df_season["team_name"].apply(normalize_team_name)
        df_season["league_name"] = df_season["league_name"].apply(safe_normalize_league_name)
        result = []
        for league, slot in slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)

    def get_coppa_winners(df_coppa_local, stagione):
        if df_coppa_local.empty or "season" not in df_coppa_local.columns or "team_name" not in df_coppa_local.columns:
            return set()
        df_coppa_season = df_coppa_local[df_coppa_local["season"] == stagione].copy()
        df_coppa_season["team_name"] = df_coppa_season["team_name"].apply(normalize_team_name)
        return set(df_coppa_season["team_name"].tolist())

    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    squadre_filtrate = []
    for league, slot in champions_slots.items():
        league_norm = normalize_league_name(league)
        df_league = df[(df["season"] == stagione_corrente) & (df["league_name"].apply(safe_normalize_league_name) == league_norm)].copy()
        df_league["team_name"] = df_league["team_name"].apply(normalize_team_name)
        if df_league.empty:
            continue
        df_sorted = df_league.sort_values("rank")
        if len(df_sorted) < slot:
            continue
        soglia_champions = df_sorted.iloc[slot - 1]["points"]

        def get_matches(row):
            if "matches" in row and not pd.isnull(row["matches"]):
                return row["matches"]
            if "played" in row and not pd.isnull(row["played"]):
                return row["played"]
            return None

        partite_champions = get_matches(df_sorted.iloc[slot - 1])
        df_out = df_league[df_league["rank"] > slot]
        for _, row in df_out.iterrows():
            team = row["team_name"]
            punti = row["points"]
            partite_giocate = get_matches(row)
            punti_dalla_zona_champions = soglia_champions - punti
            partite_in_meno_rispetto_ultima_champions = (
                partite_champions - partite_giocate
                if partite_champions is not None and partite_giocate is not None
                else None
            )
            condizione_precedente = team in qualificate_precedente
            if condizione_precedente:
                if partite_in_meno_rispetto_ultima_champions == 1:
                    if 0 <= punti_dalla_zona_champions <= 6:
                        squadre_filtrate.append(team)
                else:
                    if 0 <= punti_dalla_zona_champions <= 3:
                        squadre_filtrate.append(team)
    return squadre_filtrate


def filtro_3(df, stagione_corrente, stagione_penultima, stagione_terzultima, champions_slots):
    squadre_filtrate = []
    leghe_correnti = (
        df[df["season"] == stagione_corrente]["league_name"]
        .dropna()
        .astype(str)
        .apply(safe_normalize_league_name)
        .unique()
    )

    for league_norm in leghe_correnti:
        df_corrente = df[(df["season"] == stagione_corrente) & (df["league_name"].apply(safe_normalize_league_name) == league_norm)].copy()
        if df_corrente.empty:
            continue

        df_corrente = df_corrente.sort_values("rank")
        prima = df_corrente.iloc[0]
        punti_prima = prima["points"]
        partite_prima = prima["matches"] if "matches" in df_corrente.columns else None

        slot_champions = 4
        for league_key in champions_slots.keys():
            if normalize_league_name(league_key) == league_norm:
                slot_champions = champions_slots[league_key]
                break
        if len(df_corrente) < slot_champions:
            slot_champions = len(df_corrente)
        squadre_zona_champions = set(df_corrente.sort_values("rank").head(slot_champions)["team_name"].tolist())

        for _, row in df_corrente.iterrows():
            team = row["team_name"]
            punti_team = row["points"]
            partite_team = row["matches"] if "matches" in df_corrente.columns else None

            distacco_dalla_prima = punti_prima - punti_team
            partite_in_meno_della_prima = None
            if partite_prima is not None and partite_team is not None:
                partite_in_meno_della_prima = partite_prima - partite_team

            condizione_corrente = (
                (row["rank"] <= 2) or
                (distacco_dalla_prima <= 6) or
                (distacco_dalla_prima <= 8 and partite_in_meno_della_prima == 1)
            )

            condizione_zona_champions = team in squadre_zona_champions

            storico_penultima = df[
                (df["season"] == stagione_penultima) &
                (df["league_name"].apply(safe_normalize_league_name) == league_norm) &
                (df["team_name"] == team)
            ]
            storico_terzultima = df[
                (df["season"] == stagione_terzultima) &
                (df["league_name"].apply(safe_normalize_league_name) == league_norm) &
                (df["team_name"] == team)
            ]

            condizione_storica = (
                (not storico_penultima.empty and storico_penultima.iloc[0]["rank"] <= 2) or
                (not storico_terzultima.empty and storico_terzultima.iloc[0]["rank"] <= 2)
            )

            if condizione_corrente and condizione_zona_champions and condizione_storica:
                squadre_filtrate.append(team)

    return squadre_filtrate


def filtro_4(df, df_coppa, df_upcoming, champions_slots_penultima, stagione_corrente, stagione_penultima):
    def get_champions_zone(df_local, stagione, slots):
        df_season = df_local[df_local["season"] == stagione].copy()
        df_season["team_name"] = df_season["team_name"].apply(normalize_team_name)
        df_season["league_name"] = df_season["league_name"].apply(safe_normalize_league_name)
        result = []
        for league, slot in slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)

    def get_coppa_winners(df_coppa_local, stagione):
        if df_coppa_local.empty or "season" not in df_coppa_local.columns or "team_name" not in df_coppa_local.columns:
            return set()
        df_coppa_season = df_coppa_local[df_coppa_local["season"] == stagione].copy()
        df_coppa_season["team_name"] = df_coppa_season["team_name"].apply(normalize_team_name)
        return set(df_coppa_season["team_name"].tolist())

    squadre_in_casa = set(df_upcoming["home_team"].astype(str).apply(normalize_team_name).unique())
    qualificate_penultima = get_champions_zone(df, stagione_penultima, champions_slots_penultima)
    vincitrici_coppa_penultima = get_coppa_winners(df_coppa, stagione_penultima)
    ammesse_penultima = qualificate_penultima.union(vincitrici_coppa_penultima)

    squadre_filtrate = []
    for league in champions_slots_penultima.keys():
        league_norm = normalize_league_name(league)
        df_league = df[(df["season"] == stagione_corrente) & (df["league_name"].apply(safe_normalize_league_name) == league_norm)].copy()
        if df_league.empty:
            continue

        df_sorted = df_league.sort_values("rank")
        if len(df_sorted) < 2:
            continue
        seconda = df_sorted.iloc[1]
        punti_seconda = seconda["points"]
        partite_seconda = seconda["matches"] if "matches" in df_sorted.columns else None

        for _, row in df_sorted.iterrows():
            team = row["team_name"]
            team_norm = normalize_team_name(team)
            punti_team = row["points"]
            partite_team = row["matches"] if "matches" in df_sorted.columns else None

            distacco_dalla_seconda = punti_seconda - punti_team
            partite_in_meno_della_seconda = None
            if partite_seconda is not None and partite_team is not None:
                partite_in_meno_della_seconda = partite_seconda - partite_team

            condizione_corrente = (
                (row["rank"] <= 2) or
                (partite_in_meno_della_seconda == 1 and distacco_dalla_seconda <= 3)
            )
            condizione_casa = team_norm in squadre_in_casa
            condizione_penultima = team_norm in ammesse_penultima

            if condizione_casa and condizione_penultima and condizione_corrente:
                squadre_filtrate.append(team)

    return squadre_filtrate


def build_selection(df_all, df_coppa, champions_slots_all, df_upcoming, matches_df):
    stagione_corrente = season_config.STAGIONE_CORRENTE
    stagione_precedente = season_config.STAGIONE_PRECEDENTE
    stagione_penultima = season_config.STAGIONE_PENULTIMA
    stagione_terzultima = season_config.STAGIONE_TERZULTIMA

    champions_slots = champions_slots_all[str(stagione_corrente)]
    champions_slots_prev = champions_slots_all[str(stagione_precedente)]
    champions_slots_penultima = champions_slots_all[str(stagione_penultima)]

    filtri = [
        ("F1", filtro_1, "coppa"),
        ("F2", filtro_2, "coppa"),
        ("F3", filtro_3, "storico"),
        ("F4", filtro_4, "casa_penultima"),
    ]

    filtri_per_squadra = defaultdict(set)
    for nome_filtro, filtro_attivo, tipo_parametri in filtri:
        if tipo_parametri == "coppa":
            squadre_filtrate = filtro_attivo(
                df_all,
                df_coppa,
                champions_slots,
                champions_slots_prev,
                stagione_corrente,
                stagione_precedente,
            )
        elif tipo_parametri == "storico":
            squadre_filtrate = filtro_attivo(
                df_all,
                stagione_corrente,
                stagione_penultima,
                stagione_terzultima,
                champions_slots,
            )
        elif tipo_parametri == "casa_penultima":
            squadre_filtrate = filtro_attivo(
                df_all,
                df_coppa,
                df_upcoming,
                champions_slots_penultima,
                stagione_corrente,
                stagione_penultima,
            )
        else:
            squadre_filtrate = []
        for team in squadre_filtrate:
            filtri_per_squadra[team].add(nome_filtro)

    remove_runaway_leaders(df_all, filtri_per_squadra, df_upcoming, matches_df, stagione_corrente)
    remove_mid_gap_teams(df_all, filtri_per_squadra, stagione_corrente, champions_slots)

    df_season = df_all[df_all["season"] == stagione_corrente].copy()
    df_season = df_season[df_season["team_name"].isin(filtri_per_squadra.keys())].copy()
    df_season = df_season.rename(columns={"team_name": "squadra", "league_name": "lega"})

    df_season[str(stagione_corrente)] = df_season["rank"]
    df_prev = df_all[df_all["season"] == stagione_precedente][["team_name", "rank"]].rename(
        columns={"rank": str(stagione_precedente), "team_name": "squadra"}
    )
    df_season = df_season.merge(df_prev, on="squadra", how="left")
    df_season["filtri"] = df_season["squadra"].apply(
        lambda t: ",".join(sorted(filtri_per_squadra[t], key=lambda x: int(x[1:]))) if t in filtri_per_squadra else ""
    )

    colonne_finali = ["squadra", "lega", str(stagione_corrente), str(stagione_precedente), "filtri"]
    df_out = df_season[colonne_finali].copy()
    df_out.insert(0, "#", range(1, len(df_out) + 1))
    return df_out


def build_day_matches(matches_df, selected_df, as_of_date):
    if matches_df.empty:
        return pd.DataFrame()

    df = matches_df.copy()
    df = df.dropna(subset=["date"]).copy()
    df["date_only"] = df["date"].dt.date
    df = df[df["date_only"] == as_of_date].copy()
    if df.empty:
        return pd.DataFrame()

    selected = set(selected_df["squadra"].astype(str).apply(normalize_team_name))
    df["home_norm"] = df["home_team"].astype(str).apply(normalize_team_name)
    df["away_norm"] = df["away_team"].astype(str).apply(normalize_team_name)
    df = df[df["home_norm"].isin(selected) | df["away_norm"].isin(selected)].copy()
    if df.empty:
        return pd.DataFrame()

    rows = []
    for _, row in df.iterrows():
        home_team = str(row.get("home_team", "")).strip()
        away_team = str(row.get("away_team", "")).strip()
        home_norm = str(row.get("home_norm", "")).strip()
        away_norm = str(row.get("away_norm", "")).strip()
        if home_norm in selected and away_norm in selected:
            selected_team = home_team
            sc = "SI"
        elif home_norm in selected:
            selected_team = home_team
            sc = ""
        else:
            selected_team = away_team
            sc = ""

        rows.append({
            "match_id": row.get("match_id", ""),
            "squadra selezionata": selected_team,
            "squadra in casa": home_team,
            "squadra fuori casa": away_team,
            "data": pd.Timestamp(row["date"]).strftime("%d/%m/%y"),
            "SC": sc,
            "quota": "",
            "quota_1": "",
            "quota_x": "",
            "quota_2": "",
        })

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        return df_out

    df_out = apply_direct_match_selection(df_out)
    df_out = apply_odds(df_out)

    ordered = [
        "match_id",
        "data",
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
        "quota",
        "quota_1",
        "quota_x",
        "quota_2",
        "SC",
    ]
    ordered = [c for c in ordered if c in df_out.columns]
    extra = [c for c in df_out.columns if c not in ordered]
    df_out = df_out[ordered + extra]
    return df_out


def main():
    parser = argparse.ArgumentParser(description="Time travel per regola_1 e partite del giorno.")
    parser.add_argument("--as-of-date", help="Data di riferimento (YYYY-MM-DD o DD/MM/YY)")
    args = parser.parse_args()

    as_of_date = parse_as_of_date(args.as_of_date) if args.as_of_date else None
    if as_of_date is None:
        raw = input("Inserisci la data (YYYY-MM-DD o DD/MM/YY): ").strip()
        as_of_date = parse_as_of_date(raw)

    matches_df = load_matches()
    if matches_df.empty:
        print("Nessun match disponibile nei raw.")
        sys.exit(1)

    df_archive = pd.read_csv(ARCHIVE_STATS_PATH) if os.path.exists(ARCHIVE_STATS_PATH) else pd.DataFrame()
    df_current = pd.read_csv(CURRENT_STATS_PATH) if os.path.exists(CURRENT_STATS_PATH) else pd.DataFrame()
    df_coppa = pd.read_csv(COPPA_PATH) if os.path.exists(COPPA_PATH) else pd.DataFrame()

    standings_current = compute_standings_as_of(matches_df, season_config.STAGIONE_CORRENTE, as_of_date)
    df_all = pd.concat([df_archive, df_current], ignore_index=True)
    if not standings_current.empty:
        df_all = df_all[df_all["season"] != season_config.STAGIONE_CORRENTE].copy()
        df_all = pd.concat([df_all, standings_current], ignore_index=True)

    df_upcoming = matches_df.copy()
    df_upcoming = df_upcoming.dropna(subset=["date"]).copy()
    df_upcoming = df_upcoming[df_upcoming["date"].dt.date == as_of_date].copy()

    with open(CHAMPIONS_SLOTS_PATH, "r") as f:
        champions_slots_all = json.load(f)

    selection_df = build_selection(df_all, df_coppa, champions_slots_all, df_upcoming, matches_df)
    os.makedirs(os.path.dirname(OUTPUT_SELECTION_PATH), exist_ok=True)
    selection_df.to_csv(OUTPUT_SELECTION_PATH, index=False)
    print(f"Selezione salvata in {OUTPUT_SELECTION_PATH} ({len(selection_df)})")

    day_df = build_day_matches(matches_df, selection_df, as_of_date)
    os.makedirs(os.path.dirname(OUTPUT_DAY_PATH), exist_ok=True)
    day_df.to_csv(OUTPUT_DAY_PATH, index=False)
    print(f"Partite del giorno salvate in {OUTPUT_DAY_PATH} ({len(day_df)})")


if __name__ == "__main__":
    main()
