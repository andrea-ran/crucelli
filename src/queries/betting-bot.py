import argparse
import json
import os
import importlib.util
from datetime import datetime

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

loader_spec = importlib.util.spec_from_file_location(
    "project_loader", os.path.join(PROJECT_ROOT, "project_loader.py")
)
if loader_spec is None or loader_spec.loader is None:
    raise ImportError("Impossibile caricare project_loader.py")
project_loader = importlib.util.module_from_spec(loader_spec)
loader_spec.loader.exec_module(project_loader)
load_project_module = project_loader.load_project_module
PROJECT_ROOT = project_loader.PROJECT_ROOT

season_config = load_project_module("season_config", "season_config.py")
STAGIONE_CORRENTE = season_config.STAGIONE_CORRENTE
STAGIONE_PRECEDENTE = season_config.STAGIONE_PRECEDENTE
STAGIONE_PENULTIMA = season_config.STAGIONE_PENULTIMA
STAGIONE_TERZULTIMA = season_config.STAGIONE_TERZULTIMA

synonyms = load_project_module("synonyms", "synonyms.py")
normalize_league_name = synonyms.normalize_league_name


def normalize_team_name(name):
    return str(name).lower().replace("sl ", "").replace("fc ", "").strip()


def format_table(df, columns, headers=None, max_col_width=32):
    if df.empty:
        return ""
    headers = headers or columns
    rows = [headers]
    for _, row in df.iterrows():
        row_values = []
        for col in columns:
            value = str(row.get(col, "")).replace("\n", " ").strip()
            if len(value) > max_col_width:
                value = value[: max_col_width - 1] + "…"
            row_values.append(value)
        rows.append(row_values)

    widths = [max(len(str(cell)) for cell in col) for col in zip(*rows)]
    lines = []
    header_line = " | ".join(str(cell).ljust(width) for cell, width in zip(rows[0], widths))
    sep_line = "-+-".join("-" * width for width in widths)
    lines.append(header_line)
    lines.append(sep_line)
    for row in rows[1:]:
        line = " | ".join(str(cell).ljust(width) for cell, width in zip(row, widths))
        lines.append(line)
    return "\n".join(lines)


# --- FILTRI (da regola_1.py) ---

def filtro_1(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def get_champions_zone(df_in, stagione, champions_slots_in):
        df_season = df_in[df_in["season"] == stagione].copy()
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots_in.items():
            league_norm = normalize_league_name(league)
            squadre = (
                df_season[df_season["league_name"] == league_norm]
                .sort_values("rank")
                .head(slot)["team_name"]
                .tolist()
            )
            result.extend(squadre)
        return set(result)

    def get_near_champions_zone(df_in, stagione, champions_slots_in, max_gap_points=3):
        df_season = df_in[df_in["season"] == stagione].copy()
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots_in.items():
            league_norm = normalize_league_name(league)
            df_league = df_season[df_season["league_name"] == league_norm].sort_values("rank")
            if df_league.empty or len(df_league) < slot:
                continue
            soglia_champions = df_league.iloc[slot - 1]["points"]
            squadre_vicine = df_league[df_league["points"] >= (soglia_champions - max_gap_points)][
                "team_name"
            ].tolist()
            result.extend(squadre_vicine)
        return set(result)

    def get_coppa_winners(df_coppa_in, stagione):
        return set(df_coppa_in[df_coppa_in["season"] == stagione]["team_name"].tolist())

    zone_corrente = get_champions_zone(df, stagione_corrente, champions_slots)
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots_prev)
    zone_vicina_precedente = get_near_champions_zone(
        df, stagione_precedente, champions_slots_prev, max_gap_points=3
    )
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    ammesse_stagione_precedente = qualificate_precedente.union(zone_vicina_precedente)
    squadre_filtrate = [team for team in zone_corrente if team in ammesse_stagione_precedente]
    return squadre_filtrate


def filtro_2(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def get_champions_zone(df_in, stagione, champions_slots_in):
        df_season = df_in[df_in["season"] == stagione].copy()
        df_season["team_name"] = df_season["team_name"].apply(normalize_team_name)
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots_in.items():
            league_norm = normalize_league_name(league)
            squadre = (
                df_season[df_season["league_name"] == league_norm]
                .sort_values("rank")
                .head(slot)["team_name"]
                .tolist()
            )
            result.extend(squadre)
        return set(result)

    def get_coppa_winners(df_coppa_in, stagione):
        if df_coppa_in.empty or "season" not in df_coppa_in.columns or "team_name" not in df_coppa_in.columns:
            return set()
        df_coppa_season = df_coppa_in[df_coppa_in["season"] == stagione].copy()
        df_coppa_season["team_name"] = df_coppa_season["team_name"].apply(normalize_team_name)
        return set(df_coppa_season["team_name"].tolist())

    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    squadre_filtrate = []
    for league, slot in champions_slots.items():
        league_norm = normalize_league_name(league)
        df_league = df[
            (df["season"] == stagione_corrente)
            & (df["league_name"].apply(normalize_league_name) == league_norm)
        ].copy()
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
        .apply(normalize_league_name)
        .unique()
    )

    for league_norm in leghe_correnti:
        df_corrente = df[
            (df["season"] == stagione_corrente)
            & (df["league_name"].apply(normalize_league_name) == league_norm)
        ].copy()
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
        squadre_zona_champions = set(
            df_corrente.sort_values("rank").head(slot_champions)["team_name"].tolist()
        )

        for _, row in df_corrente.iterrows():
            team = row["team_name"]
            punti_team = row["points"]
            partite_team = row["matches"] if "matches" in df_corrente.columns else None

            distacco_dalla_prima = punti_prima - punti_team
            partite_in_meno_della_prima = None
            if partite_prima is not None and partite_team is not None:
                partite_in_meno_della_prima = partite_prima - partite_team

            condizione_corrente = (
                (row["rank"] <= 2)
                or (distacco_dalla_prima <= 6)
                or (distacco_dalla_prima <= 8 and partite_in_meno_della_prima == 1)
            )

            condizione_zona_champions = team in squadre_zona_champions

            storico_penultima = df[
                (df["season"] == stagione_penultima)
                & (df["league_name"].apply(normalize_league_name) == league_norm)
                & (df["team_name"] == team)
            ]
            storico_terzultima = df[
                (df["season"] == stagione_terzultima)
                & (df["league_name"].apply(normalize_league_name) == league_norm)
                & (df["team_name"] == team)
            ]

            condizione_storica = (
                (not storico_penultima.empty and storico_penultima.iloc[0]["rank"] <= 2)
                or (not storico_terzultima.empty and storico_terzultima.iloc[0]["rank"] <= 2)
            )

            if condizione_corrente and condizione_zona_champions and condizione_storica:
                squadre_filtrate.append(team)

    return squadre_filtrate


def filtro_4(df, df_coppa, df_upcoming, champions_slots_penultima, stagione_corrente, stagione_penultima):
    def get_champions_zone(df_in, stagione, champions_slots_in):
        df_season = df_in[df_in["season"] == stagione].copy()
        df_season["team_name"] = df_season["team_name"].apply(normalize_team_name)
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots_in.items():
            league_norm = normalize_league_name(league)
            squadre = (
                df_season[df_season["league_name"] == league_norm]
                .sort_values("rank")
                .head(slot)["team_name"]
                .tolist()
            )
            result.extend(squadre)
        return set(result)

    def get_coppa_winners(df_coppa_in, stagione):
        if df_coppa_in.empty or "season" not in df_coppa_in.columns or "team_name" not in df_coppa_in.columns:
            return set()
        df_coppa_season = df_coppa_in[df_coppa_in["season"] == stagione].copy()
        df_coppa_season["team_name"] = df_coppa_season["team_name"].apply(normalize_team_name)
        return set(df_coppa_season["team_name"].tolist())

    squadre_in_casa = set(df_upcoming["home_team"].astype(str).apply(normalize_team_name).unique())
    qualificate_penultima = get_champions_zone(df, stagione_penultima, champions_slots_penultima)
    vincitrici_coppa_penultima = get_coppa_winners(df_coppa, stagione_penultima)
    ammesse_penultima = qualificate_penultima.union(vincitrici_coppa_penultima)

    squadre_filtrate = []
    for league in champions_slots_penultima.keys():
        league_norm = normalize_league_name(league)
        df_league = df[
            (df["season"] == stagione_corrente)
            & (df["league_name"].apply(normalize_league_name) == league_norm)
        ].copy()
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
                (row["rank"] <= 2) or (partite_in_meno_della_seconda == 1 and distacco_dalla_seconda <= 3)
            )
            condizione_casa = team_norm in squadre_in_casa
            condizione_penultima = team_norm in ammesse_penultima

            if condizione_casa and condizione_penultima and condizione_corrente:
                squadre_filtrate.append(team)

    return squadre_filtrate


def is_selected_match_last_home(upcoming_df, matches_df, season, league_norm, team_norm):
    if upcoming_df.empty or matches_df.empty:
        return False
    df_up = upcoming_df[upcoming_df["season"] == season].copy()
    if df_up.empty:
        return False
    df_up = df_up.dropna(subset=["date"]).copy()
    if df_up.empty:
        return False
    df_up = df_up[df_up["league_name"].apply(normalize_league_name) == league_norm].copy()
    if df_up.empty:
        return False
    df_up["home_norm"] = df_up["home_team"].astype(str).apply(normalize_team_name)
    df_up = df_up[df_up["home_norm"] == team_norm].copy()
    if df_up.empty:
        return False

    df_all = matches_df[matches_df["season"] == season].copy()
    df_all = df_all.dropna(subset=["date"]).copy()
    df_all = df_all[df_all["league_name"].apply(normalize_league_name) == league_norm].copy()
    if df_all.empty:
        return False
    df_all["home_norm"] = df_all["home_team"].astype(str).apply(normalize_team_name)
    df_home = df_all[df_all["home_norm"] == team_norm].copy()
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
        df_season["league_name"].dropna().astype(str).apply(normalize_league_name).unique()
    )

    for league_norm in leagues:
        df_league = df_season[df_season["league_name"].apply(normalize_league_name) == league_norm].copy()
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
        df_season["league_name"].dropna().astype(str).apply(normalize_league_name).unique()
    )

    for league_norm in leagues:
        slot_champions = 4
        for league_key in champions_slots.keys():
            if normalize_league_name(league_key) == league_norm:
                slot_champions = champions_slots[league_key]
                break

        df_league = df_season[df_season["league_name"].apply(normalize_league_name) == league_norm].copy()
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


def build_selected_teams(df_all, df_coppa, df_matches, df_upcoming, champions_slots_all):
    champions_slots = champions_slots_all[str(STAGIONE_CORRENTE)]
    champions_slots_prev = champions_slots_all[str(STAGIONE_PRECEDENTE)]
    champions_slots_penultima = champions_slots_all[str(STAGIONE_PENULTIMA)]

    filtri = [
        ("F1", filtro_1, "coppa"),
        ("F2", filtro_2, "coppa"),
        ("F3", filtro_3, "storico"),
        ("F4", filtro_4, "casa_penultima"),
    ]

    from collections import defaultdict

    filtri_per_squadra = defaultdict(set)

    for nome_filtro, filtro_attivo, tipo_parametri in filtri:
        if tipo_parametri == "coppa":
            squadre_filtrate = filtro_attivo(
                df_all,
                df_coppa,
                champions_slots,
                champions_slots_prev,
                STAGIONE_CORRENTE,
                STAGIONE_PRECEDENTE,
            )
        elif tipo_parametri == "storico":
            squadre_filtrate = filtro_attivo(
                df_all,
                STAGIONE_CORRENTE,
                STAGIONE_PENULTIMA,
                STAGIONE_TERZULTIMA,
                champions_slots,
            )
        elif tipo_parametri == "casa_penultima":
            squadre_filtrate = filtro_attivo(
                df_all,
                df_coppa,
                df_upcoming,
                champions_slots_penultima,
                STAGIONE_CORRENTE,
                STAGIONE_PENULTIMA,
            )
        else:
            squadre_filtrate = []
        for team in squadre_filtrate:
            filtri_per_squadra[team].add(nome_filtro)

    remove_runaway_leaders(df_all, filtri_per_squadra, df_upcoming, df_matches, STAGIONE_CORRENTE)
    remove_mid_gap_teams(df_all, filtri_per_squadra, STAGIONE_CORRENTE, champions_slots)

    df_season = df_all[df_all["season"] == STAGIONE_CORRENTE].copy()
    df_season = df_season[df_season["team_name"].isin(filtri_per_squadra.keys())].copy()
    df_season = df_season.rename(columns={"team_name": "squadra", "league_name": "lega"})
    df_season["2025"] = df_season["rank"]
    df_2024 = (
        df_all[df_all["season"] == STAGIONE_PRECEDENTE][["team_name", "rank"]]
        .rename(columns={"rank": "2024", "team_name": "squadra"})
    )
    df_season = df_season.merge(df_2024, on="squadra", how="left")
    df_season["filtri"] = df_season["squadra"].apply(
        lambda t: ",".join(sorted(filtri_per_squadra[t], key=lambda x: int(x[1:])))
        if t in filtri_per_squadra
        else ""
    )

    colonne_finali = ["squadra", "lega", "2025", "2024", "filtri"]
    df_out = df_season[colonne_finali].copy()
    df_out.insert(0, "#", range(1, len(df_out) + 1))
    return df_out


def build_today_matches(df_selected, df_matches, as_of_date):
    if df_selected.empty or df_matches.empty:
        return pd.DataFrame()

    as_of_utc = pd.Timestamp(as_of_date, tz="UTC").normalize()
    df_matches = df_matches.copy()
    df_matches["date"] = pd.to_datetime(df_matches["date"], utc=True, errors="coerce")
    df_matches = df_matches[df_matches["date"].dt.normalize() == as_of_utc].copy()
    if df_matches.empty:
        return pd.DataFrame()

    team_info = {}
    for _, row in df_selected.iterrows():
        team_norm = normalize_team_name(row["squadra"])
        team_info[team_norm] = {
            "squadra": row["squadra"],
            "lega": row.get("lega", ""),
            "2025": row.get("2025", ""),
            "2024": row.get("2024", ""),
            "filtri": row.get("filtri", ""),
        }

    rows = []
    for _, row in df_matches.iterrows():
        home_team = str(row.get("home_team", "")).strip()
        away_team = str(row.get("away_team", "")).strip()
        home_norm = normalize_team_name(home_team)
        away_norm = normalize_team_name(away_team)
        home_selected = home_norm in team_info
        away_selected = away_norm in team_info
        if not home_selected and not away_selected:
            continue

        home_info = team_info.get(home_norm, {})
        away_info = team_info.get(away_norm, {})
        match_date = row.get("date")
        data_fmt = match_date.strftime("%d/%m/%y ore %H:%M") if pd.notna(match_date) else ""

        rows.append(
            {
                "match_id": row.get("match_id", ""),
                "lega_match": row.get("league_name", ""),
                "squadra in casa": home_team,
                "squadra fuori casa": away_team,
                "data": data_fmt,
                "sc": "SI" if home_selected and away_selected else "",
                "selezione casa": "SI" if home_selected else "",
                "selezione trasferta": "SI" if away_selected else "",
                "filtri casa": home_info.get("filtri", ""),
                "filtri trasferta": away_info.get("filtri", ""),
                "lega casa": home_info.get("lega", ""),
                "lega trasferta": away_info.get("lega", ""),
                "2025 casa": home_info.get("2025", ""),
                "2025 trasferta": away_info.get("2025", ""),
                "2024 casa": home_info.get("2024", ""),
                "2024 trasferta": away_info.get("2024", ""),
            }
        )

    if not rows:
        return pd.DataFrame()

    df_out = pd.DataFrame(rows)
    df_out["data_sort"] = pd.to_datetime(df_out["data"], format="%d/%m/%y ore %H:%M", errors="coerce")
    df_out = df_out.sort_values("data_sort").drop(columns=["data_sort"])
    df_out.insert(0, "n", range(1, len(df_out) + 1))
    return df_out


def main():
    parser = argparse.ArgumentParser(
        description="Seleziona le squadre e stampa le partite del giorno."
    )
    parser.add_argument(
        "--as-of-date",
        help="Data di riferimento (YYYY-MM-DD o DD/MM/YY).",
    )
    parser.add_argument(
        "--matches-source",
        choices=["upcoming", "all_current"],
        default="upcoming",
        help="Sorgente partite: upcoming (default) o all_current.",
    )
    parser.add_argument(
        "--output",
        default=os.path.join("data", "processed", "bet.csv"),
        help="Path del CSV con le partite di oggi.",
    )
    args = parser.parse_args()

    as_of_date = parse_as_of_date(args.as_of_date)
    if as_of_date is None:
        as_of_date = datetime.now().date()

    archive_path = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_archive.csv")
    current_path = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_current.csv")
    coppa_path = os.path.join(PROJECT_ROOT, "data", "raw", "coppa_nazionale.csv")
    champions_slots_path = os.path.join(PROJECT_ROOT, "champions_slots.json")
    matches_current_path = os.path.join(PROJECT_ROOT, "data", "raw", "all_matches_current.csv")
    upcoming_path = os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_matches.csv")

    df_archive = pd.read_csv(archive_path)
    df_current = pd.read_csv(current_path)
    df_all = pd.concat([df_archive, df_current], ignore_index=True)
    df_coppa = pd.read_csv(coppa_path)
    df_matches_all = pd.read_csv(matches_current_path) if os.path.exists(matches_current_path) else pd.DataFrame()
    df_upcoming = pd.read_csv(upcoming_path) if os.path.exists(upcoming_path) else pd.DataFrame()

    if not df_matches_all.empty and "date" in df_matches_all.columns:
        df_matches_all["date"] = pd.to_datetime(df_matches_all["date"], utc=True, errors="coerce")
    if not df_upcoming.empty and "date" in df_upcoming.columns:
        df_upcoming["date"] = pd.to_datetime(df_upcoming["date"], utc=True, errors="coerce")

    with open(champions_slots_path, "r") as handle:
        champions_slots_all = json.load(handle)

    df_selected = build_selected_teams(
        df_all,
        df_coppa,
        df_matches_all,
        df_upcoming,
        champions_slots_all,
    )

    if df_selected.empty:
        print("Nessuna squadra selezionata.")
        return

    print("===== SQUADRE SELEZIONATE =====")
    print(df_selected.to_string(index=False))
    print(f"Totale squadre selezionate: {len(df_selected)}\n")

    matches_path = upcoming_path if args.matches_source == "upcoming" else matches_current_path
    if not os.path.exists(matches_path):
        raise FileNotFoundError(f"File non trovato: {matches_path}")

    df_matches = pd.read_csv(matches_path)
    df_matches["date"] = pd.to_datetime(df_matches["date"], utc=True, errors="coerce")
    if args.matches_source == "upcoming" and "status" in df_matches.columns:
        allowed_statuses = {"NS", "TBD"}
        df_matches = df_matches[
            df_matches["status"].astype(str).str.upper().isin(allowed_statuses)
        ].copy()

    df_out = build_today_matches(df_selected, df_matches, as_of_date)
    if df_out.empty:
        print("Nessuna partita trovata per le squadre selezionate.")
        return

    print("===== PARTITE DI OGGI =====")
    df_print = df_out.copy()
    df_print["incontro"] = (
        df_print["squadra in casa"].astype(str).str.strip()
        + " vs "
        + df_print["squadra fuori casa"].astype(str).str.strip()
    )
    match_columns = ["data", "incontro", "lega_match"]
    table = format_table(df_print, match_columns, headers=["data", "incontro", "lega"])
    if table:
        print(table)
    else:
        print(df_out.to_string(index=False))
    print(f"Totale partite di oggi: {len(df_out)}\n")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df_out.to_csv(args.output, index=False)
    print(f"Partite del giorno esportate: {len(df_out)} -> {args.output}")


if __name__ == "__main__":
    main()
