# Funzione di normalizzazione nomi squadra (ex synonyms.py)
def normalize_team_name(name):
    return name.lower().replace("sl ", "").replace("fc ", "").strip()
import pandas as pd
import json
import os
import importlib.util
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

loader_spec = importlib.util.spec_from_file_location("project_loader", os.path.join(PROJECT_ROOT, "project_loader.py"))
if loader_spec is None or loader_spec.loader is None:
    raise ImportError("Impossibile caricare project_loader.py")
project_loader = importlib.util.module_from_spec(loader_spec)
loader_spec.loader.exec_module(project_loader)
load_project_module = project_loader.load_project_module
PROJECT_ROOT = project_loader.PROJECT_ROOT

season_config = load_project_module("season_config", "season_config.py")
STAGIONE_CORRENTE = season_config.STAGIONE_CORRENTE
STAGIONE_PRECEDENTE = season_config.STAGIONE_PRECEDENTE

synonyms = load_project_module("synonyms", "synonyms.py")
normalize_league_name = synonyms.normalize_league_name

# --- INIZIO FUNZIONI FILTRI (ex rules.py) ---
def filtro_1(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def get_champions_zone(df, stagione, champions_slots):
        df_season = df[df["season"] == stagione].copy()
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)

    def get_near_champions_zone(df, stagione, champions_slots, max_gap_points=3):
        df_season = df[df["season"] == stagione].copy()
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots.items():
            league_norm = normalize_league_name(league)
            df_league = df_season[df_season["league_name"] == league_norm].sort_values("rank")
            if df_league.empty or len(df_league) < slot:
                continue
            soglia_champions = df_league.iloc[slot - 1]["points"]
            squadre_vicine = df_league[df_league["points"] >= (soglia_champions - max_gap_points)]["team_name"].tolist()
            result.extend(squadre_vicine)
        return set(result)

    def get_coppa_winners(df_coppa, stagione):
        return set(df_coppa[df_coppa["season"] == stagione]["team_name"].tolist())

    zone_corrente = get_champions_zone(df, stagione_corrente, champions_slots)
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots_prev)
    zone_vicina_precedente = get_near_champions_zone(df, stagione_precedente, champions_slots_prev, max_gap_points=3)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    ammesse_stagione_precedente = qualificate_precedente.union(zone_vicina_precedente)
    squadre_filtrate = [team for team in zone_corrente if team in ammesse_stagione_precedente]
    return squadre_filtrate

def filtro_2(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def get_champions_zone(df, stagione, champions_slots):
        df_season = df[df["season"] == stagione].copy()
        df_season["team_name"] = df_season["team_name"].apply(normalize_team_name)
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)
    def get_coppa_winners(df_coppa, stagione):
        df_coppa_season = df_coppa[df_coppa["season"] == stagione].copy()
        df_coppa_season["team_name"] = df_coppa_season["team_name"].apply(normalize_team_name)
        return set(df_coppa_season["team_name"].tolist())
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    squadre_filtrate = []
    for league, slot in champions_slots.items():
        league_norm = normalize_league_name(league)
        df_league = df[(df["season"] == stagione_corrente) & (df["league_name"].apply(normalize_league_name) == league_norm)].copy()
        df_league["team_name"] = df_league["team_name"].apply(normalize_team_name)
        if df_league.empty:
            continue
        df_sorted = df_league.sort_values("rank")
        if len(df_sorted) < slot:
            continue
        soglia_champions = df_sorted.iloc[slot-1]["points"]
        partite_champions = df_sorted.iloc[slot-1]["matches"] if "matches" in df_sorted.columns else None
        df_out = df_league[df_league["rank"] > slot]
        for _, row in df_out.iterrows():
            team = row["team_name"]
            punti = row["points"]
            partite_giocate = row["matches"] if "matches" in row else None
            punti_dalla_zona_champions = soglia_champions - punti
            partite_in_meno_rispetto_ultima_champions = None
            if partite_champions is not None and partite_giocate is not None:
                partite_in_meno_rispetto_ultima_champions = partite_champions - partite_giocate

            condizione_precedente = team in qualificate_precedente
            condizione_punti_base = punti_dalla_zona_champions <= 3
            condizione_punti_con_partita_in_meno = (
                punti_dalla_zona_champions <= 6 and
                partite_in_meno_rispetto_ultima_champions == 1
            )

            if condizione_precedente and (condizione_punti_base or condizione_punti_con_partita_in_meno):
                squadre_filtrate.append(team)
    return squadre_filtrate

def filtro_3(df, stagione_corrente, stagione_penultima, stagione_terzultima):
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
            (df["season"] == stagione_corrente) &
            (df["league_name"].apply(normalize_league_name) == league_norm)
        ].copy()
        if df_corrente.empty:
            continue

        df_corrente = df_corrente.sort_values("rank")
        prima = df_corrente.iloc[0]
        punti_prima = prima["points"]
        partite_prima = prima["matches"] if "matches" in df_corrente.columns else None

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

            storico_penultima = df[
                (df["season"] == stagione_penultima) &
                (df["league_name"].apply(normalize_league_name) == league_norm) &
                (df["team_name"] == team)
            ]
            storico_terzultima = df[
                (df["season"] == stagione_terzultima) &
                (df["league_name"].apply(normalize_league_name) == league_norm) &
                (df["team_name"] == team)
            ]

            condizione_storica = (
                (not storico_penultima.empty and storico_penultima.iloc[0]["rank"] <= 2) or
                (not storico_terzultima.empty and storico_terzultima.iloc[0]["rank"] <= 2)
            )

            if condizione_corrente and condizione_storica:
                squadre_filtrate.append(team)

    return squadre_filtrate

def filtro_4(df, df_coppa, df_upcoming, champions_slots_penultima, stagione_corrente, stagione_penultima):
    def get_champions_zone(df, stagione, champions_slots):
        df_season = df[df["season"] == stagione].copy()
        df_season["team_name"] = df_season["team_name"].apply(normalize_team_name)
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)

    def get_coppa_winners(df_coppa, stagione):
        df_coppa_season = df_coppa[df_coppa["season"] == stagione].copy()
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
            (df["season"] == stagione_corrente) &
            (df["league_name"].apply(normalize_league_name) == league_norm)
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
                (row["rank"] <= 2) or
                (partite_in_meno_della_seconda == 1 and distacco_dalla_seconda <= 3)
            )
            condizione_casa = team_norm in squadre_in_casa
            condizione_penultima = team_norm in ammesse_penultima

            if condizione_casa and condizione_penultima and condizione_corrente:
                squadre_filtrate.append(team)

    return squadre_filtrate

# --- FINE FUNZIONI FILTRI ---

# Percorsi file aggiornati
ARCHIVE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_archive.csv")
CURRENT_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_current.csv")
COPPA_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "coppa_nazionale.csv")
CHAMPIONS_SLOTS_PATH = os.path.join(PROJECT_ROOT, "champions_slots.json")

# Carica e concatena dati archivio + stagione corrente
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
df = pd.concat([df_archive, df_current], ignore_index=True)
df_coppa = pd.read_csv(COPPA_PATH)

# Carica i posti Champions specifici per la stagione
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots_all = json.load(f)

champions_slots = champions_slots_all[str(STAGIONE_CORRENTE)]
champions_slots_prev = champions_slots_all[str(STAGIONE_PRECEDENTE)]
champions_slots_penultima = champions_slots_all[str(season_config.STAGIONE_PENULTIMA)]




filtri = [
    ("F1", filtro_1, "coppa"),
    ("F2", filtro_2, "coppa"),
    ("F3", filtro_3, "storico"),
    ("F4", filtro_4, "casa_penultima"),
]


# Aggregazione squadre filtrate da tutti i filtri
from collections import defaultdict
filtri_per_squadra = defaultdict(set)  # team_name -> set di filtri


# Costruzione mappa filtri per squadra

# Applica tutti i filtri e aggrega le squadre filtrate
for nome_filtro, filtro_attivo, tipo_parametri in filtri:
    if tipo_parametri == "coppa":
        squadre_filtrate = filtro_attivo(df, df_coppa, champions_slots, champions_slots_prev, STAGIONE_CORRENTE, STAGIONE_PRECEDENTE)
    elif tipo_parametri == "storico":
        STAGIONE_PENULTIMA = season_config.STAGIONE_PENULTIMA
        STAGIONE_TERZULTIMA = season_config.STAGIONE_TERZULTIMA
        squadre_filtrate = filtro_attivo(df, STAGIONE_CORRENTE, STAGIONE_PENULTIMA, STAGIONE_TERZULTIMA)
    elif tipo_parametri == "casa_penultima":
        STAGIONE_PENULTIMA = season_config.STAGIONE_PENULTIMA
        UPCOMING_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_matches.csv")
        df_upcoming = pd.read_csv(UPCOMING_PATH)
        squadre_filtrate = filtro_attivo(df, df_coppa, df_upcoming, champions_slots_penultima, STAGIONE_CORRENTE, STAGIONE_PENULTIMA)
    else:
        squadre_filtrate = []
    for team in squadre_filtrate:
        filtri_per_squadra[team].add(nome_filtro)





# Crea DataFrame aggregato SOLO con le squadre filtrate e solo le colonne richieste
df_season = df[df["season"] == STAGIONE_CORRENTE].copy()
df_season = df_season[df_season["team_name"].isin(filtri_per_squadra.keys())].copy()
df_season = df_season.rename(columns={
    "team_name": "squadra",
    "league_name": "lega"
})
df_season["2025"] = df_season["rank"]
df_2024 = df[(df["season"] == STAGIONE_PRECEDENTE)][["team_name", "rank"]].rename(columns={"rank": "2024", "team_name": "squadra"})
df_season = df_season.merge(df_2024, on="squadra", how="left")
df_season["filtri"] = df_season["squadra"].apply(lambda t: ','.join(sorted(filtri_per_squadra[t], key=lambda x: int(x[1:]))) if t in filtri_per_squadra else "")

colonne_finali = ["squadra", "lega", "2025", "2024", "filtri"]

output_path = os.path.join(PROJECT_ROOT, "data", "processed", "selezione_regola_1.csv")
df_out = df_season[colonne_finali].copy()
df_out.insert(0, "#", range(1, len(df_out) + 1))
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df_out.to_csv(output_path, index=False)
print(f"\n===== RISULTATO REGOLA 1 (SOLO FILTRATE) =====")
print(df_out)
print(f"Totale squadre filtrate: {len(df_out)}\n")
elenco_squadre_alfabetico = ", ".join(sorted(df_out["squadra"].astype(str).tolist()))
print(f"Elenco squadre: {elenco_squadre_alfabetico}")
