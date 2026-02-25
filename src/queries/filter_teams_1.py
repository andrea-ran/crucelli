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
def selezione_filtro_1(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def get_champions_zone(df, stagione, champions_slots):
        df_season = df[df["season"] == stagione].copy()
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)
    def get_coppa_winners(df_coppa, stagione):
        return set(df_coppa[df_coppa["season"] == stagione]["team_name"].tolist())
    zone_corrente = get_champions_zone(df, stagione_corrente, champions_slots)
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots_prev)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    selezionate = [team for team in zone_corrente if team in qualificate_precedente]
    return selezionate

def selezione_filtro_2(df, champions_slots, stagione_corrente, stagione_precedente):
    def get_champions_points_threshold(df, stagione, league, champions_slots):
        slot = champions_slots.get(league, 4)
        league_norm = normalize_league_name(league)
        df_season = df[(df["season"] == stagione) & (df["league_name"].apply(normalize_league_name) == league_norm)]
        if df_season.empty:
            return None
        league_stats_sorted = df_season.nsmallest(slot, "rank")
        if league_stats_sorted.empty:
            return None
        return league_stats_sorted.iloc[-1]["points"]

    # Squadre in zona Champions nella stagione corrente
    zone_champions = set()
    for league, slot in champions_slots.items():
        league_norm = normalize_league_name(league)
        df_league = df[(df["season"] == stagione_corrente) & (df["league_name"].apply(normalize_league_name) == league_norm)]
        squadre = df_league.sort_values("rank").head(slot)["team_name"].tolist()
        zone_champions.update(squadre)

    risultati = []
    for team in zone_champions:
        row_team = df[(df["season"] == stagione_corrente) & (df["team_name"] == team)]
        if row_team.empty:
            continue
        league = row_team.iloc[0]["league_name"]
        league_norm = normalize_league_name(league)
        # Trova avversario prossimo match
        # Serve upcoming_matches.csv
        try:
            df_upcoming = pd.read_csv("data/raw/upcoming_matches.csv")
        except Exception:
            continue
        match = df_upcoming[(df_upcoming["home_team"] == team) | (df_upcoming["away_team"] == team)]
        if match.empty:
            continue
        row_match = match.iloc[0]
        opponent = row_match["away_team"] if row_match["home_team"] == team else row_match["home_team"]
        is_home = row_match["home_team"] == team
        # Punti avversario
        opponent_stats = df[(df["season"] == stagione_corrente) & (df["team_name"] == opponent) & (df["league_name"] == league)]
        opponent_points = opponent_stats["points"].iloc[0] if not opponent_stats.empty else 0
        # Soglia Champions
        champions_points = get_champions_points_threshold(df, stagione_corrente, league, champions_slots)
        # Differenza partite giocate
        matches_played_diff = (opponent_stats["matches"].iloc[0] - row_team["matches"].iloc[0]) if (not opponent_stats.empty and "matches" in opponent_stats.columns and "matches" in row_team.columns) else 0
        # Delta punti
        delta_points = champions_points - opponent_points if champions_points is not None else None
        elimina = False
        # Logica eliminazione
        if "pro league" in league.lower() or "arabia" in league.lower() or "saudi" in league.lower():
            if champions_points is None:
                elimina = True
            elif delta_points <= 0:
                elimina = True
            elif matches_played_diff == 1 and delta_points <= 1:
                elimina = True
        else:
            if champions_points is None:
                elimina = True
            elif delta_points <= 10:
                elimina = True
            elif matches_played_diff == 1 and delta_points <= 11:
                elimina = True
            elif matches_played_diff == 2 and delta_points <= 12:
                elimina = True
        # Eccezioni ripescaggio
        if elimina:
            t_points = int(row_team["points"].iloc[0]) if not row_team.empty else 0
            if (t_points - opponent_points) >= 15:
                elimina = False
            elif ("matches" in row_team.columns and row_team["matches"].iloc[0] <= 11 and is_home):
                elimina = False
        if not elimina:
            risultati.append(team)
    return risultati

def selezione_filtro_3(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
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
    results = []
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
            diff = punti - soglia_champions
            diff_partite = None
            if partite_champions is not None and partite_giocate is not None:
                diff_partite = partite_champions - partite_giocate
            condizione = False
            if team in qualificate_precedente:
                if diff >= -3 and diff <= 0:
                    condizione = True
                elif diff >= -6 and diff <= 0 and diff_partite == -1:
                    condizione = True
            if condizione:
                results.append(team)
    return results

def selezione_filtro_4(df, champions_slots, stagione_corrente, stagione_penultima, stagione_terzultima):
    league_synonyms = {
        "saudi pro league": ["saudi pro league", "saudi professional league"],
        "prva hnl": ["prva hnl", "hnl"]
    }
    results = []
    for league in champions_slots.keys():
        # Gestione sinonimi Saudi Pro League
        if league in league_synonyms:
            mask = (df["season"] == stagione_corrente) & (df["league_name"].str.lower().isin([s.lower() for s in league_synonyms[league]]))
            df_league = df[mask].copy()
        else:
            df_league = df[(df["season"] == stagione_corrente) & (df["league_name"] == league)].copy()
        if df_league.empty:
            continue
        df_sorted = df_league.sort_values("rank")
        if len(df_sorted) < 2:
            continue
        prima = df_sorted.iloc[0]
        seconda = df_sorted.iloc[1]
        for _, row in df_league.iterrows():
            team = row["team_name"]
            punti = row["points"]
            partite = row["matches"] if "matches" in row else None
            punti_prima = prima["points"]
            partite_prima = prima["matches"] if "matches" in prima else None
            diff = punti - punti_prima
            diff_partite = None
            if partite is not None and partite_prima is not None:
                diff_partite = partite_prima - partite
            condizione = False
            if row["rank"] in [1,2]:
                condizione = True
            elif diff == -1:
                condizione = True
            elif diff == -3 and diff_partite == -1:
                condizione = True
            elif diff in [-2, -6, -8] and diff_partite == -1:
                condizione = True
            if not condizione:
                continue
            penultima = df[(df["season"] == stagione_penultima) & (df["league_name"] == league) & (df["team_name"] == team)]
            terzultima = df[(df["season"] == stagione_terzultima) & (df["league_name"] == league) & (df["team_name"] == team)]
            condizione_storica = False
            if not penultima.empty and penultima.iloc[0]["rank"] in [1,2]:
                condizione_storica = True
            if not terzultima.empty and terzultima.iloc[0]["rank"] in [1,2]:
                condizione_storica = True
            if condizione_storica:
                results.append(team)
    return results

def selezione_filtro_5(df, df_coppa, df_upcoming, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def get_champions_zone(df, stagione, champions_slots):
        df_season = df[df["season"] == stagione]
        result = []
        league_synonyms = {
            "saudi pro league": ["saudi pro league", "saudi professional league"]
        }
        for league, slot in champions_slots.items():
            if league in league_synonyms:
                mask = df_season["league_name"].str.lower().isin([s.lower() for s in league_synonyms[league]])
                squadre = df_season[mask].sort_values("rank").head(slot)["team_name"].tolist()
            else:
                squadre = df_season[df_season["league_name"] == league].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)
    def get_coppa_winners(df_coppa, stagione):
        return set(df_coppa[df_coppa["season"] == stagione]["team_name"].tolist())
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    results = []
    for league in champions_slots.keys():
        df_league = df[(df["season"] == stagione_corrente) & (df["league_name"] == league)].copy()
        if df_league.empty:
            continue
        df_sorted = df_league.sort_values("rank")
        if len(df_sorted) < 2:
            continue
        seconda = df_sorted.iloc[1]
        punti_seconda = seconda["points"]
        partite_seconda = seconda["matches"] if "matches" in seconda else None
        home_teams = set(df_upcoming["home_team"].unique())
        for _, row in df_league.iterrows():
            team = row["team_name"]
            punti = row["points"]
            partite = row["matches"] if "matches" in row else None
            diff = punti - punti_seconda
            diff_partite = None
            if partite is not None and partite_seconda is not None:
                diff_partite = partite_seconda - partite
            condizione = False
            if row["rank"] in [1,2]:
                condizione = True
            elif diff < 0 and diff_partite == -1:
                condizione = True
            if condizione and team in home_teams:
                if team in qualificate_precedente:
                    results.append(team)
    return results
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




filtri = [
    ("F1", selezione_filtro_1, "coppa"),
    ("F2", selezione_filtro_2, "base"),
    ("F3", selezione_filtro_3, "coppa"),
    ("F4", selezione_filtro_4, "storico"),
    ("F5", selezione_filtro_5, "upcoming"),
]


# Aggregazione risultati di tutti i filtri
from collections import defaultdict
selezioni = defaultdict(set)  # team_name -> set di filtri


# Stampa risultati per ogni filtro come prima

# Applica tutti i filtri e aggrega i risultati
for nome_filtro, funzione_filtro, tipo_parametri in filtri:
    if tipo_parametri == "coppa":
        selezionate = funzione_filtro(df, df_coppa, champions_slots, champions_slots_prev, STAGIONE_CORRENTE, STAGIONE_PRECEDENTE)
    elif tipo_parametri == "storico":
        STAGIONE_PENULTIMA = season_config.STAGIONE_PENULTIMA
        STAGIONE_TERZULTIMA = season_config.STAGIONE_TERZULTIMA
        selezionate = funzione_filtro(df, champions_slots, STAGIONE_CORRENTE, STAGIONE_PENULTIMA, STAGIONE_TERZULTIMA)
    elif tipo_parametri == "upcoming":
        UPCOMING_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_matches.csv")
        df_upcoming = pd.read_csv(UPCOMING_PATH)
        selezionate = funzione_filtro(df, df_coppa, df_upcoming, champions_slots, champions_slots_prev, STAGIONE_CORRENTE, STAGIONE_PRECEDENTE)
    else:
        selezionate = funzione_filtro(df, champions_slots, STAGIONE_CORRENTE, STAGIONE_PRECEDENTE)
    for team in selezionate:
        selezioni[team].add(nome_filtro)





# Crea DataFrame aggregato SOLO con le squadre filtrate e solo le colonne richieste
df_season = df[df["season"] == STAGIONE_CORRENTE].copy()
df_season = df_season[df_season["team_name"].isin(selezioni.keys())].copy()
df_season = df_season.rename(columns={
    "team_name": "squadra",
    "league_name": "lega"
})
df_season["2025"] = df_season["rank"]
df_2024 = df[(df["season"] == STAGIONE_PRECEDENTE)][["team_name", "rank"]].rename(columns={"rank": "2024", "team_name": "squadra"})
df_season = df_season.merge(df_2024, on="squadra", how="left")
df_season["filtri"] = df_season["squadra"].apply(lambda t: ','.join(sorted(selezioni[t], key=lambda x: int(x[1:]))) if t in selezioni else "")

colonne_finali = ["squadra", "lega", "2025", "2024", "filtri"]

output_path = os.path.join(PROJECT_ROOT, "data", "processed", "selected_teams_F1.csv")
df_out = df_season[colonne_finali].copy()
df_out.insert(0, "#", range(1, len(df_out) + 1))
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df_out.to_csv(output_path, index=False)
print(f"\n===== RISULTATO F1 (SOLO FILTRATE) =====")
print(df_out)
print(f"Totale squadre filtrate: {len(df_out)}\n")
