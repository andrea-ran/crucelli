
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import pandas as pd
import json
from season_config import STAGIONE_CORRENTE, STAGIONE_PRECEDENTE
from synonyms import normalize_league_name


# Percorsi file aggiornati
ARCHIVE_PATH = "data/raw/team_stats_archive.csv"
CURRENT_PATH = "data/raw/team_stats_current.csv"
CHAMPIONS_SLOTS_PATH = "champions_slots.json"
OUTPUT_PATH = "data/processed/selected_teams_filter2.csv"


# Carica e concatena dati archivio + stagione corrente
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
team_stats = pd.concat([df_archive, df_current], ignore_index=True)
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots_all = json.load(f)
champions_slots = champions_slots_all[str(STAGIONE_CORRENTE)]

# Funzione per ottenere le squadre in zona Champions
# (come in filtro 1, senza chiave stagione)
def get_champions_zone(df, stagione, champions_slots):
    df_season = df[df["season"] == stagione].copy()
    # Normalizza i nomi delle leghe nei dati
    df_season["league_name_norm"] = df_season["league_name"].apply(normalize_league_name)
    result = []
    for league, slot in champions_slots.items():
        league_norm = normalize_league_name(league)
        squadre = df_season[df_season["league_name_norm"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
        result.extend(squadre)
    return set(result)

# Funzione per ottenere la soglia punti Champions per ogni lega
def get_champions_points_threshold(df, stagione, league, champions_slots):
    slot = champions_slots.get(league, 4)
    league_norm = normalize_league_name(league)
    df_season = df[df["season"] == stagione].copy()
    df_season["league_name_norm"] = df_season["league_name"].apply(normalize_league_name)
    league_teams = df_season[df_season["league_name_norm"] == league_norm]
    if league_teams.empty:
        return None
    sorted_teams = league_teams.nsmallest(slot, "rank")
    if sorted_teams.empty:
        return None
    return sorted_teams.iloc[-1]["points"]

# Squadre in zona Champions stagione corrente
zone_corrente = get_champions_zone(team_stats, STAGIONE_CORRENTE, champions_slots)

# Prepara risultati
results = []

for team in zone_corrente:
    # Trova la lega della squadra nella stagione corrente
    row_corr = team_stats[(team_stats["season"] == STAGIONE_CORRENTE) & (team_stats["team_name"] == team)].copy()
    if row_corr.empty:
        continue
    league = row_corr.iloc[0]["league_name"]
    league_norm = normalize_league_name(league)
    # Trova punti squadra stagione precedente
    row_prev = team_stats[(team_stats["season"] == STAGIONE_PRECEDENTE) & (team_stats["team_name"] == team)].copy()
    # Normalizza anche qui la lega
    row_prev["league_name_norm"] = row_prev["league_name"].apply(normalize_league_name)
    row_prev = row_prev[row_prev["league_name_norm"] == league_norm]
    if row_prev.empty:
        continue
    punti_squadra = row_prev.iloc[0]["points"]
    # Trova soglia punti Champions stagione precedente
    soglia_champions = get_champions_points_threshold(team_stats, STAGIONE_PRECEDENTE, league, champions_slots)
    if soglia_champions is None:
        continue
    diff = punti_squadra - soglia_champions
    if diff >= -3:
        results.append({
            "team_name": team,
            "league_name": league,
            "points_prev": punti_squadra,
            "champions_threshold_prev": soglia_champions,
            "diff": diff
        })

# Output finale
df_out = pd.DataFrame(results)
if not df_out.empty:
    df_out = df_out[["team_name", "league_name", "points_prev", "champions_threshold_prev", "diff"]]
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Filtro 2 completato. File salvato in {OUTPUT_PATH}")
    print(df_out)
else:
    print("Nessuna squadra selezionata dal filtro 2.")
