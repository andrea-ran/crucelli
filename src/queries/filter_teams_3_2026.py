import pandas as pd
import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from season_config import STAGIONE_CORRENTE, STAGIONE_PRECEDENTE


# Percorsi file aggiornati
ARCHIVE_PATH = "data/raw/team_stats_archive.csv"
CURRENT_PATH = "data/raw/team_stats_current.csv"
COPPA_PATH = "data/raw/coppa_nazionale.csv"
OUTPUT_PATH = "data/processed/selected_teams_filter3.csv"

# Carica i posti Champions per ogni lega
CHAMPIONS_SLOTS_PATH = "champions_slots.json"
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots_all = json.load(f)
champions_slots = champions_slots_all[str(STAGIONE_CORRENTE)]

# Definisci i posti Champions per ogni lega (come negli altri filtri)
champions_slots = {
    "serie a": 4,
    "premier league": 4,
    "la liga": 4,
    "bundesliga": 4,
    "ligue 1": 4,
    "primeira liga": 3,
    "eredivisie": 2,
    "prva hnl": 1,
    "saudi pro league": 2,
    "super lig": 2
}

def get_champions_zone(df, stagione, champions_slots):
    df_season = df[df["season"] == stagione]
    result = []
    league_synonyms = {
        "saudi pro league": ["saudi pro league", "saudi professional league"],
        "prva hnl": ["prva hnl", "hnl"]
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


# Carica e concatena dati archivio + stagione corrente
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
team_stats = pd.concat([df_archive, df_current], ignore_index=True)


# Carica dati coppa nazionale
df_coppa = pd.read_csv(COPPA_PATH)
# Squadre qualificate Champions stagione precedente (classifica + coppa)
zone_precedente = get_champions_zone(team_stats, STAGIONE_PRECEDENTE, champions_slots)
coppa_winners = get_coppa_winners(df_coppa, STAGIONE_PRECEDENTE)
qualificate_precedente = zone_precedente.union(coppa_winners)

results = []

for league, slot in champions_slots.items():
    df_league = team_stats[(team_stats["season"] == STAGIONE_CORRENTE) & (team_stats["league_name"] == league)].copy()
    if df_league.empty:
        continue
    # Trova la soglia punti Champions
    df_sorted = df_league.sort_values("rank")
    if len(df_sorted) < slot:
        continue
    soglia_champions = df_sorted.iloc[slot-1]["points"]
    # Squadre fuori dalla zona Champions
    df_out = df_league[df_league["rank"] > slot]
    for _, row in df_out.iterrows():
        team = row["team_name"]
        punti = row["points"]
        partite_giocate = row["matches"] if "matches" in row else None
        partite_giocate_champ = df_sorted.iloc[slot-1]["matches"] if "matches" in row else None
        diff = punti - soglia_champions
        # Calcola differenza partite giocate
        diff_partite = None
        if partite_giocate is not None and partite_giocate_champ is not None:
            diff_partite = partite_giocate_champ - partite_giocate
        # Condizioni filtro 3
        condizione = False
        if diff in [0, -1, -2]:
            condizione = True
        elif diff == -3 and diff_partite == -1:
            condizione = True
        elif diff == -4 and diff_partite == -1:
            condizione = True
        elif diff == -5 and diff_partite == -1:
            condizione = True
        elif diff == -6 and diff_partite == -1:
            condizione = True
        elif diff == -5 and row.get("is_home", False):
            condizione = True
        # Solo se qualificata la stagione precedente
        if condizione and team in qualificate_precedente:
            results.append({
                "team_name": team,
                "league_name": league,
                "points": punti,
                "champions_threshold": soglia_champions,
                "diff": diff,
                "matches": partite_giocate,
                "matches_champ": partite_giocate_champ,
                "diff_partite": diff_partite
            })

# Output finale
import pandas as pd

df_out = pd.DataFrame(results)
if not df_out.empty:
    df_out = df_out[["team_name", "league_name", "points", "champions_threshold", "diff", "matches", "matches_champ", "diff_partite"]]
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Filtro 3 completato. File salvato in {OUTPUT_PATH}")
    print(df_out)
else:
    print("Nessuna squadra selezionata dal filtro 3.")
