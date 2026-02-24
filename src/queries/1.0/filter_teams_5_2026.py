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
UPCOMING_PATH = "data/raw/upcoming_matches.csv"
OUTPUT_PATH = "data/processed/selected_teams_filter5.csv"

# Definisci i posti Champions per ogni lega (come negli altri filtri)
CHAMPIONS_SLOTS_PATH = "champions_slots.json"
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots_all = json.load(f)
champions_slots = champions_slots_all[str(STAGIONE_CORRENTE)]

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


# Carica e concatena dati archivio + stagione corrente
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
team_stats = pd.concat([df_archive, df_current], ignore_index=True)
df_coppa = pd.read_csv(COPPA_PATH)
df_upcoming = pd.read_csv(UPCOMING_PATH)

# Squadre qualificate Champions stagione precedente (classifica + coppa)
zone_precedente = get_champions_zone(team_stats, STAGIONE_PRECEDENTE, champions_slots)
coppa_winners = get_coppa_winners(df_coppa, STAGIONE_PRECEDENTE)
qualificate_precedente = zone_precedente.union(coppa_winners)

results = []

for league in champions_slots.keys():
    # Squadre stagione corrente
    df_league = team_stats[(team_stats["season"] == STAGIONE_CORRENTE) & (team_stats["league_name"] == league)].copy()
    if df_league.empty:
        continue
    df_sorted = df_league.sort_values("rank")
    if len(df_sorted) < 2:
        continue
    seconda = df_sorted.iloc[1]
    punti_seconda = seconda["points"]
    partite_seconda = seconda["matches"] if "matches" in seconda else None
    # Squadre che giocano in casa la prossima partita
    home_teams = set(df_upcoming["home_team"].unique())
    for _, row in df_league.iterrows():
        team = row["team_name"]
        punti = row["points"]
        partite = row["matches"] if "matches" in row else None
        diff = punti - punti_seconda
        diff_partite = None
        if partite is not None and partite_seconda is not None:
            diff_partite = partite_seconda - partite
        # Condizioni filtro 5
        condizione = False
        # Prima o seconda in classifica
        if row["rank"] in [1,2]:
            condizione = True
        # Potenzialmente con una partita in meno rispetto alla seconda
        elif diff < 0 and diff_partite == -1:
            condizione = True
        # Solo se gioca in casa la prossima
        if condizione and team in home_teams:
            # Solo se qualificata la stagione precedente
            if team in qualificate_precedente:
                results.append({
                    "team_name": team,
                    "league_name": league,
                    "points": punti,
                    "points_seconda": punti_seconda,
                    "diff": diff,
                    "matches": partite,
                    "matches_seconda": partite_seconda,
                    "diff_partite": diff_partite
                })

# Output finale
df_out = pd.DataFrame(results)
if not df_out.empty:
    df_out = df_out[["team_name", "league_name", "points", "points_seconda", "diff", "matches", "matches_seconda", "diff_partite"]]
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Filtro 5 completato. File salvato in {OUTPUT_PATH}")
    print(df_out)
else:
    print("Nessuna squadra selezionata dal filtro 5.")
