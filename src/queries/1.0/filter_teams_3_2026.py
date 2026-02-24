import pandas as pd
import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from season_config import STAGIONE_CORRENTE, STAGIONE_PRECEDENTE
from synonyms import normalize_league_name, normalize_team_name


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
 # champions_slots ora viene solo dal JSON

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
    league_norm = normalize_league_name(league)
    df_league = team_stats[(team_stats["season"] == STAGIONE_CORRENTE) & (team_stats["league_name"].apply(normalize_league_name) == league_norm)].copy()
    df_league["team_name"] = df_league["team_name"].apply(normalize_team_name)
    if df_league.empty:
        continue
    # Trova la soglia punti Champions
    df_sorted = df_league.sort_values("rank")
    if len(df_sorted) < slot:
        continue
    soglia_champions = df_sorted.iloc[slot-1]["points"]
    partite_champions = df_sorted.iloc[slot-1]["matches"] if "matches" in df_sorted.columns else None
    # Squadre fuori dalla zona Champions
    df_out = df_league[df_league["rank"] > slot]
    for _, row in df_out.iterrows():
        team = row["team_name"]
        punti = row["points"]
        partite_giocate = row["matches"] if "matches" in row else None
        diff = punti - soglia_champions
        diff_partite = None
        if partite_champions is not None and partite_giocate is not None:
            diff_partite = partite_champions - partite_giocate
        # Applicazione nuova regola
        condizione = False
        # Squadre qualificate in Champions o vincitrici coppa nazionale stagione precedente
        if team in qualificate_precedente:
            # A 3 o meno punti dalla zona Champions (incluso 0)
            if diff >= -3 and diff <= 0:
                condizione = True
            # A 6 o meno punti dalla zona Champions se hanno giocato una partita in meno rispetto all'ultima squadra in zona champions (incluso 0)
            elif diff >= -6 and diff <= 0 and diff_partite == -1:
                condizione = True
        if condizione:
            results.append({
                "team_name": team,
                "league_name": league_norm,  # Salva il nome normalizzato
                "points": punti,
                "champions_threshold": soglia_champions,
                "diff": diff,
                "matches": partite_giocate,
                "matches_champ": partite_champions,
                "diff_partite": diff_partite
            })

# Output finale
df_out = pd.DataFrame(results)
import pandas as pd


df_out = pd.DataFrame(results)
if not df_out.empty:
    df_out = df_out[["team_name", "league_name", "points", "champions_threshold", "diff", "matches", "matches_champ", "diff_partite"]]
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Filtro 3 completato. File salvato in {OUTPUT_PATH}")
    print(df_out)
else:
    print("Nessuna squadra selezionata dal filtro 3.")
