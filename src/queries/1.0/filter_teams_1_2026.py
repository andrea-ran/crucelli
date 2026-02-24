import pandas as pd
import sys
import json
sys.path.append("/Users/andrea/Desktop/crucelli")
from season_config import STAGIONE_CORRENTE, STAGIONE_PRECEDENTE


# Percorsi file aggiornati
ARCHIVE_PATH = "/Users/andrea/Desktop/crucelli/data/raw/team_stats_archive.csv"
CURRENT_PATH = "/Users/andrea/Desktop/crucelli/data/raw/team_stats_current.csv"
COPPA_PATH = "/Users/andrea/Desktop/crucelli/data/raw/coppa_nazionale.csv"
OUTPUT_PATH = "/Users/andrea/Desktop/crucelli/data/processed/selected_teams_filter1.csv"

CHAMPIONS_SLOTS_PATH = "champions_slots.json"


# Carica e concatena dati archivio + stagione corrente
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
df = pd.concat([df_archive, df_current], ignore_index=True)
df_coppa = pd.read_csv(COPPA_PATH)

# Carica i posti Champions specifici per la stagione
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots_all = json.load(f)

# Usa i posti Champions specifici per la stagione
champions_slots = champions_slots_all[str(STAGIONE_CORRENTE)]

def get_champions_zone(df, stagione, champions_slots):
    """
    Restituisce le squadre in zona Champions League per una stagione.
    Gestisce sinonimi per la Saudi Pro League.
    """
    from synonyms import normalize_league_name
    df_season = df[df["season"] == stagione].copy()
    df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
    result = []
    for league, slot in champions_slots.items():
        league_norm = normalize_league_name(league)
        squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
        result.extend(squadre)
    return set(result)

def get_coppa_winners(df_coppa, stagione):
    """
    Restituisce le squadre che hanno vinto la coppa nazionale in una stagione.
    """
    return set(df_coppa[df_coppa["season"] == stagione]["team_name"].tolist())

# Squadre in zona Champions stagione corrente
zone_corrente = get_champions_zone(df, STAGIONE_CORRENTE, champions_slots)

# Squadre qualificate Champions stagione precedente (classifica + coppa)
champions_slots_prev = champions_slots_all[str(STAGIONE_PRECEDENTE)]
zone_precedente = get_champions_zone(df, STAGIONE_PRECEDENTE, champions_slots_prev)
coppa_winners = get_coppa_winners(df_coppa, STAGIONE_PRECEDENTE)
qualificate_precedente = zone_precedente.union(coppa_winners)

# Seleziona squadre che soddisfano entrambe le condizioni
selezionate = [team for team in zone_corrente if team in qualificate_precedente]

# Output finale
df_out = df[(df["season"] == STAGIONE_CORRENTE) & (df["team_name"].isin(selezionate))]
df_out.to_csv(OUTPUT_PATH, index=False)
print(f"✅ Filtro 1 completato. File salvato in {OUTPUT_PATH}")
print(df_out)