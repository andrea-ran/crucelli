import pandas as pd
import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from season_config import STAGIONE_CORRENTE, STAGIONE_PENULTIMA, STAGIONE_TERZULTIMA


# Percorsi file aggiornati
ARCHIVE_PATH = "data/raw/team_stats_archive.csv"
CURRENT_PATH = "data/raw/team_stats_current.csv"
OUTPUT_PATH = "data/processed/selected_teams_filter4.csv"

# Carica i champions slots dal file JSON per la stagione corrente
CHAMPIONS_SLOTS_PATH = "champions_slots.json"
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots_all = json.load(f)
champions_slots = champions_slots_all[str(STAGIONE_CORRENTE)]

# Definisci i posti Champions per ogni lega (come negli altri filtri)
champions_slots = champions_slots_all[str(STAGIONE_CORRENTE)]


# Carica e concatena dati archivio + stagione corrente
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
team_stats = pd.concat([df_archive, df_current], ignore_index=True)

results = []

league_synonyms = {
    "saudi pro league": ["saudi pro league", "saudi professional league"],
    "prva hnl": ["prva hnl", "hnl"]
}
for league in champions_slots.keys():
    # Gestione sinonimi Saudi Pro League
    if league in league_synonyms:
        mask = (team_stats["season"] == STAGIONE_CORRENTE) & (team_stats["league_name"].str.lower().isin([s.lower() for s in league_synonyms[league]]))
        df_league = team_stats[mask].copy()
    else:
        df_league = team_stats[(team_stats["season"] == STAGIONE_CORRENTE) & (team_stats["league_name"] == league)].copy()
    if df_league.empty:
        continue
    df_sorted = df_league.sort_values("rank")
    if len(df_sorted) < 2:
        continue
    prima = df_sorted.iloc[0]
    seconda = df_sorted.iloc[1]
    # Squadre potenzialmente "vicine" alla prima
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
        # Condizioni filtro 4
        condizione = False
        # Prima o seconda in classifica
        if row["rank"] in [1,2]:
            condizione = True
        # Potenzialmente a meno 1 punto dalla prima
        elif diff == -1:
            condizione = True
        # Meno 3 punti con 1 partita in meno
        elif diff == -3 and diff_partite == -1:
            condizione = True
        # Meno 2, meno 6 o meno 8 punti con 1 partita in meno
        elif diff in [-2, -6, -8] and diff_partite == -1:
            condizione = True
        if not condizione:
            continue
        # Verifica posizione penultima o terzultima stagione
        penultima = team_stats[(team_stats["season"] == STAGIONE_PENULTIMA) & (team_stats["league_name"] == league) & (team_stats["team_name"] == team)]
        terzultima = team_stats[(team_stats["season"] == STAGIONE_TERZULTIMA) & (team_stats["league_name"] == league) & (team_stats["team_name"] == team)]
        condizione_storica = False
        if not penultima.empty and penultima.iloc[0]["rank"] in [1,2]:
            condizione_storica = True
        if not terzultima.empty and terzultima.iloc[0]["rank"] in [1,2]:
            condizione_storica = True
        if condizione_storica:
            results.append({
                "team_name": team,
                "league_name": league,
                "points": punti,
                "points_prima": punti_prima,
                "diff": diff,
                "matches": partite,
                "matches_prima": partite_prima,
                "diff_partite": diff_partite
            })

# Output finale
df_out = pd.DataFrame(results)
if not df_out.empty:
    df_out = df_out[["team_name", "league_name", "points", "points_prima", "diff", "matches", "matches_prima", "diff_partite"]]
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Filtro 4 completato. File salvato in {OUTPUT_PATH}")
    print(df_out)
else:
    print("Nessuna squadra selezionata dal filtro 4.")
