import pandas as pd
import sys
import json
sys.path.append("/Users/andrea/Desktop/crucelli")
from season_config import STAGIONE_CORRENTE, STAGIONE_PRECEDENTE
from synonyms import normalize_league_name
from rules import selezione_filtro_1, selezione_filtro_2, selezione_filtro_3, selezione_filtro_4, selezione_filtro_5

# Percorsi file aggiornati
ARCHIVE_PATH = "data/raw/team_stats_archive.csv"
CURRENT_PATH = "data/raw/team_stats_current.csv"
COPPA_PATH = "data/raw/coppa_nazionale.csv"
CHAMPIONS_SLOTS_PATH = "champions_slots.json"

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
for nome_filtro, funzione_filtro, tipo_parametri in filtri:
    if tipo_parametri == "coppa":
        selezionate = funzione_filtro(df, df_coppa, champions_slots, champions_slots_prev, STAGIONE_CORRENTE, STAGIONE_PRECEDENTE)
    elif tipo_parametri == "storico":
        from season_config import STAGIONE_PENULTIMA, STAGIONE_TERZULTIMA
        selezionate = funzione_filtro(df, champions_slots, STAGIONE_CORRENTE, STAGIONE_PENULTIMA, STAGIONE_TERZULTIMA)
    elif tipo_parametri == "upcoming":
        UPCOMING_PATH = "data/raw/upcoming_matches.csv"
        df_upcoming = pd.read_csv(UPCOMING_PATH)
        selezionate = funzione_filtro(df, df_coppa, df_upcoming, champions_slots, champions_slots_prev, STAGIONE_CORRENTE, STAGIONE_PRECEDENTE)
    else:
        selezionate = funzione_filtro(df, champions_slots, STAGIONE_CORRENTE, STAGIONE_PRECEDENTE)
    for team in selezionate:
        selezioni[team].add(nome_filtro)
    df_out = df[(df["season"] == STAGIONE_CORRENTE) & (df["team_name"].isin(selezionate))]
    output_path = f"data/processed/selected_teams_{nome_filtro}.csv"
    df_out.to_csv(output_path, index=False)
    print(f"\n===== RISULTATO {nome_filtro.upper()} =====")
    print(df_out[["team_name", "league_name", "points", "rank"]])
    print(f"Totale squadre selezionate: {len(df_out)}\n")




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

output_path = "data/processed/selected_teams_aggregato.csv"
df_season[colonne_finali].to_csv(output_path, index=False)
print(f"\n===== RISULTATO AGGREGATO (SOLO FILTRATE) =====")
print(df_season[colonne_finali])
print(f"Totale squadre filtrate: {len(df_season)}\n")
