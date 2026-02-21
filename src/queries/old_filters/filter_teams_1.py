import pandas as pd
import json
import os

# Percorsi file
STATS_PATH = "/Users/andrea/Desktop/crucelli/data/raw/team_stats.csv"
SLOTS_PATH = "data/processed/champions_slots.json"
OUTPUT_PATH = "/Users/andrea/Desktop/crucelli/data/processed/selected_teams_filter1.csv"

# Funzione per caricare il file champions_slots.json
def load_champions_slots(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

# Funzione per ottenere slot dato lega e stagione
def get_slot(league: str, season: int) -> int:
    season_str = str(season)
    slot = champions_slots.get(season_str, {}).get(league, 4)  # Fallback a 4
    return slot

# Carica i dati
df = pd.read_csv(STATS_PATH)
champions_slots = load_champions_slots(SLOTS_PATH)

# Determina la giornata corrente
current_matchday = df["current_matchday"].max()
print(f"Giornata corrente: {current_matchday}")

# Logica per le prime 10 giornate
if current_matchday <= 10:
    print("Siamo nelle prime 10 giornate. Modifica delle stagioni...")
    stagione_corrente = 2023
    stagione_precedente = 2022
    stagione_penultima = 2021
else:
    stagione_corrente = 2024
    stagione_precedente = 2023
    stagione_penultima = 2022

# Filtra i dati per la stagione corrente
df_curr = df[df["season"] == stagione_corrente].copy()
df_curr["slot"] = df_curr["league_name"].apply(lambda l: get_slot(l, stagione_corrente))
df_curr_champ = df_curr[df_curr["rank"] <= df_curr["slot"]]

# Condizione 1: Squadre in zona Champions sia stagione corrente che precedente
df_prev = df[df["season"] == stagione_precedente].copy()
df_prev["slot"] = df_prev["league_name"].apply(lambda l: get_slot(l, stagione_precedente))
df_prev_champ = df_prev[df_prev["rank"] <= df_prev["slot"]]

merged_1 = pd.merge(df_curr_champ, df_prev_champ, on="team_name", suffixes=("_2024", "_2023"))
condizione_1 = merged_1[["team_name", "league_name_2024", "rank_2024", "rank_2023", "slot_2024"]]
condizione_1 = condizione_1.rename(columns={
    "league_name_2024": "league_name",
    "slot_2024": "slot"
})
condizione_1["caso"] = "1"

# Condizione 2: Squadre prime in classifica stagione corrente e penultima
df_1corr = df_curr[df_curr["rank"] == 1]
df_penultima = df[df["season"] == stagione_penultima].copy()
df_1penultima = df_penultima[df_penultima["rank"] == 1]

merged_2 = pd.merge(df_1corr, df_1penultima, on="team_name", suffixes=("_2024", "_2022"))
condizione_2 = merged_2[["team_name", "league_name_2024", "rank_2024", "rank_2022", "slot"]]
condizione_2 = condizione_2.rename(columns={
    "league_name_2024": "league_name"
})
condizione_2["rank_2023"] = None
condizione_2["caso"] = "2"

# Unione finale delle condizioni
selezionate = pd.concat([condizione_1, condizione_2], ignore_index=True)
selezionate = selezionate.drop_duplicates(subset="team_name")

# Aggiungi una colonna con la numerazione progressiva
selezionate.insert(0, "numero", range(1, len(selezionate) + 1))

# Converti i valori numerici in interi e sostituisci NaN con "N/A"
for col in ["rank_2024", "rank_2023", "rank_2022"]:
    selezionate[col] = selezionate[col].apply(lambda x: int(x) if pd.notnull(x) else "N/A")

# Riorganizza le colonne per l'output
output_columns = ["numero", "team_name", "league_name", "rank_2024", "rank_2023", "rank_2022", "slot", "caso"]

# Salva il risultato in un CSV con le colonne desiderate
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
selezionate.to_csv(OUTPUT_PATH, columns=output_columns, index=False)

# Log e output
print(f"\n✅ File salvato in {OUTPUT_PATH} con {len(selezionate)} squadre selezionate")
print("\n📊 Squadre selezionate:")
print(selezionate[output_columns].sort_values("numero").to_string(index=False))