import pandas as pd
import os
import json

# === Percorsi file ===
INPUT_PATH = "/Users/andrea/Desktop/crucelli/data/processed/selected_teams_filter2.csv"
STATS_PATH = "/Users/andrea/Desktop/crucelli/data/raw/team_stats.csv"
CHAMPIONS_SLOTS_PATH = "/Users/andrea/Desktop/crucelli/data/processed/champions_slots.json"
EXCLUDED_PATH = "/Users/andrea/Desktop/crucelli/data/processed/excluded_teams.csv"
OUTPUT_PATH = "/Users/andrea/Desktop/crucelli/data/processed/selected_teams_filter3.csv"

# === Carica dati ===
df_input = pd.read_csv(INPUT_PATH)
df_stats = pd.read_csv(STATS_PATH)
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots = json.load(f)

def is_qualified(team, league, season):
    season_str = str(season)
    slot = champions_slots.get(season_str, {}).get(league, 4)
    df = df_stats[(df_stats["season"] == season) & (df_stats["league_name"] == league)]
    qualified = df[df["rank"] <= slot]["team_name"].tolist()
    return team in qualified

# FILTRO 3: elimina squadre che devono giocare contro squadre qualificate in Champions nella stagione precedente E anche nella penultima O terzultima
selected = []
eliminate = []

for _, row in df_input.iterrows():
    team_name = row["team_name"]
    league_name = row["league_name"]
    opponent = row["opponent"]

    stagione_precedente = 2023
    stagione_penultima = 2022
    stagione_terzultima = 2021

    qualified_prev = is_qualified(opponent, league_name, stagione_precedente)
    qualified_penult = is_qualified(opponent, league_name, stagione_penultima)
    qualified_terzult = is_qualified(opponent, league_name, stagione_terzultima)

    if qualified_prev and (qualified_penult or qualified_terzult):
        eliminate.append({
            "team_name": team_name,
            "league_name": league_name,
            "reason": "eliminata_filtro3",
            "opponent": opponent,
            "motivo_elim": "avversario qualificato Champions in stagioni consecutive"
        })
        print(f"❌ ELIMINATA: {team_name} vs {opponent} ({league_name}) - Avversario qualificato Champions in stagioni consecutive")
    else:
        selected.append(row)
        print(f"✅ SELEZIONATA: {team_name} vs {opponent} ({league_name})")

# Salva le squadre che superano il filtro 3
df_selected = pd.DataFrame(selected)
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
df_selected.to_csv(OUTPUT_PATH, index=False)
print(f"\n✅ Filtro 3 completato. File salvato in {OUTPUT_PATH}")

# Aggiorna excluded_teams.csv SOLO con le nuove eliminate
if eliminate:
    if os.path.exists(EXCLUDED_PATH):
        df_excl_prev = pd.read_csv(EXCLUDED_PATH)
        df_eliminate = pd.DataFrame(eliminate)
        df_excl_final = pd.concat([df_excl_prev, df_eliminate], ignore_index=True).drop_duplicates(subset=["team_name", "league_name", "opponent", "reason"])
    else:
        df_excl_final = pd.DataFrame(eliminate)
    df_excl_final.to_csv(EXCLUDED_PATH, index=False)
    print(f"✅ Aggiornato {EXCLUDED_PATH} con le nuove eliminate dal filtro 3")

print("\n--- RIEPILOGO ---")
print(f"Squadre selezionate: {len(df_selected)}")
print(f"Squadre eliminate dal filtro 3: {len(eliminate)}") 