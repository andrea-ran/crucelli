import pandas as pd
import os
import json
import importlib.util
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

loader_spec = importlib.util.spec_from_file_location("project_loader", os.path.join(PROJECT_ROOT, "project_loader.py"))
if loader_spec is None or loader_spec.loader is None:
    raise ImportError("Impossibile caricare project_loader.py")
project_loader = importlib.util.module_from_spec(loader_spec)
loader_spec.loader.exec_module(project_loader)
load_project_module = project_loader.load_project_module
PROJECT_ROOT = project_loader.PROJECT_ROOT

synonyms = load_project_module("synonyms", "synonyms.py")
normalize_league_name = synonyms.normalize_league_name

# === Percorsi file ===
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "selected_teams_F2.csv")
ARCHIVE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_archive.csv")
CURRENT_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_current.csv")
CHAMPIONS_SLOTS_PATH = os.path.join(PROJECT_ROOT, "champions_slots.json")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "selected_teams_F3.csv")
EXCLUDED_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "excluded_teams_F3.csv")

# === Carica dati ===
df_input = pd.read_csv(INPUT_PATH)
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
df_stats = pd.concat([df_archive, df_current], ignore_index=True)
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots = json.load(f)

# Funzione normalizzata per verifica qualificazione
def is_qualified(team, league, season):
    season_str = str(season)
    league_norm = normalize_league_name(league)
    slot = champions_slots.get(season_str, {}).get(league_norm, 4)
    df = df_stats[(df_stats["season"] == season) & (df_stats["league_name"].apply(normalize_league_name) == league_norm)]
    qualified = df[df["rank"] <= slot]["team_name"].tolist()
    return team in qualified

selected = []
eliminate = []

for _, row in df_input.iterrows():
    team_name = row["team_name"] if "team_name" in row else row["squadra"]
    league_name = row["league_name"] if "league_name" in row else row["lega"]
    opponent = row["opponent"]
    league_name = normalize_league_name(league_name)

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
if not df_selected.empty:
    if "#" in df_selected.columns:
        df_selected = df_selected.drop(columns=["#"])
    df_selected.insert(0, "#", range(1, len(df_selected) + 1))
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
df_selected.to_csv(OUTPUT_PATH, index=False)

# Non salviamo le eliminate su file (si leggono dal log a terminale)