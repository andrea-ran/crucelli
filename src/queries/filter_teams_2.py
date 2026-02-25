import pandas as pd
import os
import json
import unicodedata
from datetime import datetime
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

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    levels = {
        "INFO": "ℹ️",
        "SUCCESS": "✅",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "ELIMINATA": "❌",
        "RIPESCATA": "🔄"
    }
    print(f"{timestamp} {levels.get(level, 'ℹ️')} {message}")

# Percorsi file
FILTER1_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "selected_teams_F1.csv")
ARCHIVE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_archive.csv")
CURRENT_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_current.csv")
UPCOMING_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_matches.csv")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "selected_teams_F2.csv")
CHAMPIONS_SLOTS_PATH = os.path.join(PROJECT_ROOT, "champions_slots.json")
UPCOMING_CHAMPIONS_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_champions.csv")

# Caricamento dati
log("Caricamento dei dati...")
df_filter1 = pd.read_csv(FILTER1_PATH)
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
df_stats = pd.concat([df_archive, df_current], ignore_index=True)
df_upcoming = pd.read_csv(UPCOMING_PATH)
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots = json.load(f)
df_upcoming_champions = pd.read_csv(UPCOMING_CHAMPIONS_PATH)
log("Dati caricati con successo.", "SUCCESS")

# Filtra solo per la stagione corrente (2024)
df_stats = df_stats[df_stats["season"] == 2024]

def calculate_champions_threshold(league_name, stats_df, champions_slots):
    season_str = "2024"
    league_name_norm = normalize_league_name(league_name)
    if season_str in champions_slots and league_name_norm in champions_slots[season_str]:
        slots = champions_slots[season_str][league_name_norm]
        league_stats = stats_df[stats_df["league_name"].apply(normalize_league_name) == league_name_norm]
        if league_stats.empty:
            return None
        league_stats_sorted = league_stats.nsmallest(slots, "rank")
        if league_stats_sorted.empty:
            return None
        return league_stats_sorted.iloc[-1]["points"]
    return None

def plays_next_champions(team, df_upcoming_champions):
    df = df_upcoming_champions.copy()
    found = False
    for col in df.columns:
        if "team" in col:
            found = True
            df = df.rename(columns={col: "team_name"})
            df["team_name"] = df["team_name"].astype(str).str.lower().str.strip()
    if not found:
        return False
    team = team.lower().strip()
    return not df[df["team_name"] == team].empty

results = []
eliminate = []
ripescate = []
excluded_teams = []

for _, row in df_filter1.iterrows():
    team_name = row["squadra"] if "squadra" in row else row["team_name"]
    league_name = row["lega"] if "lega" in row else row["league_name"]
    league_name = normalize_league_name(league_name)

    # Trova la prossima partita (sia home che away)
    match = df_upcoming[
        (df_upcoming["home_team"] == team_name) | (df_upcoming["away_team"] == team_name)
    ]
    if match.empty:
        log(f"ATTENZIONE: {team_name:<20} | Nessuna prossima partita trovata. Questo non dovrebbe accadere!", "ERROR")
        continue

    row_match = match.iloc[0]
    opponent = row_match["away_team"] if row_match["home_team"] == team_name else row_match["home_team"]
    is_home = row_match["home_team"] == team_name

    # Trova i punti dell'avversario
    opponent_stats = df_stats[
        (df_stats["team_name"] == opponent) & (df_stats["league_name"].apply(normalize_league_name) == league_name)
    ]
    opponent_points = opponent_stats["points"].iloc[0] if not opponent_stats.empty else 0

    # Calcola i punti soglia Champions
    champions_points = calculate_champions_threshold(league_name, df_stats, champions_slots)

    # Calcola la differenza di partite giocate
    matches_played_diff = (
        opponent_stats["played"].iloc[0] - row["played"]
        if "played" in opponent_stats.columns and "played" in row.index and not opponent_stats.empty
        else 0
    )

    # Calcola il delta tra i punti dell'avversario e i punti soglia Champions
    delta_points = champions_points - opponent_points if champions_points is not None else None

    # --- LOGICA ELIMINAZIONE ---
    elimina = False
    motivo_elim = ""
    motivo_ripescaggio = ""

    # ARABIA SAUDITA (Pro League)
    if "pro league" in league_name or "arabia" in league_name or "saudi" in league_name:
        if champions_points is None:
            elimina = True
            motivo_elim = "champions_points_missing"
        elif delta_points <= 0:
            elimina = True
            motivo_elim = "opponent_in_champions_zone"
        elif matches_played_diff == 1 and delta_points <= 1:
            elimina = True
            motivo_elim = "opponent_close_to_champions_slot"
    else:
        if champions_points is None:
            elimina = True
            motivo_elim = "champions_points_missing"
        elif delta_points <= 10:
            elimina = True
            motivo_elim = "opponent_close_to_champions_slot"
        elif matches_played_diff == 1 and delta_points <= 11:
            elimina = True
            motivo_elim = "opponent_close_slot_1_match_less"
        elif matches_played_diff == 2 and delta_points <= 12:
            elimina = True
            motivo_elim = "opponent_close_slot_2_matches_less"

    # ECCEZIONI RIPESCAGGIO (solo se eliminata)
    if elimina:
        team_stats = df_stats[(df_stats["team_name"] == team_name) & (df_stats["league_name"].apply(normalize_league_name) == league_name)]
        t_points = int(team_stats.iloc[0]["points"]) if not team_stats.empty else 0
        if (t_points - opponent_points) >= 15:
            motivo_ripescaggio = "diff_punti_>=15"
            elimina = False
        elif (
            "played" in row and row["played"] <= 11
            and is_home
        ):
            motivo_ripescaggio = "prime_11_giornate_casa_vs_non_qualificata"
            elimina = False

    if elimina:
        eliminate.append((team_name, opponent, league_name, motivo_elim))
        # log eliminata rimosso
        excluded_teams.append({
            "team_name": team_name,
            "league_name": league_name,
            "reason": "eliminata_filtro2",
            "opponent": opponent,
            "motivo_elim": motivo_elim
        })
        continue
    else:
        if motivo_ripescaggio:
            ripescate.append((team_name, opponent, league_name, motivo_ripescaggio))
            # log ripescata rimosso

    t_points = int(df_stats[(df_stats["team_name"] == team_name) & (df_stats["league_name"].apply(normalize_league_name) == league_name)]["points"].iloc[0]) if not df_stats[(df_stats["team_name"] == team_name) & (df_stats["league_name"].apply(normalize_league_name) == league_name)].empty else ""

    results.append({
        "team_name": team_name,
        "league_name": league_name,
        "opponent": opponent,
        "t_points": t_points,
        "op_points": opponent_points,
        "champ_points": champions_points,
        "delta": delta_points,
        "matches_diff": matches_played_diff,
        "ripescaggio": motivo_ripescaggio
    })

# Crea DataFrame e impagina come richiesto
df_results = pd.DataFrame(results)
df_results.insert(0, "#", range(1, len(df_results) + 1))

output_columns = [
    "#", "team_name", "league_name", "opponent",
    "t_points", "op_points", "champ_points", "delta", "matches_diff", "ripescaggio"
]
df_results = df_results[output_columns]

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
df_results.to_csv(OUTPUT_PATH, index=False)
log(f"✅ File generato: {OUTPUT_PATH}", "SUCCESS")
print(df_results)


log("Non salviamo più le squadre eliminate su file (si leggono dal log a terminale)")
print("\n--- ELIMINATE ---")
for team, opponent, league, motivo in eliminate:
    print(f"{team} vs {opponent} ({league}) - {motivo}")
print("\n--- RIPESCATE ---")
for team, opponent, league, motivo in ripescate:
    print(f"{team} vs {opponent} ({league}) - {motivo}")