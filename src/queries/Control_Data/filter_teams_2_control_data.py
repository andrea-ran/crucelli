import pandas as pd
import json

# Percorsi file
FILTER1_PATH = "/Users/andrea/Desktop/crucelli/data/processed/selected_teams_filter1.csv"
UPCOMING_PATH = "/Users/andrea/Desktop/crucelli/data/raw/upcoming_matches.csv"
STATS_PATH = "/Users/andrea/Desktop/crucelli/data/raw/team_stats.csv"
CHAMPIONS_SLOTS_PATH = "/Users/andrea/Desktop/crucelli/data/processed/champions_slots.json"
OUTPUT_PATH = "/Users/andrea/Desktop/crucelli/data/processed/filter_teams_2_control_data_output.csv"

# Carica i dati
df_filter1 = pd.read_csv(FILTER1_PATH)
df_upcoming = pd.read_csv(UPCOMING_PATH)
df_stats = pd.read_csv(STATS_PATH)
with open(CHAMPIONS_SLOTS_PATH, "r") as f:
    champions_slots = json.load(f)

# Dizionario per correggere i nomi delle squadre e dei campionati
TEAM_NAME_MAPPING = {
    "bayern munchen": "bayern munich",
    "borussia monchengladbach": "borussia m'gladbach",
    "borussia mönchengladbach": "borussia m'gladbach",  # Correzione per il nome con dieresi
    "premiership": "scottish premiership",
    # Aggiungi altre correzioni se necessario
}

# Normalizza i nomi delle squadre e dei campionati
def normalize(s):
    s = str(s).strip().lower()
    return TEAM_NAME_MAPPING.get(s, s)  # Corregge il nome se presente nel dizionario

df_filter1["team_name"] = df_filter1["team_name"].apply(normalize)
df_filter1["league_name"] = df_filter1["league_name"].apply(normalize)
df_upcoming["home_team"] = df_upcoming["home_team"].apply(normalize)
df_upcoming["away_team"] = df_upcoming["away_team"].apply(normalize)
df_stats["team_name"] = df_stats["team_name"].apply(normalize)
df_stats["league_name"] = df_stats["league_name"].apply(normalize)

# Debug temporaneo
print("Valori unici di team_name in df_stats:")
print(df_stats["team_name"].unique())

# Filtra solo per la stagione corrente (2024)
df_stats = df_stats[df_stats["season"] == 2024]

# Funzione per calcolare i punti dell'ultima squadra in zona Champions
def calculate_champions_threshold(league_name, stats_df, champions_slots):
    season_str = "2024"
    if season_str in champions_slots and league_name in champions_slots[season_str]:
        slots = champions_slots[season_str][league_name]
        league_stats = stats_df[(stats_df["league_name"] == league_name)]
        
        # Verifica se ci sono squadre valide
        if league_stats.empty:
            return None
        
        # Ordina per rank e prendi i punti dell'ultima squadra in zona Champions
        league_stats_sorted = league_stats.nsmallest(slots, "rank")
        if league_stats_sorted.empty:
            return None
        
        return league_stats_sorted.iloc[-1]["points"]
    return None

# Lista per salvare i risultati
results = []

# Itera sulle squadre selezionate
for _, row in df_filter1.iterrows():
    team_name = row["team_name"]
    league_name = row["league_name"]

    # Trova i punti della squadra
    team_stats = df_stats[(df_stats["team_name"] == team_name) & (df_stats["league_name"] == league_name)]
    team_points = team_stats["points"].iloc[0] if not team_stats.empty else None

    # Trova la prossima partita
    match = df_upcoming[
        (df_upcoming["home_team"] == team_name) | (df_upcoming["away_team"] == team_name)
    ]
    if match.empty:
        results.append({
            "team_name": team_name,
            "opponent": "N/A",
            "opponent_points": 0,
            "champions_points": None,
            "delta_points": None
        })
        continue

    # Determina l'avversario
    row_match = match.iloc[0]
    opponent = row_match["away_team"] if row_match["home_team"] == team_name else row_match["home_team"]

    # Trova i punti dell'avversario
    opponent_stats = df_stats[
        (df_stats["team_name"] == opponent) & (df_stats["league_name"] == league_name)
    ]
    opponent_points = opponent_stats["points"].iloc[0] if not opponent_stats.empty else 0

    # Calcola i punti soglia Champions
    champions_points = calculate_champions_threshold(league_name, df_stats, champions_slots)

    # Aggiungi i risultati
    results.append({
        "team_name": team_name,
        "opponent": opponent,
        "opponent_points": opponent_points,
        "champions_points": champions_points,
        "delta_points": opponent_points - champions_points if champions_points is not None else None
    })

# Crea un DataFrame con i risultati
df_results = pd.DataFrame(results)

# Salva il risultato in un CSV
df_results.to_csv(OUTPUT_PATH, index=False)

# Mostra solo l'output richiesto
print(f"✅ File di analisi generato: {OUTPUT_PATH}")
pd.set_option("display.max_columns", None)  # Mostra tutte le colonne
pd.set_option("display.width", 1000)  # Imposta una larghezza maggiore per l'output
print(df_results[["team_name", "opponent", "opponent_points", "champions_points", "delta_points"]])