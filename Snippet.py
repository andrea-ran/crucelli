import pandas as pd

ARCHIVE_PATH = "data/raw/team_stats_archive.csv"
CURRENT_PATH = "data/raw/team_stats_current.csv"

# Carica i dati
df_archive = pd.read_csv(ARCHIVE_PATH)
df_current = pd.read_csv(CURRENT_PATH)
team_stats = pd.concat([df_archive, df_current], ignore_index=True)

# Stampa tutti i valori unici della colonna "season"
print("Valori unici della colonna 'season':")
print(team_stats["season"].unique())