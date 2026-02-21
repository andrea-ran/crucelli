import pandas as pd
from synonyms import normalize_league_name, normalize_team_name

team_stats = pd.concat([
    pd.read_csv("data/raw/team_stats_archive.csv"),
    pd.read_csv("data/raw/team_stats_current.csv")
], ignore_index=True)

team_stats["league_name"] = team_stats["league_name"].apply(normalize_league_name)
team_stats["team_name"] = team_stats["team_name"].apply(normalize_team_name)

# Slot Champions per Primeira Liga 2025
slot = 2

# Qualificate Champions stagione precedente (Primeira Liga, 2024, slot=2)
df_prev = team_stats[(team_stats["season"] == 2024) & (team_stats["league_name"] == "primeira liga")].sort_values("rank")
qualificate_precedente = set(df_prev.head(slot)["team_name"])
print("Qualificate precedente:", qualificate_precedente)

# Stagione corrente
df_curr = team_stats[(team_stats["season"] == 2025) & (team_stats["league_name"] == "primeira liga")].sort_values("rank")
soglia_champions = df_curr.iloc[slot-1]["points"]  # slot=2

print(f"Soglia Champions (2°): {soglia_champions}")
for _, row in df_curr.iterrows():
    team = row["team_name"]
    punti = row["points"]
    rank = row["rank"]
    diff = punti - soglia_champions
    condizione = False
    if team in qualificate_precedente:
        if diff >= -3 and diff <= 0:
            condizione = True
    print(f"team: {team}, rank: {rank}, diff: {diff}, slot: {slot}, qualif: {team in qualificate_precedente}, condizione: {condizione}")