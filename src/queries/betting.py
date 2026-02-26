import pandas as pd
import os
import sys
from datetime import datetime

# Usa solo l'output di regola_1 (F1). Se manca, esegui regola_1.py per generarlo.
F1_PATH = "data/processed/selezione_regola_1.csv"
UPCOMING_PATH = "data/raw/upcoming_matches.csv"
OUTPUT_PATH = "data/processed/bet.csv"

# Carica squadre selezionate da F1; se mancante, prova a generarlo eseguendo lo script `regola_1.py`
if not os.path.exists(F1_PATH):
    print(f"{F1_PATH} non trovato. Genera il file eseguendo `src/queries/regola_1.py` e riprova.")
    sys.exit(1)

df_agg = pd.read_csv(F1_PATH)
if "team_name" in df_agg.columns and "squadra" not in df_agg.columns:
    df_agg = df_agg.rename(columns={"team_name": "squadra"})
if "league_name" in df_agg.columns and "lega" not in df_agg.columns:
    df_agg = df_agg.rename(columns={"league_name": "lega"})
print(f"Squadre filtrate caricate da: {F1_PATH}")

# Carica partite in programma
if not os.path.exists(UPCOMING_PATH):
    raise FileNotFoundError(f"File non trovato: {UPCOMING_PATH}")
df_upcoming = pd.read_csv(UPCOMING_PATH)
df_upcoming["date"] = pd.to_datetime(df_upcoming["date"], utc=True)

# Mantieni solo partite future e non giocate
now_utc = pd.Timestamp.now(tz="UTC")
if "status" in df_upcoming.columns:
    allowed_statuses = {"NS", "TBD"}
    df_upcoming = df_upcoming[
        (df_upcoming["date"] >= now_utc) &
        (df_upcoming["status"].astype(str).str.upper().isin(allowed_statuses))
    ].copy()
else:
    df_upcoming = df_upcoming[df_upcoming["date"] >= now_utc].copy()

# Trova il prossimo incontro per ogni squadra selezionata
team_next_match = {}
for _, row in df_agg.iterrows():
    squadra = row["squadra"].strip().lower()
    filtri = row["filtri"]
    # Trova tutti i match futuri dove la squadra è home o away
    team_matches = df_upcoming[(df_upcoming["home_team"].str.strip().str.lower() == squadra) |
                               (df_upcoming["away_team"].str.strip().str.lower() == squadra)].copy()
    if not team_matches.empty:
        # Prendi il match con la data più vicina
        next_match = team_matches.sort_values("date").iloc[0]
        # Crea le colonne indicatori filtri
        filtro_cols = {}
        for f in ["F1", "F2", "F3", "F4"]:
            filtro_cols[f] = 'x' if f in filtri.split(',') else ''
        team_next_match[squadra] = {
            "squadra selezionata": next_match["home_team"] if next_match["home_team"].strip().lower() == squadra else next_match["away_team"],
            "campionato": next_match["league_name"],
            "squadra in casa": next_match["home_team"],
            "squadra fuori casa": next_match["away_team"],
            "data": next_match["date"].strftime("%d/%m/%y ore %H:%M"),
            "2025": row["2025"],
            "2024": row["2024"],
            **filtro_cols
        }

# Output finale
if team_next_match:
    df_out = pd.DataFrame(team_next_match.values())
    # Ordina per data (convertendo la colonna in datetime per ordinare correttamente)
    df_out['data_sort'] = pd.to_datetime(df_out['data'], format="%d/%m/%y ore %H:%M")
    oggi = datetime.now().strftime("%d/%m/%y")
    df_out['oggi'] = df_out['data'].apply(lambda x: 'OGGI' if x.startswith(oggi) else '')
    df_out = df_out.sort_values('data_sort').drop(columns=['data_sort'])
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Merge completato. File salvato in {OUTPUT_PATH}\n")
else:
    print("Nessuna partita trovata per le squadre selezionate.")
