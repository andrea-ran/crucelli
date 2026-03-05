import pandas as pd
import os
import sys
from datetime import datetime

# Usa solo l'output di regola_1 (F1). Se manca, esegui regola_1.py per generarlo.
F1_PATH = "data/processed/selezione_regola_1.csv"
UPCOMING_PATH = "data/raw/upcoming_matches.csv"
OUTPUT_PATH = "data/processed/bet.csv"
STORICO_PATH = "data/processed/storico.csv"


def append_and_update_storico(df_bet):
    storico_min_cols = [
        "match_id",
        "data",
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
    ]

    if os.path.exists(STORICO_PATH):
        storico_df = pd.read_csv(STORICO_PATH)
    else:
        storico_df = pd.DataFrame(columns=storico_min_cols)

    if "data" in storico_df.columns:
        storico_df["data"] = storico_df["data"].astype(str).str.split(" ore").str[0].str.strip()

    for col in storico_min_cols:
        if col not in storico_df.columns:
            storico_df[col] = ""
    for col in list(storico_df.columns):
        if col not in storico_min_cols:
            storico_df = storico_df.drop(columns=[col])

    if not df_bet.empty:
        df_all = df_bet.copy()
        df_all["data"] = df_all["data"].astype(str).str.split(" ore").str[0].str.strip()
        df_all_min = df_all[[
            "match_id",
            "data",
            "squadra selezionata",
            "squadra in casa",
            "squadra fuori casa",
        ]].copy()

        storico_df["_key"] = storico_df["match_id"].astype(str).str.strip()
        df_all_min["_key"] = df_all_min["match_id"].astype(str).str.strip()
        existing_keys = set(storico_df["_key"].tolist())
        df_new = df_all_min[~df_all_min["_key"].isin(existing_keys)].copy()
        if not df_new.empty:
            storico_df = pd.concat([storico_df, df_new], ignore_index=True)
        storico_df = storico_df.drop(columns=["_key"], errors="ignore")

    try:
        storico_df["data_sort"] = pd.to_datetime(storico_df["data"], format="%d/%m/%y")
        storico_df = storico_df.sort_values("data_sort", ascending=False).drop(columns=["data_sort"])
    except Exception:
        pass

    os.makedirs(os.path.dirname(STORICO_PATH), exist_ok=True)
    storico_df.to_csv(STORICO_PATH, index=False)


if not os.path.exists(F1_PATH):
    print(f"{F1_PATH} non trovato. Genera il file eseguendo `src/queries/regola_1.py` e riprova.")
    sys.exit(1)

df_agg = pd.read_csv(F1_PATH)
if "team_name" in df_agg.columns and "squadra" not in df_agg.columns:
    df_agg = df_agg.rename(columns={"team_name": "squadra"})
if "league_name" in df_agg.columns and "lega" not in df_agg.columns:
    df_agg = df_agg.rename(columns={"league_name": "lega"})
print(f"Squadre filtrate caricate da: {F1_PATH}")

if not os.path.exists(UPCOMING_PATH):
    raise FileNotFoundError(f"File non trovato: {UPCOMING_PATH}")
df_upcoming = pd.read_csv(UPCOMING_PATH)
df_upcoming["date"] = pd.to_datetime(df_upcoming["date"], utc=True)

now_utc = pd.Timestamp.now(tz="UTC")
today_utc = now_utc.normalize()
if "status" in df_upcoming.columns:
    allowed_statuses = {"NS", "TBD"}
    df_upcoming = df_upcoming[
        (df_upcoming["date"].dt.normalize() >= today_utc) &
        (df_upcoming["status"].astype(str).str.upper().isin(allowed_statuses))
    ].copy()
else:
    df_upcoming = df_upcoming[df_upcoming["date"].dt.normalize() >= today_utc].copy()

from collections import OrderedDict

selected_teams = set(df_agg["squadra"].astype(str).str.strip().str.lower())
team_next_match = OrderedDict()
for _, row in df_agg.iterrows():
    squadra = row["squadra"].strip().lower()
    filtri = row["filtri"] if "filtri" in row else ""
    lega = row["lega"] if "lega" in row else ""
    rank_2025 = row["2025"] if "2025" in row else ""
    rank_2024 = row["2024"] if "2024" in row else ""
    team_matches = df_upcoming[(df_upcoming["home_team"].str.strip().str.lower() == squadra) |
                               (df_upcoming["away_team"].str.strip().str.lower() == squadra)].copy()
    if not team_matches.empty:
        next_match = team_matches.sort_values("date").iloc[0]
        scontro_diretto = "SI" if {
            next_match["home_team"].strip().lower(),
            next_match["away_team"].strip().lower(),
        }.issubset(selected_teams) else ""
        team_next_match[squadra] = {
            "match_id": next_match["match_id"],
            "squadra selezionata": next_match["home_team"] if next_match["home_team"].strip().lower() == squadra else next_match["away_team"],
            "squadra in casa": next_match["home_team"],
            "squadra fuori casa": next_match["away_team"],
            "data": next_match["date"].strftime("%d/%m/%y ore %H:%M"),
            "SC": scontro_diretto,
            "lega": lega,
            "2025": rank_2025,
            "2024": rank_2024,
            "filtri": filtri,
        }

if team_next_match:
    df_out = pd.DataFrame(team_next_match.values())
    df_out["squadre_set"] = df_out.apply(
        lambda r: frozenset([r["squadra in casa"].strip().lower(), r["squadra fuori casa"].strip().lower()]),
        axis=1,
    )
    df_out = df_out.drop_duplicates(subset=["squadre_set", "data"])
    df_out = df_out.drop(columns=["squadre_set"])
    df_out["data_sort"] = pd.to_datetime(df_out["data"], format="%d/%m/%y ore %H:%M")
    oggi = datetime.now().strftime("%d/%m/%y")
    df_out["oggi"] = df_out["data"].apply(lambda x: "OGGI" if x.startswith(oggi) else "")
    df_out = df_out.sort_values("data_sort").drop(columns=["data_sort"])

    append_and_update_storico(df_out.copy())

    df_out = df_out.drop(columns=["match_id"], errors="ignore")
    colonne_finali = [
        "squadra selezionata",
        "squadra in casa",
        "squadra fuori casa",
        "data",
        "oggi",
        "SC",
        "lega",
        "2025",
        "2024",
        "filtri",
    ]
    colonne_presenti = [c for c in colonne_finali if c in df_out.columns]
    altre_colonne = [c for c in df_out.columns if c not in colonne_presenti]
    df_out = df_out[colonne_presenti + altre_colonne]
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Merge completato. File salvato in {OUTPUT_PATH}\n")
    print(f"✅ Storico aggiornato in {STORICO_PATH}\n")
else:
    append_and_update_storico(pd.DataFrame())
    print("Nessuna partita trovata per le squadre selezionate.")
