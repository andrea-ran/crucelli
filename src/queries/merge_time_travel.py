import argparse
import os
import shutil
from datetime import datetime

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STORICO_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "storico.csv")
TIME_TRAVEL_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "bet_time_travel.csv")

EXPECTED_COLS = [
    "match_id",
    "data",
    "squadra selezionata",
    "squadra in casa",
    "squadra fuori casa",
    "quota",
    "quota_1",
    "quota_x",
    "quota_2",
    "SC",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Merge storico.csv con bet_time_travel.csv.")
    parser.add_argument(
        "--storico",
        default=STORICO_PATH,
        help="Percorso storico.csv",
    )
    parser.add_argument(
        "--time-travel",
        default=TIME_TRAVEL_PATH,
        help="Percorso bet_time_travel.csv",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Non creare backup dello storico.",
    )
    return parser.parse_args()


def normalize_date(series):
    return series.astype(str).str.split(" ore").str[0].str.strip()


def main():
    args = parse_args()

    if not os.path.exists(args.storico):
        raise FileNotFoundError(args.storico)
    if not os.path.exists(args.time_travel):
        raise FileNotFoundError(args.time_travel)

    storico_df = pd.read_csv(args.storico)
    travel_df = pd.read_csv(args.time_travel)

    for col in EXPECTED_COLS:
        if col not in storico_df.columns:
            storico_df[col] = ""
        if col not in travel_df.columns:
            travel_df[col] = ""

    storico_df = storico_df[EXPECTED_COLS].copy()
    travel_df = travel_df[EXPECTED_COLS].copy()

    if "data" in storico_df.columns:
        storico_df["data"] = normalize_date(storico_df["data"])
    if "data" in travel_df.columns:
        travel_df["data"] = normalize_date(travel_df["data"])

    storico_df["match_id"] = storico_df["match_id"].astype(str).str.strip()
    travel_df["match_id"] = travel_df["match_id"].astype(str).str.strip()

    existing = set(storico_df["match_id"].tolist())
    new_rows = travel_df[~travel_df["match_id"].isin(existing)].copy()

    merged = pd.concat([storico_df, new_rows], ignore_index=True)

    try:
        merged["data_sort"] = pd.to_datetime(merged["data"], format="%d/%m/%y", errors="coerce")
        merged = merged.sort_values("data_sort", ascending=False).drop(columns=["data_sort"])
    except Exception:
        pass

    if not args.no_backup:
        backup_dir = os.path.join(os.path.dirname(args.storico), "backup_storici")
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(
            backup_dir,
            f"storico_{datetime.now().strftime('%d-%m-%Y_%H%M%S')}.csv",
        )
        shutil.copy2(args.storico, backup_path)
        print(f"Backup storico: {backup_path}")

    merged.to_csv(args.storico, index=False)
    print(f"Aggiunte righe: {len(new_rows)}")
    print(f"Totale storico: {len(merged)}")


if __name__ == "__main__":
    main()
