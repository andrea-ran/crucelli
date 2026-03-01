import os
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STORICO_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "storico.csv")
REPORT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "storico_report.csv")


def compute_esito_from_scores(row):
    try:
        hs = int(float(row.get("hs", "")))
        a_s = int(float(row.get("as", "")))
    except (TypeError, ValueError):
        return row

    selected = str(row.get("squadra selezionata", "")).strip().lower()
    home_team = str(row.get("squadra in casa", "")).strip().lower()
    away_team = str(row.get("squadra fuori casa", "")).strip().lower()

    if not selected or not home_team or not away_team:
        return row

    if selected == home_team:
        if hs > a_s:
            row["esito_pick"] = "VINTA"
        elif hs < a_s:
            row["esito_pick"] = "PERSA"
        else:
            row["esito_pick"] = "PERSA"
    elif selected == away_team:
        if a_s > hs:
            row["esito_pick"] = "VINTA"
        elif a_s < hs:
            row["esito_pick"] = "PERSA"
        else:
            row["esito_pick"] = "PERSA"
    return row


def pick_max_quota(group):
    if len(group) == 1:
        row = group.iloc[0].copy()
        row = compute_esito_from_scores(row)
        return row

    group = group.copy()
    group["quota_num"] = pd.to_numeric(group.get("quota", ""), errors="coerce")
    if group["quota_num"].isnull().all():
        row = group.iloc[0].copy()
        row["esito_pick"] = "n.p."
        return row

    idx_max = group["quota_num"].idxmax()
    return group.loc[idx_max].drop("quota_num")


def main():
    df = pd.read_csv(STORICO_PATH)

    if "quota" not in df.columns:
        if "quota_pick_api" in df.columns:
            df["quota"] = df["quota_pick_api"]
        else:
            df["quota"] = ""

    report_df = df.groupby("match_id", as_index=False).apply(pick_max_quota)
    if isinstance(report_df.index, pd.MultiIndex):
        report_df = report_df.reset_index(drop=True)

    mask_giocata = (
        (~report_df["hs"].isnull())
        & (~report_df["as"].isnull())
        & (report_df["hs"].astype(str).str.strip() != "")
        & (report_df["as"].astype(str).str.strip() != "")
    )
    report_df = report_df[mask_giocata].copy()

    try:
        report_df["data_sort"] = pd.to_datetime(report_df["data"], format="%d/%m/%y")
        report_df = report_df.sort_values("data_sort", ascending=False).drop(columns=["data_sort"])
    except Exception as exc:
        print(f"[WARN] Ordinamento per data fallito: {exc}")

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    report_df.to_csv(REPORT_PATH, index=False)
    print(f"✅ Report storico creato: {REPORT_PATH}")


if __name__ == "__main__":
    main()
