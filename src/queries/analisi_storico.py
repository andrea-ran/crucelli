import argparse
import os
import sys
from datetime import datetime
import unicodedata

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from api_config import API_KEY, HEADERS
from src.synonyms import normalize_team_name


STORICO_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "storico.csv")
STORICO_REPORT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "storico_report.csv")

DEFAULT_TIMEOUT = 10
FINISHED_STATUSES = {"FT", "AET", "PEN"}


def create_session(retries=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = create_session()


def normalize_text(value):
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode()
    return text


def fetch_fixture_result(match_id):
    if not HEADERS:
        return None
    url = f"https://v3.football.api-sports.io/fixtures?id={match_id}"
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        payload = response.json().get("response", [])
        if not payload:
            return None
        match = payload[0]
        home_team = match["teams"]["home"]["name"]
        away_team = match["teams"]["away"]["name"]
        home_score = match["goals"]["home"]
        away_score = match["goals"]["away"]
        status_short = match["fixture"]["status"]["short"]

        winner = ""
        if home_score is not None and away_score is not None:
            if home_score > away_score:
                winner = home_team
            elif away_score > home_score:
                winner = away_team
            else:
                winner = "pareggio"

        return {
            "status": status_short,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "winner": winner,
        }
    except requests.RequestException:
        return None


def fetch_fixture_events(match_id):
    if not HEADERS:
        return []
    url = f"https://v3.football.api-sports.io/fixtures/events?fixture={match_id}"
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json().get("response", [])
    except requests.RequestException:
        return []


def _event_minute(event):
    time_info = event.get("time", {}) if isinstance(event, dict) else {}
    elapsed = time_info.get("elapsed")
    extra = time_info.get("extra")
    if elapsed is None:
        return None
    try:
        minute = int(elapsed)
    except (TypeError, ValueError):
        return None
    if extra is not None:
        try:
            minute += int(extra)
        except (TypeError, ValueError):
            pass
    return minute


def _scoring_side(event_team, home_team, away_team, detail):
    team_norm = normalize_team_name(event_team)
    home_norm = normalize_team_name(home_team)
    away_norm = normalize_team_name(away_team)
    if not team_norm:
        return ""

    side = ""
    if team_norm == home_norm:
        side = "home"
    elif team_norm == away_norm:
        side = "away"

    if not side:
        return ""

    if normalize_text(detail) == "own goal":
        return "away" if side == "home" else "home"
    return side


def compute_late_draw_cashout(selected_team, home_team, away_team, events, threshold_minute=85):
    selected_norm = normalize_team_name(selected_team)
    home_norm = normalize_team_name(home_team)
    away_norm = normalize_team_name(away_team)
    if selected_norm not in {home_norm, away_norm}:
        return False

    selected_side = "home" if selected_norm == home_norm else "away"
    opponent_side = "away" if selected_side == "home" else "home"

    goal_events = []
    for idx, event in enumerate(events):
        if not isinstance(event, dict):
            continue
        if event.get("type") != "Goal":
            continue
        detail = str(event.get("detail", "")).strip()
        detail_norm = normalize_text(detail)
        if detail_norm in {"missed penalty", "penalty missed"}:
            continue

        minute = _event_minute(event)
        if minute is None:
            continue

        event_team = event.get("team", {}).get("name", "")
        side = _scoring_side(event_team, home_team, away_team, detail)
        if not side:
            continue
        goal_events.append((minute, idx, side))

    if not goal_events:
        return False

    goal_events.sort(key=lambda item: (item[0], item[1]))

    score_home = 0
    score_away = 0
    for minute, _, side in goal_events:
        if side == "home":
            score_home += 1
        elif side == "away":
            score_away += 1

        if side != opponent_side:
            continue

        pre_home = score_home - (1 if side == "home" else 0)
        pre_away = score_away - (1 if side == "away" else 0)

        if selected_side == "home":
            was_selected_leading = pre_home > pre_away
        else:
            was_selected_leading = pre_away > pre_home

        now_draw = score_home == score_away
        if was_selected_leading and now_draw and minute > threshold_minute:
            return True
    return False


def compute_esito_from_scores(row):
    existing_esito = str(row.get("esito_pick", "")).strip().upper()
    if existing_esito == "VINTAP":
        return row

    try:
        hs = int(float(row.get("hs", "")))
        a_s = int(float(row.get("as", "")))
    except (TypeError, ValueError):
        return row

    selected = normalize_team_name(str(row.get("squadra selezionata", "")).strip())
    home_team = normalize_team_name(str(row.get("squadra in casa", "")).strip())
    away_team = normalize_team_name(str(row.get("squadra fuori casa", "")).strip())

    if not selected or not home_team or not away_team:
        return row

    if selected == home_team:
        if hs > a_s:
            row["esito_pick"] = "VINTA"
        else:
            row["esito_pick"] = "PERSA"
    elif selected == away_team:
        if a_s > hs:
            row["esito_pick"] = "VINTA"
        else:
            row["esito_pick"] = "PERSA"
    return row


def _parse_quota(value):
    if value is None:
        return None
    raw = str(value).strip().replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def compute_profit(row):
    esito = str(row.get("esito_pick", "")).strip().upper()
    quota = _parse_quota(row.get("quota", ""))
    if esito in {"VINTA", "VINTAP"}:
        return "" if quota is None else round(quota - 1, 2)
    if esito == "PERSA":
        return -1
    return ""


def main():
    parser = argparse.ArgumentParser(description="Genera il report storico dalle partite archiviate.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ricalcola tutto ignorando lo storico_report.csv",
    )
    args = parser.parse_args()

    if not API_KEY:
        print("API_FOOTBALL_KEY non impostata. Esempio: export API_FOOTBALL_KEY=la_tua_chiave")
        return

    if not os.path.exists(STORICO_PATH):
        print(f"Nessuno storico trovato: {STORICO_PATH}")
        return

    cutoff_date = datetime(2026, 3, 1)

    storico_df = pd.read_csv(STORICO_PATH)
    if storico_df.empty or "match_id" not in storico_df.columns:
        print("Storico vuoto o privo di match_id, nessuna analisi eseguita.")
        return

    if "data" in storico_df.columns:
        storico_df["_data_raw"] = storico_df["data"].astype(str).str.split(" ore").str[0].str.strip()
        storico_df["_data_dt"] = pd.to_datetime(
            storico_df["_data_raw"],
            format="%d/%m/%y",
            errors="coerce",
        )
        storico_df = storico_df[storico_df["_data_dt"].notna()].copy()
        storico_df = storico_df[storico_df["_data_dt"] >= cutoff_date].copy()
        storico_df = storico_df.drop(columns=["_data_raw", "_data_dt"], errors="ignore")

    report_existing = pd.DataFrame()
    if not args.force and os.path.exists(STORICO_REPORT_PATH):
        report_existing = pd.read_csv(STORICO_REPORT_PATH)

    if "data" in report_existing.columns:
        report_existing["_data_raw"] = (
            report_existing["data"].astype(str).str.split(" ore").str[0].str.strip()
        )
        report_existing["_data_dt"] = pd.to_datetime(
            report_existing["_data_raw"],
            format="%d/%m/%y",
            errors="coerce",
        )
        report_existing = report_existing[report_existing["_data_dt"].notna()].copy()
        report_existing = report_existing[report_existing["_data_dt"] >= cutoff_date].copy()
        report_existing = report_existing.drop(columns=["_data_raw", "_data_dt"], errors="ignore")

    if not report_existing.empty and "match_id" in report_existing.columns:
        report_existing["match_id"] = report_existing["match_id"].astype(str).str.strip()
        report_existing = report_existing.drop_duplicates(subset=["match_id"], keep="last")

    def is_complete_report(row):
        hs = str(row.get("hs", "")).strip()
        a_s = str(row.get("as", "")).strip()
        esito = str(row.get("esito_pick", "")).strip()
        quota = str(row.get("quota", "")).strip()
        return hs != "" and a_s != "" and esito != "" and quota != ""

    processed_ids = set()
    if not report_existing.empty:
        processed_ids = set(
            report_existing[report_existing.apply(is_complete_report, axis=1)]["match_id"].tolist()
        )

    fixture_cache = {}
    events_cache = {}

    rows = []
    for _, row in storico_df.iterrows():
        match_id = str(row.get("match_id", "")).strip()
        if not match_id:
            continue

        if match_id in processed_ids:
            continue

        if match_id not in fixture_cache:
            fixture_cache[match_id] = fetch_fixture_result(match_id)
        fixture_data = fixture_cache.get(match_id)
        if not fixture_data:
            continue

        home_team = fixture_data.get("home_team") or str(row.get("squadra in casa", "")).strip()
        away_team = fixture_data.get("away_team") or str(row.get("squadra fuori casa", "")).strip()

        raw_selected = row.get("squadra selezionata", "")
        if pd.isna(raw_selected):
            selected_team = ""
        else:
            selected_team = str(raw_selected).strip()
        if selected_team.lower() in {"nan", "none"}:
            selected_team = ""

        if not selected_team:
            continue

        raw_quota = row.get("quota", "")
        if pd.isna(raw_quota):
            selected_odd = ""
        else:
            selected_odd = str(raw_quota).strip()
        if selected_odd.lower() in {"nan", "none"}:
            selected_odd = ""

        home_score = fixture_data.get("home_score")
        away_score = fixture_data.get("away_score")
        status = fixture_data.get("status", "")
        winner = fixture_data.get("winner", "")

        data_value = str(row.get("data", "")).strip()
        if data_value:
            try:
                parsed = datetime.strptime(data_value, "%d/%m/%y ore %H:%M")
                data_value = parsed.strftime("%d/%m/%y")
            except ValueError:
                if " ore" in data_value:
                    data_value = data_value.split(" ore")[0].strip()

        esito_pick = ""
        if status in FINISHED_STATUSES and selected_team:
            if match_id not in events_cache:
                events_cache[match_id] = fetch_fixture_events(match_id)
            events = events_cache.get(match_id) or []
            if compute_late_draw_cashout(
                selected_team=selected_team,
                home_team=home_team,
                away_team=away_team,
                events=events,
                threshold_minute=85,
            ):
                esito_pick = "VINTAP"

        row_out = {
            "match_id": match_id,
            "data": data_value,
            "squadra selezionata": selected_team,
            "squadra in casa": home_team,
            "squadra fuori casa": away_team,
            "hs": "" if home_score is None else str(int(home_score)),
            "as": "" if away_score is None else str(int(away_score)),
            "vincitore": winner,
            "esito_pick": esito_pick,
            "quota": selected_odd,
        }

        row_out = compute_esito_from_scores(row_out)
        row_out["profitto"] = compute_profit(row_out)
        rows.append(row_out)

    if not rows:
        print("Nessun dato disponibile per il report storico.")
        return

    report_df = pd.DataFrame(rows)
    mask_giocata = (
        (~report_df["hs"].isnull())
        & (~report_df["as"].isnull())
        & (report_df["hs"].astype(str).str.strip() != "")
        & (report_df["as"].astype(str).str.strip() != "")
    )
    report_df = report_df[mask_giocata].copy()

    if not report_existing.empty and "match_id" in report_existing.columns:
        updated_ids = set(report_df["match_id"].astype(str).str.strip().tolist())
        existing_keep = report_existing[~report_existing["match_id"].isin(updated_ids)].copy()
        report_df = pd.concat([report_df, existing_keep], ignore_index=True)

    for col in ["hs", "as"]:
        if col in report_df.columns:
            col_num = pd.to_numeric(report_df[col], errors="coerce")
            report_df[col] = col_num.map(lambda v: "" if pd.isna(v) else str(int(v)))

    try:
        report_df["data_sort"] = pd.to_datetime(report_df["data"], format="%d/%m/%y")
        report_df = report_df.sort_values("data_sort", ascending=False).drop(columns=["data_sort"])
    except Exception as exc:
        print(f"[WARN] Ordinamento per data fallito: {exc}")

    profit_series = pd.to_numeric(report_df.get("profitto"), errors="coerce")
    total_profit = profit_series.sum(min_count=1)
    if pd.notna(total_profit):
        esito_series = report_df.get("esito_pick", pd.Series([], dtype=str))
        esito_count = esito_series.astype(str).str.strip().ne("").sum()
        roi_value = round((float(total_profit) / esito_count) * 100, 2) if esito_count else ""
        win_count = esito_series.astype(str).str.strip().isin(["VINTA", "VINTAP"]).sum()
        success_value = round((win_count / esito_count) * 100, 2) if esito_count else ""
        total_wins = profit_series[profit_series > 0].sum(min_count=1)
        total_losses = profit_series[profit_series < 0].abs().sum(min_count=1)
        pf_value = round((total_wins / total_losses), 2) if total_losses else ""
        total_row = {
            "match_id": "TOTALE",
            "profitto": round(float(total_profit), 2),
            "ROI %": roi_value,
            "Successo %": success_value,
            "PF": pf_value,
        }
        report_df = pd.concat([pd.DataFrame([total_row]), report_df], ignore_index=True)

    os.makedirs(os.path.dirname(STORICO_REPORT_PATH), exist_ok=True)
    report_df.to_csv(STORICO_REPORT_PATH, index=False)
    print(f"✅ Report storico creato: {STORICO_REPORT_PATH}")


if __name__ == "__main__":
    main()
