import argparse
import os
import sys
import subprocess
from datetime import datetime

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

STEPS = [
    ("update_data", os.path.join(PROJECT_ROOT, "src", "data_update", "update_data.py")),
    ("update_national_cup", os.path.join(PROJECT_ROOT, "src", "data_update", "update_national_cup.py")),
    ("update_upcoming_champions", os.path.join(PROJECT_ROOT, "src", "data_update", "update_upcoming_champions.py")),
    ("update_upcoming", os.path.join(PROJECT_ROOT, "src", "data_update", "update_upcoming.py")),
    ("regola_1", os.path.join(PROJECT_ROOT, "src", "queries", "regola_1.py")),
    ("betting", os.path.join(PROJECT_ROOT, "src", "queries", "betting.py")),
    ("analisi_storico", os.path.join(PROJECT_ROOT, "src", "queries", "analisi_storico.py")),
]

OUTPUT_CHECKS = {
    "all_matches_current": os.path.join(PROJECT_ROOT, "data", "raw", "all_matches_current.csv"),
    "team_stats_current": os.path.join(PROJECT_ROOT, "data", "raw", "team_stats_current.csv"),
    "coppa_nazionale": os.path.join(PROJECT_ROOT, "data", "raw", "coppa_nazionale.csv"),
    "upcoming_champions": os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_champions.csv"),
    "upcoming_matches": os.path.join(PROJECT_ROOT, "data", "raw", "upcoming_matches.csv"),
    "selezione_regola_1": os.path.join(PROJECT_ROOT, "data", "processed", "selezione_regola_1.csv"),
    "bet": os.path.join(PROJECT_ROOT, "data", "processed", "bet.csv"),
    "storico": os.path.join(PROJECT_ROOT, "data", "processed", "storico.csv"),
    "storico_report": os.path.join(PROJECT_ROOT, "data", "processed", "storico_report.csv"),
}

API_WARNINGS = [
    "API_FOOTBALL_KEY non impostata",
    "Connessione API-FOOTBALL non disponibile",
    "Errore API",
]


def format_header(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def run_step(name, script_path, python_executable, stop_on_error):
    if not os.path.exists(script_path):
        print(f"[ERROR] Script non trovato: {script_path}")
        return 1, ["Script non trovato"], []

    print(f"\n[RUN] {name} -> {script_path}")
    start = datetime.now()

    process = subprocess.Popen(
        [python_executable, script_path],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout_lines = []
    stderr_lines = []

    while True:
        out = process.stdout.readline() if process.stdout else ""
        if out:
            print(out, end="")
            stdout_lines.append(out)
        err = process.stderr.readline() if process.stderr else ""
        if err:
            print(err, end="", file=sys.stderr)
            stderr_lines.append(err)
        if out == "" and err == "" and process.poll() is not None:
            break

    exit_code = process.wait()
    elapsed = (datetime.now() - start).total_seconds()

    print(f"[DONE] {name} (exit={exit_code}, {elapsed:.1f}s)")

    if exit_code != 0 and stop_on_error:
        print("[STOP] Interruzione per errore.")

    return exit_code, stdout_lines, stderr_lines


def read_csv_rows(path):
    if not os.path.exists(path):
        return None
    try:
        return len(pd.read_csv(path))
    except Exception:
        return None


def summarize_outputs():
    print("\n[SUMMARY] Output files")
    for label, path in OUTPUT_CHECKS.items():
        rows = read_csv_rows(path)
        if rows is None:
            status = "missing or unreadable"
        else:
            status = f"{rows} rows"
        print(f"- {label}: {status}")


def summarize_selection_details():
    selection_path = OUTPUT_CHECKS.get("selezione_regola_1")
    bet_path = OUTPUT_CHECKS.get("bet")
    if selection_path and os.path.exists(selection_path):
        try:
            df_sel = pd.read_csv(selection_path)
            print("\n[DETAIL] Selezione regola 1")
            print(f"- squadre selezionate: {len(df_sel)}")
            if "filtri" in df_sel.columns and not df_sel.empty:
                filtro_counts = (
                    df_sel["filtri"]
                    .astype(str)
                    .str.split(",")
                    .explode()
                    .str.strip()
                )
                filtro_counts = filtro_counts[filtro_counts != ""]
                if not filtro_counts.empty:
                    counts = filtro_counts.value_counts().sort_index()
                    for filtro, count in counts.items():
                        print(f"  - {filtro}: {count}")
        except Exception:
            print("\n[DETAIL] Selezione regola 1: errore lettura CSV")

    if bet_path and os.path.exists(bet_path):
        try:
            df_bet = pd.read_csv(bet_path)
            print("\n[DETAIL] Bet")
            print(f"- partite selezionate: {len(df_bet)}")
            if "squadra selezionata" in df_bet.columns and not df_bet.empty:
                top = df_bet["squadra selezionata"].astype(str).value_counts().head(10)
                if not top.empty:
                    print("- squadre piu' frequenti:")
                    for team, count in top.items():
                        print(f"  - {team}: {count}")
        except Exception:
            print("\n[DETAIL] Bet: errore lettura CSV")


def extract_api_warnings(lines):
    warnings = []
    for line in lines:
        text = line.strip()
        for needle in API_WARNINGS:
            if needle in text and text not in warnings:
                warnings.append(text)
    return warnings


def main():
    parser = argparse.ArgumentParser(description="Run Crucelli data pipeline")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop pipeline on first error")
    parser.add_argument(
        "--details",
        action="store_true",
        help="Mostra dettagli su squadre e partite selezionate",
    )
    args = parser.parse_args()

    format_header("Crucelli pipeline")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Python: {sys.executable}")

    all_warnings = []
    for name, script_path in STEPS:
        exit_code, stdout_lines, stderr_lines = run_step(
            name,
            script_path,
            sys.executable,
            args.stop_on_error,
        )
        warnings = extract_api_warnings(stdout_lines + stderr_lines)
        all_warnings.extend(warnings)

        if exit_code != 0 and args.stop_on_error:
            break

    summarize_outputs()
    if args.details:
        summarize_selection_details()

    if all_warnings:
        print("\n[WARNINGS] API or connection issues")
        for warning in sorted(set(all_warnings)):
            print(f"- {warning}")


if __name__ == "__main__":
    main()
