import csv
import os
import subprocess
import sys
from datetime import datetime
from email.message import EmailMessage
import smtplib


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


def load_env_file(path=None):
    path = path or os.path.join(PROJECT_ROOT, ".env")
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def get_env(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def run_script(script_path, args=None):
    args = args or []
    cmd = [sys.executable, script_path] + args
    result = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Script failed:\n"
            f"Command: {' '.join(cmd)}\n"
            f"Exit code: {result.returncode}\n"
            f"Stdout:\n{result.stdout}\n"
            f"Stderr:\n{result.stderr}"
        )
    return result.stdout


def format_match_list(csv_path):
    if not os.path.exists(csv_path):
        return []

    with open(csv_path, "r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    if not rows:
        return []

    headers = rows[0]
    data_rows = rows[1:]
    if not data_rows:
        return []

    def col_index(name):
        try:
            return headers.index(name)
        except ValueError:
            return None

    idx_date = col_index("data")
    idx_home = col_index("squadra in casa")
    idx_away = col_index("squadra fuori casa")
    idx_league = col_index("lega_match")
    idx_sc = col_index("sc")
    idx_sel_home = col_index("selezione casa")
    idx_sel_away = col_index("selezione trasferta")

    lines = []
    for row in data_rows:
        date_raw = row[idx_date] if idx_date is not None and idx_date < len(row) else ""
        home = row[idx_home] if idx_home is not None and idx_home < len(row) else ""
        away = row[idx_away] if idx_away is not None and idx_away < len(row) else ""
        league = row[idx_league] if idx_league is not None and idx_league < len(row) else ""
        sc = row[idx_sc] if idx_sc is not None and idx_sc < len(row) else ""
        sel_home = row[idx_sel_home] if idx_sel_home is not None and idx_sel_home < len(row) else ""
        sel_away = row[idx_sel_away] if idx_sel_away is not None and idx_sel_away < len(row) else ""

        date_raw = str(date_raw).strip()
        home = str(home).strip()
        away = str(away).strip()
        league = str(league).strip()

        if " ore " in date_raw:
            date_part, time_part = date_raw.split(" ore ", 1)
            date_text = f"{date_part} {time_part}"
        else:
            date_text = date_raw

        pick = ""
        if str(sc).strip().upper() == "SI":
            pick = "SC (entrambe)"
        elif str(sel_home).strip().upper() == "SI":
            pick = f"CASA: {home}"
        elif str(sel_away).strip().upper() == "SI":
            pick = f"TRASFERTA: {away}"
        else:
            pick = "N/D"

        line = f"- {date_text} | {home} vs {away} | puntare: {pick}"
        if league:
            line += f" ({league})"
        lines.append(line)

    return lines


def send_email(subject, body, sender, recipients, smtp_host, smtp_port, smtp_user, smtp_pass):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(smtp_user, smtp_pass)
        smtp.send_message(msg)


def main():
    load_env_file()

    smtp_host = get_env("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(get_env("SMTP_PORT", "587"))
    smtp_user = get_env("SMTP_USER", required=True)
    smtp_pass = get_env("SMTP_PASS", required=True)
    email_from = get_env("EMAIL_FROM", smtp_user)
    raw_recipients = get_env("EMAIL_TO", required=True)
    recipients = [r.strip() for r in raw_recipients.split(",") if r.strip()]
    if not recipients:
        raise RuntimeError("EMAIL_TO is empty")

    update_data = os.path.join(PROJECT_ROOT, "src", "data_update", "update_data.py")
    update_upcoming = os.path.join(PROJECT_ROOT, "src", "data_update", "update_upcoming.py")
    update_cup = os.path.join(PROJECT_ROOT, "src", "data_update", "update_national_cup.py")
    update_upcoming_champions = os.path.join(
        PROJECT_ROOT, "src", "data_update", "update_upcoming_champions.py"
    )
    betting_bot = os.path.join(PROJECT_ROOT, "src", "queries", "betting-bot.py")

    print("[1/4] Aggiorno dati stagionali...")
    run_script(update_data)

    print("[2/4] Aggiorno partite in programma...")
    run_script(update_upcoming)

    print("[3/4] Aggiorno coppe nazionali...")
    run_script(update_cup)

    print("[4/5] Aggiorno upcoming champions...")
    run_script(update_upcoming_champions)

    print("[5/5] Calcolo selezioni...")
    output_csv = os.path.join(PROJECT_ROOT, "data", "processed", "bet.csv")
    run_script(betting_bot, ["--output", output_csv])

    match_lines = format_match_list(output_csv)
    today = datetime.now().strftime("%d/%m/%Y")
    subject = f"Crucelli - Selezioni {today}"
    if match_lines:
        count = len(match_lines)
        body = "Selezione incontri:\n"
        body += f"Totale partite: {count}\n\n"
        body += "\n".join(match_lines) + "\n"
    else:
        body = "oggi nessuna partita\n"

    send_email(
        subject=subject,
        body=body,
        sender=email_from,
        recipients=recipients,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
    )

    print("Email inviata.")


if __name__ == "__main__":
    main()
