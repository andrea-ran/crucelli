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


def format_csv_table(csv_path, max_col_width=36):
    if not os.path.exists(csv_path):
        return ""

    with open(csv_path, "r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    if not rows:
        return ""

    headers = rows[0]
    data_rows = rows[1:]
    if not data_rows:
        return ""

    # Limit columns to a compact summary
    keep_cols = []
    for idx, name in enumerate(headers):
        if name in {"data", "squadra in casa", "squadra fuori casa", "lega_match"}:
            keep_cols.append(idx)

    if not keep_cols:
        keep_cols = list(range(min(6, len(headers))))

    filtered = [[headers[i] for i in keep_cols]]
    for row in data_rows:
        filtered.append([row[i] if i < len(row) else "" for i in keep_cols])

    widths = []
    for col in zip(*filtered):
        widths.append(min(max(len(str(cell)) for cell in col), max_col_width))

    lines = []
    for r_idx, row in enumerate(filtered):
        cells = []
        for c_idx, cell in enumerate(row):
            value = str(cell).replace("\n", " ").strip()
            if len(value) > widths[c_idx]:
                value = value[: widths[c_idx] - 1] + "..."
            cells.append(value.ljust(widths[c_idx]))
        lines.append(" | ".join(cells))
        if r_idx == 0:
            lines.append("-+-".join("-" * w for w in widths))

    return "\n".join(lines)


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
    betting_bot = os.path.join(PROJECT_ROOT, "src", "queries", "betting-bot.py")

    print("[1/4] Aggiorno dati stagionali...")
    run_script(update_data)

    print("[2/4] Aggiorno partite in programma...")
    run_script(update_upcoming)

    print("[3/4] Aggiorno coppe nazionali...")
    run_script(update_cup)

    print("[4/4] Calcolo selezioni...")
    output_csv = os.path.join(PROJECT_ROOT, "data", "processed", "bet.csv")
    run_script(betting_bot, ["--output", output_csv])

    table = format_csv_table(output_csv)
    today = datetime.now().strftime("%d/%m/%Y")
    subject = f"Crucelli - Selezioni {today}"
    if table:
        body = "Selezione incontri:\n\n" + table + "\n"
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
