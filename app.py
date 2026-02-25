from flask import Flask, render_template, redirect, url_for, request, session
import pandas as pd
import subprocess
import os
import sys

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'cambia-questa-chiave')
PASSWORD = os.getenv('APP_PASSWORD')
AUTH_ENABLED = bool(PASSWORD)

BET_PATH = os.path.join('data', 'processed', 'bet.csv')
BETTING_SCRIPT = os.path.join('src', 'queries', 'betting.py')

def login_required(f):
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not AUTH_ENABLED:
            return f(*args, **kwargs)
        if not session.get('logged_in'):
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
def login():
    if not AUTH_ENABLED:
        return redirect(url_for('index'))
    error = None
    if request.method == 'POST':
        if request.form.get('password') == PASSWORD:
            session['logged_in'] = True
            next_page = request.args.get('next') or url_for('index')
            return redirect(next_page)
        else:
            error = 'Password errata!'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    if not AUTH_ENABLED:
        return redirect(url_for('index'))
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    msg = request.args.get('msg')
    if os.path.exists(BET_PATH):
        df = pd.read_csv(BET_PATH)
        # Ordina sempre per data
        if 'data' in df.columns:
            df['data_sort'] = pd.to_datetime(df['data'], format="%d/%m/%y ore %H:%M")
            df = df.sort_values('data_sort').drop(columns=['data_sort'])
        # Aggiungi numerazione da 1 come prima colonna (dopo ordinamento)
        df.insert(0, 'N', range(1, len(df) + 1))
        # Sostituisci NaN con stringa vuota nelle colonne dei filtri (F1-F5) e nella colonna 'oggi'
        for col in [f'F{i}' for i in range(1, 6)] + ['oggi']:
            if col in df.columns:
                df[col] = df[col].fillna('')
        # Passa i dati e le intestazioni al template, rimuovendo la colonna 'oggi' se presente
        if 'oggi' in df.columns:
            df = df.drop(columns=['oggi'])
        table_headers = df.columns.tolist()
        table_rows = df.to_dict(orient='records')
        # Passa anche la lista degli indici delle righe da evidenziare (quelle che erano 'OGGI')
        highlight_rows = []
        df_oggi = pd.read_csv(BET_PATH)
        if 'oggi' in df_oggi.columns:
            highlight_rows = df_oggi.index[df_oggi['oggi'] == 'OGGI'].tolist()
        return render_template('index.html', table_headers=table_headers, table_rows=table_rows, highlight_rows=highlight_rows, msg=msg)
    else:
        return render_template('index.html', table_headers=None, table_rows=None, highlight_rows=None, no_data=True, msg=msg)


# Percorsi script da eseguire in sequenza
DATA_UPDATE_SCRIPTS = [
    os.path.join('src', 'data_update', 'update_data.py'),
    os.path.join('src', 'data_update', 'update_upcoming.py'),
    os.path.join('src', 'data_update', 'update_national_cup.py'),
    os.path.join('src', 'data_update', 'update_upcoming_champions.py'),
]
FILTER_SCRIPT = os.path.join('src', 'queries', 'filter_teams_1.py')


@app.route('/aggiorna')
@login_required
def aggiorna():
    # Esegui tutti gli script di aggiornamento dati
    for script in DATA_UPDATE_SCRIPTS:
        subprocess.run([sys.executable, script], check=False)
    # Esegui il nuovo script unico di filtro
    subprocess.run([sys.executable, FILTER_SCRIPT], check=False)
    # Esegui lo script betting.py
    subprocess.run([sys.executable, BETTING_SCRIPT], check=False)
    return redirect(url_for('index', msg='Aggiornamento completato!'))

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
