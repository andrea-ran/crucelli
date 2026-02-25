# ⚽️ Crucelli - WebApp Flask

Crucelli è un progetto Python per la selezione automatizzata di squadre di calcio tramite filtri personalizzati, con interfaccia web sviluppata in Flask.

---

## 🚀 Come avviare il progetto

### 1. Clona il repository
```bash
git clone https://github.com/tuo-utente/PrevisioniCalcio.git
cd PrevisioniCalcio
```

### 2. Crea un ambiente virtuale e attivalo
```bash
python3 -m venv venv
source venv/bin/activate  # Su Windows: venv\Scripts\activate
```

### 3. Installa le dipendenze
```bash
pip install -r requirements.txt
```

### 4. Avvia l'interfaccia web
```bash
python app.py
```
Apri il browser e vai su [http://127.0.0.1:5000](http://127.0.0.1:5000)

---

## ⚙️ Funzionalità principali

- Estrae statistiche delle squadre da file CSV
- Applica filtri multipli per selezionare squadre con condizioni specifiche
- Salva i risultati in `data/processed/selected_teams_F1.csv`, `selected_teams_F2.csv`, `selected_teams_F3.csv` e `bet.csv`
- Visualizza le squadre selezionate tramite interfaccia web Flask
- Accesso protetto da password

---

## 📦 Dipendenze principali

- Flask
- Pandas
- Requests
- BeautifulSoup4

Tutte incluse in `requirements.txt`.

---

## 🧪 Script utili

- `src/data_update/update_data.py`: aggiorna i dati delle squadre
- `src/data_update/update_upcoming.py`: aggiorna le partite in programma
- `src/data_update/update_national_cup.py`: aggiorna i vincitori delle coppe nazionali
- `src/queries/filter_teams_1.py`: genera `selected_teams_F1.csv`
- `src/queries/filter_teams_2.py`: genera `selected_teams_F2.csv`
- `src/queries/filter_teams_3.py`: genera `selected_teams_F3.csv`
- `src/queries/betting.py`: genera il file `bet.csv` per la visualizzazione
- `app.py`: avvia la webapp Flask

---

## 🔁 Esecuzione pipeline (ordine consigliato)

Esegui questi comandi dalla root del progetto:

```bash
python src/data_update/update_data.py
python src/data_update/update_upcoming.py
python src/data_update/update_national_cup.py
python src/queries/filter_teams_1.py
python src/queries/filter_teams_2.py
python src/queries/filter_teams_3.py
python src/queries/betting.py
python app.py
```

Nota: se usi Windows con `.venv`, attiva prima l'ambiente virtuale (`.venv\Scripts\Activate.ps1`).

---

## 🌐 Deploy su PythonAnywhere

1. Carica tutti i file del progetto (escludi `.venv/`, `__pycache__/`, `.git/`, `.DS_Store`)
2. Installa le dipendenze:
	```bash
	pip install --user -r requirements.txt
	```
3. Configura il file WSGI per puntare a `app.py`:
	```python
	import sys
	path = '/home/tuo_username/nome_cartella_progetto'
	if path not in sys.path:
		 sys.path.append(path)
	from app import app as application
	```
4. Riavvia la webapp da PythonAnywhere

---

## 🧩 Avvio su Replit

1. Crea un nuovo Repl Python e importa questo progetto (upload ZIP o da GitHub).
2. Imposta i Secrets in Replit:
	- `SECRET_KEY`
	- `APP_PASSWORD`
3. Premi **Run**: Replit userà il file `.replit` e avvierà `gunicorn` su `app:app`.
4. Apri l'URL pubblico del Repl e accedi con la password impostata in `APP_PASSWORD`.

---

## 📬 Contatti

Per domande o segnalazioni, contattami o apri una issue sul repository.

---

Buon divertimento con le previsioni calcistiche! ⚽️
