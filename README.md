# ⚽️ Crucelli - Betting Bot

Crucelli è un progetto Python per la selezione automatizzata di squadre e partite tramite filtri personalizzati, con invio email giornaliero.

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

### 3b. Configura la chiave API (obbligatoria)
Scegli uno dei due metodi:

**Metodo A (consigliato): file .env**
1. Copia `.env.example` in `.env`
2. Inserisci la tua chiave:
```bash
API_FOOTBALL_KEY=la_tua_chiave
```

**Metodo B: variabile d'ambiente**
```bash
export API_FOOTBALL_KEY="la_tua_chiave"
```

## ⚙️ Funzionalità principali

- Estrae statistiche delle squadre da file CSV
- Applica filtri multipli per selezionare squadre con condizioni specifiche
- Salva i risultati in `data/processed/bet.csv`
- Calcola le partite del giorno e prepara il report
- Invia una mail giornaliera con le partite selezionate

---

## 📦 Dipendenze principali

- Pandas
- Requests
- BeautifulSoup4

Tutte incluse in `requirements.txt`.

---

## 🧪 Script utili

- `src/data_update/update_data.py`: aggiorna i dati delle squadre
- `src/data_update/update_upcoming.py`: aggiorna le partite in programma
- `src/data_update/update_national_cup.py`: aggiorna i vincitori delle coppe nazionali
- `src/queries/betting-bot.py`: applica i filtri F1–F4 e genera `bet.csv`
- `src/automation/daily_email.py`: esegue la routine completa e manda la mail

---

## 🔁 Esecuzione pipeline (ordine consigliato)

Esegui questi comandi dalla root del progetto:

```bash
python src/data_update/update_data.py
python src/data_update/update_upcoming.py
python src/data_update/update_national_cup.py
python src/queries/betting-bot.py
```

---

## 📧 Invio email giornaliero (07:00)

Script: `python src/automation/daily_email.py`

Variabili richieste (.env):

```
API_FOOTBALL_KEY=la_tua_chiave
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=crucelli.bot@gmail.com
SMTP_PASS=app_password_gmail
EMAIL_FROM=crucelli.bot@gmail.com
EMAIL_TO=andreanove@gmail.com
```

Note:
- Per Gmail serve una App Password (non la password normale).
- `EMAIL_TO` accetta piu' indirizzi separati da virgola.

Su Railway: crea un Cron Job giornaliero alle 07:00 (Europe/Rome) che esegue
`python src/automation/daily_email.py`.

Nota: se usi Windows con `.venv`, attiva prima l'ambiente virtuale (`.venv\Scripts\Activate.ps1`).
