# Magazzino Cucina — HACCP

App web per la gestione del magazzino di cucina con tracciabilità HACCP, collegata a Google Sheets.

## Stack

- **Backend**: Python / Flask
- **Database**: SQLite (locale + sincronizzazione Google Sheets)
- **Auth**: Flask-Login (ruoli: admin / staff)
- **AI**: Anthropic Claude per scansione etichette via fotocamera
- **Frontend**: SPA vanilla JS, mobile-first, PWA

---

## Avvio locale

```bash
pip install -r requirements.txt
python3 app.py
```

Apri **http://localhost:5001**

Credenziali default: `admin@admin.com` / `admin123`

---

## Configurazione Google Sheets

1. Crea un Service Account su [Google Cloud Console](https://console.cloud.google.com)
2. Abilita le API: **Google Sheets API** e **Google Drive API**
3. Scarica il file `credentials.json` e mettilo nella root del progetto
4. Condividi il foglio Google Sheets con l'email del Service Account

---

## Deploy su Render.com

### 1. Crea il repository Git

```bash
git init
git add .
git commit -m "Initial commit"
```

Carica su GitHub/GitLab.

### 2. Crea il servizio su Render

- Nuovo **Web Service** → collega il repository
- Render rileva automaticamente `render.yaml`

### 3. Configura le variabili d'ambiente nel dashboard Render

| Variabile | Valore |
|---|---|
| `SECRET_KEY` | generato automaticamente da render.yaml |
| `GOOGLE_CREDENTIALS_JSON` | contenuto intero del file `credentials.json` (copia-incolla) |
| `ANTHROPIC_API_KEY` | la tua API key da [console.anthropic.com](https://console.anthropic.com) |

### 4. Disco persistente

Il file `render.yaml` configura un disco da 1 GB montato in `/data` per il database SQLite.
Senza disco il database si azzera ad ogni deploy.

---

## Struttura foglio Google Sheets

| Foglio | Contenuto |
|---|---|
| `REGISTRO` | Tutti i movimenti (carico / scarico) con lotto, scadenza, rimanenza |
| `LISTINO` | Anagrafica prodotti con fornitore, unità, scorta minima, categoria |
| `GIACENZA` | Riepilogo automatico per prodotto (formule) |
| `REPORT MENSILE` | Consumi mensili per prodotto (formule) |
| `TEMPERATURE` | Registro giornaliero temperature frigo/freezer |

---

## Variabili d'ambiente

| Variabile | Descrizione | Default |
|---|---|---|
| `SECRET_KEY` | Chiave segreta Flask per le sessioni | `haccp-brigade-2024` |
| `GOOGLE_CREDENTIALS_JSON` | JSON del Service Account Google | legge `credentials.json` |
| `ANTHROPIC_API_KEY` | API key per scansione etichette | nessuno |
