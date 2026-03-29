import gspread
from google.oauth2.service_account import Credentials
import os
import re

SPREADSHEET_ID   = '1Dg0J181wAEUctJCWrNI1pmtOb7YlF_msOqTsqRPDILw'
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), 'credentials.json')
SCOPES           = ['https://www.googleapis.com/auth/spreadsheets']

SHEET_LISTINO  = 'LISTINO'
SHEET_REGISTRO = 'REGISTRO'

# Colonne attese nel REGISTRO (ordine fisso per le scritture)
REGISTRO_COLS = [
    'data', 'fornitore', 'prodotto', 'lotto', 'scadenza',
    'carico', 'scarico', 'unita', 'etichetta', 'movimento_id', 'rimanenza',
    'operatore', 'reparto', 'ddt'
]

# Sinonimi per il rilevamento automatico delle colonne
_SYNS = {
    # LISTINO
    'prodotto':    ['prodotto', 'nome', 'articolo', 'item', 'alimento'],
    'fornitore':   ['fornitore', 'supplier', 'vendor'],
    'unita':       ['unità', 'unita', 'um', 'unit', 'kg/lt', 'misura'],
    'scorta_min':  ['scorta', 'minimo', 'min', 'scorta min', 'minimum'],
    'categoria':   ['categoria', 'category', 'cat', 'tipo', 'reparto'],
    # REGISTRO
    'data':        ['data', 'date', 'giorno'],
    'lotto':       ['lotto', 'lot', 'batch', 'n. lotto', 'n.lotto'],
    'scadenza':    ['scadenza', 'scad', 'expiry', 'best before'],
    'carico':      ['carico', 'entrata', 'caric', 'load', 'acquisto'],
    'scarico':     ['scarico', 'uscita', 'scaric', 'consumo', 'out'],
    'etichetta':   ['etichetta', 'label', 'tag'],
    'movimento_id':['id', 'codice', 'cod', 'movement'],
    'rimanenza':   ['rimanenza', 'saldo', 'balance', 'stock', 'residuo'],
    'operatore':   ['operatore', 'operator', 'utente', 'user'],
    'reparto':     ['reparto', 'department', 'dept', 'settore'],
    'ddt':         ['ddt', 'bolla', 'documento', 'doc', 'ddt/bolla'],
}


def _parse_credentials_json(raw: str) -> dict:
    """
    Parsifica GOOGLE_CREDENTIALS_JSON gestendo i formati comuni
    che emergono quando si incolla credentials.json in Render.
    """
    import json, ast

    s = raw.strip()

    # Tentativi in ordine dal più comune al più raro
    attempts = [
        s,                                      # 1. valore as-is
        s[1:-1] if s[:1] == "'" else None,      # 2. avvolto in singole virgolette '...'
        s[1:-1] if s[:1] == '"' else None,      # 3. avvolto in doppie virgolette "..."
        s.replace('\\\\n', '\\n'),              # 4. \n doppiamente escaped (\\n → \n)
    ]

    last_err = None
    for candidate in attempts:
        if candidate is None:
            continue
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as e:
            last_err = e

    # Ultimo tentativo: ast.literal_eval per dict Python con singole virgolette
    try:
        result = ast.literal_eval(s)
        if isinstance(result, dict):
            return result
    except (ValueError, SyntaxError):
        pass

    raise ValueError(
        f"GOOGLE_CREDENTIALS_JSON non è parsabile: {last_err}\n"
        f"Primi 120 caratteri: {repr(raw[:120])}"
    )


def _get_client():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if creds_json:
        info = _parse_credentials_json(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def _detect(headers, key):
    """Trova l'indice (0-based) della colonna che corrisponde alla chiave."""
    syns = _SYNS.get(key, [key])
    for i, h in enumerate(headers):
        h_low = h.strip().lower()
        for s in syns:
            if s in h_low:
                return i
    return None


def _to_float(val):
    if not val:
        return 0.0
    try:
        return float(str(val).replace(',', '.').strip())
    except ValueError:
        return 0.0


def normalize_date(raw):
    """Normalizza date in formato ISO yyyy-mm-dd."""
    if not raw:
        return ''
    raw = str(raw).strip()
    m = re.match(r'^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{4})$', raw)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    if re.match(r'^\d{4}-\d{2}-\d{2}$', raw):
        return raw
    return raw


# ─── LETTURA ─────────────────────────────────────────────────────────────────

def load_listino():
    """Legge il foglio LISTINO e ritorna lista di dict."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_LISTINO)
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        return []

    # Indici fissi reali del foglio: A=0, B=1, C=2, D=3, G=6
    _FIXED = {'prodotto': 0, 'fornitore': 1, 'unita': 2, 'scorta_min': 3, 'categoria': 6}

    headers = [h.strip() for h in all_rows[0]]
    # _detect affina l'indice se l'header è riconosciuto, altrimenti usa il fisso
    col = {}
    for k in ('prodotto', 'fornitore', 'unita', 'scorta_min', 'categoria'):
        detected = _detect(headers, k)
        col[k] = detected if detected is not None else _FIXED[k]

    result = []
    for row in all_rows[1:]:
        def v(key):
            idx = col.get(key)
            return row[idx].strip() if idx is not None and idx < len(row) else ''

        prodotto = v('prodotto')
        if not prodotto:
            continue
        result.append({
            'prodotto':   prodotto,
            'fornitore':  v('fornitore'),
            'unita':      v('unita') or 'kg',
            'scorta_min': _to_float(v('scorta_min')),
            'categoria':  v('categoria'),
        })
    return result


def load_registro():
    """Legge il foglio REGISTRO e ritorna lista di dict con gs_row."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_REGISTRO)
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        return []

    headers = [h.strip() for h in all_rows[0]]
    col = {
        k: _detect(headers, k)
        for k in ('data', 'fornitore', 'prodotto', 'lotto', 'scadenza',
                  'carico', 'scarico', 'unita', 'etichetta', 'movimento_id', 'rimanenza',
                  'operatore', 'reparto')
    }

    if col['prodotto'] is None:
        raise ValueError(f"Colonna 'prodotto' non trovata nel REGISTRO. Header: {headers}")

    result = []
    for i, row in enumerate(all_rows[1:], start=2):
        def v(key):
            idx = col.get(key)
            return row[idx].strip() if idx is not None and idx < len(row) else ''

        prodotto = v('prodotto')
        if not prodotto:
            continue
        result.append({
            'gs_row':      i,
            'data':        normalize_date(v('data')),
            'fornitore':   v('fornitore'),
            'prodotto':    prodotto,
            'lotto':       v('lotto'),
            'scadenza':    normalize_date(v('scadenza')),
            'carico':      _to_float(v('carico')),
            'scarico':     _to_float(v('scarico')),
            'unita':       v('unita') or 'kg',
            'etichetta':   v('etichetta'),
            'movimento_id':v('movimento_id'),
            'rimanenza':   _to_float(v('rimanenza')),
            'operatore':   v('operatore'),
            'reparto':     v('reparto'),
        })
    return result


# ─── SCRITTURA ───────────────────────────────────────────────────────────────

def append_registro(row_data):
    """
    Appende una riga al foglio REGISTRO.
    L'ordine delle colonne segue REGISTRO_COLS (fisso, come da schema utente).
    """
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_REGISTRO)

    # Rileva l'ordine delle colonne dall'header per costruire la riga correttamente
    headers = [h.strip() for h in ws.row_values(1)]
    col = {k: _detect(headers, k) for k in REGISTRO_COLS}

    n_cols = len(headers) if headers else len(REGISTRO_COLS)
    new_row = [''] * n_cols

    # Carico e Scarico sono mutualmente esclusivi: solo uno dei due può
    # avere un valore in una stessa riga.
    carico_val  = row_data.get('carico',  0) or 0
    scarico_val = row_data.get('scarico', 0) or 0
    if carico_val:
        scarico_val = ''   # riga di carico → scarico vuoto
    elif scarico_val:
        carico_val  = ''   # riga di scarico → carico vuoto

    field_vals = {
        'data':         row_data.get('data', ''),
        'fornitore':    row_data.get('fornitore', ''),
        'prodotto':     row_data.get('prodotto', ''),
        'lotto':        row_data.get('lotto', ''),
        'scadenza':     row_data.get('scadenza', ''),
        'carico':       carico_val,
        'scarico':      scarico_val,
        'unita':        row_data.get('unita', ''),
        'etichetta':    row_data.get('etichetta', ''),
        'movimento_id': row_data.get('movimento_id', ''),
        'rimanenza':    row_data.get('rimanenza', 0),
        'operatore':    row_data.get('operatore', ''),
        'reparto':      row_data.get('reparto', ''),
        'ddt':          row_data.get('ddt', ''),
    }

    for key, val in field_vals.items():
        idx = col.get(key)
        if idx is not None and idx < n_cols:
            new_row[idx] = val

    ws.append_row(new_row, value_input_option='USER_ENTERED')


def append_listino(row_data):
    """Appende una nuova riga al foglio LISTINO (A=nome, B=fornitore, C=unità, D=scorta, G=categoria)."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_LISTINO)

    prodotto   = row_data.get('prodotto', '')
    fornitore  = row_data.get('fornitore', '')
    unita      = row_data.get('unita', 'kg')
    scorta_min = row_data.get('scorta_min', 0)
    categoria  = row_data.get('categoria', '')

    import sys
    all_values = ws.get_all_values()
    print(f"[DEBUG] Totale righe: {len(all_values)}", flush=True, file=sys.stderr)
    for i, row in enumerate(all_values[:5]):
        print(f"[DEBUG] Riga {i+1}, colonna A: '{row[0] if row else 'VUOTA'}'", flush=True, file=sys.stderr)
    next_row = 2
    for i, row in enumerate(all_values):
        if row and row[0].strip():
            next_row = i + 2
            print(f"[DEBUG] Ultima riga trovata: {i+1}, next_row={next_row}", flush=True, file=sys.stderr)
    print(f"[DEBUG] Scrivo in riga: {next_row}", flush=True, file=sys.stderr)

    ws.update(f'A{next_row}:D{next_row}', [[prodotto, fornitore, unita, scorta_min]])
    ws.update(f'G{next_row}', [[categoria]])
