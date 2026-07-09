import gspread
from google.oauth2.service_account import Credentials
import os
import re

SPREADSHEET_ID   = '1Dg0J181wAEUctJCWrNI1pmtOb7YlF_msOqTsqRPDILw'
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), 'credentials.json')
SCOPES           = ['https://www.googleapis.com/auth/spreadsheets']

SHEET_LISTINO      = 'LISTINO'
SHEET_REGISTRO     = 'REGISTRO'
SHEET_TEMPERATURE  = 'TEMPERATURE'

# Colonne reali del foglio TEMPERATURE (già esistente):
# Data, Apparecchio, Temp. rilevata (°C), Temp. limite (°C), Esito (OK/NO), Operatore, Note
TEMPERATURE_COLS = [
    'data', 'apparecchio', 'temp_rilevata', 'temp_limite', 'esito', 'operatore', 'nota'
]

# Il foglio usa 'NO' per un esito fuori soglia; internamente l'app usa 'FUORI_SOGLIA'.
_ESITO_DISPLAY = {'OK': 'OK', 'FUORI_SOGLIA': 'NO'}


def _normalizza_esito(raw):
    t = (raw or '').strip().upper()
    if t in ('NO', 'FUORI SOGLIA', 'FUORI_SOGLIA', 'KO'):
        return 'FUORI_SOGLIA'
    if t == 'OK':
        return 'OK'
    return None

# Colonne attese nel REGISTRO (ordine fisso per le scritture)
REGISTRO_COLS = [
    'data', 'fornitore', 'prodotto', 'lotto', 'scadenza',
    'carico', 'scarico', 'unita', 'etichetta', 'movimento_id', 'rimanenza',
    'operatore', 'reparto', 'tipo_movimento'
]

# Valore da scrivere nella colonna "Tipo movimento" per ciascun tipo interno
_TIPO_DISPLAY = {'CARICO': 'CARICO', 'IN_USO': 'IN USO', 'SCARICO': 'SCARICO'}


def _normalizza_tipo(raw):
    """Converte il testo della colonna 'Tipo movimento' nel codice interno
    usato dall'app ('CARICO' | 'IN_USO' | 'SCARICO'). None se non riconosciuto."""
    t = (raw or '').strip().upper()
    if t in ('IN USO', 'IN_USO', 'INUSO'):
        return 'IN_USO'
    if t == 'SCARICO':
        return 'SCARICO'
    if t == 'CARICO':
        return 'CARICO'
    return None

# Sinonimi per il rilevamento automatico delle colonne
_SYNS = {
    # LISTINO
    'prodotto':    ['prodotto', 'nome', 'articolo', 'item', 'alimento'],
    'fornitore':   ['fornitore', 'supplier', 'vendor'],
    'unita':       ['unità', 'unita', 'um', 'unit', 'kg/lt', 'misura'],
    'scorta_min':  ['scorta', 'minimo', 'min', 'scorta min', 'minimum'],
    'categoria':   ['categoria', 'category', 'cat'],
    'reparto_prodotto': ['reparto'],
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
    'tipo_movimento': ['tipo movimento'],
    # TEMPERATURE
    'apparecchio':   ['apparecchio', 'dispositivo', 'frigo'],
    'temp_rilevata': ['rilevata'],
    'temp_limite':   ['limite', 'range'],
    'esito':         ['esito', 'stato'],
    'nota':          ['note', 'nota', 'osservazioni'],
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


def _to_sheet_date(raw):
    """Converte una data ISO yyyy-mm-dd interna nel formato italiano
    dd/mm/yyyy usato dal foglio REGISTRO. Usata solo in scrittura verso
    Sheets: non tocca il formato ISO usato internamente da app.py/database.py."""
    if not raw:
        return ''
    raw = str(raw).strip()
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', raw)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    return raw


# ─── LETTURA ─────────────────────────────────────────────────────────────────

def load_listino():
    """Legge il foglio LISTINO e ritorna lista di dict."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_LISTINO)
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        return []

    # Indici fissi di fallback: A=0, B=1, C=2, D=3, G=6 (Categoria), I=8 (Reparto)
    _FIXED = {'prodotto': 0, 'fornitore': 1, 'unita': 2, 'scorta_min': 3,
              'reparto_prodotto': 8, 'categoria': 6}

    headers = [h.strip() for h in all_rows[0]]
    # _detect affina l'indice se l'header è riconosciuto, altrimenti usa il fisso
    col = {}
    for k in ('prodotto', 'fornitore', 'unita', 'scorta_min', 'categoria', 'reparto_prodotto'):
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
            'reparto':    v('reparto_prodotto') or 'Cucina',
        })
    result.sort(key=lambda r: r['prodotto'].lower())
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
                  'operatore', 'reparto', 'tipo_movimento')
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
        scarico_val = _to_float(v('scarico'))
        tipo = _normalizza_tipo(v('tipo_movimento'))
        if tipo is None:
            # Righe storiche senza colonna 'Tipo movimento' valorizzata:
            # deduci dal segno del movimento (nessuna distinzione IN USO possibile).
            tipo = 'SCARICO' if scarico_val else 'CARICO'
        result.append({
            'gs_row':      i,
            'data':        normalize_date(v('data')),
            'fornitore':   v('fornitore'),
            'prodotto':    prodotto,
            'lotto':       v('lotto'),
            'scadenza':    normalize_date(v('scadenza')),
            'carico':      _to_float(v('carico')),
            'scarico':     scarico_val,
            'unita':       v('unita') or 'kg',
            'etichetta':   v('etichetta'),
            'movimento_id':v('movimento_id'),
            'rimanenza':   _to_float(v('rimanenza')),
            'operatore':   v('operatore'),
            'reparto':     v('reparto'),
            'tipo':        tipo,
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
        'data':         _to_sheet_date(row_data.get('data', '')),
        'fornitore':    row_data.get('fornitore', ''),
        'prodotto':     row_data.get('prodotto', ''),
        'lotto':        row_data.get('lotto', ''),
        'scadenza':     _to_sheet_date(row_data.get('scadenza', '')),
        'carico':       carico_val,
        'scarico':      scarico_val,
        'unita':        row_data.get('unita', ''),
        'etichetta':    row_data.get('etichetta', ''),
        'movimento_id': row_data.get('movimento_id', ''),
        'rimanenza':    row_data.get('rimanenza', 0),
        'operatore':    row_data.get('operatore', ''),
        'reparto':      row_data.get('reparto', ''),
        'tipo_movimento': _TIPO_DISPLAY.get(row_data.get('tipo', 'CARICO'), 'CARICO'),
    }

    for key, val in field_vals.items():
        idx = col.get(key)
        if idx is not None and idx < n_cols:
            new_row[idx] = val

    ws.append_row(new_row, value_input_option='USER_ENTERED')


def aggiorna_tipo_movimento(movimento_id, nuovo_tipo):
    """
    Trova la riga del REGISTRO con questo movimento_id (colonna 'ID') e
    aggiorna la colonna 'Tipo movimento' — es. da 'IN USO' a 'SCARICO'
    quando un prelievo in uso viene finalizzato. Non crea nuove righe.
    Ritorna True se una riga è stata trovata e aggiornata, False altrimenti.
    """
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_REGISTRO)

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        return False

    headers  = [h.strip() for h in all_values[0]]
    col_id   = _detect(headers, 'movimento_id')
    col_tipo = _detect(headers, 'tipo_movimento')
    if col_id is None or col_tipo is None:
        raise ValueError(f"Colonna 'ID' o 'Tipo movimento' non trovata nel REGISTRO. Header: {headers}")

    for i, row in enumerate(all_values[1:], start=2):
        val = row[col_id].strip() if col_id < len(row) else ''
        if val == movimento_id:
            ws.update_cell(i, col_tipo + 1, _TIPO_DISPLAY.get(nuovo_tipo, nuovo_tipo))
            return True
    return False


def append_listino(row_data):
    """Appende una nuova riga al foglio LISTINO, rilevando dinamicamente le
    colonne dall'header (nessun indice fisso). La colonna 'nome' (SORT/FILTER
    su A) non viene mai scritta: si aggiorna da sola."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_LISTINO)

    prodotto   = row_data.get('prodotto', '')
    fornitore  = row_data.get('fornitore', '')
    unita      = row_data.get('unita', 'kg')
    scorta_min = row_data.get('scorta_min', 0)
    categoria  = row_data.get('categoria', '')
    reparto    = row_data.get('reparto') or 'Cucina'

    all_values = ws.get_all_values()
    headers = [h.strip() for h in all_values[0]] if all_values else []

    # Indici fissi di fallback (struttura attuale: A-D base, F formula, G Categoria, I Reparto)
    _FIXED = {'prodotto': 0, 'fornitore': 1, 'unita': 2, 'scorta_min': 3,
              'categoria': 6, 'reparto_prodotto': 8}
    col = {}
    for k in ('prodotto', 'fornitore', 'unita', 'scorta_min', 'categoria', 'reparto_prodotto'):
        detected = _detect(headers, k)
        col[k] = detected if detected is not None else _FIXED[k]

    next_row = 2
    for i, row in enumerate(all_values[1:], start=2):
        idx = col['prodotto']
        if idx < len(row) and row[idx].strip():
            next_row = i + 1

    field_vals = {
        'prodotto': prodotto, 'fornitore': fornitore, 'unita': unita,
        'scorta_min': scorta_min, 'categoria': categoria, 'reparto_prodotto': reparto,
    }
    for key, val in field_vals.items():
        ws.update_cell(next_row, col[key] + 1, val)


# ─── TEMPERATURE ─────────────────────────────────────────────────────────────

def append_temperatura(row_data):
    """Appende una rilevazione al foglio TEMPERATURE esistente, rilevando
    dinamicamente le colonne dall'header (Data, Apparecchio, Temp. rilevata,
    Temp. limite, Esito, Operatore, Note)."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TEMPERATURE)

    headers = [h.strip() for h in ws.row_values(1)]
    col = {k: _detect(headers, k) for k in TEMPERATURE_COLS}

    n_cols = len(headers) if headers else len(TEMPERATURE_COLS)
    new_row = [''] * n_cols

    data_ora = row_data.get('data', '')
    ora      = row_data.get('ora', '')
    data_str = _to_sheet_date(data_ora)
    if ora:
        data_str = f"{data_str} {ora}"

    temp_min = row_data.get('temp_min', 0)
    temp_max = row_data.get('temp_max', 0)
    limite   = f"{temp_min:g} / {temp_max:g}"

    field_vals = {
        'data':          data_str,
        'apparecchio':   row_data.get('apparecchio', ''),
        'temp_rilevata': row_data.get('temperatura', ''),
        'temp_limite':   limite,
        'esito':         _ESITO_DISPLAY.get(row_data.get('esito', 'OK'), 'OK'),
        'operatore':     row_data.get('operatore', ''),
        'nota':          row_data.get('nota', ''),
    }
    for key, val in field_vals.items():
        idx = col.get(key)
        if idx is not None and idx < n_cols:
            new_row[idx] = val

    ws.append_row(new_row, value_input_option='USER_ENTERED')


def load_temperatura():
    """Legge il foglio TEMPERATURE e ritorna lista di dict."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TEMPERATURE)
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        return []

    headers = [h.strip() for h in all_rows[0]]
    col = {k: _detect(headers, k) for k in TEMPERATURE_COLS}
    if col['apparecchio'] is None:
        raise ValueError(f"Colonna 'Apparecchio' non trovata nel foglio TEMPERATURE. Header: {headers}")

    result = []
    for row in all_rows[1:]:
        def v(key):
            idx = col.get(key)
            return row[idx].strip() if idx is not None and idx < len(row) else ''

        apparecchio = v('apparecchio')
        if not apparecchio:
            continue

        data_raw = v('data')
        data_parte, _, ora_parte = data_raw.partition(' ')

        limite = v('temp_limite')
        tmin, tmax = 0.0, 0.0
        m = re.match(r'^\s*(-?[\d.,]+)\s*/\s*(-?[\d.,]+)\s*$', limite)
        if m:
            tmin = _to_float(m.group(1))
            tmax = _to_float(m.group(2))

        esito = _normalizza_esito(v('esito')) or 'OK'

        result.append({
            'apparecchio': apparecchio,
            'tipo':        '',
            'data':        normalize_date(data_parte),
            'ora':         ora_parte,
            'temperatura': _to_float(v('temp_rilevata')),
            'temp_min':    tmin,
            'temp_max':    tmax,
            'esito':       esito,
            'nota':        v('nota'),
            'operatore':   v('operatore'),
        })
    return result
