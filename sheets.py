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
# Data, Ora, Fascia oraria, Apparecchio, Temp. rilevata (°C), Temp. limite (°C),
# Esito (OK/NO), Operatore, Note
TEMPERATURE_COLS = [
    'data', 'ora', 'fascia_oraria', 'apparecchio', 'temp_rilevata', 'temp_limite',
    'esito', 'operatore', 'nota'
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
_TIPO_DISPLAY = {
    'CARICO': 'CARICO', 'IN_USO': 'IN USO', 'SCARICO': 'SCARICO',
    'PREPARAZIONE': 'PREPARAZIONE', 'SCARICO_PREPARAZIONE': 'SCARICO PREPARAZIONE',
}


def _normalizza_tipo(raw):
    """Converte il testo della colonna 'Tipo movimento' nel codice interno
    usato dall'app ('CARICO' | 'IN_USO' | 'SCARICO' | 'PREPARAZIONE'). None
    se non riconosciuto."""
    t = (raw or '').strip().upper()
    if t in ('IN USO', 'IN_USO', 'INUSO'):
        return 'IN_USO'
    if t == 'SCARICO PREPARAZIONE':
        return 'SCARICO_PREPARAZIONE'
    if t == 'SCARICO':
        return 'SCARICO'
    if t == 'CARICO':
        return 'CARICO'
    if t == 'PREPARAZIONE':
        return 'PREPARAZIONE'
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
    'ora':           ['ora', 'orario', 'time'],
    'fascia_oraria': ['fascia'],
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


def aggiorna_tipo_movimento(movimento_id, nuovo_tipo, nuovo_scarico=None):
    """
    Trova la riga del REGISTRO con questo movimento_id (colonna 'ID') e
    aggiorna la colonna 'Tipo movimento' — es. da 'IN USO' a 'SCARICO'
    quando un prelievo in uso viene finalizzato. Se nuovo_scarico non è
    None, aggiorna anche la colonna 'Scarico' (usato quando si finalizza
    solo una parte della quantità in uso). Non crea nuove righe.
    Solleva ValueError se la riga non viene trovata, così il chiamante non
    finalizza il movimento solo in locale credendo che la scrittura su
    Sheets sia andata a buon fine.
    """
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_REGISTRO)

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        raise ValueError(f"Foglio REGISTRO vuoto: movimento '{movimento_id}' non trovato")

    headers     = [h.strip() for h in all_values[0]]
    col_id      = _detect(headers, 'movimento_id')
    col_tipo    = _detect(headers, 'tipo_movimento')
    col_scarico = _detect(headers, 'scarico') if nuovo_scarico is not None else None
    if col_id is None or col_tipo is None:
        raise ValueError(f"Colonna 'ID' o 'Tipo movimento' non trovata nel REGISTRO. Header: {headers}")

    for i, row in enumerate(all_values[1:], start=2):
        val = row[col_id].strip() if col_id < len(row) else ''
        if val == movimento_id:
            ws.update_cell(i, col_tipo + 1, _TIPO_DISPLAY.get(nuovo_tipo, nuovo_tipo))
            if col_scarico is not None:
                ws.update_cell(i, col_scarico + 1, nuovo_scarico)
            return True

    raise ValueError(f"Movimento '{movimento_id}' non trovato nel foglio REGISTRO")


def aggiorna_riga_registro(movimento_id, lotto, scadenza, rimanenza):
    """
    Trova la riga del REGISTRO con questo movimento_id e aggiorna Lotto,
    Scadenza e Rimanenza lotto (usato per correggere un carico esistente).
    Non crea nuove righe. Solleva ValueError se la riga non viene trovata,
    così il chiamante non salva la modifica solo in locale credendo che la
    scrittura su Sheets sia andata a buon fine.
    """
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_REGISTRO)

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        raise ValueError(f"Foglio REGISTRO vuoto: movimento '{movimento_id}' non trovato")

    headers = [h.strip() for h in all_values[0]]
    col_id        = _detect(headers, 'movimento_id')
    col_lotto     = _detect(headers, 'lotto')
    col_scadenza  = _detect(headers, 'scadenza')
    col_rimanenza = _detect(headers, 'rimanenza')
    if col_id is None:
        raise ValueError(f"Colonna 'ID' non trovata nel REGISTRO. Header: {headers}")

    for i, row in enumerate(all_values[1:], start=2):
        val = row[col_id].strip() if col_id < len(row) else ''
        if val == movimento_id:
            if col_lotto is not None:
                ws.update_cell(i, col_lotto + 1, lotto)
            if col_scadenza is not None:
                ws.update_cell(i, col_scadenza + 1, _to_sheet_date(scadenza))
            if col_rimanenza is not None:
                ws.update_cell(i, col_rimanenza + 1, rimanenza)
            return True

    raise ValueError(f"Movimento '{movimento_id}' non trovato nel foglio REGISTRO")


def elimina_riga_registro(movimento_id):
    """
    Elimina dal foglio REGISTRO la riga con questo movimento_id (usato per
    cancellare un carico registrato per errore). Solleva ValueError se la
    riga non viene trovata, così il chiamante non elimina il carico solo in
    locale credendo che la scrittura su Sheets sia andata a buon fine.
    """
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_REGISTRO)

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        raise ValueError(f"Foglio REGISTRO vuoto: movimento '{movimento_id}' non trovato")

    headers = [h.strip() for h in all_values[0]]
    col_id = _detect(headers, 'movimento_id')
    if col_id is None:
        raise ValueError(f"Colonna 'ID' non trovata nel REGISTRO. Header: {headers}")

    for i, row in enumerate(all_values[1:], start=2):
        val = row[col_id].strip() if col_id < len(row) else ''
        if val == movimento_id:
            ws.delete_rows(i)
            return True

    raise ValueError(f"Movimento '{movimento_id}' non trovato nel foglio REGISTRO")


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

def _fascia_oraria(ora):
    """'Mattina' per 00:00-20:59, 'Sera' per 21:00-23:59."""
    try:
        h = int((ora or '').split(':')[0])
    except (ValueError, IndexError):
        return ''
    return 'Sera' if h >= 21 else 'Mattina'


def append_temperatura(row_data):
    """Appende una rilevazione al foglio TEMPERATURE esistente, rilevando
    dinamicamente le colonne dall'header (Data, Ora, Fascia oraria,
    Apparecchio, Temp. rilevata, Temp. limite, Esito, Operatore, Note)."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TEMPERATURE)

    headers = [h.strip() for h in ws.row_values(1)]
    col = {k: _detect(headers, k) for k in TEMPERATURE_COLS}

    n_cols = len(headers) if headers else len(TEMPERATURE_COLS)
    new_row = [''] * n_cols

    ora      = row_data.get('ora', '')
    data_str = _to_sheet_date(row_data.get('data', ''))

    temp_min = row_data.get('temp_min', 0)
    temp_max = row_data.get('temp_max', 0)
    limite   = f"{temp_min:g} / {temp_max:g}"

    field_vals = {
        'data':          data_str,
        'ora':           ora,
        'fascia_oraria': _fascia_oraria(ora),
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


def elimina_temperatura_foglio(apparecchio, data_iso, ora):
    """Elimina dal foglio TEMPERATURE la riga che corrisponde esattamente ad
    apparecchio + data + ora. Ritorna True se una riga è stata trovata ed
    eliminata, False altrimenti."""
    gc = _get_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TEMPERATURE)

    headers = [h.strip() for h in ws.row_values(1)]
    col_data = _detect(headers, 'data')
    col_ora  = _detect(headers, 'ora')
    col_app  = _detect(headers, 'apparecchio')
    if col_data is None or col_app is None:
        raise ValueError(f"Colonne 'Data'/'Apparecchio' non trovate nel foglio TEMPERATURE. Header: {headers}")

    target_data = _to_sheet_date(data_iso)

    all_values = ws.get_all_values()
    for i, row in enumerate(all_values[1:], start=2):
        cella_data = row[col_data].strip() if col_data < len(row) else ''
        cella_ora  = row[col_ora].strip()  if (col_ora is not None and col_ora < len(row)) else ''
        cella_app  = row[col_app].strip()  if col_app < len(row) else ''
        if cella_data == target_data and cella_ora == (ora or '') and cella_app == apparecchio:
            ws.delete_rows(i)
            return True
    return False


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
            'data':        normalize_date(v('data')),
            'ora':         v('ora'),
            'temperatura': _to_float(v('temp_rilevata')),
            'temp_min':    tmin,
            'temp_max':    tmax,
            'esito':       esito,
            'nota':        v('nota'),
            'operatore':   v('operatore'),
        })
    return result
