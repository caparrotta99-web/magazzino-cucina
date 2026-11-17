"""
Rimette in ordine l'ASPETTO dei fogli Google di Brigade (REGISTRO, TEMPERATURE,
LISTINO). Non tocca nessun valore: cambia solo colori, larghezze, formati e
regole di colore.

Perché serve: col tempo si sono accumulati colori manuali, regole di colore e
menu a tendina colorati; in più Google, quando l'app aggiunge una riga in
fondo, le copia il formato della riga sopra, così i colori si propagano a caso.
Dopo questo script ogni riga ha lo stesso aspetto (righe alternate bianco /
grigio chiarissimo) e l'unico colore è quello della colonna "Tipo movimento"
(o "Esito" nelle temperature), sempre uguale per lo stesso tipo.

Uso (dalla cartella del progetto, con credentials.json presente):
    python3 strumenti/sistema_aspetto_fogli.py            # prova: mostra cosa farebbe
    python3 strumenti/sistema_aspetto_fogli.py --applica  # lo fa davvero

Per tornare indietro: nel foglio, File > Cronologia versioni.
I fogli GIACENZA e REPORT MENSILE non vengono toccati.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _rgb(hex_):
    hex_ = hex_.lstrip('#')
    return {k: int(hex_[i:i + 2], 16) / 255 for k, i in (('red', 0), ('green', 2), ('blue', 4))}


VERDE_TESTATA = _rgb('2E5E3A')
BIANCO        = _rgb('FFFFFF')
GRIGIO_RIGA   = _rgb('F5F6F7')
NERO          = _rgb('1F2328')

# Un colore fisso per ogni tipo di movimento: (sfondo, testo)
COLORI_TIPO = {
    'CARICO':               ('E3F4E1', '1E6B2B'),
    'IN USO':               ('FFF3CD', '8A6100'),
    'SCARICO':              ('ECEDEF', '4B5058'),
    'BUTTATO':              ('FDE2E1', 'B3261E'),
    'PREPARAZIONE':         ('E3EEFB', '1F4E8C'),
    'SCARICO PREPARAZIONE': ('E3EEFB', '1F4E8C'),
}
COLORI_ESITO = {
    'OK':           ('E3F4E1', '1E6B2B'),
    'FUORI SOGLIA': ('FDE2E1', 'B3261E'),
    'FUORI_SOGLIA': ('FDE2E1', 'B3261E'),
}

# Larghezza in pixel per colonna, riconosciuta dal nome in testata
LARGHEZZE = {
    'data': 95, 'fornitore': 150, 'prodotto': 210, 'lotto': 130, 'scadenza': 95,
    'carico': 75, 'scarico': 75, 'unità': 70, 'unita': 70, 'misura': 70,
    'etichetta': 220, 'id': 120, 'rimanenza': 90, 'operatore': 140,
    'reparto': 90, 'tipo movimento': 150, 'ora': 60, 'fascia': 90,
    'apparecchio': 170, 'rilevata': 90, 'limite': 100, 'esito': 110,
    'nota': 260, 'note': 260, 'scorta': 80, 'categoria': 170,
}
COLONNE_NUMERICHE = ('carico', 'scarico', 'rimanenza', 'rilevata', 'scorta')
COLONNE_DATA      = ('data', 'scadenza')
COLONNE_NASCOSTE  = ('id',)  # tecnica: serve all'app, non a chi legge


def _tipo_colonna(header):
    """Chiave di LARGHEZZE che corrisponde a questo nome di colonna."""
    h = header.strip().lower()
    if h in LARGHEZZE:
        return h
    if h == 'id':
        return 'id'
    for k in sorted(LARGHEZZE, key=len, reverse=True):
        if k != 'id' and k in h:
            return k
    return None


def costruisci_richieste(sheet_id, headers, n_righe, n_regole_esistenti, bande_esistenti,
                         colonna_colore=None, colori=None):
    """Lista di richieste batchUpdate per un foglio. Funzione pura, testabile
    senza Google. n_righe = righe della griglia (testata compresa)."""
    ncol = len(headers)
    tutte = {'sheetId': sheet_id, 'startColumnIndex': 0, 'endColumnIndex': ncol}
    dati  = dict(tutte, startRowIndex=1)
    req = []

    # 1. via tutte le vecchie regole di colore e righe alternate
    for i in reversed(range(n_regole_esistenti)):
        req.append({'deleteConditionalFormatRule': {'sheetId': sheet_id, 'index': i}})
    for banda_id in bande_esistenti:
        req.append({'deleteBanding': {'bandedRangeId': banda_id}})

    # 2. righe dati: stesso aspetto per tutte, niente colori manuali, niente
    #    menu a tendina colorati
    req.append({'repeatCell': {
        'range': dati,
        'cell': {'userEnteredFormat': {
            'backgroundColor': BIANCO,
            'textFormat': {'bold': False, 'italic': False, 'strikethrough': False,
                           'fontSize': 10, 'foregroundColor': NERO},
            'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT', 'wrapStrategy': 'CLIP',
        }},
        'fields': 'userEnteredFormat(backgroundColor,textFormat,verticalAlignment,'
                  'horizontalAlignment,wrapStrategy)',
    }})
    req.append({'setDataValidation': {'range': dati}})  # senza regola = toglie i menu

    # 3. testata
    req.append({'repeatCell': {
        'range': dict(tutte, startRowIndex=0, endRowIndex=1),
        'cell': {'userEnteredFormat': {
            'backgroundColor': VERDE_TESTATA,
            'textFormat': {'bold': True, 'fontSize': 10, 'foregroundColor': BIANCO},
            'horizontalAlignment': 'CENTER', 'verticalAlignment': 'MIDDLE', 'wrapStrategy': 'WRAP',
        }},
        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,'
                  'verticalAlignment,wrapStrategy)',
    }})
    req.append({'updateSheetProperties': {
        'properties': {'sheetId': sheet_id, 'gridProperties': {'frozenRowCount': 1}},
        'fields': 'gridProperties.frozenRowCount',
    }})

    # 4. altezza righe uniforme (prima il testo lungo le allargava a caso)
    req.append({'updateDimensionProperties': {
        'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
        'properties': {'pixelSize': 36}, 'fields': 'pixelSize'}})
    if n_righe > 1:
        req.append({'updateDimensionProperties': {
            'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': 1, 'endIndex': n_righe},
            'properties': {'pixelSize': 24}, 'fields': 'pixelSize'}})

    # 5. colonne: larghezza, formato numeri e date, colonne tecniche nascoste
    for idx, h in enumerate(headers):
        tipo = _tipo_colonna(h)
        col = {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': idx, 'endIndex': idx + 1}
        props, fields = {}, []
        if tipo in LARGHEZZE:
            props['pixelSize'] = LARGHEZZE[tipo]; fields.append('pixelSize')
        props['hiddenByUser'] = tipo in COLONNE_NASCOSTE; fields.append('hiddenByUser')
        req.append({'updateDimensionProperties': {'range': col, 'properties': props,
                                                   'fields': ','.join(fields)}})
        cella = dict(dati, startColumnIndex=idx, endColumnIndex=idx + 1)
        if tipo in COLONNE_NUMERICHE:
            req.append({'repeatCell': {'range': cella, 'cell': {'userEnteredFormat': {
                'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0.###'},
                'horizontalAlignment': 'RIGHT'}},
                'fields': 'userEnteredFormat(numberFormat,horizontalAlignment)'}})
        elif tipo in COLONNE_DATA:
            req.append({'repeatCell': {'range': cella, 'cell': {'userEnteredFormat': {
                'numberFormat': {'type': 'DATE', 'pattern': 'dd/mm/yyyy'},
                'horizontalAlignment': 'CENTER'}},
                'fields': 'userEnteredFormat(numberFormat,horizontalAlignment)'}})

    # 6. righe alternate (valgono anche per le righe che l'app aggiungerà)
    req.append({'addBanding': {'bandedRange': {
        'range': dict(tutte, startRowIndex=0),
        'rowProperties': {'headerColor': VERDE_TESTATA, 'firstBandColor': BIANCO,
                          'secondBandColor': GRIGIO_RIGA},
    }}})

    # 7. un solo colore significativo: la colonna del tipo / esito
    if colonna_colore is not None and colori:
        rng = dict(dati, startColumnIndex=colonna_colore, endColumnIndex=colonna_colore + 1)
        for valore, (sfondo, testo) in colori.items():
            req.append({'addConditionalFormatRule': {'index': 0, 'rule': {
                'ranges': [rng],
                'booleanRule': {
                    'condition': {'type': 'TEXT_EQ', 'values': [{'userEnteredValue': valore}]},
                    'format': {'backgroundColor': _rgb(sfondo),
                               'textFormat': {'bold': True, 'foregroundColor': _rgb(testo)}},
                },
            }}})
        req.append({'repeatCell': {'range': rng, 'cell': {'userEnteredFormat': {
            'horizontalAlignment': 'CENTER'}}, 'fields': 'userEnteredFormat.horizontalAlignment'}})

    # 8. filtri sulla testata, per cercare per prodotto, fornitore, tipo...
    req.append({'setBasicFilter': {'filter': {'range': dict(tutte, startRowIndex=0)}}})
    return req


def _indice(headers, *nomi):
    low = [h.strip().lower() for h in headers]
    for n in nomi:
        for i, h in enumerate(low):
            if n in h:
                return i
    return None


def main(applica):
    import sheets
    gc = sheets._get_client()
    sh = gc.open_by_key(sheets.SPREADSHEET_ID)
    meta = sh.fetch_sheet_metadata({'fields': 'sheets(properties,conditionalFormats,bandedRanges)'})
    per_nome = {s['properties']['title']: s for s in meta['sheets']}

    piano = [
        (sheets.SHEET_REGISTRO,    ('tipo movimento',), COLORI_TIPO),
        (sheets.SHEET_TEMPERATURE, ('esito',),          COLORI_ESITO),
        (sheets.SHEET_LISTINO,     (),                  None),
    ]
    for nome, col_colore, colori in piano:
        s = per_nome.get(nome)
        if not s:
            print(f'- {nome}: foglio non trovato, salto')
            continue
        props = s['properties']
        headers = sh.worksheet(nome).row_values(1)
        while headers and not headers[-1].strip():
            headers.pop()
        idx_colore = _indice(headers, *col_colore) if col_colore else None
        req = costruisci_richieste(
            props['sheetId'], headers, props['gridProperties']['rowCount'],
            len(s.get('conditionalFormats', [])),
            [b['bandedRangeId'] for b in s.get('bandedRanges', [])],
            idx_colore, colori,
        )
        print(f"- {nome}: {len(headers)} colonne, {len(s.get('conditionalFormats', []))} vecchie regole "
              f"di colore e {len(s.get('bandedRanges', []))} righe alternate da togliere, "
              f"{len(req)} modifiche di aspetto")
        if applica:
            sh.batch_update({'requests': req})
            print('  fatto')
    if not applica:
        print('\nProva: non ho cambiato niente. Per applicare: --applica')


if __name__ == '__main__':
    main('--applica' in sys.argv)
