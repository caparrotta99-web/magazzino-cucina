"""
Pulizia del listino di Brigade: unisce i nomi doppi dei prodotti e sistema
unità e categorie, secondo le scelte fatte da Simone nella pagina
"Pulizia del listino" (18/11/2026).

Cosa fa:
- LISTINO: se due righe sono lo stesso prodotto con lo STESSO fornitore,
  svuota la riga doppia (non la cancella: le formule del foglio restano dove
  sono). Se il fornitore è diverso, rinomina la riga col nome tenuto: il
  prodotto resta con due fornitori, come già Cozze, Lonza, Pollo, Roast-beef.
- LISTINO: unità scritte tutte uguali (pz, kg, lt, conf) e le correzioni di
  categoria/unità scelte.
- REGISTRO: rinomina il prodotto nelle righe col nome vecchio e scrive le
  unità nello stesso modo. Non riordina, non aggiunge e non cancella righe;
  non tocca lotti, quantità, date né etichette HACCP.
- Non scrive mai in una cella che contiene una formula.
- Dopo aver scritto rilegge i fogli e controlla che sia cambiato SOLO quello
  previsto.

Uso (dalla cartella del progetto, con credentials.json presente):
    python3 strumenti/pulisci_listino.py            # prova: mostra cosa farebbe
    python3 strumenti/pulisci_listino.py --applica  # lo fa davvero

Dopo --applica serve un passo nell'app (lo fa Claude dal browser, da admin):
POST /api/admin/pulizia-listino rinomina i prodotti uniti anche in lista spesa,
ordini salvati, abbinamenti delle etichette e ricette; poi "Sincronizza" (o un
riavvio) ricarica listino e registro dai fogli.
Per tornare indietro: nel foglio, File > Cronologia versioni.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# nome vecchio → nome tenuto (scelte D1-D3, P1-P5, C3)
UNIONI = {
    'fernet menta':          'Fernet Menta',
    'pomodori':              'Pomodori',
    'Zucchero canna bustine': 'zucchero di canna bustine',
    'Patatine fritte':       'Patatine',
    'Costate':               'Costate di manzo',
    'Tuorlo d’uovo':         'Tuorlo',
    'Sesamo':                'semi di sesamo',
    'zucchero di canna':     'zucchero di canna sciolto',
    'Gamberi':               'Gamberi L1 (scatola da 2kg)',
}

# unità scritte in un modo solo (scelta U1)
UNITA = {'pz': 'pz', 'kg': 'kg', 'lt': 'lt', 'conf': 'conf', 'confezioni': 'conf', 'confezione': 'conf'}

# correzioni puntuali nel LISTINO (scelte K1-K5); K6 scialatielli: lasciato vuoto
CORREZIONI = {
    'penne senza glutine':     {'categoria': 'Pasta e cereali'},
    'spaghetti senza glutine': {'categoria': 'Pasta e cereali'},
    'zucchero a velo':         {'categoria': 'Dolci'},
    'Lime':                    {'categoria': 'Verdure e frutta fresche', 'unita': 'kg'},
    'Carta igienica':          {'unita': 'conf'},
}


def _k(s):
    return ' '.join(str(s or '').split()).lower()


def _unita(u):
    """'Pz' → 'pz', 'confezioni' → 'conf'; le altre restano come sono."""
    return UNITA.get(_k(u), u)


def colonne_con_formula(formule):
    return {j for r in formule for j, v in enumerate(r) if str(v).startswith('=')}


def _col(headers, *nomi):
    for i, h in enumerate(headers):
        hl = h.strip().lower()
        if any(n in hl for n in nomi):
            return i
    return None


def _cella(riga, i):
    return riga[i] if i is not None and i < len(riga) else ''


def pianifica_listino(valori, formule):
    """valori/formule: righe del LISTINO (testata compresa), come testo e come
    formula. Restituisce le modifiche [(riga, colonna, vecchio, nuovo, motivo)],
    indici 0-based."""
    h = [x.strip() for x in valori[0]]
    c_nome, c_forn = _col(h, 'prodotto', 'nome') or 0, _col(h, 'fornitore')
    c_unita, c_cat = _col(h, 'unità', 'unita', 'misura'), _col(h, 'categoria')
    unioni = {_k(a): b for a, b in UNIONI.items()}
    correz = {_k(a): b for a, b in CORREZIONI.items()}
    righe = valori[1:]
    modifiche = []
    # colonne con una formula da qualche parte (es. l'elenco in ordine
    # alfabetico): una formula che si "allarga" su tutta la colonna si
    # romperebbe se scrivessi anche in una sola delle sue celle → mai toccarle
    col_formula = colonne_con_formula(formule)

    def scrivi(i_riga, i_col, nuovo, motivo):
        if i_col is None or i_col in col_formula:
            return
        vecchio = _cella(valori[i_riga], i_col)
        if vecchio != nuovo:
            modifiche.append((i_riga, i_col, vecchio, nuovo, motivo))

    for n, r in enumerate(righe, start=1):
        nome = _cella(r, c_nome).strip()
        if not nome:
            continue
        k = _k(nome)
        if k in unioni and nome != unioni[k]:
            tenuto = unioni[k]
            forn = _k(_cella(r, c_forn))
            gemella = any(_cella(x, c_nome).strip() == tenuto and _k(_cella(x, c_forn)) == forn for x in righe)
            if gemella:
                # stesso prodotto, stesso fornitore: riga doppia, la svuoto (tranne le formule)
                for i_col in range(len(r)):
                    if _cella(r, i_col) != '':
                        scrivi(n, i_col, '', f'riga doppia di «{tenuto}» ({_cella(r, c_forn)}): svuotata')
                continue
            # fornitore diverso: stesso nome, il prodotto resta con due fornitori
            scrivi(n, c_nome, tenuto, f'nome unito a «{tenuto}»')
            k = _k(tenuto)
        cor = correz.get(k, {})
        if 'categoria' in cor:
            scrivi(n, c_cat, cor['categoria'], 'categoria corretta')
        nuova_u = cor.get('unita') or _unita(_cella(r, c_unita))
        if c_unita is not None and _cella(r, c_unita) != '':
            scrivi(n, c_unita, nuova_u, 'unità corretta' if 'unita' in cor else 'unità scritta in un modo solo')
    return modifiche


def pianifica_registro(valori, formule):
    h = [x.strip() for x in valori[0]]
    c_nome, c_unita = _col(h, 'prodotto'), _col(h, 'unità', 'unita', 'misura')
    col_formula = colonne_con_formula(formule)
    if c_nome in col_formula:
        c_nome = None
    if c_unita in col_formula:
        c_unita = None
    unioni = {_k(a): b for a, b in UNIONI.items()}
    modifiche = []
    for n, r in enumerate(valori[1:], start=1):
        nome = _cella(r, c_nome).strip()
        k = _k(nome)
        if c_nome is not None and k in unioni and nome != unioni[k]:
            modifiche.append((n, c_nome, _cella(r, c_nome), unioni[k], f'nome unito a «{unioni[k]}»'))
        u = _cella(r, c_unita)
        if c_unita is not None and u and _unita(u) != u:
            modifiche.append((n, c_unita, u, _unita(u), 'unità scritta in un modo solo'))
    return modifiche


def giacenze_aperte(valori):
    """Prodotti col nome vecchio che hanno ancora merce in magazzino:
    per ogni lotto, carichi meno scarichi (la colonna "Rimanenza" delle vecchie
    righe di carico non si aggiorna quando il lotto viene scaricato, quindi da
    sola darebbe falsi allarmi)."""
    h = [x.strip() for x in valori[0]]
    c_nome, c_lotto, c_unita = _col(h, 'prodotto'), _col(h, 'lotto'), _col(h, 'unità', 'unita')
    c_car, c_sca = _col(h, 'carico'), _col(h, 'scarico')
    unioni = {_k(a) for a in UNIONI}

    def num(x):
        try:
            return float(str(x).replace(',', '.') or 0)
        except ValueError:
            return 0.0

    saldi = {}
    for r in valori[1:]:
        if _k(_cella(r, c_nome)) in unioni:
            chiave = (_cella(r, c_nome).strip(), _cella(r, c_lotto).strip(), _unita(_cella(r, c_unita)))
            saldi[chiave] = saldi.get(chiave, 0) + num(_cella(r, c_car)) - num(_cella(r, c_sca))
    aperte = {}
    for (nome, _lotto, unita), saldo in saldi.items():
        if saldo > 1e-9:
            aperte.setdefault(nome, []).append((round(saldo, 3), unita))
    return aperte


def _lettera(i):
    s = ''
    i += 1
    while i:
        i, resto = divmod(i - 1, 26)
        s = chr(65 + resto) + s
    return s


def _applica_attese(valori, modifiche):
    attesi = [list(r) for r in valori]
    for riga, col, _v, nuovo, _m in modifiche:
        while len(attesi[riga]) <= col:
            attesi[riga].append('')
        attesi[riga][col] = nuovo
    return attesi


def _uguali(a, b, colonne_formula):
    n = max(len(a), len(b))
    diff = []
    for i in range(n):
        ra = a[i] if i < len(a) else []
        rb = b[i] if i < len(b) else []
        for j in range(max(len(ra), len(rb))):
            if j in colonne_formula:
                continue
            if _cella(ra, j) != _cella(rb, j):
                diff.append((i + 1, _lettera(j), _cella(ra, j), _cella(rb, j)))
    return diff


def main(applica):
    import sheets
    gc = sheets._get_client()
    sh = gc.open_by_key(sheets.SPREADSHEET_ID)
    ok = True
    for nome_foglio, pianifica in (('LISTINO', pianifica_listino), ('REGISTRO', pianifica_registro)):
        ws = sh.worksheet(nome_foglio)
        valori = ws.get_all_values()
        formule = ws.get_all_values(value_render_option='FORMULA')
        colonne_formula = colonne_con_formula(formule)
        modifiche = pianifica(valori, formule)
        print(f'\n=== {nome_foglio}: {len(modifiche)} celle da cambiare ===')
        for riga, col, vecchio, nuovo, motivo in modifiche:
            print(f'  {_lettera(col)}{riga + 1}: «{vecchio}» → «{nuovo}»   ({motivo})')
        if nome_foglio == 'REGISTRO':
            aperte = giacenze_aperte(valori)
            if aperte:
                ok = False
                print('\n  ATTENZIONE: c\'è ancora merce in magazzino con questi nomi vecchi:')
                for p, lotti in aperte.items():
                    print(f'    {p}: ' + ', '.join(f'{q} {u}' for q, u in lotti))
                print('  Controllala prima di applicare (unità diverse non si sommano).')
        if not applica or not modifiche:
            continue
        ws.batch_update([{'range': f'{_lettera(c)}{r + 1}', 'values': [[n]]}
                         for r, c, _v, n, _m in modifiche], value_input_option='RAW')
        dopo = ws.get_all_values()
        diff = _uguali(_applica_attese(valori, modifiche), dopo, colonne_formula)
        if diff:
            ok = False
            print(f'  CONTROLLO NON SUPERATO su {nome_foglio}: {len(diff)} celle diverse dal previsto, per esempio:')
            for d in diff[:10]:
                print(f'    {d[1]}{d[0]}: atteso «{d[2]}», trovato «{d[3]}»')
        else:
            print(f'  Controllo superato: su {nome_foglio} è cambiato solo quello previsto.')
    if not applica:
        print('\nERA SOLO UNA PROVA: niente è stato scritto. Per applicare: --applica')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main('--applica' in sys.argv))
