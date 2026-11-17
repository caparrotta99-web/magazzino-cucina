from datetime import date, timedelta

from programma_spesa import calcola_suggeriti

OGGI = date(2026, 11, 24)  # martedì


def ordine(prodotto, giorno, fornitore='DOCKS', quantita=0):
    return {'prodotto': prodotto, 'fornitore': fornitore,
            'data': giorno.isoformat() + 'T10:00:00', 'quantita': quantita, 'unita': 'kg'}


def ogni_settimana(prodotto, giorno, settimane=4, **kw):
    return [ordine(prodotto, giorno - timedelta(days=7 * i), **kw) for i in range(1, settimane + 1)]


def test_quattro_martedi_di_fila_viene_suggerito_oggi():
    s = calcola_suggeriti(ogni_settimana('Orata', OGGI, quantita=3), oggi=OGGI)
    assert [(x['prodotto'], x['quando'], x['giorno_settimana'], x['quantita']) for x in s] == \
        [('Orata', 'oggi', 'martedì', 3)]


def test_suggerito_anche_il_giorno_prima_come_domani():
    domani = OGGI + timedelta(days=1)
    s = calcola_suggeriti(ogni_settimana('Pane', domani), oggi=OGGI)
    assert [(x['prodotto'], x['quando']) for x in s] == [('Pane', 'domani')]


def test_tre_settimane_non_bastano():
    assert calcola_suggeriti(ogni_settimana('Orata', OGGI, settimane=3), oggi=OGGI) == []


def test_una_settimana_saltata_non_viene_suggerito():
    ordini = ogni_settimana('Orata', OGGI)
    ordini = [o for o in ordini if o['data'][:10] != (OGGI - timedelta(days=14)).isoformat()]
    assert calcola_suggeriti(ordini, oggi=OGGI) == []


def test_giorno_diverso_non_conta():
    # comprato 4 mercoledì: oggi (martedì) no, domani (mercoledì) sì
    mer = OGGI + timedelta(days=1)
    s = calcola_suggeriti(ogni_settimana('Cozze', mer), oggi=OGGI)
    assert [x['quando'] for x in s] == ['domani']


def test_gia_in_lista_o_gia_comprato_oggi():
    ordini = ogni_settimana('Orata', OGGI)
    assert calcola_suggeriti(ordini, gia_in_lista=['ORATA'], oggi=OGGI) == []
    assert calcola_suggeriti(ordini + [ordine('Orata', OGGI)], oggi=OGGI) == []


def test_fornitore_dell_ultimo_ordine():
    ordini = ogni_settimana('Limoni', OGGI, fornitore='DOCKS')
    ordini[0]['fornitore'] = 'GLOBAL'          # la settimana scorsa
    assert calcola_suggeriti(ordini, oggi=OGGI)[0]['fornitore'] == 'GLOBAL'


def test_ora_utc_di_notte_conta_come_giorno_italiano():
    # 23:30 italiane di lunedì = 22:30 UTC: è lunedì, non martedì
    lun = OGGI - timedelta(days=1)
    ordini = [{'prodotto': 'Uova', 'fornitore': 'DOCKS',
               'data': (lun - timedelta(days=7 * i)).isoformat() + 'T22:30:00+00:00'} for i in range(0, 4)]
    s = calcola_suggeriti(ordini, oggi=OGGI + timedelta(days=6))  # lunedì prossimo
    assert [x['prodotto'] for x in s] == ['Uova']


def test_solo_admin_e_non_guarda_i_carichi(db):
    import app as A
    from werkzeug.security import generate_password_hash
    oggi = A.today_it()
    for i in range(1, 5):
        g = (oggi - timedelta(days=7 * i)).isoformat()
        with db.get_conn() as conn:
            conn.execute(
                "INSERT INTO spesa_comprati (prodotto, fornitore, quantita, unita, comprato_il, stato) "
                "VALUES (?, ?, ?, ?, ?, 'registrato')", ('Vongole', 'GLOBAL', 2, 'kg', g + 'T10:00:00'))
        # un carico in magazzino con lo stesso ritmo non deve contare
        with db.get_conn() as conn:
            conn.execute("INSERT INTO registro (data, fornitore, prodotto, carico, tipo) VALUES (?, 'DOCKS', 'Farina', 5, 'CARICO')", (g,))
    A.app.config['TESTING'] = True
    db.create_user('Capo Cucina', 'capo', generate_password_hash('pw123456', method='pbkdf2'), 'cucina', role='admin')
    db.create_user('Aiuto Cuoco', 'aiuto', generate_password_hash('pw123456', method='pbkdf2'), 'cucina')

    staff = A.app.test_client()
    staff.post('/login', data={'nome': 'Aiuto Cuoco', 'password': 'pw123456'})
    assert staff.get('/api/spesa-suggeriti').status_code == 403

    admin = A.app.test_client()
    admin.post('/login', data={'nome': 'Capo Cucina', 'password': 'pw123456'})
    r = admin.get('/api/spesa-suggeriti').get_json()
    assert [(s['prodotto'], s['fornitore'], s['quando']) for s in r['suggeriti']] == [('Vongole', 'GLOBAL', 'oggi')]

    db.add_lista_spesa_item('Vongole', 'GLOBAL', '', 2, 'kg')
    assert admin.get('/api/spesa-suggeriti').get_json()['suggeriti'] == []
