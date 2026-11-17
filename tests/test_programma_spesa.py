from datetime import date, timedelta

from programma_spesa import calcola_suggeriti, giorni_abituali

OGGI = date(2026, 11, 20)  # venerdì


def compra(prodotto, giorni_fa, fornitore='DOCKS', quantita=0, unita='kg'):
    return {'prodotto': prodotto, 'fornitore': fornitore,
            'data': (OGGI - timedelta(days=giorni_fa)).isoformat(),
            'quantita': quantita, 'unita': unita}


def test_propone_il_prodotto_quando_e_ora():
    # comprato ogni 4 giorni, l'ultima volta 4 giorni fa: serve oggi
    acquisti = [compra('Mozzarella', d, quantita=2) for d in (16, 12, 8, 4)]
    s = calcola_suggeriti(acquisti, oggi=OGGI)
    assert len(s) == 1
    assert s[0]['prodotto'] == 'Mozzarella'
    assert s[0]['ogni_giorni'] == 4
    assert s[0]['mancano'] == 0
    assert s[0]['quantita'] == 2


def test_non_propone_se_manca_ancora_tempo():
    acquisti = [compra('Farina', d) for d in (21, 14, 7, 1)]  # ogni 7, comprata ieri
    assert calcola_suggeriti(acquisti, oggi=OGGI) == []


def test_servono_almeno_tre_acquisti():
    acquisti = [compra('Basilico', d) for d in (10, 5)]
    assert calcola_suggeriti(acquisti, oggi=OGGI) == []


def test_non_ripropone_quello_gia_in_lista():
    acquisti = [compra('Mozzarella', d) for d in (16, 12, 8, 4)]
    assert calcola_suggeriti(acquisti, gia_in_lista=['mozzarella '], oggi=OGGI) == []


def test_fornitore_dell_ultimo_acquisto_e_ritardo():
    acquisti = [compra('Limoni', 15, 'DOCKS'), compra('Limoni', 12, 'DOCKS'),
                compra('Limoni', 9, 'GLOBAL')]
    s = calcola_suggeriti(acquisti, oggi=OGGI)
    assert s[0]['fornitore'] == 'GLOBAL'
    assert s[0]['mancano'] == -6          # ogni 3 giorni, ultimo 9 giorni fa


def test_stesso_giorno_conta_una_volta_e_nomi_uguali_senza_maiuscole():
    acquisti = [compra('Cozze', 9), compra('cozze', 9), compra('COZZE ', 6), compra('Cozze', 3)]
    s = calcola_suggeriti(acquisti, oggi=OGGI)
    assert s[0]['acquisti'] == 3 and s[0]['ogni_giorni'] == 3


def test_prodotto_abbandonato_non_viene_proposto():
    acquisti = [compra('Panettone', d) for d in (80, 77, 74)]
    assert calcola_suggeriti(acquisti, oggi=OGGI) == []


def test_giorni_abituali_del_fornitore():
    # DOCKS il martedì e il venerdì
    acquisti = [compra('x', d, 'DOCKS') for d in (3, 7, 10, 14)]
    assert giorni_abituali(acquisti)['DOCKS'] == ['mar', 'ven']


def test_endpoint_usa_comprati_e_registro(db, monkeypatch):
    import app as A
    oggi = date.today()
    for giorni_fa in (9, 6, 3):
        g = (oggi - timedelta(days=giorni_fa)).isoformat()
        with db.get_conn() as conn:
            conn.execute(
                "INSERT INTO spesa_comprati (prodotto, fornitore, quantita, unita, comprato_il, stato) "
                "VALUES (?, ?, ?, ?, ?, 'registrato')", ('Vongole', 'GLOBAL', 2, 'kg', g + 'T10:00:00'))
    A.app.config['TESTING'] = True
    client = A.app.test_client()
    from werkzeug.security import generate_password_hash
    db.create_user('Prova Utente', 'prova', generate_password_hash('pw123456', method='pbkdf2'), 'cucina')
    client.post('/login', data={'nome': 'Prova Utente', 'password': 'pw123456'})
    r = client.get('/api/spesa-suggeriti').get_json()
    assert r['success']
    assert [s['prodotto'] for s in r['suggeriti']] == ['Vongole']
    assert r['suggeriti'][0]['fornitore'] == 'GLOBAL'

    # se è già in lista spesa non viene riproposto
    db.add_lista_spesa_item('Vongole', 'GLOBAL', '', 2, 'kg')
    r = client.get('/api/spesa-suggeriti').get_json()
    assert r['suggeriti'] == []
