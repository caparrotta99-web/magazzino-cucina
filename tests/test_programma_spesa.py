from datetime import date, timedelta

from programma_spesa import calcola_suggeriti

OGGI = date(2026, 11, 24)


def ordine(prodotto, giorni_fa, fornitore='DOCKS', quantita=0):
    return {'prodotto': prodotto, 'fornitore': fornitore,
            'data': (OGGI - timedelta(days=giorni_fa)).isoformat() + 'T10:00:00',
            'quantita': quantita, 'unita': 'kg'}


def test_ritmi_diversi_consigliati_il_giorno_prima():
    # roast-beef ogni 7 giorni, ultimo 6 giorni fa → serve domani
    # mele ogni 15 giorni, ultimo 14 giorni fa → serve domani
    ordini = [ordine('Roast-beef', d, quantita=2) for d in (27, 20, 13, 6)] + \
             [ordine('Mele', d) for d in (59, 44, 29, 14)]
    s = calcola_suggeriti(ordini, oggi=OGGI)
    assert {(x['prodotto'], x['ogni_giorni'], x['quando']) for x in s} == \
        {('Roast-beef', 7, 'domani'), ('Mele', 15, 'domani')}
    rb = next(x for x in s if x['prodotto'] == 'Roast-beef')
    assert rb['quantita'] == 2 and (rb['regolari'], rb['intervalli']) == (3, 3)


def test_resta_consigliato_il_giorno_stesso_se_non_ancora_ordinato():
    s = calcola_suggeriti([ordine('Roast-beef', d) for d in (28, 21, 14, 7)], oggi=OGGI)
    assert [(x['prodotto'], x['quando']) for x in s] == [('Roast-beef', 'oggi')]


def test_non_consiglia_prima_del_giorno_prima():
    assert calcola_suggeriti([ordine('Roast-beef', d) for d in (26, 19, 12, 5)], oggi=OGGI) == []


def test_giorno_passato_smette_di_proporlo():
    assert calcola_suggeriti([ordine('Roast-beef', d) for d in (30, 23, 16, 9)], oggi=OGGI) == []


def test_servono_almeno_quattro_ordini():
    assert calcola_suggeriti([ordine('Mele', d) for d in (44, 29, 14)], oggi=OGGI) == []


def test_ritmo_irregolare_non_viene_consigliato():
    # intervalli 2, 10, 4, 9, 3: nessun ritmo vero
    giorni = [29, 27, 17, 13, 4, 1]
    assert calcola_suggeriti([ordine('Peperoni', d) for d in giorni], oggi=OGGI) == []


def test_una_volta_strana_non_sballa_il_ritmo():
    # ogni 7 giorni, una volta sola dopo 3: 3 intervalli su 4 regolari
    s = calcola_suggeriti([ordine('Lonza', d) for d in (30, 23, 20, 13, 6)], oggi=OGGI)
    assert s and s[0]['ogni_giorni'] == 7 and s[0]['quando'] == 'domani'
    assert (s[0]['regolari'], s[0]['intervalli']) == (3, 4)


def test_il_ritmo_si_adegua_agli_ordini_recenti():
    # prima ogni 14 giorni, ora ogni 4: contano gli ultimi intervalli
    vecchi = [100, 86, 72]
    nuovi = [35, 31, 27, 23, 19, 15, 11, 7, 3]
    s = calcola_suggeriti([ordine('Zucchine', d) for d in vecchi + nuovi], oggi=OGGI)
    assert s and s[0]['ogni_giorni'] == 4 and s[0]['quando'] == 'domani'


def _vecchie_righe(prodotti, giorni_fa, secondi):
    return [{'prodotto': p, 'fornitore': 'DOCKS', 'utente': 'Simone', 'fonte': 'log',
             'data': (OGGI - timedelta(days=giorni_fa)).isoformat() + f'T21:04:{sec:02d}+00:00'}
            for p, sec in zip(prodotti, secondi)]


def test_svuota_lista_non_conta_come_acquisto():
    # ogni 7 giorni, più un vecchio "svuota": 10 prodotti tolti nello stesso secondo
    ordini = [ordine('Orata', d) for d in (27, 20, 13, 6)]
    svuota = _vecchie_righe(['Orata'] + [f'altro {n}' for n in range(9)], 10, [5] * 10)
    s = calcola_suggeriti(ordini + svuota, oggi=OGGI)
    assert [(x['prodotto'], x['ogni_giorni'], x['quando']) for x in s] == [('Orata', 7, 'domani')]


def test_tutta_la_lista_spuntata_insieme_conta_come_ordine():
    # ordine su WhatsApp: 10 prodotti spuntati uno dopo l'altro in pochi secondi,
    # ogni 7 giorni → è un ordine vero
    prodotti = ['Orata'] + [f'altro {n}' for n in range(9)]
    ordini = []
    for d in (27, 20, 13, 6):
        ordini += _vecchie_righe(prodotti, d, range(0, 50, 5))
    s = calcola_suggeriti(ordini, oggi=OGGI)
    assert 'Orata' in [x['prodotto'] for x in s]
    assert all(x['ogni_giorni'] == 7 and x['quando'] == 'domani' for x in s)


def test_gia_in_lista_e_nomi_senza_maiuscole():
    # stesso giorno due volte = un ordine solo; maiuscole e spazi non contano
    ordini = [ordine('Cozze', 12), ordine('Cozze', 9), ordine('cozze ', 6), ordine('COZZE', 3), ordine('Cozze', 3)]
    s = calcola_suggeriti(ordini, oggi=OGGI)
    assert [(x['ordini'], x['ogni_giorni'], x['quando']) for x in s] == [(4, 3, 'oggi')]
    assert calcola_suggeriti(ordini, gia_in_lista=['Cozze'], oggi=OGGI) == []


def test_fornitore_dell_ultimo_ordine():
    ordini = [ordine('Limoni', 28, 'DOCKS'), ordine('Limoni', 21, 'DOCKS'),
              ordine('Limoni', 14, 'DOCKS'), ordine('Limoni', 7, 'GLOBAL')]
    assert calcola_suggeriti(ordini, oggi=OGGI)[0]['fornitore'] == 'GLOBAL'


def test_ora_utc_di_notte_conta_come_giorno_italiano():
    # 22:30 UTC = 23:30 italiane dello stesso giorno: il giorno non cambia
    ordini = [{'prodotto': 'Uova', 'fornitore': 'DOCKS',
               'data': (OGGI - timedelta(days=d)).isoformat() + 'T22:30:00+00:00'} for d in (22, 15, 8, 1)]
    s = calcola_suggeriti(ordini, oggi=OGGI + timedelta(days=5))   # ogni 7, ultimo 6 giorni prima
    assert [(x['prodotto'], x['quando']) for x in s] == [('Uova', 'domani')]


def test_tutti_la_vedono_e_non_guarda_i_carichi(db):
    import app as A
    from werkzeug.security import generate_password_hash
    oggi = A.today_it()
    for d in (27, 20, 13, 6):     # ogni 7, ultimo 6 giorni fa → domani
        g = (oggi - timedelta(days=d)).isoformat()
        with db.get_conn() as conn:
            conn.execute(
                "INSERT INTO spesa_comprati (prodotto, fornitore, quantita, unita, comprato_il, stato) "
                "VALUES (?, ?, ?, ?, ?, 'registrato')", ('Vongole', 'GLOBAL', 2, 'kg', g + 'T10:00:00'))
            # carichi in magazzino con lo stesso ritmo: non devono contare
            conn.execute("INSERT INTO registro (data, fornitore, prodotto, carico, tipo) "
                         "VALUES (?, 'DOCKS', 'Farina', 5, 'CARICO')", (g,))
    A.app.config['TESTING'] = True
    db.create_user('Aiuto Cuoco', 'aiuto', generate_password_hash('pw123456', method='pbkdf2'), 'cucina')

    staff = A.app.test_client()
    staff.post('/login', data={'nome': 'Aiuto Cuoco', 'password': 'pw123456'})
    r = staff.get('/api/spesa-suggeriti').get_json()
    assert [(s['prodotto'], s['fornitore'], s['quando']) for s in r['suggeriti']] == [('Vongole', 'GLOBAL', 'domani')]

    db.add_lista_spesa_item('Vongole', 'GLOBAL', '', 2, 'kg')
    assert staff.get('/api/spesa-suggeriti').get_json()['suggeriti'] == []
