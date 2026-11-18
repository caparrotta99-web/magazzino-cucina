from datetime import date, timedelta

from programma_menu import proponi_settimana, alternative, famiglia

LUNEDI = date(2026, 11, 23)


def piatto(nome, tipo='primo', categoria=''):
    return {'nome': nome, 'tipo': tipo, 'categoria': categoria}


def archivio_grande():
    primi = ['Risotto ai funghi', 'Risotto allo zafferano', 'Gnocchi al pesto', 'Lasagne', 'Spaghetti alle vongole',
             'Penne all\'arrabbiata', 'Tagliatelle al ragù', 'Pasta e fagioli', 'Orecchiette con cime di rapa',
             'Ravioli burro e salvia', 'Paccheri al pomodoro', 'Linguine allo scoglio', 'Minestrone', 'Trofie al pesto',
             'Bucatini all\'amatriciana', 'Fusilli al tonno', 'Crespelle', 'Pizzoccheri', 'Zuppa di ceci', 'Carbonara',
             'Cannelloni', 'Mezze maniche alla norma', 'Farfalle al salmone', 'Gnocchi alla sorrentina',
             'Strozzapreti', 'Pici cacio e pepe', 'Tortellini in brodo', 'Casoncelli']
    secondi = [(n, 'pesce') for n in ['Orata al forno', 'Branzino', 'Salmone', 'Calamari fritti', 'Baccalà',
                                       'Seppie con piselli', 'Tonno scottato', 'Merluzzo', 'Gamberoni', 'Polpo e patate',
                                       'Sgombro', 'Pesce spada', 'Trota', 'Fritto misto']] + \
              [(n, 'carne') for n in ['Arrosto di vitello', 'Pollo alla cacciatora', 'Scaloppine', 'Cotoletta',
                                       'Brasato', 'Spezzatino', 'Roast-beef', 'Salsiccia e patate', 'Lonza al latte',
                                       'Ossobuco', 'Tagliata', 'Polpette', 'Coniglio', 'Saltimbocca']] + \
              [(n, 'verdure') for n in ['Parmigiana', 'Frittata di zucchine', 'Verdure grigliate', 'Sformato']]
    return [piatto(n) for n in primi] + [piatto(n, 'secondo', c) for n, c in secondi]


def test_riempie_tutta_la_settimana_4_primi_e_4_secondi():
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    assert len(proposte) == 7 * 8
    for g in range(7):
        giorno = (LUNEDI + timedelta(days=g)).isoformat()
        assert sorted(p['posizione'] for p in proposte if p['data'] == giorno and p['tipo'] == 'primo') == [1, 2, 3, 4]
        assert sorted(p['posizione'] for p in proposte if p['data'] == giorno and p['tipo'] == 'secondo') == [1, 2, 3, 4]


def test_nessun_piatto_ripetuto_in_14_giorni():
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    nomi = [p['piatto'] for p in proposte]
    assert len(nomi) == len(set(nomi))


def test_ogni_giorno_almeno_un_pesce_e_una_carne_nei_secondi():
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    for g in range(7):
        giorno = (LUNEDI + timedelta(days=g)).isoformat()
        cat = {p['categoria'] for p in proposte if p['data'] == giorno and p['tipo'] == 'secondo'}
        assert {'pesce', 'carne'} <= cat


def test_mai_due_primi_della_stessa_famiglia_nello_stesso_giorno():
    assert famiglia('Risotto ai funghi') == 'risotto' and famiglia('Gnocchi al pesto') == 'gnocchi'
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    for g in range(7):
        giorno = (LUNEDI + timedelta(days=g)).isoformat()
        fam = [famiglia(p['piatto']) for p in proposte if p['data'] == giorno and p['tipo'] == 'primo']
        assert len(fam) == len(set(fam))


def test_non_tocca_i_posti_gia_scritti_e_i_giorni_chiusi():
    gia = [{'data': LUNEDI.isoformat(), 'tipo': 'primo', 'posizione': 2, 'piatto': 'Lasagne', 'categoria': ''}]
    martedi = (LUNEDI + timedelta(days=1)).isoformat()
    proposte = proponi_settimana(LUNEDI, archivio_grande(), gia, giorni_chiusi=[martedi])
    assert not any(p['data'] == LUNEDI.isoformat() and p['tipo'] == 'primo' and p['posizione'] == 2 for p in proposte)
    assert not any(p['data'] == martedi for p in proposte)
    assert 'Lasagne' not in [p['piatto'] for p in proposte]     # già in menu lunedì: non si ripete


def test_piatto_fatto_di_recente_non_viene_riproposto_e_si_preferisce_il_meno_recente():
    archivio = [piatto('Lasagne'), piatto('Carbonara')]
    settimana_scorsa = [{'data': (LUNEDI - timedelta(days=5)).isoformat(), 'tipo': 'primo', 'posizione': 1,
                         'piatto': 'Lasagne', 'categoria': ''}]
    proposte = proponi_settimana(LUNEDI, archivio, settimana_scorsa)
    # lunedì: la lasagna è di 5 giorni fa → no; c'è solo la carbonara
    lun = [p['piatto'] for p in proposte if p['data'] == LUNEDI.isoformat()]
    assert lun == ['Carbonara']
    # la lasagna torna dopo 14 giorni dall'ultima volta (cioè dal lunedì della settimana dopo... non in questa)
    assert 'Lasagne' not in [p['piatto'] for p in proposte]


def test_archivio_vuoto_non_propone_niente():
    assert proponi_settimana(LUNEDI, [], []) == []


def test_alternative_escludono_i_recenti_e_quelli_del_giorno():
    archivio = [piatto('Lasagne'), piatto('Carbonara'), piatto('Minestrone')]
    menu = [{'data': LUNEDI.isoformat(), 'tipo': 'primo', 'posizione': 1, 'piatto': 'Carbonara', 'categoria': ''},
            {'data': (LUNEDI - timedelta(days=20)).isoformat(), 'tipo': 'primo', 'posizione': 1, 'piatto': 'Minestrone', 'categoria': ''}]
    alt = alternative(LUNEDI, 'primo', archivio, menu)
    assert [a['nome'] for a in alt] == ['Lasagne', 'Minestrone']   # mai fatto prima, poi il meno recente


def _login(A, db, nome, ruolo='staff'):
    from werkzeug.security import generate_password_hash
    db.create_user(nome, nome.lower().replace(' ', ''), generate_password_hash('pw123456', method='pbkdf2'), 'cucina', role=ruolo)
    c = A.app.test_client()
    c.post('/login', data={'nome': nome, 'password': 'pw123456'})
    return c


def test_api_bozza_solo_admin_poi_pubblicata_per_tutti(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    staff = _login(A, db, 'Aiuto Cuoco')
    oggi = A.today_it()
    lunedi = (oggi - timedelta(days=oggi.weekday())).isoformat()

    r = admin.post('/api/piano/posto', json={'data': oggi.isoformat(), 'tipo': 'primo', 'posizione': 1,
                                             'piatto': 'Risotto ai funghi', 'categoria': 'verdure'}).get_json()
    assert r['success'] and r['puo_modificare']
    # lo staff non può modificare e non vede la bozza
    assert staff.post('/api/piano/posto', json={'data': oggi.isoformat(), 'tipo': 'primo', 'posizione': 2,
                                                'piatto': 'X'}).status_code == 403
    s = staff.get('/api/piano').get_json()
    assert not s['visibile'] and not s['pubblicato']
    assert staff.get('/api/piano/oggi').get_json()['pubblicato'] is False

    # il piatto è finito nell'archivio con il suo tipo
    assert {'nome': 'Risotto ai funghi', 'tipo': 'primo', 'categoria': 'verdure'} in \
        [{k: p[k] for k in ('nome', 'tipo', 'categoria')} for p in db.get_piatti()]

    admin.post('/api/piano/pubblica', json={'settimana': lunedi})
    s = staff.get('/api/piano').get_json()
    assert s['visibile'] and s['pubblicato']
    giorno = next(g for g in s['giorni'] if g['data'] == oggi.isoformat())
    assert giorno['primi']['1']['piatto'] == 'Risotto ai funghi'
    assert staff.get('/api/piano/oggi').get_json()['primi'] == ['Risotto ai funghi']


def test_api_proponi_usa_l_archivio(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    for p in archivio_grande():
        db.salva_piatto_archivio(p['nome'], p['tipo'], p['categoria'])
    oggi = A.today_it()
    lunedi = (oggi - timedelta(days=oggi.weekday())).isoformat()
    r = admin.post('/api/piano/proponi', json={'settimana': lunedi}).get_json()
    assert r['aggiunti'] == 56
    assert all(len(g['primi']) == 4 and len(g['secondi']) == 4 for g in r['giorni'])
