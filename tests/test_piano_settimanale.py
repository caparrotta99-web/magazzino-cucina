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


def test_riempie_da_martedi_a_venerdi_4_primi_e_4_secondi():
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    assert len(proposte) == 4 * 8
    giorni = {p['data'] for p in proposte}
    assert giorni == {(LUNEDI + timedelta(days=g)).isoformat() for g in (1, 2, 3, 4)}
    for g in (1, 2, 3, 4):
        giorno = (LUNEDI + timedelta(days=g)).isoformat()
        assert sorted(p['posizione'] for p in proposte if p['data'] == giorno and p['tipo'] == 'primo') == [1, 2, 3, 4]
        assert sorted(p['posizione'] for p in proposte if p['data'] == giorno and p['tipo'] == 'secondo') == [1, 2, 3, 4]


def test_nessun_piatto_ripetuto_in_14_giorni():
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    nomi = [p['piatto'] for p in proposte]
    assert len(nomi) == len(set(nomi))


def test_ogni_giorno_almeno_un_pesce_e_una_carne_nei_secondi():
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    for g in (1, 2, 3, 4):
        giorno = (LUNEDI + timedelta(days=g)).isoformat()
        cat = {p['categoria'] for p in proposte if p['data'] == giorno and p['tipo'] == 'secondo'}
        assert {'pesce', 'carne'} <= cat


def test_mai_due_primi_della_stessa_famiglia_nello_stesso_giorno():
    assert famiglia('Risotto ai funghi') == 'risotto' and famiglia('Gnocchi al pesto') == 'gnocchi'
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    for g in (1, 2, 3, 4):
        giorno = (LUNEDI + timedelta(days=g)).isoformat()
        fam = [famiglia(p['piatto']) for p in proposte if p['data'] == giorno and p['tipo'] == 'primo']
        assert len(fam) == len(set(fam))


def test_non_tocca_i_posti_gia_scritti_e_i_giorni_chiusi():
    martedi = (LUNEDI + timedelta(days=1)).isoformat()
    mercoledi = (LUNEDI + timedelta(days=2)).isoformat()
    gia = [{'data': martedi, 'tipo': 'primo', 'posizione': 2, 'piatto': 'Lasagne', 'categoria': ''}]
    proposte = proponi_settimana(LUNEDI, archivio_grande(), gia, giorni_chiusi=[mercoledi])
    assert not any(p['data'] == martedi and p['tipo'] == 'primo' and p['posizione'] == 2 for p in proposte)
    assert not any(p['data'] == mercoledi for p in proposte)
    assert 'Lasagne' not in [p['piatto'] for p in proposte]     # già in menu martedì: non si ripete


def test_niente_menu_lunedi_sabato_e_domenica():
    proposte = proponi_settimana(LUNEDI, archivio_grande(), [])
    assert not any(date.fromisoformat(p['data']).weekday() in (0, 5, 6) for p in proposte)


def test_piatto_fatto_di_recente_non_viene_riproposto_e_si_preferisce_il_meno_recente():
    archivio = [piatto('Lasagne'), piatto('Carbonara')]
    settimana_scorsa = [{'data': (LUNEDI - timedelta(days=5)).isoformat(), 'tipo': 'primo', 'posizione': 1,
                         'piatto': 'Lasagne', 'categoria': ''}]
    proposte = proponi_settimana(LUNEDI, archivio, settimana_scorsa)
    # martedì: la lasagna è di 6 giorni fa → no; c'è solo la carbonara
    mar = [p['piatto'] for p in proposte if p['data'] == (LUNEDI + timedelta(days=1)).isoformat()]
    assert mar == ['Carbonara']
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


def test_api_bozza_solo_admin_poi_pubblicata_per_tutti(db, monkeypatch):
    import app as A
    A.app.config['TESTING'] = True
    # la Dashboard dalle 16 passa a domani: qui guardo sempre il pranzo di oggi
    monkeypatch.setattr(A, '_giorno_pranzo_da_mostrare', lambda adesso=None: (A.today_it(), 'oggi'))
    admin = _login(A, db, 'Capo Cucina', 'admin')
    staff = _login(A, db, 'Aiuto Cuoco')
    oggi = A.today_it()
    lunedi = (oggi - timedelta(days=oggi.weekday())).isoformat()
    con_menu = oggi.weekday() in (1, 2, 3, 4)
    giorno_menu = oggi if con_menu else date.fromisoformat(lunedi) + timedelta(days=1)

    r = admin.post('/api/piano/posto', json={'data': giorno_menu.isoformat(), 'tipo': 'primo', 'posizione': 1,
                                             'piatto': 'Risotto ai funghi', 'categoria': 'verdure'}).get_json()
    assert r['success'] and r['puo_modificare']
    # lo staff non può modificare e non vede la bozza
    assert staff.post('/api/piano/posto', json={'data': giorno_menu.isoformat(), 'tipo': 'primo', 'posizione': 2,
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
    giorno = next(g for g in s['giorni'] if g['data'] == giorno_menu.isoformat())
    assert giorno['primi']['1']['piatto'] == 'Risotto ai funghi'
    assert staff.get('/api/piano/oggi').get_json()['primi'] == (['Risotto ai funghi'] if con_menu else [])


def test_api_proponi_usa_l_archivio(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    for p in archivio_grande():
        db.salva_piatto_archivio(p['nome'], p['tipo'], p['categoria'])
    oggi = A.today_it()
    lunedi = (oggi - timedelta(days=oggi.weekday())).isoformat()
    r = admin.post('/api/piano/proponi', json={'settimana': lunedi}).get_json()
    assert r['aggiunti'] == 32 and len(r['giorni']) == 4
    assert all(len(g['primi']) == 4 and len(g['secondi']) == 4 for g in r['giorni'])


def test_non_propone_piatti_sopra_il_costo_per_porzione():
    archivio = [{'nome': 'Linguine allo scoglio', 'tipo': 'primo', 'categoria': 'pesce', 'costo': 3.20},
                {'nome': 'Pasta al pomodoro', 'tipo': 'primo', 'categoria': 'verdure', 'costo': 0.60},
                {'nome': 'Gamberoni', 'tipo': 'secondo', 'categoria': 'pesce', 'costo': 5.0},
                {'nome': 'Pollo arrosto', 'tipo': 'secondo', 'categoria': 'carne', 'costo': 1.80},
                {'nome': 'Frittata', 'tipo': 'secondo', 'categoria': 'verdure', 'costo': 0}]   # costo sconosciuto: va bene
    nomi = {p['piatto'] for p in proponi_settimana(LUNEDI, archivio, [])}
    assert 'Linguine allo scoglio' not in nomi and 'Gamberoni' not in nomi
    assert {'Pasta al pomodoro', 'Pollo arrosto', 'Frittata'} <= nomi
    alt = [a['nome'] for a in alternative(LUNEDI + timedelta(days=1), 'secondo', archivio, [])]
    assert 'Gamberoni' not in alt


def test_api_costo_salvato_avviso_budget_e_costi_nascosti_allo_staff(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    staff = _login(A, db, 'Aiuto Cuoco')
    oggi = A.today_it()
    martedi = (oggi - timedelta(days=oggi.weekday()) + timedelta(days=1)).isoformat()
    admin.post('/api/piano/posto', json={'data': martedi, 'tipo': 'primo', 'posizione': 1,
                                         'piatto': 'Pasta e fagioli', 'categoria': 'verdure', 'costo': '0,90'})
    r = admin.post('/api/piano/posto', json={'data': martedi, 'tipo': 'secondo', 'posizione': 1,
                                             'piatto': 'Tagliata', 'categoria': 'carne', 'costo': '3,40'}).get_json()
    g = next(x for x in r['giorni'] if x['data'] == martedi)
    assert g['primi']['1']['costo'] == 0.9 and g['secondi']['1']['caro'] is True
    assert g['costo_max'] == 4.3 and g['sopra_budget'] is True
    # il costo resta nell'archivio
    assert next(p for p in db.get_piatti() if p['nome'] == 'Tagliata')['costo'] == 3.4
    # costo scritto male
    assert admin.post('/api/piano/posto', json={'data': martedi, 'tipo': 'primo', 'posizione': 2,
                                                'piatto': 'X', 'costo': 'tanto'}).status_code == 400

    admin.post('/api/piano/pubblica', json={'settimana': martedi})
    s = staff.get('/api/piano').get_json()
    g = next(x for x in s['giorni'] if x['data'] == martedi)
    assert 'costo_max' not in g and 'costo' not in g['secondi']['1']


def test_i_colleghi_vedono_solo_i_giorni_scritti(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    staff = _login(A, db, 'Aiuto Cuoco')
    oggi = A.today_it()
    lunedi = oggi - timedelta(days=oggi.weekday())
    mercoledi = (lunedi + timedelta(days=2)).isoformat()
    venerdi = (lunedi + timedelta(days=4)).isoformat()
    admin.post('/api/piano/posto', json={'data': mercoledi, 'tipo': 'primo', 'posizione': 1, 'piatto': 'Lasagne'})
    admin.post('/api/piano/posto', json={'data': mercoledi, 'tipo': 'secondo', 'posizione': 3, 'piatto': 'Polpette'})
    admin.post('/api/piano/chiuso', json={'data': venerdi, 'chiuso': True})
    admin.post('/api/piano/pubblica', json={'settimana': lunedi.isoformat()})

    s = staff.get('/api/piano').get_json()
    assert [g['data'] for g in s['giorni']] == [mercoledi, venerdi]      # martedì e giovedì vuoti: nascosti
    assert list(s['giorni'][0]['primi']) == ['1'] and list(s['giorni'][0]['secondi']) == ['3']
    # l'admin continua a vedere tutta la settimana
    assert len(admin.get('/api/piano').get_json()['giorni']) == 4


def test_suggerimenti_dei_colleghi_li_vede_solo_lo_chef_che_decide(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    aiuto = _login(A, db, 'Aiuto Cuoco')
    altro = _login(A, db, 'Altro Collega')
    oggi = A.today_it()
    lunedi = oggi - timedelta(days=oggi.weekday())
    giovedi = (lunedi + timedelta(days=3)).isoformat()

    r = aiuto.post('/api/piano/suggerisci', json={'data': giovedi, 'tipo': 'primo', 'piatto': 'Gnocchi alla romana',
                                                  'nota': 'abbiamo semolino'}).get_json()
    assert r['success'] and [x['piatto'] for x in r['miei_suggerimenti']] == ['Gnocchi alla romana']
    aiuto.post('/api/piano/suggerisci', json={'data': giovedi, 'tipo': 'secondo', 'piatto': 'Seppie in umido'})
    # niente lunedì, sabato, domenica; serve primo/secondo e il nome
    lun = lunedi.isoformat()
    assert aiuto.post('/api/piano/suggerisci', json={'data': lun, 'tipo': 'primo', 'piatto': 'X'}).status_code == 400
    assert aiuto.post('/api/piano/suggerisci', json={'data': giovedi, 'tipo': 'dolce', 'piatto': 'X'}).status_code == 400

    # un altro collega non vede i suggerimenti degli altri, e nel menu non c'è niente
    s = altro.get('/api/piano').get_json()
    assert s['miei_suggerimenti'] == [] and s['giorni'] == []
    # lo chef li vede nel giorno giusto
    a = admin.get('/api/piano').get_json()
    assert a['suggerimenti_totali'] == 2
    gio = next(g for g in a['giorni'] if g['data'] == giovedi)
    ids = {x['piatto']: x['id'] for x in gio['suggerimenti']}
    # i colleghi non possono decidere
    assert aiuto.post(f"/api/piano/suggerimento/{ids['Gnocchi alla romana']}/usa").status_code == 403

    # usa: va nel primo posto libero; scarta: sparisce
    a = admin.post(f"/api/piano/suggerimento/{ids['Gnocchi alla romana']}/usa").get_json()
    gio = next(g for g in a['giorni'] if g['data'] == giovedi)
    assert gio['primi']['1']['piatto'] == 'Gnocchi alla romana'
    a = admin.post(f"/api/piano/suggerimento/{ids['Seppie in umido']}/scarta").get_json()
    assert a['suggerimenti_totali'] == 0
    assert admin.post(f"/api/piano/suggerimento/{ids['Seppie in umido']}/usa").status_code == 404

    # dopo la pubblicazione tutti vedono il piatto scelto; il suggeritore non lo vede più "in attesa"
    admin.post('/api/piano/pubblica', json={'settimana': lunedi.isoformat()})
    s = aiuto.get('/api/piano').get_json()
    assert s['miei_suggerimenti'] == []
    assert s['giorni'][0]['primi']['1']['piatto'] == 'Gnocchi alla romana'


def test_usa_suggerimento_con_giorno_pieno(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    aiuto = _login(A, db, 'Aiuto Cuoco')
    oggi = A.today_it()
    martedi = (oggi - timedelta(days=oggi.weekday()) + timedelta(days=1)).isoformat()
    for n in range(1, 5):
        admin.post('/api/piano/posto', json={'data': martedi, 'tipo': 'primo', 'posizione': n, 'piatto': f'Primo {n}'})
    aiuto.post('/api/piano/suggerisci', json={'data': martedi, 'tipo': 'primo', 'piatto': 'Lasagne'})
    sid = admin.get('/api/piano').get_json()['giorni'][0]['suggerimenti'][0]['id']
    r = admin.post(f'/api/piano/suggerimento/{sid}/usa')
    assert r.status_code == 400 and 'pieni' in r.get_json()['error']


def test_dashboard_mostra_domani_dalle_16():
    import app as A
    from datetime import datetime
    mer = datetime(2026, 11, 18, 15, 59, tzinfo=A.TZ_ITALIA)
    assert A._giorno_pranzo_da_mostrare(mer) == (date(2026, 11, 18), 'oggi')
    assert A._giorno_pranzo_da_mostrare(mer.replace(hour=16, minute=0)) == (date(2026, 11, 19), 'domani')
    # venerdì sera: domani è sabato, niente menu del giorno ma nessun errore
    ven = datetime(2026, 11, 20, 18, 0, tzinfo=A.TZ_ITALIA)
    assert A._giorno_pranzo_da_mostrare(ven) == (date(2026, 11, 21), 'domani')


def test_api_domani_anche_nella_settimana_dopo(db, monkeypatch):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    # lunedì sera: domani è martedì della stessa settimana; domenica sera → lunedì (chiuso, nessun menu)
    oggi = A.today_it()
    martedi = oggi - timedelta(days=oggi.weekday()) + timedelta(days=8)
    admin.post('/api/piano/posto', json={'data': martedi.isoformat(), 'tipo': 'primo', 'posizione': 1,
                                         'piatto': 'Pasta e fagioli', 'categoria': 'verdure'})
    admin.post('/api/piano/pubblica', json={'settimana': (martedi - timedelta(days=1)).isoformat()})
    monkeypatch.setattr(A, '_giorno_pranzo_da_mostrare', lambda adesso=None: (martedi, 'domani'))
    r = admin.get('/api/piano/oggi').get_json()
    assert r['pubblicato'] and r['quando'] == 'domani' and r['primi'] == ['Pasta e fagioli']


def test_simone_decide_chi_vede_il_menu_del_giorno(db, monkeypatch):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    staff = _login(A, db, 'Aiuto Cuoco')
    oggi = A.today_it()
    martedi = oggi - timedelta(days=oggi.weekday()) + timedelta(days=1)
    admin.post('/api/piano/posto', json={'data': martedi.isoformat(), 'tipo': 'primo', 'posizione': 1,
                                         'piatto': 'Gnocchi al pesto', 'categoria': 'verdure'})
    admin.post('/api/piano/pubblica', json={'settimana': (martedi - timedelta(days=1)).isoformat()})
    monkeypatch.setattr(A, '_giorno_pranzo_da_mostrare', lambda adesso=None: (martedi, 'oggi'))
    # di base lo vede anche lo staff
    assert staff.get('/api/piano/oggi').get_json()['primi'] == ['Gnocchi al pesto']
    uid = db.get_user_by_nome('Aiuto Cuoco')['id']
    # lo staff non può cambiarsi il permesso da solo
    assert staff.post('/admin/menu-giorno-permesso', data={'user_id': uid, 'permesso': '0'}).status_code in (302, 403)
    assert staff.get('/api/piano/oggi').get_json()['primi'] == ['Gnocchi al pesto']
    # Simone lo nasconde: niente menu nella Dashboard di quel collega
    admin.post('/admin/menu-giorno-permesso', data={'user_id': uid, 'permesso': '0'})
    r = staff.get('/api/piano/oggi').get_json()
    assert r['pubblicato'] is False and r['primi'] == []
    # l'admin lo vede sempre
    assert admin.get('/api/piano/oggi').get_json()['primi'] == ['Gnocchi al pesto']
    admin.post('/admin/menu-giorno-permesso', data={'user_id': uid, 'permesso': '1'})
    assert staff.get('/api/piano/oggi').get_json()['primi'] == ['Gnocchi al pesto']
