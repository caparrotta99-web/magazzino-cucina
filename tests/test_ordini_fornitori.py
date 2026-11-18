"""Ordini ai fornitori dalla lista spesa: contatti WhatsApp/sito salvati in Admin."""


def _login(A, db, nome, ruolo='staff'):
    from werkzeug.security import generate_password_hash
    db.create_user(nome, nome.lower().replace(' ', ''), generate_password_hash('pw123456', method='pbkdf2'),
                   'cucina', role=ruolo)
    c = A.app.test_client()
    c.post('/login', data={'nome': nome, 'password': 'pw123456'})
    return c


def test_numero_whatsapp_normalizzato(db):
    assert db.normalizza_whatsapp('333 123 4567') == '393331234567'
    assert db.normalizza_whatsapp('+39 333-1234567') == '393331234567'
    assert db.normalizza_whatsapp('0039 333 1234567') == '393331234567'
    assert db.normalizza_whatsapp('0381 123456') == '390381123456'   # fisso con WhatsApp Business
    assert db.normalizza_whatsapp('') == ''


def test_contatti_salvati_e_tolti(db):
    db.set_contatto_fornitore('GLOBAL', whatsapp='333 1234567')
    db.set_contatto_fornitore('METRO', sito='www.esempio.it/ordini')
    c = db.get_contatti_fornitori()
    assert c['GLOBAL'] == {'whatsapp': '393331234567', 'sito': ''}
    assert c['METRO']['sito'] == 'https://www.esempio.it/ordini'
    # campi vuoti = il fornitore non ha più il tasto
    db.set_contatto_fornitore('GLOBAL', whatsapp='', sito='')
    assert 'GLOBAL' not in db.get_contatti_fornitori()


def test_solo_admin_vede_e_cambia_i_contatti(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    staff = _login(A, db, 'Aiuto Cuoco')
    staff.post('/admin/fornitore-contatto', data={'nome': 'DAC', 'whatsapp': '3330000000'})
    assert 'DAC' not in db.get_contatti_fornitori()

    admin.post('/admin/fornitore-contatto', data={'nome': 'DAC', 'whatsapp': '333 0000000'})
    r = admin.get('/api/fornitori-contatti').get_json()
    assert r['contatti']['DAC']['whatsapp'] == '393330000000'
    # i colleghi non vedono il tasto "Ordina" (niente ordini doppi)
    assert staff.get('/api/fornitori-contatti').status_code in (302, 403)
    # la pagina Admin elenca anche METRO, che non ha prodotti nel listino
    pagina = admin.get('/admin').get_data(as_text=True)
    assert 'Fornitori: come ordinare' in pagina and '>METRO<' in pagina and '393330000000' in pagina


def test_prenotazioni_solo_per_admin(db):
    import app as A
    A.app.config['TESTING'] = True
    admin = _login(A, db, 'Capo Cucina', 'admin')
    staff = _login(A, db, 'Aiuto Sala')
    assert admin.get('/api/prenotazioni').status_code == 200
    assert staff.get('/api/prenotazioni').status_code == 403
    assert staff.get('/api/prenotazioni/calendario').status_code == 403
    assert staff.post('/api/prenotazioni', json={}).status_code == 403
    # niente voce "Prenotazioni" nel menu dei colleghi
    assert 'data-page="prenotazioni"' in admin.get('/').get_data(as_text=True)
    assert 'data-page="prenotazioni"' not in staff.get('/').get_data(as_text=True)
