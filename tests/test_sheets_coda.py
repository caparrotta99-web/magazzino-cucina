"""Fase 1 — conferme istantanee.

L'app salva subito nel database e risponde; a portare tutto su Google Sheets
pensa il worker di sottofondo. Questi test controllano le due cose che non
devono mai rompersi:
  * un movimento resta salvato anche se Sheets non risponde;
  * quello che finisce sul foglio e' identico a prima (stesse colonne, stessi
    valori, stesso formato), solo inviato qualche secondo dopo.
"""
import importlib

import pytest

import finto_foglio


@pytest.fixture
def app_mod(db, monkeypatch):
    monkeypatch.setenv('ADMIN_PASSWORD', 'prova-test-123')
    monkeypatch.delenv('SECRET_KEY', raising=False)
    import app as app_module
    app_module = importlib.reload(app_module)
    app_module.app.config['TESTING'] = True
    return app_module


def _login(app_mod):
    c = app_mod.app.test_client()
    c.post('/login', data={'nome': 'Admin', 'password': 'prova-test-123'})
    return c


def _prodotto(db, nome='Macinato', fornitore='Fornitore Test', unita='kg'):
    db.insert_listino_row(nome, fornitore, unita, 0, 'Carne', 'Cucina')


# ─── IL MOVIMENTO NON SI PERDE ───────────────────────────────────────────────

def test_carico_resta_salvato_con_sheets_bloccato(app_mod, db):
    """Sheets e' irraggiungibile (conftest lo blocca): il carico deve comunque
    essere salvato e la risposta arrivare subito, senza errore."""
    _prodotto(db)
    c = _login(app_mod)
    r = c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })
    assert r.status_code == 200
    assert r.get_json()['success'] is True

    righe = db.get_unsynced_registro()
    assert len(righe) == 1
    assert righe[0]['prodotto'] == 'Macinato'
    assert righe[0]['synced'] == 0


def test_scarico_non_fallisce_se_sheets_non_risponde(app_mod, db):
    """Prima lo scarico restituiva 'Errore Google Sheets' e non registrava
    niente: ora la modifica va in coda e lo scarico riesce comunque."""
    _prodotto(db)
    c = _login(app_mod)
    c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })
    r = c.post('/api/in-uso', json={'prodotto': 'Macinato', 'lotto': 'L1', 'qty': 3})
    in_uso_id = r.get_json()['id']

    r = c.post('/api/scarico', json={'id': in_uso_id, 'qty': 3})
    assert r.status_code == 200
    assert r.get_json()['success'] is True
    assert len(db.get_coda_sheets()) == 1
    assert db.get_coda_sheets()[0]['azione'] == 'aggiorna_tipo'


# ─── IL RICARICAMENTO NON CANCELLA QUELLO CHE NON E' PARTITO ─────────────────

def test_ricaricamento_rimandato_se_ci_sono_pendenti(app_mod, db, monkeypatch):
    """replace_registro svuota la tabella: se qualcosa non e' ancora arrivato
    su Sheets, il ricaricamento va rimandato o quei movimenti sparirebbero."""
    _prodotto(db)
    c = _login(app_mod)
    c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })
    assert db.conta_pendenti_sheets() > 0

    # Se qualcuno provasse a ricaricare, il foglio risponderebbe vuoto.
    monkeypatch.setattr(app_mod, 'load_registro', lambda: [])
    monkeypatch.setattr(app_mod, 'load_listino', lambda: [])
    monkeypatch.setattr(app_mod, 'load_temperatura', lambda: [])

    esito = app_mod._ricarica_da_sheets()
    assert esito['ricaricato'] is False
    assert esito['pendenti'] > 0
    # il movimento e' ancora li'
    assert len(db.get_unsynced_registro()) == 1


def test_ricaricamento_non_cancella_le_impostazioni_dell_app(app_mod, db, monkeypatch):
    """Ricette e abbinamenti etichetta->prodotto vivono solo nel database
    dell'app: il ricaricamento da Sheets non deve toccarli."""
    db.set_impostazione('zona_rifiuti', 'centro_ristoranti')
    monkeypatch.setattr(app_mod, 'load_registro', lambda: [])
    monkeypatch.setattr(app_mod, 'load_listino', lambda: [])
    monkeypatch.setattr(app_mod, 'load_temperatura', lambda: [])

    esito = app_mod._ricarica_da_sheets()
    assert esito['ricaricato'] is True
    assert db.get_impostazione('zona_rifiuti') == 'centro_ristoranti'


# ─── SUL FOGLIO ARRIVA ESATTAMENTE QUELLO DI PRIMA ───────────────────────────

def test_il_carico_arriva_sul_foglio_identico(app_mod, db, monkeypatch):
    import sheets
    foglio = finto_foglio.installa(monkeypatch, sheets)

    _prodotto(db)
    c = _login(app_mod)
    c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })
    # finche' il worker non gira, sul foglio non c'e' niente
    assert foglio.worksheet('REGISTRO').righe_dati == []

    app_mod._invia_pendenti()

    righe = foglio.worksheet('REGISTRO').righe_dati
    assert len(righe) == 1
    riga = dict(zip(finto_foglio.HEADER_REGISTRO, righe[0]))
    assert riga['Prodotto'] == 'Macinato'
    assert riga['Fornitore'] == 'Fornitore Test'
    assert riga['Lotto'] == 'L1'
    assert riga['Carico'] == 5
    assert riga['Scarico'] == ''        # carico e scarico si escludono, come prima
    assert riga['Unità'] == 'kg'
    assert riga['Operatore'] == 'Admin'
    assert riga['Tipo movimento'] == 'CARICO'
    assert riga['Scadenza'] == '31/12/2026'   # formato del foglio, non ISO
    assert db.conta_pendenti_sheets() == 0


def test_scarico_parziale_sul_foglio_come_prima(app_mod, db, monkeypatch):
    """Lo scarico parziale deve lasciare sul foglio le stesse due righe di
    prima: quella originale aggiornata a SCARICO con la quantita' scaricata,
    piu' una riga nuova IN USO con il residuo."""
    import sheets
    foglio = finto_foglio.installa(monkeypatch, sheets)

    _prodotto(db)
    c = _login(app_mod)
    c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })
    r = c.post('/api/in-uso', json={'prodotto': 'Macinato', 'lotto': 'L1', 'qty': 4})
    in_uso_id = r.get_json()['id']
    app_mod._invia_pendenti()   # carico + riga IN USO arrivano sul foglio

    c.post('/api/scarico', json={'id': in_uso_id, 'qty': 3})
    app_mod._invia_pendenti()

    righe = [dict(zip(finto_foglio.HEADER_REGISTRO, r))
             for r in foglio.worksheet('REGISTRO').righe_dati]
    assert len(righe) == 3   # carico, riga finalizzata, residuo

    finalizzata = righe[1]
    assert finalizzata['Tipo movimento'] == 'SCARICO'
    assert finalizzata['Scarico'] == 3

    residuo = righe[2]
    assert residuo['Tipo movimento'] == 'IN USO'
    assert residuo['Scarico'] == 1
    assert residuo['Carico'] == ''
    assert residuo['Lotto'] == 'L1'
    assert db.conta_pendenti_sheets() == 0


def test_allineamento_segnala_le_differenze(app_mod, db, monkeypatch):
    """Il controllo deve accorgersi se la giacenza del database e quella del
    foglio non coincidono piu'."""
    _prodotto(db)
    c = _login(app_mod)
    c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })

    # foglio d'accordo col database
    uguale = [{'prodotto': 'Macinato', 'lotto': 'L1', 'carico': 5, 'scarico': 0}]
    assert app_mod._controlla_allineamento(uguale) == []

    # foglio che racconta un'altra storia
    diverso = [{'prodotto': 'Macinato', 'lotto': 'L1', 'carico': 2, 'scarico': 0}]
    differenze = app_mod._controlla_allineamento(diverso)
    assert len(differenze) == 1
    assert differenze[0]['database'] == 5
    assert differenze[0]['foglio'] == 2


def test_stato_sheets_per_admin(app_mod, db):
    _prodotto(db)
    c = _login(app_mod)
    c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })
    r = c.get('/api/stato-sheets').get_json()
    assert r['stato'] == 'in_attesa'
    assert 'in attesa di invio' in r['testo']


# ─── BUTTATO (FASE 5) ────────────────────────────────────────────────────────

def test_buttato_arriva_sul_foglio_come_BUTTATO(app_mod, db, monkeypatch):
    """Il prodotto buttato resta un movimento del registro HACCP: sul foglio
    la colonna Tipo movimento deve leggersi 'BUTTATO'."""
    import sheets
    foglio = finto_foglio.installa(monkeypatch, sheets)

    _prodotto(db)
    c = _login(app_mod)
    c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })
    r = c.post('/api/in-uso', json={'prodotto': 'Macinato', 'lotto': 'L1', 'qty': 2})
    in_uso_id = r.get_json()['id']
    app_mod._invia_pendenti()

    r = c.post('/api/scarico', json={'id': in_uso_id, 'qty': 2, 'tipo': 'SCARTO'})
    assert r.get_json()['tipo'] == 'SCARTO'
    app_mod._invia_pendenti()

    righe = [dict(zip(finto_foglio.HEADER_REGISTRO, x))
             for x in foglio.worksheet('REGISTRO').righe_dati]
    buttata = righe[1]
    assert buttata['Tipo movimento'] == 'BUTTATO'
    assert buttata['Scarico'] == 2
    assert db.conta_pendenti_sheets() == 0


def test_buttato_torna_SCARTO_quando_si_ricarica_dai_fogli(app_mod, db, monkeypatch):
    """Rileggendo il foglio, 'BUTTATO' deve tornare a essere SCARTO dentro
    l'app, altrimenti il movimento si perderebbe a ogni ricaricamento."""
    import sheets
    assert sheets._normalizza_tipo('BUTTATO') == 'SCARTO'

    righe_foglio = [{
        'data': '2026-11-16', 'fornitore': 'Fornitore Test', 'prodotto': 'Macinato',
        'lotto': 'L1', 'scadenza': '2026-12-31', 'carico': 0, 'scarico': 2,
        'unita': 'kg', 'etichetta': '', 'movimento_id': 'X1', 'rimanenza': 3,
        'operatore': 'Admin', 'reparto': 'Cucina', 'tipo': 'SCARTO',
    }]
    monkeypatch.setattr(app_mod, 'load_registro', lambda: righe_foglio)
    monkeypatch.setattr(app_mod, 'load_listino', lambda: [])
    monkeypatch.setattr(app_mod, 'load_temperatura', lambda: [])

    assert app_mod._ricarica_da_sheets()['ricaricato'] is True
    with db.get_conn() as conn:
        tipi = [r['tipo'] for r in db._rows(conn.execute("SELECT tipo FROM registro"))]
    assert tipi == ['SCARTO']


def test_scarico_parziale_di_un_buttato_lascia_il_residuo_in_uso(app_mod, db, monkeypatch):
    import sheets
    foglio = finto_foglio.installa(monkeypatch, sheets)

    _prodotto(db)
    c = _login(app_mod)
    c.post('/api/carico', json={
        'prodotto': 'Macinato', 'fornitore': 'Fornitore Test', 'lotto': 'L1',
        'scadenza': '2026-12-31', 'qty': 5, 'unita': 'kg',
    })
    r = c.post('/api/in-uso', json={'prodotto': 'Macinato', 'lotto': 'L1', 'qty': 4})
    in_uso_id = r.get_json()['id']
    app_mod._invia_pendenti()

    c.post('/api/scarico', json={'id': in_uso_id, 'qty': 1, 'tipo': 'SCARTO'})
    app_mod._invia_pendenti()

    righe = [dict(zip(finto_foglio.HEADER_REGISTRO, x))
             for x in foglio.worksheet('REGISTRO').righe_dati]
    assert righe[1]['Tipo movimento'] == 'BUTTATO'
    assert righe[1]['Scarico'] == 1
    assert righe[2]['Tipo movimento'] == 'IN USO'   # il resto e' ancora aperto
    assert righe[2]['Scarico'] == 3
