"""Fase 6 — preparazioni con ricette salvate.

Le preparazioni che si rifanno sempre uguali si salvano come ricetta, e la
volta dopo si compilano da sole. Il lotto di ogni ingrediente lo sceglie
l'app: prima quello gia' aperto, altrimenti quello che scade prima.
"""
import importlib

import pytest


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


@pytest.fixture
def magazzino(db, app_mod):
    db.insert_listino_row('Pomodoro pelato', 'Ortofrutta', 'kg', 0, 'Conserve e prodotti in scatola', 'Cucina')
    db.insert_listino_row('Latte intero', 'Latteria', 'lt', 0, 'Latticini e uova', 'Cucina')
    c = _login(app_mod)
    c.post('/api/carico', json={'prodotto': 'Pomodoro pelato', 'fornitore': 'Ortofrutta',
                                'lotto': 'P1', 'scadenza': '2027-03-01', 'qty': 10, 'unita': 'kg'})
    c.post('/api/carico', json={'prodotto': 'Latte intero', 'fornitore': 'Latteria',
                                'lotto': 'LT1', 'scadenza': '2026-12-01', 'qty': 6, 'unita': 'lt'})
    c.post('/api/carico', json={'prodotto': 'Latte intero', 'fornitore': 'Latteria',
                                'lotto': 'LT2', 'scadenza': '2026-11-25', 'qty': 4, 'unita': 'lt'})
    return c


# ─── SCELTA AUTOMATICA DEL LOTTO ─────────────────────────────────────────────

def test_sceglie_il_lotto_che_scade_prima(magazzino):
    r = magazzino.get('/api/ingrediente-lotto?prodotto=Latte intero').get_json()
    assert r['scelto']['lotto'] == 'LT2'          # 25/11 viene prima di 01/12
    assert r['scelto']['fonte'] == 'magazzino'
    assert r['scelto']['descrizione'] == 'lotto LT2'
    assert len(r['alternative']) == 2             # si puo' sempre cambiare


def test_preferisce_il_lotto_gia_aperto(magazzino):
    """Se una confezione e' gia' aperta si finisce quella, invece di aprirne
    un'altra: vale anche se in magazzino ce n'e' una che scade prima."""
    magazzino.post('/api/in-uso', json={'prodotto': 'Latte intero', 'lotto': 'LT1', 'qty': 2})
    r = magazzino.get('/api/ingrediente-lotto?prodotto=Latte intero').get_json()
    assert r['scelto']['fonte'] == 'in_uso'
    assert r['scelto']['lotto'] == 'LT1'
    assert r['scelto']['descrizione'] == 'lotto LT1 (aperto)'


def test_prodotto_senza_lotti_non_rompe_niente(magazzino):
    r = magazzino.get('/api/ingrediente-lotto?prodotto=Non esiste').get_json()
    assert r['scelto'] is None
    assert r['alternative'] == []


# ─── RICETTE ─────────────────────────────────────────────────────────────────

def test_salva_la_ricetta_insieme_alla_preparazione(magazzino, db):
    r = magazzino.post('/api/preparazioni', json={
        'nome': 'Ragu della casa', 'reparto': 'Cucina', 'quantita': 5, 'unita': 'kg',
        'scadenza': '2026-11-19', 'note': '',
        'ingredienti': [
            {'prodotto': 'Pomodoro pelato', 'fonte': 'magazzino', 'lotto': 'P1', 'quantita': 2},
            {'prodotto': 'Latte intero', 'fonte': 'magazzino', 'lotto': 'LT2', 'quantita': 1},
        ],
        'salva_ricetta': True, 'giorni_conservazione': 3,
    }).get_json()
    assert r['success'] is True
    assert r['ricetta_salvata'] is True

    ricette = db.get_ricette()
    assert len(ricette) == 1
    assert ricette[0]['nome'] == 'Ragu della casa'
    assert ricette[0]['giorni_conservazione'] == 3
    assert ricette[0]['quantita_solita'] == 5
    assert sorted(i['prodotto'] for i in ricette[0]['ingredienti']) == ['Latte intero', 'Pomodoro pelato']


def test_la_preparazione_si_registra_anche_se_la_ricetta_esiste_gia(magazzino, db):
    db.insert_ricetta('Ragu della casa', 'Cucina', 'kg', 5, 3,
                      [{'prodotto': 'Pomodoro pelato', 'quantita': 2}])
    r = magazzino.post('/api/preparazioni', json={
        'nome': 'Ragu della casa', 'reparto': 'Cucina', 'quantita': 5, 'unita': 'kg',
        'scadenza': '', 'note': '',
        'ingredienti': [{'prodotto': 'Pomodoro pelato', 'fonte': 'magazzino', 'lotto': 'P1', 'quantita': 2}],
        'salva_ricetta': True, 'giorni_conservazione': 3,
    }).get_json()
    # la preparazione passa comunque, la ricetta non viene duplicata
    assert r['success'] is True
    assert r['ricetta_salvata'] is False
    assert len(db.get_ricette()) == 1


def test_non_si_salvano_due_ricette_con_lo_stesso_nome(magazzino):
    corpo = {'nome': 'Besciamella', 'reparto': 'Cucina', 'unita': 'lt',
             'quantita_solita': 2, 'giorni_conservazione': 2,
             'ingredienti': [{'prodotto': 'Latte intero', 'quantita': 1}]}
    assert magazzino.post('/api/ricette', json=corpo).get_json()['success'] is True
    r = magazzino.post('/api/ricette', json=corpo)
    assert r.status_code == 400
    assert 'gia' in r.get_json()['error'].lower()


def test_ricetta_serve_almeno_un_ingrediente(magazzino):
    r = magazzino.post('/api/ricette', json={'nome': 'Vuota', 'ingredienti': []})
    assert r.status_code == 400


def test_si_puo_cancellare_una_ricetta(magazzino, db):
    rid = db.insert_ricetta('Fondo bruno', 'Cucina', 'lt', 1, 5,
                            [{'prodotto': 'Latte intero', 'quantita': 1}])
    assert magazzino.delete(f'/api/ricette/{rid}').get_json()['success'] is True
    assert db.get_ricette() == []
    with db.get_conn() as conn:
        resti = db._rows(conn.execute("SELECT * FROM ricette_ingredienti WHERE ricetta_id = ?", (rid,)))
    assert resti == []   # niente ingredienti orfani


def test_le_ricette_sopravvivono_al_ricaricamento_da_sheets(magazzino, db, app_mod, monkeypatch):
    """Le ricette sono roba dell'app: non stanno sui fogli e il ricaricamento
    da Sheets non le deve cancellare."""
    db.insert_ricetta('Ragu', 'Cucina', 'kg', 5, 3,
                      [{'prodotto': 'Pomodoro pelato', 'quantita': 2}])
    monkeypatch.setattr(app_mod, 'load_registro', lambda: [])
    monkeypatch.setattr(app_mod, 'load_listino', lambda: [])
    monkeypatch.setattr(app_mod, 'load_temperatura', lambda: [])

    # prima si svuota la coda, altrimenti il ricaricamento viene rimandato
    app_mod._invia_pendenti()
    esito = app_mod._ricarica_da_sheets()

    ricette = db.get_ricette()
    assert len(ricette) == 1
    assert ricette[0]['ingredienti'][0]['prodotto'] == 'Pomodoro pelato'
