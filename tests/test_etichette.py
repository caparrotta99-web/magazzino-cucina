"""Fase 2 — la scansione riconosce il prodotto.

Sull'etichetta il prodotto si chiama quasi sempre in un altro modo rispetto al
listino. L'app prima cerca tra gli abbinamenti gia' insegnati, poi va per
somiglianza di nome; il fornitore aiuta solo quando l'etichetta lo riporta.
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
def listino(db):
    db.insert_listino_row('Filetto', 'Carni Rossi', 'kg', 0, 'Carne', 'Cucina')
    db.insert_listino_row('Pomodoro pelato', 'Ortofrutta Blu', 'kg', 0, 'Conserve e prodotti in scatola', 'Cucina')
    db.insert_listino_row('Latte intero', 'Latteria Verde', 'lt', 0, 'Latticini e uova', 'Cucina')
    return db


# ─── NORMALIZZAZIONE ─────────────────────────────────────────────────────────

def test_normalizza_toglie_accenti_e_punteggiatura(db):
    assert db.normalizza_testo_etichetta('FILETTO di MANZO, 1Kg.') == 'filetto di manzo 1kg'
    assert db.normalizza_testo_etichetta('Caffè  Crème') == 'caffe creme'


# ─── ABBINAMENTO PER SOMIGLIANZA ─────────────────────────────────────────────

def test_riconosce_il_prodotto_col_nome_lungo_dell_etichetta(app_mod, listino):
    p, come = app_mod._abbina_prodotto('FILETTO DI MANZO IRLANDESE 1KG', '')
    assert p['prodotto'] == 'Filetto'
    assert come == 'somiglianza'


def test_non_inventa_abbinamenti_se_non_somiglia_niente(app_mod, listino):
    p, come = app_mod._abbina_prodotto('Detersivo per pavimenti', '')
    assert p is None
    assert come is None


def test_il_fornitore_aiuta_a_scegliere_ma_non_decide_da_solo(app_mod, db):
    db.insert_listino_row('Mozzarella', 'Latteria Verde', 'kg', 0, 'Latticini e uova', 'Cucina')
    db.insert_listino_row('Mozzarella', 'Caseificio Sud', 'kg', 0, 'Latticini e uova', 'Cucina')

    p, _ = app_mod._abbina_prodotto('MOZZARELLA FIORDILATTE', 'Caseificio Sud')
    assert p['fornitore'] == 'Caseificio Sud'

    # senza fornitore in etichetta si abbina comunque il prodotto
    p, _ = app_mod._abbina_prodotto('MOZZARELLA FIORDILATTE', '')
    assert p['prodotto'] == 'Mozzarella'


# ─── ABBINAMENTO INSEGNATO DALL'UTENTE ───────────────────────────────────────

def test_l_abbinamento_insegnato_vale_la_volta_dopo(app_mod, listino, db):
    etichetta = 'ORO ROSSO SELEZIONE 2500'
    assert app_mod._abbina_prodotto(etichetta, '')[0] is None   # non somiglia a niente

    c = _login(app_mod)
    r = c.post('/api/etichette/abbina', json={
        'testo': etichetta, 'prodotto': 'Pomodoro pelato', 'fornitore': 'Ortofrutta Blu'})
    assert r.get_json()['success'] is True

    p, come = app_mod._abbina_prodotto(etichetta, '')
    assert p['prodotto'] == 'Pomodoro pelato'
    assert come == 'alias'


def test_l_abbinamento_ignora_maiuscole_e_punteggiatura(app_mod, listino):
    c = _login(app_mod)
    c.post('/api/etichette/abbina', json={
        'testo': 'ORO ROSSO, SELEZIONE 2500', 'prodotto': 'Pomodoro pelato', 'fornitore': ''})
    p, come = app_mod._abbina_prodotto('oro rosso selezione 2500', '')
    assert p['prodotto'] == 'Pomodoro pelato'
    assert come == 'alias'


def test_abbinamento_a_prodotto_sparito_dal_listino_non_blocca(app_mod, listino):
    """Se il prodotto abbinato non e' piu' a listino si torna alla somiglianza
    invece di restituire un prodotto che non esiste piu'."""
    c = _login(app_mod)
    c.post('/api/etichette/abbina', json={
        'testo': 'FILETTO DI MANZO IRLANDESE', 'prodotto': 'Prodotto Sparito', 'fornitore': ''})
    p, come = app_mod._abbina_prodotto('FILETTO DI MANZO IRLANDESE', '')
    assert p['prodotto'] == 'Filetto'
    assert come == 'somiglianza'


# ─── CONVERSIONE DEL PESO ────────────────────────────────────────────────────

def test_il_peso_arriva_nell_unita_del_prodotto(app_mod):
    assert app_mod._converti_quantita(500, 'g', 'kg') == (0.5, True)
    assert app_mod._converti_quantita(1.5, 'kg', 'kg') == (1.5, False)
    assert app_mod._converti_quantita(750, 'ml', 'lt') == (0.75, True)
    # unita' che non si sanno convertire: si lascia il numero com'e'
    assert app_mod._converti_quantita(3, 'pz', 'kg') == (3.0, False)


def test_gli_abbinamenti_sopravvivono_al_ricaricamento_da_sheets(app_mod, listino, db, monkeypatch):
    """Gli abbinamenti sono un'impostazione dell'app: non stanno sui fogli e il
    ricaricamento non deve cancellarli."""
    c = _login(app_mod)
    c.post('/api/etichette/abbina', json={
        'testo': 'ORO ROSSO 2500', 'prodotto': 'Pomodoro pelato', 'fornitore': ''})

    monkeypatch.setattr(app_mod, 'load_registro', lambda: [])
    monkeypatch.setattr(app_mod, 'load_listino', lambda: [
        {'prodotto': 'Pomodoro pelato', 'fornitore': 'Ortofrutta Blu', 'unita': 'kg',
         'scorta_min': 0, 'categoria': 'Conserve e prodotti in scatola', 'reparto': 'Cucina'}])
    monkeypatch.setattr(app_mod, 'load_temperatura', lambda: [])

    assert app_mod._ricarica_da_sheets()['ricaricato'] is True
    assert db.get_alias_etichetta('ORO ROSSO 2500')['prodotto'] == 'Pomodoro pelato'
