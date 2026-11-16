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


# ─── SCANSIONE A PRODOTTO GIA' SCELTO ────────────────────────────────────────
# Si arriva qui dal riquadro "Comprati, da registrare" o quando si e' cercato
# il prodotto prima di tirare fuori la confezione: dell'etichetta servono solo
# lotto, scadenza e peso.

class _FintaRisposta:
    def __init__(self, testo):
        from types import SimpleNamespace as NS
        self.content = [NS(type='text', text=testo)]


def _finto_anthropic(monkeypatch, testo_risposta):
    """Sostituisce il client Anthropic con uno che risponde sempre uguale, e
    restituisce la lista dei corpi inviati, per controllarli."""
    import anthropic
    inviati = []

    class FintoMessages:
        def create(self, **kwargs):
            inviati.append(kwargs)
            return _FintaRisposta(testo_risposta)

    class FintoClient:
        def __init__(self, **kwargs):
            self.messages = FintoMessages()

    monkeypatch.setattr(anthropic, 'Anthropic', FintoClient)
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'prova')
    return inviati


ETICHETTA = ('{"lotto": "L99", "scadenza": "31/12/2026", "nome": "PELATI ORO ROSSO", '
             '"fornitore": null, "quantita": 2500, "unita": "g"}')


def test_scansione_a_prodotto_scelto_converte_nella_sua_unita(app_mod, listino, monkeypatch):
    _finto_anthropic(monkeypatch, ETICHETTA)
    c = _login(app_mod)
    r = c.post('/api/scan-label', json={
        'image': 'xxx', 'unita_prodotto': 'kg',
    }).get_json()

    assert r['success'] is True
    assert r['lotto'] == 'L99'
    assert r['scadenza'] == '31/12/2026'
    assert r['quantita'] == 2.5          # 2500 g diventano 2.5 kg
    assert r['convertita'] is True
    assert r['unita'] == 'kg'
    # il prodotto era gia' scelto: l'app non deve proporne un altro
    assert r['prodotto'] is None
    assert r['riconosciuto_come'] is None


# Etichetta che il listino riesce a riconoscere da sola.
ETICHETTA_RICONOSCIBILE = (
    '{"lotto": "L99", "scadenza": "31/12/2026", "nome": "POMODORO PELATO IN SUCCO", '
    '"fornitore": null, "quantita": 2500, "unita": "g"}')


def test_senza_unita_prodotto_la_scansione_abbina_come_prima(app_mod, listino, monkeypatch):
    """Il comportamento della scansione dalla schermata di ricerca non cambia."""
    _finto_anthropic(monkeypatch, ETICHETTA_RICONOSCIBILE)
    c = _login(app_mod)
    r = c.post('/api/scan-label', json={'image': 'xxx'}).get_json()

    assert r['prodotto'] == 'Pomodoro pelato'
    assert r['riconosciuto_come'] == 'somiglianza'
    assert r['quantita'] == 2.5          # convertito nell'unita' del listino


def test_nome_commerciale_senza_parole_in_comune_non_si_abbina(app_mod, listino, monkeypatch):
    """"PELATI ORO ROSSO" non ha nessuna parola in comune con "Pomodoro
    pelato": l'app non tira a indovinare, restituisce il nome letto e lascia
    scegliere all'utente. Da quella scelta impara l'abbinamento."""
    _finto_anthropic(monkeypatch, ETICHETTA)
    c = _login(app_mod)
    r = c.post('/api/scan-label', json={'image': 'xxx'}).get_json()

    assert r['prodotto'] is None
    assert r['nome_letto'] == 'PELATI ORO ROSSO'

    # l'utente sceglie, e la volta dopo si riconosce da solo
    c.post('/api/etichette/abbina', json={
        'testo': 'PELATI ORO ROSSO', 'prodotto': 'Pomodoro pelato', 'fornitore': ''})
    r2 = c.post('/api/scan-label', json={'image': 'xxx'}).get_json()
    assert r2['prodotto'] == 'Pomodoro pelato'
    assert r2['riconosciuto_come'] == 'alias'


def test_scansione_a_prodotto_scelto_con_unita_strana(app_mod, listino, monkeypatch):
    """Unita' che non si sanno convertire: il numero resta com'e' letto,
    senza inventare conversioni."""
    _finto_anthropic(monkeypatch, ETICHETTA.replace('"unita": "g"', '"unita": "pz"'))
    c = _login(app_mod)
    r = c.post('/api/scan-label', json={'image': 'xxx', 'unita_prodotto': 'kg'}).get_json()
    assert r['quantita'] == 2500
    assert r['convertita'] is False
