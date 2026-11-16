"""Test sull'app Flask: login, sicurezza e testi dei promemoria."""
import importlib
from datetime import timedelta

import pytest

from conftest import carico
from timezone_utils import today_it


@pytest.fixture
def app_mod(db, monkeypatch):
    monkeypatch.setenv('ADMIN_PASSWORD', 'prova-test-123')
    monkeypatch.delenv('SECRET_KEY', raising=False)
    import app as app_module
    app_module = importlib.reload(app_module)
    app_module.app.config['TESTING'] = True
    return app_module


def test_admin_creato_con_password_da_variabile(app_mod):
    c = app_mod.app.test_client()
    r = c.post('/login', data={'nome': 'Admin', 'password': 'prova-test-123'})
    assert r.status_code in (302, 303)
    assert c.get('/api/alerts').status_code == 200


def test_admin123_non_funziona_piu(app_mod):
    c = app_mod.app.test_client()
    c.post('/login', data={'nome': 'Admin', 'password': 'admin123'})
    assert c.get('/api/alerts').status_code != 200


def test_modello_scansioni_aggiornato(app_mod):
    assert app_mod.AI_MODEL != 'claude-sonnet-4-6'


def test_promemoria_temperature(app_mod, db):
    assert app_mod.testo_promemoria_temperature() is None
    aid = db.create_apparecchio('Frigo primi', 'frigo', 0, 4)
    titolo, testo = app_mod.testo_promemoria_temperature()
    assert 'Frigo primi' in testo
    db.registra_controllo_apparecchio(aid)
    assert app_mod.testo_promemoria_temperature() is None


def test_promemoria_scadenze(app_mod, db):
    oggi = today_it()
    assert app_mod.testo_promemoria_scadenze(oggi) is None
    carico(db, 'Mozzarella', 'M1', 2, (oggi + timedelta(days=1)).isoformat())
    carico(db, 'Ricotta', 'R1', 1, (oggi - timedelta(days=2)).isoformat())
    carico(db, 'Parmigiano', 'P1', 1, (oggi + timedelta(days=30)).isoformat())
    db.insert_preparazione('Ragù', 'Primi', oggi.isoformat(), oggi.isoformat(), 5, 'kg', 1, '')
    _, testo = app_mod.testo_promemoria_scadenze(oggi)
    assert 'Scadono domani: Mozzarella' in testo
    assert 'Scaduti ancora in magazzino: Ricotta' in testo
    assert 'Preparazioni in scadenza oggi: Ragù' in testo
    assert 'Parmigiano' not in testo


def test_pagina_principale_e_file_statici(app_mod):
    import re
    c = app_mod.app.test_client()
    c.post('/login', data={'nome': 'Admin', 'password': 'prova-test-123'})
    html = c.get('/').get_data(as_text=True)
    assert 'const CURRENT_USER_ID' in html
    urls = re.findall(r'(?:href|src)="(/static/(?:css|js)/app\.(?:css|js)\?v=\w+)"', html)
    assert len(urls) == 2
    for u in urls:
        r = c.get(u)
        assert r.status_code == 200 and len(r.data) > 50_000


# ─── PREFERENZE NOTIFICHE ────────────────────────────────────────────────────

def _login(app_mod):
    c = app_mod.app.test_client()
    c.post('/login', data={'nome': 'Admin', 'password': 'prova-test-123'})
    return c


def test_notifiche_tutte_attive_di_default(app_mod):
    r = _login(app_mod).get('/api/notifiche-preferenze').get_json()
    assert r['notifiche'] == {'scadenze': True, 'temperature': True, 'rifiuti': True, 'chat': True}


def test_spegnere_una_notifica(app_mod):
    c = _login(app_mod)
    r = c.post('/api/notifiche-preferenze', json={'tipo': 'rifiuti', 'attiva': False}).get_json()
    assert r['notifiche']['rifiuti'] is False
    assert r['notifiche']['scadenze'] is True
    assert c.post('/api/notifiche-preferenze', json={'tipo': 'boh', 'attiva': False}).status_code == 400


def test_profilo_mostra_gli_interruttori(app_mod):
    html = _login(app_mod).get('/profile').get_data(as_text=True)
    for tipo in ('scadenze', 'temperature', 'rifiuti', 'chat'):
        assert f'data-notifica="{tipo}"' in html


def test_notifica_arriva_solo_a_chi_la_vuole(app_mod, db, monkeypatch):
    from werkzeug.security import generate_password_hash
    db.create_user('Aiuto', 'aiuto', generate_password_hash('xxxxxx'), 'cucina')
    aiuto = db.get_user_by_login('aiuto')['id']
    admin = db.get_user_by_login('admin')['id']
    db.update_user_notifica(aiuto, 'scadenze', False)

    inviati = {}
    monkeypatch.setattr(app_mod, 'push_to_users',
                        lambda ids, *a, **k: inviati.setdefault('ids', list(ids)) and (1, 0))
    app_mod.push_per_tipo('scadenze', 'Scadenze', 'test')
    assert inviati['ids'] == [admin]


# ─── ZONA RIFIUTI PER UTENTE ─────────────────────────────────────────────────

def test_ognuno_ha_la_sua_zona_rifiuti(app_mod, db):
    from werkzeug.security import generate_password_hash
    db.set_impostazione('zona_rifiuti', 'centro_ristoranti')
    db.create_user('Aiuto', 'aiuto', generate_password_hash('aiuto123'), 'cucina')
    aiuto = app_mod.app.test_client()
    aiuto.post('/login', data={'nome': 'Aiuto', 'password': 'aiuto123'})
    admin = _login(app_mod)

    # senza scelta: zona del ristorante
    assert aiuto.get('/api/rifiuti/oggi').get_json()['zona'] == 'centro_ristoranti'
    # l'aiuto sceglie la sua, l'admin non cambia
    assert aiuto.post('/api/rifiuti/zona', json={'zona': 'citta'}).get_json()['success']
    assert aiuto.get('/api/rifiuti/oggi').get_json()['zona'] == 'citta'
    assert admin.get('/api/rifiuti/oggi').get_json()['zona'] == 'centro_ristoranti'
    assert db.get_impostazione('zona_rifiuti') == 'centro_ristoranti'


def test_promemoria_rifiuti_per_zona(app_mod, db, monkeypatch):
    from werkzeug.security import generate_password_hash
    db.create_user('Aiuto', 'aiuto', generate_password_hash('aiuto123'), 'cucina')
    aiuto = db.get_user_by_login('aiuto')['id']
    admin = db.get_user_by_login('admin')['id']
    db.update_user_rifiuti_zona(admin, 'centro_ristoranti')
    db.update_user_rifiuti_zona(aiuto, 'citta')
    monkeypatch.setattr(app_mod, '_calcola_rifiuti_oggi',
                        lambda zona: (zona, {'x': 1}, ['carta'] if zona == 'citta' else ['vetro']))
    inviati = []
    monkeypatch.setattr(app_mod, 'push_to_users',
                        lambda ids, titolo, testo, **k: inviati.append((list(ids), testo)))
    app_mod._job_promemoria_rifiuti()
    per_utente = {ids[0]: testo for ids, testo in inviati}
    assert 'Carta' in per_utente[aiuto] and 'Vetro' not in per_utente[aiuto]
    assert 'Vetro' in per_utente[admin]
