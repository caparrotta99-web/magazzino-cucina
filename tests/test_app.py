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


def test_secret_key_non_e_piu_quella_fissa(app_mod):
    assert app_mod.app.secret_key != 'haccp-brigade-2024'


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


def test_notifiche_di_default(app_mod):
    # scadenze e temperature partono spente, rifiuti e chat accese
    r = _login(app_mod).get('/api/notifiche-preferenze').get_json()
    assert r['notifiche'] == {'scadenze': False, 'temperature': False, 'rifiuti': True, 'chat': True}


def test_spegnere_una_notifica(app_mod):
    c = _login(app_mod)
    r = c.post('/api/notifiche-preferenze', json={'tipo': 'rifiuti', 'attiva': False}).get_json()
    assert r['notifiche']['rifiuti'] is False
    assert r['notifiche']['chat'] is True
    r = c.post('/api/notifiche-preferenze', json={'tipo': 'scadenze', 'attiva': True}).get_json()
    assert r['notifiche']['scadenze'] is True
    assert c.post('/api/notifiche-preferenze', json={'tipo': 'boh', 'attiva': False}).status_code == 400


def test_profilo_mostra_gli_interruttori(app_mod):
    html = _login(app_mod).get('/profile').get_data(as_text=True)
    for tipo in ('scadenze', 'temperature', 'rifiuti', 'chat'):
        assert f'data-notifica="{tipo}"' in html


def test_notifica_arriva_solo_a_chi_la_vuole(app_mod, db, monkeypatch):
    from werkzeug.security import generate_password_hash
    db.create_user('Aiuto', 'aiuto', generate_password_hash('xxxxxx', method='pbkdf2:sha256'), 'cucina')
    aiuto = db.get_user_by_login('aiuto')['id']
    admin = db.get_user_by_login('admin')['id']
    db.update_user_notifica(admin, 'scadenze', True)   # l'admin l'ha accesa dal Profilo

    inviati = {}
    monkeypatch.setattr(app_mod, 'push_to_users',
                        lambda ids, *a, **k: inviati.setdefault('ids', list(ids)) and (1, 0))
    app_mod.push_per_tipo('scadenze', 'Scadenze', 'test')
    assert inviati['ids'] == [admin]


# ─── ZONA RIFIUTI PER UTENTE ─────────────────────────────────────────────────

def test_ognuno_ha_la_sua_zona_rifiuti(app_mod, db):
    from werkzeug.security import generate_password_hash
    db.set_impostazione('zona_rifiuti', 'centro_ristoranti')
    db.create_user('Aiuto', 'aiuto', generate_password_hash('aiuto123', method='pbkdf2:sha256'), 'cucina')
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
    db.create_user('Aiuto', 'aiuto', generate_password_hash('aiuto123', method='pbkdf2:sha256'), 'cucina')
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


# ─── PASSWORD DIMENTICATA (notifica all'admin, niente codici) ────────────────

def test_password_dimenticata_flusso_completo(app_mod, db, monkeypatch):
    from werkzeug.security import generate_password_hash
    db.create_user('Mario Rossi', 'mario', generate_password_hash('vecchia1', method='pbkdf2:sha256'), 'sala')
    admin_id = db.get_user_by_login('admin')['id']
    notifiche = []
    monkeypatch.setattr(app_mod, 'push_to_users',
                        lambda ids, titolo, testo, **k: notifiche.append((list(ids), testo)) or (1, 0))

    mario = app_mod.app.test_client()
    mario.post('/forgot-password', data={'nome': 'Mario Rossi'})
    # all'admin arriva la notifica
    assert notifiche == [([admin_id], 'Mario Rossi non ricorda la password. Tocca per farlo entrare.')]
    # Mario vede la pagina di attesa, non può ancora cambiare password
    assert 'Richiesta inviata' in mario.get('/forgot-password').get_data(as_text=True)
    assert mario.get('/api/reset-stato').get_json()['stato'] == 'attesa'
    assert mario.get('/reset-password').status_code == 302

    # l'admin tocca "Fallo entrare"
    admin = _login(app_mod)
    t = db.get_active_reset_tokens()[0]
    assert 'Fallo entrare' in admin.get('/admin').get_data(as_text=True)
    admin.post('/admin/reset-conferma', data={'token_id': t['id']})

    # Mario sceglie la nuova password ed è subito dentro
    assert mario.get('/api/reset-stato').get_json()['stato'] == 'confermata'
    r = mario.post('/reset-password', data={'password': 'nuova123', 'password2': 'nuova123'})
    assert r.status_code == 302
    assert mario.get('/api/alerts').status_code == 200
    # la richiesta è chiusa e la nuova password funziona
    assert db.get_active_reset_tokens() == []
    altro = app_mod.app.test_client()
    altro.post('/login', data={'nome': 'Mario Rossi', 'password': 'nuova123'})
    assert altro.get('/api/alerts').status_code == 200


def test_conferma_admin_vale_solo_sul_telefono_che_ha_chiesto(app_mod, db, monkeypatch):
    from werkzeug.security import generate_password_hash
    db.create_user('Mario Rossi', 'mario', generate_password_hash('vecchia1', method='pbkdf2:sha256'), 'sala')
    monkeypatch.setattr(app_mod, 'push_to_users', lambda *a, **k: (1, 0))
    mario = app_mod.app.test_client()
    mario.post('/forgot-password', data={'nome': 'Mario Rossi'})
    _login(app_mod).post('/admin/reset-conferma', data={'token_id': db.get_active_reset_tokens()[0]['id']})
    estraneo = app_mod.app.test_client()
    assert estraneo.get('/reset-password').status_code == 302
    estraneo.post('/reset-password', data={'password': 'hackerata', 'password2': 'hackerata'})
    assert estraneo.get('/api/alerts').status_code != 200


def test_niente_raffica_di_notifiche(app_mod, db, monkeypatch):
    from werkzeug.security import generate_password_hash
    db.create_user('Mario Rossi', 'mario', generate_password_hash('vecchia1', method='pbkdf2:sha256'), 'sala')
    notifiche = []
    monkeypatch.setattr(app_mod, 'push_to_users', lambda *a, **k: notifiche.append(1) or (1, 0))
    for _ in range(3):
        app_mod.app.test_client().post('/forgot-password', data={'nome': 'Mario Rossi'})
    assert len(notifiche) == 1


def test_risposta_ai_con_blocco_di_ragionamento(app_mod):
    from types import SimpleNamespace as NS
    resp = NS(content=[NS(type='thinking', thinking='...'),
                       NS(type='text', text='{"lotto": "L1", "scadenza": "01/12/2026"}')])
    assert app_mod._testo_risposta(resp) == '{"lotto": "L1", "scadenza": "01/12/2026"}'


def test_carico_senza_lotto_e_senza_scadenza(app_mod, db):
    db.insert_listino_row('Zucchine', 'Ortofrutta Rossi', 'kg', 0, 'Verdura')
    c = _login(app_mod)
    r = c.post('/api/carico', json={'prodotto': 'Zucchine', 'fornitore': 'Ortofrutta Rossi',
                                    'lotto': 'SENZA LOTTO 16/11/2026', 'scadenza': '',
                                    'qty': 3.5, 'unita': 'kg'}).get_json()
    assert r['success']
    lotti = db.get_lotti_attivi('Zucchine')
    assert lotti[0]['lotto'] == 'SENZA LOTTO 16/11/2026' and lotti[0]['scadenza'] == ''


# ─── COMPRATI DALLA LISTA SPESA ──────────────────────────────────────────────

def test_spuntato_dalla_lista_finisce_tra_i_comprati(app_mod, db):
    c = _login(app_mod)
    a = c.post('/api/lista-spesa', json={'prodotto': 'Mele', 'fornitore': 'DOCKS', 'quantita': 2, 'unita': 'kg'}).get_json()['id']
    b = c.post('/api/lista-spesa', json={'prodotto': 'Pere', 'fornitore': 'DOCKS'}).get_json()['id']
    c.delete(f'/api/lista-spesa/{a}?comprato=1')   # messo nel carrello
    c.delete(f'/api/lista-spesa/{b}')              # cestino: aggiunto per errore
    comprati = c.get('/api/spesa-comprati').get_json()['comprati']
    assert [x['prodotto'] for x in comprati] == ['Mele']
    # registrato come carico: sparisce dal riquadro
    c.post(f"/api/spesa-comprati/{comprati[0]['id']}/registrato")
    assert c.get('/api/spesa-comprati').get_json()['comprati'] == []


def test_non_trovato_torna_nella_lista(app_mod, db):
    c = _login(app_mod)
    a = c.post('/api/lista-spesa', json={'prodotto': 'Burrata', 'fornitore': 'DOCKS', 'quantita': 3, 'unita': 'pz', 'reparto': 'Cucina'}).get_json()['id']
    c.delete(f'/api/lista-spesa/{a}?comprato=1')
    cid = c.get('/api/spesa-comprati').get_json()['comprati'][0]['id']
    assert c.post(f'/api/spesa-comprati/{cid}/non-trovato').get_json()['success']
    lista = c.get('/api/lista-spesa').get_json()['items']
    assert [(i['prodotto'], i['quantita'], i['unita'], i['reparto']) for i in lista] == [('Burrata', 3.0, 'pz', 'Cucina')]
    assert c.get('/api/spesa-comprati').get_json()['comprati'] == []


def test_comprati_vecchi_spariscono_da_soli(app_mod, db):
    from datetime import timedelta
    from timezone_utils import today_it
    db.registra_comprato({'prodotto': 'Mele', 'fornitore': 'DOCKS'}, 'Simone')
    assert len(db.get_comprati_da_registrare()) == 1
    assert db.get_comprati_da_registrare(today_it() + timedelta(days=5)) == []


# ─── RIORDINO MAGAZZINO ──────────────────────────────────────────────────────

def test_riordino_chiude_lotti_e_aperti_senza_cancellare(app_mod, db):
    from timezone_utils import today_it
    carico(db, 'Ricotta', 'R1', 2, '2026-07-01')
    carico(db, 'Tonno', 'T1', 5, '2027-09-17')
    carico(db, 'Tonno', 'T1', 0, '2027-09-17', tipo='IN_USO', scarico=1, rimanenza=4)
    in_uso_id = db.get_in_uso_attivi()[0]['id']
    c = _login(app_mod)
    r = c.post('/api/admin/riordino-magazzino',
               json={'lotti': [{'prodotto': 'Ricotta', 'lotto': 'R1'}], 'in_uso': [in_uso_id]}).get_json()
    assert r['success'] and r['lotti'] == ['Ricotta (R1)'] and r['in_uso'] == ['Tonno']
    giac = {g['prodotto']: g for g in db.get_giacenze()}
    assert 'Ricotta' not in giac                  # lotto chiuso
    assert giac['Tonno']['in_uso'] == 0           # aperto chiuso
    assert giac['Tonno']['totale'] == 4           # il resto del tonno non si tocca
    # nel registro resta la riga con la nota
    with db.get_conn() as conn:
        righe = db._rows(conn.execute("SELECT etichetta, tipo, synced FROM registro WHERE prodotto = 'Ricotta' ORDER BY id"))
    assert righe[-1]['tipo'] == 'SCARICO' and 'Riordino magazzino' in righe[-1]['etichetta'] and righe[-1]['synced'] == 0


def test_riordino_solo_admin(app_mod, db):
    from werkzeug.security import generate_password_hash
    db.create_user('Aiuto', 'aiuto', generate_password_hash('aiuto123', method='pbkdf2:sha256'), 'cucina')
    c = app_mod.app.test_client()
    c.post('/login', data={'nome': 'Aiuto', 'password': 'aiuto123'})
    assert c.post('/api/admin/riordino-magazzino', json={'lotti': []}).status_code == 403


def test_riordino_coda_sheets_nel_formato_del_worker(app_mod, db):
    """La modifica in coda deve avere le chiavi che il worker si aspetta,
    altrimenti blocca l'invio a Sheets di tutto quello che viene dopo."""
    carico(db, 'Tonno', 'T1', 5, '2027-09-17')
    carico(db, 'Tonno', 'T1', 0, '2027-09-17', tipo='IN_USO', scarico=1, rimanenza=4)
    _login(app_mod).post('/api/admin/riordino-magazzino', json={'in_uso': [db.get_in_uso_attivi()[0]['id']]})
    chiamate = []
    import app as modulo
    orig = modulo.aggiorna_tipo_movimento
    modulo.aggiorna_tipo_movimento = lambda mid, tipo, nuovo_scarico=None: chiamate.append((tipo, nuovo_scarico))
    try:
        for item in db.get_coda_sheets():
            modulo._esegui_azione_coda(item['azione'], item['payload'])
    finally:
        modulo.aggiorna_tipo_movimento = orig
    assert chiamate == [('SCARICO', 1.0)]
