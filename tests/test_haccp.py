from datetime import timedelta, datetime

from conftest import carico
from timezone_utils import today_it


def giorni(n):
    return (today_it() + timedelta(days=n)).isoformat()


# ─── GIACENZE ────────────────────────────────────────────────────────────────

def test_giacenza_somma_i_lotti_dello_stesso_prodotto(db):
    carico(db, 'Farina 00', 'L1', 10, giorni(30))
    carico(db, 'Farina 00', 'L2', 5, giorni(60))
    g = {x['prodotto']: x for x in db.get_giacenze()}
    assert g['Farina 00']['totale'] == 15
    assert len(g['Farina 00']['lotti']) == 2


def test_giacenza_usa_l_ultima_rimanenza_del_lotto(db):
    carico(db, 'Burro', 'B1', 4, giorni(10))
    # presa in uso di 1 kg: la rimanenza del lotto scende a 3
    carico(db, 'Burro', 'B1', 0, giorni(10), tipo='IN_USO', scarico=1, rimanenza=3)
    g = {x['prodotto']: x for x in db.get_giacenze()}
    assert g['Burro']['totale'] == 3
    assert g['Burro']['in_uso'] == 1


def test_lotto_finito_sparisce_dalle_giacenze(db):
    carico(db, 'Panna', 'P1', 2, giorni(5))
    carico(db, 'Panna', 'P1', 0, giorni(5), tipo='SCARICO', scarico=2, rimanenza=0)
    assert all(x['lotti'] == [] for x in db.get_giacenze() if x['prodotto'] == 'Panna')


def test_prossima_scadenza_e_la_piu_vicina(db):
    carico(db, 'Uova', 'U2', 30, giorni(12))
    carico(db, 'Uova', 'U1', 30, giorni(3))
    g = {x['prodotto']: x for x in db.get_giacenze()}
    assert g['Uova']['prossima_scadenza'] == giorni(3)


def test_lotti_attivi_ordinati_prima_quello_che_scade_prima(db):
    carico(db, 'Latte', 'tardi', 5, giorni(9))
    carico(db, 'Latte', 'presto', 5, giorni(2))
    carico(db, 'Latte', 'senza', 5, '')
    lotti = [l['lotto'] for l in db.get_lotti_attivi('Latte')]
    assert lotti == ['presto', 'tardi', 'senza']


# ─── ALERT ───────────────────────────────────────────────────────────────────

def test_alert_scaduto_e_in_scadenza(db):
    carico(db, 'Ricotta', 'R1', 1, giorni(-1))
    carico(db, 'Mozzarella', 'M1', 1, giorni(3))
    carico(db, 'Parmigiano', 'PA1', 1, giorni(90))
    a = {x['prodotto']: x for x in db.get_alerts()['scadenze']}
    assert a['Ricotta']['tipo'] == 'scaduto'
    assert a['Mozzarella']['tipo'] == 'in_scadenza'
    assert a['Mozzarella']['giorni'] == 3
    assert 'Parmigiano' not in a


def test_alert_ignora_lotti_gia_consumati(db):
    carico(db, 'Ricotta', 'R1', 1, giorni(-1))
    carico(db, 'Ricotta', 'R1', 0, giorni(-1), tipo='SCARICO', scarico=1, rimanenza=0)
    assert db.get_alerts()['scadenze'] == []


def test_alert_sotto_scorta(db):
    db.insert_listino_row('Olio EVO', 'Fornitore Test', 'lt', 10, 'Dispensa')
    carico(db, 'Olio EVO', 'O1', 4)
    scorte = [x['prodotto'] for x in db.get_alerts()['scorte']]
    assert scorte == ['Olio EVO']


# ─── TEMPERATURE / TURNI ─────────────────────────────────────────────────────

def test_turni_controllo_temperature():
    f = __import__('database').turno_e_data_riferimento
    assert f(datetime(2026, 11, 16, 9, 0)) == ('mattina', datetime(2026, 11, 16).date())
    assert f(datetime(2026, 11, 16, 22, 0)) == ('sera', datetime(2026, 11, 16).date())
    # dopo mezzanotte è ancora la sera del giorno prima
    assert f(datetime(2026, 11, 17, 2, 0)) == ('sera', datetime(2026, 11, 16).date())


def test_apparecchio_da_controllare_finche_non_si_registra(db):
    aid = db.create_apparecchio('Frigo primi', 'frigo', 0, 4)
    assert db.get_apparecchi()[0]['stato_controllo'] == 'DA_CONTROLLARE'
    db.registra_controllo_apparecchio(aid)
    assert db.get_apparecchi()[0]['stato_controllo'] == 'OK'


# ─── PREPARAZIONI ────────────────────────────────────────────────────────────

def test_preparazione_salvata_con_ingredienti(db):
    pid = db.insert_preparazione('Ragù', 'Primi', giorni(0), giorni(3), 5, 'kg', 1, '')
    db.insert_preparazione_ingrediente(pid, 'Macinato', 'MC1', 3, 'kg')
    assert db.get_preparazione_by_id(pid)['nome'] == 'Ragù'
    assert db.get_preparazione_ingredienti(pid)[0]['lotto'] == 'MC1'


def test_preparazione_completata(db):
    pid = db.insert_preparazione('Besciamella', 'Primi', giorni(0), giorni(2), 2, 'lt', 1, '')
    db.completa_preparazione(pid)
    assert db.get_preparazione_by_id(pid)['completata']
