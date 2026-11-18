"""La pulizia del listino deve unire i nomi scelti e non toccare nient'altro."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'strumenti'))
from pulisci_listino import (pianifica_listino, pianifica_registro, giacenze_aperte,
                             _applica_attese, _uguali, colonne_con_formula)

TESTATA_L = ['Prodotto', 'Fornitore', 'Unità', 'Scorta minima', '', 'Elenco A-Z', 'Categoria', '', 'Reparto']


def listino():
    righe = [
        ['Fernet Menta', 'DOCKS', 'Pz', '1', '', 'aglio', 'Bevande', '', 'Sala'],
        ['fernet menta', 'DOCKS', 'Pz', '2', '', 'Fernet Menta', 'Bevande', '', 'Sala'],
        ['Patatine', 'DOCKS', 'pz', '', '', 'Patatine', 'Surgelati', '', ''],
        ['Patatine fritte', 'DAC', 'Pz', '', '', 'Patatine fritte', 'Surgelati', '', ''],
        ['Cozze', 'DAC', 'Kg', '', '', 'Cozze', 'Pesce', '', ''],
        ['penne senza glutine', 'DOCKS', 'kg', '', '', 'penne', 'Altro', '', ''],
        ['Lime', 'DOCKS', 'bt', '', '', 'Lime', 'Altro', '', ''],
        ['coca', 'DOCKS', 'confezioni', '', '', 'coca', 'Bevande', '', ''],
        ['zucchero di canna', 'DOCKS', 'conf', '', '', 'z', 'Monouso', '', ''],
        ['zucchero di canna sciolto', 'DOCKS', 'kg', '', '', 'z2', 'Condimenti', '', ''],
    ]
    valori = [TESTATA_L] + righe
    formule = [list(r) for r in valori]
    formule[1][5] = '=SORT(A2:A)'      # formula in F2 che si allarga su tutta la colonna F
    return valori, formule


def test_listino_svuota_doppioni_stesso_fornitore_e_rinomina_se_fornitore_diverso():
    valori, formule = listino()
    m = {(r, c): (v, n) for r, c, v, n, _ in pianifica_listino(valori, formule)}
    # "fernet menta" (riga 2), stesso fornitore di "Fernet Menta": svuotata, tranne la colonna F della formula
    assert m[(2, 0)] == ('fernet menta', '') and m[(2, 1)] == ('DOCKS', '') and m[(2, 6)] == ('Bevande', '')
    assert (2, 5) not in m
    # "Patatine fritte" (DAC) prende il nome "Patatine" e tiene il suo fornitore
    assert m[(4, 0)] == ('Patatine fritte', 'Patatine') and (4, 1) not in m
    # "zucchero di canna" (DOCKS) è doppione di "zucchero di canna sciolto" (DOCKS): svuotata
    assert m[(9, 0)] == ('zucchero di canna', '')
    # "Fernet Menta" e "Patatine" restano
    assert (1, 0) not in m and (3, 0) not in m


def test_listino_unita_e_categorie():
    valori, formule = listino()
    m = {(r, c): (v, n) for r, c, v, n, _ in pianifica_listino(valori, formule)}
    assert m[(1, 2)] == ('Pz', 'pz') and m[(5, 2)] == ('Kg', 'kg') and m[(8, 2)] == ('confezioni', 'conf')
    assert m[(6, 6)] == ('Altro', 'Pasta e cereali')
    assert m[(7, 6)] == ('Altro', 'Verdure e frutta fresche') and m[(7, 2)] == ('bt', 'kg')
    assert (3, 2) not in m      # 'pz' è già giusto


def test_mai_nella_colonna_con_la_formula():
    valori, formule = listino()
    col_f = colonne_con_formula(formule)
    assert col_f == {5}
    assert all(c not in col_f for _, c, *_ in pianifica_listino(valori, formule))


TESTATA_R = ['Data', 'Fornitore', 'Prodotto', 'Lotto', 'Scadenza', 'Carico', 'Scarico', 'Unità',
             'Etichetta', 'ID', 'Rimanenza lotto', 'Operatore', 'Reparto', 'Tipo movimento']


def registro():
    righe = [
        ['01/07/2026', 'DAC', 'Patatine fritte', 'L1', '01/01/2027', '10', '', 'Pz', 'PATATINE FRITTE L1', 'a1', '0', 'Simone', 'Cucina', 'CARICO'],
        ['02/07/2026', 'DOCKS', 'fernet menta', '', '', '2', '', 'Pz', '', 'a2', '0', 'Simone', 'Sala', 'CARICO'],
        ['03/07/2026', 'DOCKS', 'Gamberi', 'G7', '', '3', '', 'pz', '', 'a3', '1', 'Simone', 'Cucina', 'CARICO'],
        ['04/07/2026', 'DOCKS', 'Orata', 'O1', '', '5', '', 'Pz', '', 'a4', '0', 'Simone', 'Cucina', 'CARICO'],
    ]
    valori = [TESTATA_R] + righe
    return valori, [list(r) for r in valori]


def test_registro_rinomina_solo_il_prodotto_e_scrive_le_unita_uguali():
    valori, formule = registro()
    m = {(r, c): (v, n) for r, c, v, n, _ in pianifica_registro(valori, formule)}
    assert m[(1, 2)] == ('Patatine fritte', 'Patatine')
    assert m[(2, 2)] == ('fernet menta', 'Fernet Menta')
    assert m[(3, 2)] == ('Gamberi', 'Gamberi L1 (scatola da 2kg)')
    assert m[(1, 7)] == ('Pz', 'pz') and m[(4, 7)] == ('Pz', 'pz')
    assert (4, 2) not in m                         # Orata non c'entra
    assert all(c in (2, 7) for _, c in m)          # niente lotti, quantità, date, etichette


def test_segnala_merce_ancora_in_magazzino_col_nome_vecchio():
    valori, _ = registro()
    assert giacenze_aperte(valori) == {'Gamberi': [(1.0, 'pz')]}


def test_controllo_dopo_la_scrittura():
    valori, formule = registro()
    modifiche = pianifica_registro(valori, formule)
    attesi = _applica_attese(valori, modifiche)
    assert _uguali(attesi, attesi, set()) == []
    sbagliato = [list(r) for r in attesi]
    sbagliato[4][5] = '999'
    assert _uguali(attesi, sbagliato, set()) == [(5, 'F', '5', '999')]


def test_rinomina_nelle_tabelle_dell_app(db):
    from pulisci_listino import UNIONI
    db.add_lista_spesa_item('patatine fritte ', 'DAC', 'Surgelati', 2, 'pz')
    db.add_lista_spesa_item('Orata', 'DOCKS', 'Pesce', 2, 'pz')
    with db.get_conn() as conn:
        conn.execute("INSERT INTO spesa_comprati (prodotto, fornitore, quantita, unita, comprato_il, stato) "
                     "VALUES ('fernet menta', 'DOCKS', 1, 'pz', '2026-11-10T10:00:00', 'registrato')")
    cambiate = db.rinomina_prodotti(UNIONI)
    assert cambiate['lista_spesa'] == 1 and cambiate['spesa_comprati'] == 1
    nomi = sorted(i['prodotto'] for i in db.get_lista_spesa())
    assert nomi == ['Orata', 'Patatine']


def test_endpoint_solo_admin(db):
    import app as A
    from werkzeug.security import generate_password_hash
    A.app.config['TESTING'] = True
    for nome, ruolo in (('Capo Cucina', 'admin'), ('Aiuto Cuoco', 'staff')):
        db.create_user(nome, nome.split()[0].lower(), generate_password_hash('pw123456', method='pbkdf2'), 'cucina', role=ruolo)
    staff = A.app.test_client(); staff.post('/login', data={'nome': 'Aiuto Cuoco', 'password': 'pw123456'})
    assert staff.post('/api/admin/pulizia-listino').status_code in (302, 403)
    db.add_lista_spesa_item('Sesamo', 'DOCKS', '', 1, 'pz')
    admin = A.app.test_client(); admin.post('/login', data={'nome': 'Capo Cucina', 'password': 'pw123456'})
    r = admin.post('/api/admin/pulizia-listino').get_json()
    assert r['success'] and r['cambiate']['lista_spesa'] == 1
