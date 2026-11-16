"""Ogni test parte da un database SQLite vuoto in una cartella temporanea:
il vero haccp.db non viene mai toccato."""
import os
import sys
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.pop('DATABASE_URL', None)
os.environ.setdefault('BRIGADE_NO_SCHEDULER', '1')

import database  # noqa: E402
import sheets  # noqa: E402


# I test non devono MAI parlare con il Google Sheets vero del ristorante:
# qualunque tentativo di collegarsi fallisce subito, come se Sheets fosse
# irraggiungibile (l'app ha già la gestione per questo caso).
def _sheets_bloccato(*a, **k):
    raise sheets.SheetsUnavailableError('Google Sheets disattivato durante i test')


sheets._get_client = _sheets_bloccato


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(database, 'DB_PATH', str(tmp_path / 'test.db'))
    database.db_init()
    return database


def carico(db, prodotto, lotto, qty, scadenza='', fornitore='Fornitore Test', unita='kg', tipo='CARICO', scarico=0, rimanenza=None):
    db.insert_movimento({
        'data': '2026-11-01', 'fornitore': fornitore, 'prodotto': prodotto,
        'lotto': lotto, 'scadenza': scadenza, 'carico': qty if tipo == 'CARICO' else 0,
        'scarico': scarico, 'unita': unita, 'etichetta': '', 'movimento_id': f'{prodotto}-{lotto}-{tipo}-{qty}-{scarico}',
        'rimanenza': qty if rimanenza is None else rimanenza, 'tipo': tipo,
    })
