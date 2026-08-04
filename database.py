import os
import re
import math
import base64
import sqlite3
from datetime import date, datetime, timedelta, timezone
from collections import OrderedDict
from urllib.parse import unquote

from timezone_utils import now_it, today_it

DATABASE_URL = os.environ.get('DATABASE_URL', '')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
_USE_PG = bool(DATABASE_URL)


def _parse_db_url(url):
    m = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+?)(\?.*)?$', url)
    if not m:
        raise ValueError(f"Invalid DATABASE_URL")
    return {
        'user':     unquote(m.group(1)),
        'password': unquote(m.group(2)),
        'host':     m.group(3),
        'port':     int(m.group(4)),
        'dbname':   m.group(5).split('?')[0],
        'sslmode':  'require',
    }


if _USE_PG:
    _PG_PARAMS = _parse_db_url(DATABASE_URL)
    print(f"[DB] host={_PG_PARAMS['host']} port={_PG_PARAMS['port']} "
          f"dbname={_PG_PARAMS['dbname']} user={_PG_PARAMS['user']}", flush=True)

_data_dir = '/data' if os.path.isdir('/data') else os.path.dirname(__file__)
DB_PATH = os.path.join(_data_dir, 'haccp.db')

# Orario di confine tra Pranzo e Cena per raggruppare le prenotazioni e
# calcolare i coperti (HH:MM, confronto lessicografico su stringa).
PRANZO_CENA_CUTOFF = '17:00'

# Durata di un lock su un movimento (prodotto/lotto/apparecchio) prima che
# scada automaticamente e torni disponibile per un altro utente.
LOCK_DURATA_MINUTI = 10

# ─── RACCOLTA RIFIUTI ────────────────────────────────────────────────────────
# Calendario settimanale porta a porta ASM Vigevano 2026 (fonte: asmisa.it,
# incrociato con vigevano.city per orari di esposizione e la frazione
# "verde e ramaglie" che mancava). giorno_settimana segue date.weekday():
# 0=lunedì ... 6=domenica.
RIFIUTI_ZONE_INFO = {
    'centro_ristoranti': {
        'label': 'Centro città — Ristoranti e Bar',
        # esposizione la sera stessa del giorno di raccolta
        'espone_sera_precedente': False,
        'orario_esposizione': 'Esponi tra le 18:30 e le 20:30 del giorno di raccolta.',
        'vetro_scadenza': 'Vetro e lattine: esponi entro le 12:00.',
    },
    'centro_negozi': {
        'label': 'Centro città — Negozi',
        'espone_sera_precedente': False,
        'orario_esposizione': 'Esponi tra le 18:30 e le 20:30 del giorno di raccolta.',
        'vetro_scadenza': 'Vetro e lattine: esponi entro le 12:00.',
    },
    'citta': {
        'label': 'Città',
        # esposizione dalle 21.00 del giorno precedente la raccolta
        'espone_sera_precedente': True,
        'orario_esposizione': 'Esponi dalle 21:00 del giorno precedente alle 4:00 del giorno di raccolta.',
        'vetro_scadenza': 'Vetro e lattine: esponi entro le 7:30.',
    },
}

RACCOLTA_RIFIUTI_SEED = [
    ('centro_ristoranti', 0, 'plastica'), ('centro_ristoranti', 0, 'vetro'), ('centro_ristoranti', 0, 'verde'),
    ('centro_ristoranti', 1, 'secco'),    ('centro_ristoranti', 1, 'organico'),
    ('centro_ristoranti', 2, 'carta'),    ('centro_ristoranti', 2, 'vetro'),
    ('centro_ristoranti', 3, 'organico'),
    ('centro_ristoranti', 4, 'carta'),    ('centro_ristoranti', 4, 'vetro'),
    ('centro_ristoranti', 6, 'organico'),

    ('centro_negozi', 0, 'plastica'), ('centro_negozi', 0, 'verde'),
    ('centro_negozi', 1, 'secco'),
    ('centro_negozi', 2, 'vetro'),
    ('centro_negozi', 3, 'organico'),
    ('centro_negozi', 4, 'carta'),
    ('centro_negozi', 6, 'organico'),

    ('citta', 0, 'organico'),
    ('citta', 1, 'carta'),
    ('citta', 2, 'secco'),
    ('citta', 3, 'plastica'),
    ('citta', 4, 'organico'),
    ('citta', 5, 'vetro'),
]

# Punto di riferimento del centro storico (Piazza Ducale) e raggio in metri
# usati per determinare la zona da coordinate GPS grezze.
RIFIUTI_CENTRO_LAT, RIFIUTI_CENTRO_LON = 45.3145, 8.8553
RIFIUTI_CENTRO_RAGGIO_M = 600


def _distanza_metri_rifiuti(lat1, lon1, lat2, lon2):
    R = 6371000
    to_rad = math.radians
    d_lat = to_rad(lat2 - lat1)
    d_lon = to_rad(lon2 - lon1)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(to_rad(lat1)) * math.cos(to_rad(lat2)) * math.sin(d_lon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def zona_rifiuti_da_coordinate(lat, lon):
    """Zona di raccolta rifiuti dedotta da una coppia di coordinate GPS,
    in base alla distanza dal centro storico (stessa logica già usata
    lato client per il rilevamento automatico)."""
    dist = _distanza_metri_rifiuti(lat, lon, RIFIUTI_CENTRO_LAT, RIFIUTI_CENTRO_LON)
    return 'centro_ristoranti' if dist <= RIFIUTI_CENTRO_RAGGIO_M else 'citta'

if _USE_PG:
    import psycopg2
    import psycopg2.extras


def _adapt(sql):
    """Translate SQLite SQL syntax to PostgreSQL."""
    has_ignore = bool(re.search(r'\bINSERT\s+OR\s+IGNORE\b', sql, re.IGNORECASE))
    sql = re.sub(r'\bINSERT\s+OR\s+IGNORE\b', 'INSERT', sql, flags=re.IGNORECASE)
    sql = sql.replace('?', '%s')
    if has_ignore and 'ON CONFLICT' not in sql.upper():
        sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
    return sql


class _DBConn:
    """
    Thin wrapper that normalises SQLite and PostgreSQL APIs.

    Usage:
        with get_conn() as conn:
            cur = conn.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    """

    def __init__(self):
        if _USE_PG:
            self._conn = psycopg2.connect(**_PG_PARAMS)
            self._cur  = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self._sqlite = False
        else:
            self._conn = sqlite3.connect(DB_PATH)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute('PRAGMA journal_mode=WAL')
            self._cur  = None
            self._sqlite = True

    def execute(self, sql, params=()):
        """Run a single statement, returning the underlying cursor."""
        if _USE_PG:
            self._cur.execute(_adapt(sql), params or ())
            return self._cur
        return self._conn.execute(sql, params)

    def executemany(self, sql, seq):
        if _USE_PG:
            psycopg2.extras.execute_batch(self._cur, _adapt(sql), seq)
        else:
            self._conn.executemany(sql, seq)

    def execute_insert(self, sql, params=()):
        """Execute an INSERT and return the new row id."""
        if _USE_PG:
            adapted = _adapt(sql)
            if 'RETURNING' not in adapted.upper():
                adapted = adapted.rstrip().rstrip(';') + ' RETURNING id'
            self._cur.execute(adapted, params or ())
            return self._cur.fetchone()['id']
        cur = self._conn.execute(sql, params)
        return cur.lastrowid

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *args):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        if _USE_PG:
            self._cur.close()
        self._conn.close()


def get_conn():
    return _DBConn()


def _rows(cur):
    """Convert cursor results to a list of plain dicts (works for both backends)."""
    return [dict(r) for r in cur.fetchall()]


def _row(cur):
    """Convert a single cursor result to a plain dict (or None)."""
    r = cur.fetchone()
    return dict(r) if r else None


def db_init():
    pk = 'SERIAL PRIMARY KEY' if _USE_PG else 'INTEGER PRIMARY KEY AUTOINCREMENT'

    # ── Step 1: create tables and indexes in a single transaction ──────────────
    with get_conn() as conn:
        for stmt in [
            f"""CREATE TABLE IF NOT EXISTS listino (
                id         {pk},
                prodotto   TEXT NOT NULL,
                fornitore  TEXT NOT NULL DEFAULT '',
                unita      TEXT NOT NULL DEFAULT 'kg',
                scorta_min REAL NOT NULL DEFAULT 0,
                categoria  TEXT NOT NULL DEFAULT '',
                reparto    TEXT NOT NULL DEFAULT 'Cucina',
                UNIQUE(prodotto, fornitore)
            )""",
            f"""CREATE TABLE IF NOT EXISTS registro (
                id           {pk},
                gs_row       INTEGER DEFAULT NULL,
                data         TEXT NOT NULL,
                fornitore    TEXT NOT NULL DEFAULT '',
                prodotto     TEXT NOT NULL,
                lotto        TEXT NOT NULL DEFAULT '',
                scadenza     TEXT NOT NULL DEFAULT '',
                carico       REAL NOT NULL DEFAULT 0,
                scarico      REAL NOT NULL DEFAULT 0,
                unita        TEXT NOT NULL DEFAULT 'kg',
                etichetta    TEXT NOT NULL DEFAULT '',
                movimento_id TEXT NOT NULL DEFAULT '',
                rimanenza    REAL NOT NULL DEFAULT 0,
                operatore    TEXT NOT NULL DEFAULT '',
                reparto      TEXT NOT NULL DEFAULT '',
                tipo         TEXT NOT NULL DEFAULT 'CARICO',
                data_scarico TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS users (
                id       {pk},
                nome     TEXT NOT NULL,
                email    TEXT,
                telefono TEXT,
                password TEXT NOT NULL,
                reparto  TEXT NOT NULL DEFAULT '',
                role     TEXT NOT NULL DEFAULT 'staff',
                stato    TEXT NOT NULL DEFAULT 'attivo'
            )""",
            f"""CREATE TABLE IF NOT EXISTS log_eliminazioni_temperature (
                id                  {pk},
                apparecchio         TEXT NOT NULL,
                temperatura         REAL NOT NULL DEFAULT 0,
                temp_min            REAL NOT NULL DEFAULT 0,
                temp_max            REAL NOT NULL DEFAULT 0,
                esito               TEXT NOT NULL DEFAULT '',
                data_originale      TEXT NOT NULL DEFAULT '',
                ora_originale       TEXT NOT NULL DEFAULT '',
                operatore_originale TEXT NOT NULL DEFAULT '',
                eliminato_da        TEXT NOT NULL DEFAULT '',
                eliminato_il        TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS log_lista_spesa (
                id        {pk},
                azione    TEXT NOT NULL DEFAULT '',
                prodotto  TEXT NOT NULL DEFAULT '',
                fornitore TEXT NOT NULL DEFAULT '',
                utente    TEXT NOT NULL DEFAULT '',
                quando    TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS log_modifiche_registro (
                id        {pk},
                prodotto  TEXT NOT NULL DEFAULT '',
                lotto     TEXT NOT NULL DEFAULT '',
                modifiche TEXT NOT NULL DEFAULT '',
                utente    TEXT NOT NULL DEFAULT '',
                quando    TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS log_eliminazioni_registro (
                id                  {pk},
                prodotto            TEXT NOT NULL DEFAULT '',
                lotto               TEXT NOT NULL DEFAULT '',
                scadenza            TEXT NOT NULL DEFAULT '',
                carico              REAL NOT NULL DEFAULT 0,
                scarico             REAL NOT NULL DEFAULT 0,
                unita               TEXT NOT NULL DEFAULT '',
                tipo                TEXT NOT NULL DEFAULT '',
                operatore_originale TEXT NOT NULL DEFAULT '',
                data_originale      TEXT NOT NULL DEFAULT '',
                eliminato_da        TEXT NOT NULL DEFAULT '',
                eliminato_il        TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS reset_tokens (
                id         {pk},
                user_id    INTEGER NOT NULL,
                token      TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used       INTEGER NOT NULL DEFAULT 0
            )""",
            f"""CREATE TABLE IF NOT EXISTS lista_spesa (
                id         {pk},
                prodotto   TEXT NOT NULL,
                fornitore  TEXT NOT NULL DEFAULT '',
                categoria  TEXT NOT NULL DEFAULT '',
                reparto    TEXT NOT NULL DEFAULT '',
                quantita   REAL NOT NULL DEFAULT 0,
                unita      TEXT NOT NULL DEFAULT '',
                completato INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )""",
            f"""CREATE TABLE IF NOT EXISTS apparecchi (
                id         {pk},
                nome       TEXT NOT NULL,
                tipo       TEXT NOT NULL DEFAULT 'Frigorifero',
                reparto    TEXT NOT NULL DEFAULT 'Cucina',
                marca      TEXT NOT NULL DEFAULT '',
                modello    TEXT NOT NULL DEFAULT '',
                seriale    TEXT NOT NULL DEFAULT '',
                temp_min   REAL NOT NULL DEFAULT 0,
                temp_max   REAL NOT NULL DEFAULT 4,
                attivo     INTEGER NOT NULL DEFAULT 1
            )""",
            f"""CREATE TABLE IF NOT EXISTS temperature (
                id          {pk},
                apparecchio TEXT NOT NULL,
                tipo        TEXT NOT NULL DEFAULT '',
                data        TEXT NOT NULL,
                ora         TEXT NOT NULL DEFAULT '',
                temperatura REAL NOT NULL DEFAULT 0,
                temp_min    REAL NOT NULL DEFAULT 0,
                temp_max    REAL NOT NULL DEFAULT 0,
                esito       TEXT NOT NULL DEFAULT 'OK',
                nota        TEXT NOT NULL DEFAULT '',
                operatore   TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS preparazioni (
                id                {pk},
                nome              TEXT NOT NULL,
                reparto           TEXT NOT NULL DEFAULT 'Cucina',
                data_preparazione TEXT NOT NULL,
                scadenza          TEXT NOT NULL DEFAULT '',
                quantita          REAL NOT NULL DEFAULT 0,
                unita             TEXT NOT NULL DEFAULT '',
                operatore_id      INTEGER,
                note              TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS preparazione_ingredienti (
                id               {pk},
                preparazione_id  INTEGER NOT NULL,
                prodotto         TEXT NOT NULL,
                lotto            TEXT NOT NULL DEFAULT '',
                quantita         REAL NOT NULL DEFAULT 0,
                unita            TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS sale (
                id   {pk},
                nome TEXT NOT NULL
            )""",
            f"""CREATE TABLE IF NOT EXISTS tavoli (
                id          {pk},
                numero      TEXT NOT NULL,
                forma       TEXT NOT NULL DEFAULT 'rettangolare',
                posti       INTEGER NOT NULL DEFAULT 2,
                sala_id     INTEGER NOT NULL,
                posizione_x REAL NOT NULL DEFAULT 40,
                posizione_y REAL NOT NULL DEFAULT 40,
                occupato    INTEGER NOT NULL DEFAULT 0,
                attivo      INTEGER NOT NULL DEFAULT 1
            )""",
            f"""CREATE TABLE IF NOT EXISTS prenotazioni (
                id           {pk},
                nome         TEXT NOT NULL,
                persone      INTEGER NOT NULL DEFAULT 1,
                data         TEXT NOT NULL,
                ora          TEXT NOT NULL,
                telefono     TEXT NOT NULL DEFAULT '',
                note         TEXT NOT NULL DEFAULT '',
                stato        TEXT NOT NULL DEFAULT 'confermata',
                operatore_id INTEGER
            )""",
            f"""CREATE TABLE IF NOT EXISTS combinazioni_tavoli (
                id              {pk},
                prenotazione_id INTEGER NOT NULL,
                tavolo_id       INTEGER NOT NULL
            )""",
            f"""CREATE TABLE IF NOT EXISTS log_eliminazioni_prenotazioni (
                id             {pk},
                nome           TEXT NOT NULL DEFAULT '',
                persone        INTEGER NOT NULL DEFAULT 0,
                data_prenot    TEXT NOT NULL DEFAULT '',
                ora            TEXT NOT NULL DEFAULT '',
                operatore_orig TEXT NOT NULL DEFAULT '',
                eliminato_da   TEXT NOT NULL DEFAULT '',
                eliminato_il   TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS lock_movimenti (
                id             {pk},
                tipo           TEXT NOT NULL,
                prodotto       TEXT NOT NULL DEFAULT '',
                lotto          TEXT NOT NULL DEFAULT '',
                apparecchio_id INTEGER NOT NULL DEFAULT 0,
                user_id        INTEGER NOT NULL,
                user_nome      TEXT NOT NULL,
                form           TEXT NOT NULL DEFAULT '',
                creato_il      TEXT NOT NULL,
                scade_il       TEXT NOT NULL
            )""",
            f"""CREATE TABLE IF NOT EXISTS chat_messaggi (
                id           {pk},
                canale       TEXT NOT NULL DEFAULT 'generale',
                utente_id    INTEGER NOT NULL,
                utente_nome  TEXT NOT NULL DEFAULT '',
                autore_badge TEXT NOT NULL DEFAULT '',
                testo        TEXT NOT NULL DEFAULT '',
                foto         TEXT NOT NULL DEFAULT '',
                modificato   INTEGER NOT NULL DEFAULT 0,
                eliminato    INTEGER NOT NULL DEFAULT 0,
                creato_il    TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS chat_canale_letto (
                utente_id INTEGER NOT NULL,
                canale    TEXT NOT NULL,
                last_seen TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (utente_id, canale)
            )""",
            f"""CREATE TABLE IF NOT EXISTS log_meteo (
                id           {pk},
                quando       TEXT NOT NULL DEFAULT '',
                temperatura  REAL NOT NULL DEFAULT 0,
                weather_code INTEGER NOT NULL DEFAULT 0
            )""",
            f"""CREATE TABLE IF NOT EXISTS log_chat_messaggi (
                id               {pk},
                azione           TEXT NOT NULL DEFAULT '',
                canale           TEXT NOT NULL DEFAULT '',
                autore_originale TEXT NOT NULL DEFAULT '',
                testo_originale  TEXT NOT NULL DEFAULT '',
                operatore        TEXT NOT NULL DEFAULT '',
                quando           TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS impostazioni_app (
                chiave TEXT PRIMARY KEY,
                valore TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS raccolta_rifiuti_calendario (
                id               {pk},
                zona             TEXT NOT NULL,
                giorno_settimana INTEGER NOT NULL,
                tipo_rifiuto     TEXT NOT NULL,
                UNIQUE(zona, giorno_settimana, tipo_rifiuto)
            )""",
            f"""CREATE TABLE IF NOT EXISTS push_subscriptions (
                id                 {pk},
                utente_id          INTEGER NOT NULL,
                subscription_json  TEXT NOT NULL,
                endpoint           TEXT NOT NULL DEFAULT '',
                created_at         TEXT NOT NULL DEFAULT ''
            )""",
            "CREATE INDEX IF NOT EXISTS idx_reg_prod_lotto ON registro(prodotto, lotto)",
            "CREATE INDEX IF NOT EXISTS idx_reg_prod_lotto_tipo ON registro(prodotto, lotto, tipo)",
            "CREATE INDEX IF NOT EXISTS idx_reg_data       ON registro(data)",
            "CREATE INDEX IF NOT EXISTS idx_reg_mov_id     ON registro(movimento_id)",
            "CREATE INDEX IF NOT EXISTS idx_temp_data      ON temperature(data)",
            "CREATE INDEX IF NOT EXISTS idx_prep_ingr_prep ON preparazione_ingredienti(preparazione_id)",
            "CREATE INDEX IF NOT EXISTS idx_tavoli_sala    ON tavoli(sala_id)",
            "CREATE INDEX IF NOT EXISTS idx_prenot_data    ON prenotazioni(data)",
            "CREATE INDEX IF NOT EXISTS idx_combi_prenot   ON combinazioni_tavoli(prenotazione_id)",
            "CREATE INDEX IF NOT EXISTS idx_combi_tavolo   ON combinazioni_tavoli(tavolo_id)",
            "CREATE INDEX IF NOT EXISTS idx_lock_prod_lotto  ON lock_movimenti(prodotto, lotto)",
            "CREATE INDEX IF NOT EXISTS idx_lock_apparecchio ON lock_movimenti(apparecchio_id)",
            "CREATE INDEX IF NOT EXISTS idx_chat_canale ON chat_messaggi(canale)",
            "CREATE INDEX IF NOT EXISTS idx_raccolta_zona ON raccolta_rifiuti_calendario(zona)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_push_sub_endpoint ON push_subscriptions(endpoint)",
            "CREATE INDEX IF NOT EXISTS idx_push_sub_utente ON push_subscriptions(utente_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email    ON users(email)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_telefono ON users(telefono)",
        ]:
            conn.execute(stmt)

    with get_conn() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO raccolta_rifiuti_calendario "
            "(zona, giorno_settimana, tipo_rifiuto) VALUES (?, ?, ?)",
            RACCOLTA_RIFIUTI_SEED
        )

    # ── Step 2: migrations — each in its own connection/transaction ────────────
    # On PostgreSQL a failed statement aborts the whole transaction, so we use
    # ADD COLUMN IF NOT EXISTS (PG 9.6+) which never fails.
    # On SQLite we keep the try/except approach.
    migrations = [
        ('registro', 'operatore',    "TEXT NOT NULL DEFAULT ''"),
        ('registro', 'reparto',      "TEXT NOT NULL DEFAULT ''"),
        ('registro', 'tipo',         "TEXT NOT NULL DEFAULT 'CARICO'"),
        ('registro', 'data_scarico', "TEXT NOT NULL DEFAULT ''"),
        ('listino',  'categoria',    "TEXT NOT NULL DEFAULT ''"),
        ('listino',  'reparto',      "TEXT NOT NULL DEFAULT 'Cucina'"),
        ('lista_spesa', 'reparto',   "TEXT NOT NULL DEFAULT ''"),
        ('users',    'username',     "TEXT NOT NULL DEFAULT ''"),
        ('users',    'gestisce_apparecchi', "INTEGER NOT NULL DEFAULT 0"),
        ('users',    'tema',      "TEXT NOT NULL DEFAULT 'chiaro'"),
        ('users',    'stato',     "TEXT NOT NULL DEFAULT 'attivo'"),
        ('users',    'puo_vedere_controllo', "INTEGER NOT NULL DEFAULT 0"),
        ('users',    'puo_eliminare_carichi', "INTEGER NOT NULL DEFAULT 0"),
        ('apparecchi', 'reparto', "TEXT NOT NULL DEFAULT 'Cucina'"),
        ('apparecchi', 'marca',   "TEXT NOT NULL DEFAULT ''"),
        ('apparecchi', 'modello', "TEXT NOT NULL DEFAULT ''"),
        ('apparecchi', 'seriale', "TEXT NOT NULL DEFAULT ''"),
        ('apparecchi', 'ultima_mattina', "TEXT NOT NULL DEFAULT ''"),
        ('apparecchi', 'ultima_sera',    "TEXT NOT NULL DEFAULT ''"),
        ('preparazioni', 'completata', "INTEGER NOT NULL DEFAULT 0"),
        ('registro',     'synced', "INTEGER NOT NULL DEFAULT 1"),
        ('listino',      'synced', "INTEGER NOT NULL DEFAULT 1"),
        ('temperature',  'synced', "INTEGER NOT NULL DEFAULT 1"),
        ('registro',     'creato_il', "TEXT NOT NULL DEFAULT ''"),
        ('users',        'home_card', "TEXT NOT NULL DEFAULT 'preparazioni'"),
        ('users',        'dash_card1', "TEXT NOT NULL DEFAULT 'alert'"),
        ('users',        'dash_card2', "TEXT NOT NULL DEFAULT 'temperature'"),
        ('users',        'dash_card3', "TEXT NOT NULL DEFAULT 'scadenza'"),
        ('users',        'tutorial_visto', "INTEGER NOT NULL DEFAULT 0"),
        ('users',        'puo_chat', "INTEGER NOT NULL DEFAULT 0"),
        ('users',        'chat_last_seen', "TEXT NOT NULL DEFAULT ''"),
        ('chat_messaggi', 'autore_badge', "TEXT NOT NULL DEFAULT ''"),
        ('reset_tokens', 'confermato', "INTEGER NOT NULL DEFAULT 0"),
        ('reset_tokens', 'created_at', "TEXT NOT NULL DEFAULT ''"),
        ('users', 'rifiuti_lat', "REAL NOT NULL DEFAULT 0"),
        ('users', 'rifiuti_lon', "REAL NOT NULL DEFAULT 0"),
        ('users', 'rifiuti_geo_aggiornato_il', "TEXT NOT NULL DEFAULT ''"),
    ]
    for table, col, defn in migrations:
        if _USE_PG:
            with get_conn() as conn:
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {defn}"
                )
        else:
            try:
                with get_conn() as conn:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
            except Exception:
                pass  # Column already exists

    # Backfill: le righe storiche (pre-esistenti alla colonna 'tipo') hanno
    # 'CARICO' come default; quelle con scarico > 0 sono in realtà SCARICO.
    # Idempotente: dopo il primo run, la WHERE non trova più righe da correggere.
    with get_conn() as conn:
        conn.execute(
            "UPDATE registro SET tipo = 'SCARICO' WHERE tipo = 'CARICO' AND scarico > 0"
        )

    # Backfill: i prodotti storici (pre-esistenti alla colonna 'reparto') hanno
    # 'Cucina' come default; quelli con categoria Bevande/Dolci erano già
    # classificati "Sala" tramite l'euristica SALA_CATS lato frontend.
    # Idempotente: dopo il primo run, la WHERE non trova più righe da correggere.
    with get_conn() as conn:
        conn.execute(
            "UPDATE listino SET reparto = 'Sala' "
            "WHERE reparto = 'Cucina' AND LOWER(categoria) IN ('bevande', 'dolci')"
        )

    # ── Step 3: post-migration indexes ────────────────────────────────────────
    # idx_users_username deve venire DOPO la migration che aggiunge la colonna.
    # Prima deduplicare eventuali righe duplicate (es. da deploy precedenti),
    # poi creare l'indice UNIQUE in modo sicuro.
    if _USE_PG:
        with get_conn() as conn:
            # Rimuove duplicati mantenendo l'utente con id più alto per ogni username
            conn.execute(
                "DELETE FROM users WHERE id NOT IN "
                "(SELECT MAX(id) FROM users GROUP BY username)"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)"
            )
    else:
        with get_conn() as conn:
            conn.execute(
                "DELETE FROM users WHERE id NOT IN "
                "(SELECT MAX(id) FROM users GROUP BY username)"
            )
        try:
            with get_conn() as conn:
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)"
                )
        except Exception:
            pass  # Index already exists


# ─── LISTINO ─────────────────────────────────────────────────────────────────

def replace_listino(rows):
    with get_conn() as conn:
        conn.execute("DELETE FROM listino")
        if rows:
            conn.executemany(
                "INSERT OR IGNORE INTO listino (prodotto, fornitore, unita, scorta_min, categoria, reparto) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(r['prodotto'], r['fornitore'], r['unita'], r['scorta_min'],
                  r.get('categoria', ''), r.get('reparto') or 'Cucina') for r in rows]
            )


def insert_listino_row(prodotto, fornitore, unita, scorta_min, categoria, reparto='Cucina', synced=True):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO listino (prodotto, fornitore, unita, scorta_min, categoria, reparto, synced) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (prodotto, fornitore, unita, float(scorta_min or 0), categoria or '', reparto or 'Cucina',
             1 if synced else 0)
        )


def get_unsynced_listino():
    """Prodotti scritti in locale mentre Google Sheets non era raggiungibile,
    ancora da inviare a Sheets."""
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM listino WHERE synced = 0")
        return _rows(cur)


def mark_listino_synced(prodotto, fornitore):
    with get_conn() as conn:
        conn.execute(
            "UPDATE listino SET synced = 1 WHERE prodotto = ? AND fornitore = ?",
            (prodotto, fornitore)
        )


def get_fornitori():
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT DISTINCT fornitore FROM listino WHERE fornitore != '' ORDER BY fornitore"
        )
        return [r['fornitore'] for r in cur.fetchall()]


CATEGORIE_FISSE = [
    'Carne',
    'Pesce e frutti di mare',
    'Verdure e frutta fresche',
    'Latticini e uova',
    'Salumi e affettati',
    'Pasta e cereali',
    'Conserve e prodotti in scatola',
    'Bevande',
    'Dolci',
    'Surgelati',
    'Condimenti e spezie',
    'Monouso e accessori',
    'Altro',
]


def get_categorie():
    return CATEGORIE_FISSE


def get_all_prodotti():
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT prodotto, fornitore, unita, scorta_min, categoria, reparto FROM listino ORDER BY LOWER(prodotto)"
        )
        return _rows(cur)


def get_prodotti_by_fornitore(fornitore):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT prodotto, fornitore, unita, scorta_min, categoria, reparto FROM listino "
            "WHERE fornitore = ? ORDER BY LOWER(prodotto)",
            (fornitore,)
        )
        return _rows(cur)


def get_reparto_prodotto(prodotto, fornitore):
    """Reparto (Cucina/Sala/Pizzeria) del prodotto, per attribuire correttamente
    il movimento nel REGISTRO. Fallback 'Cucina' se non trovato in LISTINO."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT reparto FROM listino WHERE prodotto = ? AND fornitore = ?",
            (prodotto, fornitore)
        )
        row = _row(cur)
        return (row['reparto'] if row and row.get('reparto') else 'Cucina')


# ─── REGISTRO ────────────────────────────────────────────────────────────────

def get_ultima_rimanenza(prodotto, lotto):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT rimanenza FROM registro WHERE prodotto = ? AND lotto = ? "
            "ORDER BY id DESC LIMIT 1",
            (prodotto, lotto)
        )
        row = _row(cur)
        return float(row['rimanenza']) if row else 0.0


def insert_movimento(row):
    with get_conn() as conn:
        return conn.execute_insert(
            """INSERT INTO registro
               (gs_row, data, fornitore, prodotto, lotto, scadenza,
                carico, scarico, unita, etichetta, movimento_id, rimanenza,
                operatore, reparto, tipo, data_scarico, synced, creato_il)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                row.get('gs_row'), row['data'], row['fornitore'], row['prodotto'],
                row['lotto'], row['scadenza'], row['carico'], row['scarico'],
                row['unita'], row['etichetta'], row['movimento_id'], row['rimanenza'],
                row.get('operatore', ''), row.get('reparto', ''),
                row.get('tipo', 'CARICO'), row.get('data_scarico', ''),
                1 if row.get('synced', True) else 0,
                now_it().isoformat(),
            )
        )


def get_carico_recente(prodotto, lotto, minuti=120):
    """Ultimo CARICO per prodotto+lotto nelle ultime `minuti`, se esiste —
    usato per avvisare l'utente di un possibile doppio inserimento."""
    cutoff = (now_it() - timedelta(minutes=minuti)).isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT operatore, creato_il FROM registro "
            "WHERE prodotto = ? AND lotto = ? AND tipo = 'CARICO' AND creato_il >= ? "
            "ORDER BY id DESC LIMIT 1",
            (prodotto, lotto, cutoff)
        )
        return _row(cur)


def get_unsynced_registro():
    """Righe scritte in locale mentre Google Sheets non era raggiungibile,
    ancora da inviare a Sheets."""
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM registro WHERE synced = 0")
        return _rows(cur)


def mark_registro_synced(row_id):
    with get_conn() as conn:
        conn.execute("UPDATE registro SET synced = 1 WHERE id = ?", (row_id,))


def get_movimento_by_id(row_id):
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM registro WHERE id = ?", (row_id,))
        return _row(cur)


def finalizza_in_uso(row_id, data_scarico, qty=None, nuovo_movimento_id=None, tipo_finale='SCARICO'):
    """
    Finalizza una riga IN_USO esistente (di norma a SCARICO; il chiamante può
    passare tipo_finale='PREPARAZIONE' quando la quantità viene consumata
    come ingrediente di una preparazione, invece che scaricata a mano).
    Se qty è minore della quantità in uso, solo quella parte viene marcata
    come finalizzata: il residuo resta "in uso" in una nuova riga con lo
    stesso lotto e la stessa data originale (nessuna nuova riga se si
    finalizza l'intera quantità, comportamento invariato).

    Ritorna (riga_finalizzata, riga_residua) — riga_residua è None se non
    c'è residuo. Ritorna (None, None) se la riga non viene trovata.
    """
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM registro WHERE id = ? AND tipo = 'IN_USO'", (row_id,))
        original = _row(cur)
    if not original:
        return None, None

    if qty is None:
        qty = original['scarico']
    qty = round(float(qty), 4)
    residuo = round(original['scarico'] - qty, 4)

    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE registro SET scarico = ?, tipo = ?, data_scarico = ? "
            "WHERE id = ? AND tipo = 'IN_USO'",
            (qty, tipo_finale, data_scarico, row_id)
        )
        if cur.rowcount == 0:
            return None, None

    riga_residua = None
    if residuo > 0:
        nuovo_id = insert_movimento({
            'data': original['data'], 'fornitore': original['fornitore'],
            'prodotto': original['prodotto'], 'lotto': original['lotto'],
            'scadenza': original['scadenza'], 'carico': 0, 'scarico': residuo,
            'unita': original['unita'], 'etichetta': '', 'movimento_id': nuovo_movimento_id or '',
            'rimanenza': original['rimanenza'], 'operatore': original['operatore'],
            'reparto': original['reparto'], 'tipo': 'IN_USO', 'data_scarico': '',
        })
        riga_residua = get_movimento_by_id(nuovo_id)

    return get_movimento_by_id(row_id), riga_residua


def get_in_uso_attivi(prodotto=None):
    """Righe IN_USO non ancora finalizzate (scaricate)."""
    with get_conn() as conn:
        base_sql = (
            """SELECT r.id, r.prodotto, r.fornitore, r.lotto, r.scadenza,
                      r.scarico AS qty, r.unita, r.data, r.reparto, r.operatore,
                      COALESCE(l.categoria, '') AS categoria,
                      (SELECT MIN(rc.data) FROM registro rc
                       WHERE rc.prodotto = r.prodotto AND rc.lotto = r.lotto AND rc.tipo = 'CARICO') AS data_carico
               FROM registro r
               LEFT JOIN listino l
                   ON l.prodotto = r.prodotto AND l.fornitore = r.fornitore
               WHERE r.tipo = 'IN_USO'"""
        )
        if prodotto:
            cur = conn.execute(
                base_sql + " AND r.prodotto = ? ORDER BY r.data ASC, r.id ASC",
                (prodotto,)
            )
        else:
            cur = conn.execute(base_sql + " ORDER BY r.data ASC, r.id ASC")
        return _rows(cur)


def get_lotti_attivi(prodotto):
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT r.lotto, r.scadenza, r.rimanenza, r.unita, r.fornitore, r.etichetta,
                      COALESCE(l.reparto, 'Cucina') AS reparto,
                      (SELECT MIN(rc.data) FROM registro rc
                       WHERE rc.prodotto = r.prodotto AND rc.lotto = r.lotto AND rc.tipo = 'CARICO') AS data_carico
               FROM registro r
               LEFT JOIN listino l
                   ON l.prodotto = r.prodotto AND l.fornitore = r.fornitore
               WHERE r.prodotto = ?
                 AND r.id = (
                     SELECT MAX(r2.id) FROM registro r2
                     WHERE r2.prodotto = r.prodotto AND r2.lotto = r.lotto
                 )
                 AND r.rimanenza > 0
               ORDER BY
                 CASE WHEN r.scadenza = '' OR r.scadenza IS NULL THEN 1 ELSE 0 END,
                 r.scadenza ASC""",
            (prodotto,)
        )
        return _rows(cur)


def get_giacenze():
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT r.id, r.prodotto, r.fornitore, r.lotto, r.scadenza,
                      r.rimanenza, r.unita, r.etichetta,
                      l.scorta_min, l.categoria, l.reparto,
                      (SELECT MIN(rc.data) FROM registro rc
                       WHERE rc.prodotto = r.prodotto AND rc.lotto = r.lotto AND rc.tipo = 'CARICO') AS data_carico
               FROM registro r
               LEFT JOIN listino l
                   ON l.prodotto = r.prodotto AND l.fornitore = r.fornitore
               WHERE r.id = (
                   SELECT MAX(r2.id) FROM registro r2
                   WHERE r2.prodotto = r.prodotto AND r2.lotto = r.lotto
               ) AND r.rimanenza > 0
               ORDER BY LOWER(r.prodotto),
                 CASE WHEN r.scadenza = '' OR r.scadenza IS NULL THEN 1 ELSE 0 END,
                 r.scadenza ASC"""
        )
        rows = _rows(cur)

        in_uso_rows = _rows(conn.execute(
            """SELECT r.prodotto, SUM(r.scarico) AS tot_in_uso,
                      MAX(r.unita) AS unita, MAX(r.fornitore) AS fornitore,
                      MAX(l.reparto) AS reparto
               FROM registro r
               LEFT JOIN listino l
                   ON l.prodotto = r.prodotto AND l.fornitore = r.fornitore
               WHERE r.tipo = 'IN_USO'
               GROUP BY r.prodotto"""
        ))

    grouped = OrderedDict()
    for r in rows:
        p = r['prodotto']
        if p not in grouped:
            grouped[p] = {
                'prodotto':          p,
                'fornitore':         r['fornitore'] or '',
                'unita':             r['unita'],
                'scorta_min':        float(r['scorta_min'] or 0),
                'categoria':         r['categoria'] or '',
                'reparto':           r['reparto'] or 'Cucina',
                'totale':            0.0,
                'in_uso':            0.0,
                'prossima_scadenza': None,
                'lotti':             [],
            }
        g = grouped[p]
        g['totale'] = round(g['totale'] + r['rimanenza'], 4)
        scad = r['scadenza']
        if scad and (not g['prossima_scadenza'] or scad < g['prossima_scadenza']):
            g['prossima_scadenza'] = scad
        g['lotti'].append({
            'id':          r['id'],
            'lotto':       r['lotto'],
            'scadenza':    r['scadenza'],
            'rimanenza':   r['rimanenza'],
            'etichetta':   r['etichetta'],
            'data_carico': r['data_carico'],
        })

    for r in in_uso_rows:
        qty = float(r['tot_in_uso'] or 0)
        if qty <= 0:
            continue
        p = r['prodotto']
        if p not in grouped:
            # Prodotto interamente "in uso": nessun lotto rimasto in magazzino,
            # ma va comunque mostrato in Giacenze con la quantità in uso.
            grouped[p] = {
                'prodotto':          p,
                'fornitore':         r['fornitore'] or '',
                'unita':             r['unita'] or '',
                'scorta_min':        0.0,
                'categoria':         '',
                'reparto':           r['reparto'] or 'Cucina',
                'totale':            0.0,
                'in_uso':            0.0,
                'prossima_scadenza': None,
                'lotti':             [],
            }
        grouped[p]['in_uso'] = round(grouped[p]['in_uso'] + qty, 4)

    result = list(grouped.values())
    for g in result:
        g['sotto_scorta'] = g['scorta_min'] > 0 and g['totale'] < g['scorta_min']
    return result


def get_alerts():
    today      = today_it()
    limite_str = (today + timedelta(days=7)).isoformat()

    with get_conn() as conn:
        scad_rows = _rows(conn.execute(
            """SELECT r.prodotto, r.lotto, r.scadenza, r.rimanenza, r.unita, r.fornitore,
                      COALESCE(l.categoria, '') AS categoria,
                      COALESCE(l.reparto, 'Cucina') AS reparto,
                      (SELECT MIN(rc.data) FROM registro rc
                       WHERE rc.prodotto = r.prodotto AND rc.lotto = r.lotto AND rc.tipo = 'CARICO') AS data_carico
               FROM registro r
               LEFT JOIN listino l ON l.prodotto = r.prodotto AND l.fornitore = r.fornitore
               WHERE r.id = (
                   SELECT MAX(r2.id) FROM registro r2
                   WHERE r2.prodotto = r.prodotto AND r2.lotto = r.lotto
               )
               AND r.rimanenza > 0
               AND r.scadenza != ''
               AND r.scadenza <= ?
               ORDER BY r.scadenza ASC""",
            (limite_str,)
        ))

        scorte_rows = _rows(conn.execute(
            """WITH latest AS (
                   SELECT r.prodotto, r.rimanenza
                   FROM registro r
                   WHERE r.id = (
                       SELECT MAX(r2.id) FROM registro r2
                       WHERE r2.prodotto = r.prodotto AND r2.lotto = r.lotto
                   ) AND r.rimanenza > 0
               ),
               totali AS (
                   SELECT prodotto, SUM(rimanenza) AS totale FROM latest GROUP BY prodotto
               )
               SELECT l.prodotto, l.fornitore, l.unita, l.scorta_min, l.categoria,
                      COALESCE(l.reparto, 'Cucina') AS reparto,
                      COALESCE(t.totale, 0) AS totale
               FROM listino l
               LEFT JOIN totali t ON t.prodotto = l.prodotto
               WHERE l.scorta_min > 0 AND COALESCE(t.totale, 0) < l.scorta_min
               ORDER BY LOWER(l.prodotto)"""
        ))

    scadenze = []
    for r in scad_rows:
        try:
            giorni = (date.fromisoformat(r['scadenza']) - today).days
        except ValueError:
            giorni = None
        scadenze.append({
            **r,
            'giorni': giorni,
            'tipo':   'scaduto' if (giorni is not None and giorni < 0) else 'in_scadenza',
        })

    return {
        'scadenze': scadenze,
        'scorte':   scorte_rows,
        'count':    len(scadenze) + len(scorte_rows),
    }


def replace_registro(rows):
    with get_conn() as conn:
        conn.execute("DELETE FROM registro")
        if rows:
            conn.executemany(
                """INSERT INTO registro
                   (gs_row, data, fornitore, prodotto, lotto, scadenza,
                    carico, scarico, unita, etichetta, movimento_id, rimanenza,
                    operatore, reparto, tipo)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                # 'tipo' arriva già determinato da sheets.load_registro() (colonna
                # 'Tipo movimento'); se assente, deduci dal segno del movimento.
                [(r.get('gs_row'), r['data'], r['fornitore'], r['prodotto'],
                  r['lotto'], r['scadenza'], r['carico'], r['scarico'],
                  r['unita'], r['etichetta'], r['movimento_id'], r['rimanenza'],
                  r.get('operatore', ''), r.get('reparto', ''),
                  r.get('tipo') or ('SCARICO' if (r.get('scarico') or 0) else 'CARICO'))
                 for r in rows]
            )


# ─── USERS ───────────────────────────────────────────────────────────────────

def create_user(nome, username, password_hash, reparto, role='staff', stato='attivo'):
    params = (nome, username.strip().lower(), password_hash, reparto, role, stato)
    with get_conn() as conn:
        if _USE_PG:
            conn.execute(
                "INSERT INTO users (nome, username, password, reparto, role, stato) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (username) DO NOTHING",
                params
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO users (nome, username, password, reparto, role, stato) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                params
            )


def get_user_by_login(username):
    """Find user by username."""
    username = (username or '').strip().lower()
    if not username:
        return None
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM users WHERE username = ? LIMIT 1",
            (username,)
        )
        return _row(cur)


def get_user_by_nome(nome):
    """Find user by full name (case-insensitive)."""
    nome = (nome or '').strip()
    if not nome:
        return None
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM users WHERE LOWER(nome) = LOWER(?) LIMIT 1",
            (nome,)
        )
        return _row(cur)


def is_username_taken(username, exclude_user_id):
    """Returns True if username is already used by another user."""
    username = (username or '').strip().lower()
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id FROM users WHERE username = ? AND id != ?",
            (username, exclude_user_id)
        )
        return _row(cur) is not None


def get_user_by_id(user_id):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM users WHERE id = ?", (user_id,)
        )
        return _row(cur)


def get_all_users():
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, nome, email, telefono, reparto, role, gestisce_apparecchi, "
            "stato, puo_vedere_controllo, puo_eliminare_carichi, puo_chat FROM users ORDER BY "
            "CASE WHEN stato = 'in_attesa' THEN 0 ELSE 1 END, nome"
        )
        return _rows(cur)


def update_user_role(user_id, role):
    with get_conn() as conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))


def update_user_controllo_permesso(user_id, permesso):
    with get_conn() as conn:
        conn.execute("UPDATE users SET puo_vedere_controllo = ? WHERE id = ?",
                     (1 if permesso else 0, user_id))


def update_user_eliminazione_carichi_permesso(user_id, permesso):
    with get_conn() as conn:
        conn.execute("UPDATE users SET puo_eliminare_carichi = ? WHERE id = ?",
                     (1 if permesso else 0, user_id))


def approva_utente(user_id):
    with get_conn() as conn:
        conn.execute("UPDATE users SET stato = 'attivo' WHERE id = ?", (user_id,))


def rifiuta_utente(user_id):
    """Rifiuta una richiesta di registrazione: elimina l'account in attesa."""
    with get_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ? AND stato = 'in_attesa'", (user_id,))


def delete_user(user_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))


# ─── RESET TOKEN ──────────────────────────────────────────────────────────────

def create_reset_token(user_id, token, expires_at):
    """Invalida eventuali token precedenti e ne crea uno nuovo (da confermare
    da un amministratore prima che possa essere usato, vedi 'confermato')."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE reset_tokens SET used = 1 WHERE user_id = ?", (user_id,)
        )
        conn.execute(
            "INSERT INTO reset_tokens (user_id, token, expires_at, used, confermato, created_at) "
            "VALUES (?, ?, ?, 0, 0, ?)",
            (user_id, token, expires_at, now_it().isoformat())
        )


def get_reset_token(token):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM reset_tokens WHERE token = ? AND used = 0 LIMIT 1",
            (token,)
        )
        return _row(cur)


def use_reset_token(token_id):
    with get_conn() as conn:
        conn.execute("UPDATE reset_tokens SET used = 1 WHERE id = ?", (token_id,))


def get_active_reset_tokens():
    """Codici non ancora usati e non scaduti, con il nome dell'utente
    richiedente, per il pannello Admin (conferma manuale del reset)."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT rt.id, rt.user_id, rt.token, rt.expires_at, rt.confermato, rt.created_at, "
            "u.nome AS utente_nome "
            "FROM reset_tokens rt JOIN users u ON u.id = rt.user_id "
            "WHERE rt.used = 0 AND rt.expires_at > ? "
            "ORDER BY rt.id DESC",
            (now_it().isoformat(),)
        )
        return _rows(cur)


def confirm_reset_token(token_id):
    with get_conn() as conn:
        conn.execute("UPDATE reset_tokens SET confermato = 1 WHERE id = ?", (token_id,))


def update_user_password(user_id, password_hash):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET password = ? WHERE id = ?", (password_hash, user_id)
        )


def update_user_profile(user_id, nome, username):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET nome = ?, username = ? WHERE id = ?",
            (nome, username.strip().lower(), user_id)
        )


# ─── LISTA SPESA ─────────────────────────────────────────────────────────────

def get_lista_spesa():
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM lista_spesa ORDER BY created_at ASC, id ASC"
        )
        return _rows(cur)


def add_lista_spesa_item(prodotto, fornitore, categoria, quantita, unita, reparto=''):
    with get_conn() as conn:
        return conn.execute_insert(
            "INSERT INTO lista_spesa (prodotto, fornitore, categoria, reparto, quantita, unita, completato, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
            (prodotto, fornitore or '', categoria or '', reparto or '', float(quantita or 0),
             unita or '', now_it().isoformat())
        )


def update_lista_spesa_completato(item_id, completato):
    with get_conn() as conn:
        conn.execute(
            "UPDATE lista_spesa SET completato = ? WHERE id = ?",
            (1 if completato else 0, item_id)
        )


def update_lista_spesa_fornitore(item_id, fornitore):
    with get_conn() as conn:
        conn.execute(
            "UPDATE lista_spesa SET fornitore = ? WHERE id = ?",
            (fornitore.strip(), item_id)
        )


def get_lista_spesa_item_by_id(item_id):
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM lista_spesa WHERE id = ?", (item_id,))
        return _row(cur)


def delete_lista_spesa_item(item_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM lista_spesa WHERE id = ?", (item_id,))


def clear_lista_spesa():
    with get_conn() as conn:
        conn.execute("DELETE FROM lista_spesa")


def update_user_reparto(user_id, reparto):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET reparto = ? WHERE id = ?", (reparto, user_id)
        )


def update_user_tema(user_id, tema):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET tema = ? WHERE id = ?", (tema, user_id)
        )


def update_user_rifiuti_posizione(user_id, lat, lon):
    """Ultima posizione GPS nota per questo utente: usata come fallback
    quando la geolocalizzazione del browser è temporaneamente non
    disponibile (non per un semplice diniego permanente del permesso)."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET rifiuti_lat = ?, rifiuti_lon = ?, rifiuti_geo_aggiornato_il = ? WHERE id = ?",
            (lat, lon, now_it().isoformat(), user_id)
        )


def get_user_rifiuti_posizione(user_id):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT rifiuti_lat, rifiuti_lon, rifiuti_geo_aggiornato_il FROM users WHERE id = ?",
            (user_id,)
        )
        return _row(cur)


def update_user_home_card(user_id, home_card):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET home_card = ? WHERE id = ?", (home_card, user_id)
        )


def update_user_dash_cards(user_id, card1, card2, card3):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET dash_card1 = ?, dash_card2 = ?, dash_card3 = ? WHERE id = ?",
            (card1, card2, card3, user_id)
        )


def update_user_tutorial_visto(user_id):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET tutorial_visto = 1 WHERE id = ?", (user_id,)
        )


# ─── CHAT ──────────────────────────────────────────────────────────────────
CHAT_CANALI = ('generale', 'cucina', 'sala', 'pizzeria')


def update_user_chat_permesso(user_id, permesso):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET puo_chat = ? WHERE id = ?", (1 if permesso else 0, user_id)
        )


def get_chat_messaggi(canale, limit=100):
    """Ultimi `limit` messaggi di un canale, in ordine cronologico crescente."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM chat_messaggi WHERE canale = ? ORDER BY id DESC LIMIT ?",
            (canale, limit)
        )
        rows = _rows(cur)
        rows.reverse()
        return rows


def get_chat_preview(canali, limit=4):
    """Ultimi messaggi non eliminati sui canali indicati, per l'anteprima in dashboard."""
    canali = list(canali)
    if not canali:
        return []
    placeholders = ','.join('?' * len(canali))
    with get_conn() as conn:
        cur = conn.execute(
            f"SELECT * FROM chat_messaggi WHERE eliminato = 0 AND canale IN ({placeholders}) "
            f"ORDER BY id DESC LIMIT ?",
            (*canali, limit)
        )
        rows = _rows(cur)
        rows.reverse()
        return rows


def get_chat_messaggio_by_id(msg_id):
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM chat_messaggi WHERE id = ?", (msg_id,))
        return _row(cur)


def insert_chat_messaggio(canale, utente_id, utente_nome, testo, foto='', autore_badge=''):
    with get_conn() as conn:
        return conn.execute_insert(
            "INSERT INTO chat_messaggi (canale, utente_id, utente_nome, testo, foto, autore_badge, creato_il) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (canale, utente_id, utente_nome, testo, foto, autore_badge, now_it().isoformat())
        )


def update_chat_messaggio(msg_id, testo):
    with get_conn() as conn:
        conn.execute(
            "UPDATE chat_messaggi SET testo = ?, modificato = 1 WHERE id = ?",
            (testo, msg_id)
        )


def elimina_chat_messaggio(msg_id):
    with get_conn() as conn:
        conn.execute("UPDATE chat_messaggi SET eliminato = 1 WHERE id = ?", (msg_id,))


def set_chat_canale_letto(utente_id, canale):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chat_canale_letto (utente_id, canale, last_seen) VALUES (?, ?, '')",
            (utente_id, canale)
        )
        conn.execute(
            "UPDATE chat_canale_letto SET last_seen = ? WHERE utente_id = ? AND canale = ?",
            (now_it().isoformat(), utente_id, canale)
        )


def get_chat_unread_counts(utente_id, canali):
    """Dict {canale: numero di messaggi di altri utenti arrivati dopo l'ultima
    visita dell'utente a quel canale}, per i soli canali indicati."""
    canali = list(canali)
    if not canali:
        return {}
    with get_conn() as conn:
        placeholders = ','.join('?' * len(canali))
        cur = conn.execute(
            f"SELECT canale, last_seen FROM chat_canale_letto "
            f"WHERE utente_id = ? AND canale IN ({placeholders})",
            (utente_id, *canali)
        )
        seen = {r['canale']: r['last_seen'] for r in _rows(cur)}

        result = {}
        for canale in canali:
            last_seen = seen.get(canale, '')
            if last_seen:
                cur2 = conn.execute(
                    "SELECT COUNT(*) AS c FROM chat_messaggi "
                    "WHERE canale = ? AND eliminato = 0 AND utente_id != ? AND creato_il > ?",
                    (canale, utente_id, last_seen)
                )
            else:
                cur2 = conn.execute(
                    "SELECT COUNT(*) AS c FROM chat_messaggi "
                    "WHERE canale = ? AND eliminato = 0 AND utente_id != ?",
                    (canale, utente_id)
                )
            row = _row(cur2)
            result[canale] = row['c'] if row else 0
        return result


def log_chat_azione(azione, canale, autore_originale, testo_originale, operatore):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO log_chat_messaggi "
            "(azione, canale, autore_originale, testo_originale, operatore, quando) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (azione, canale, autore_originale, testo_originale, operatore, now_it().isoformat())
        )


def get_log_chat_messaggi(limit=200):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM log_chat_messaggi ORDER BY id DESC LIMIT ?", (limit,)
        )
        return _rows(cur)


# ─── RACCOLTA RIFIUTI ────────────────────────────────────────────────────────

def get_impostazione(chiave, default=''):
    with get_conn() as conn:
        cur = conn.execute("SELECT valore FROM impostazioni_app WHERE chiave = ?", (chiave,))
        row = _row(cur)
        return row['valore'] if row else default


def set_impostazione(chiave, valore):
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO impostazioni_app (chiave, valore) VALUES (?, '')", (chiave,))
        conn.execute("UPDATE impostazioni_app SET valore = ? WHERE chiave = ?", (valore, chiave))


def get_raccolta_calendario(zona):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT giorno_settimana, tipo_rifiuto FROM raccolta_rifiuti_calendario "
            "WHERE zona = ? ORDER BY giorno_settimana",
            (zona,)
        )
        return _rows(cur)


# ─── PUSH NOTIFICATIONS ──────────────────────────────────────────────────────

def _b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def get_or_create_vapid_keys():
    """Coppia di chiavi VAPID (pubblica, privata) per le Web Push, generata
    una sola volta e persistita in impostazioni_app — mai hardcoded nel
    codice sorgente."""
    pub  = get_impostazione('vapid_public_key', '')
    priv = get_impostazione('vapid_private_key', '')
    if pub and priv:
        return pub, priv

    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    from cryptography.hazmat.backends import default_backend

    priv_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
    priv_raw = priv_key.private_numbers().private_value.to_bytes(32, 'big')
    pub_raw  = priv_key.public_key().public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)

    pub, priv = _b64url(pub_raw), _b64url(priv_raw)
    set_impostazione('vapid_public_key', pub)
    set_impostazione('vapid_private_key', priv)
    return pub, priv


def save_push_subscription(utente_id, subscription_json, endpoint):
    with get_conn() as conn:
        conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
        conn.execute(
            "INSERT INTO push_subscriptions (utente_id, subscription_json, endpoint, created_at) "
            "VALUES (?, ?, ?, ?)",
            (utente_id, subscription_json, endpoint, now_it().isoformat())
        )


def delete_push_subscription(endpoint):
    with get_conn() as conn:
        conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))


def get_push_subscriptions_by_users(utente_ids):
    utente_ids = list(utente_ids)
    if not utente_ids:
        return []
    placeholders = ','.join('?' * len(utente_ids))
    with get_conn() as conn:
        cur = conn.execute(
            f"SELECT id, utente_id, subscription_json, endpoint FROM push_subscriptions "
            f"WHERE utente_id IN ({placeholders})",
            tuple(utente_ids)
        )
        return _rows(cur)


def get_all_push_subscriptions():
    with get_conn() as conn:
        cur = conn.execute("SELECT id, utente_id, subscription_json, endpoint FROM push_subscriptions")
        return _rows(cur)


# ─── METEO ────────────────────────────────────────────────────────────────
# Log diagnostico temporaneo: ad ogni aggiornamento reale (non da cache) del
# meteo in dashboard salviamo temperatura e weather_code ricevuti dall'API,
# per confrontarli nei prossimi giorni con il meteo reale di Vigevano e
# decidere se serve una correzione (vedi conversazione con l'utente).

def log_meteo_lettura(temperatura, weather_code):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO log_meteo (quando, temperatura, weather_code) VALUES (?, ?, ?)",
            (now_it().isoformat(), temperatura, weather_code)
        )


def get_log_meteo(limit=500):
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM log_meteo ORDER BY id DESC LIMIT ?", (limit,))
        return _rows(cur)


def get_ultimo_carico():
    """Ultimo movimento di tipo CARICO registrato (qualsiasi prodotto),
    per la card 'Ultimo carico' della dashboard admin."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT prodotto, creato_il, data FROM registro "
            "WHERE tipo = 'CARICO' ORDER BY id DESC LIMIT 1"
        )
        return _row(cur)


# ─── APPARECCHI / TEMPERATURE ─────────────────────────────────────────────────

# Range di riferimento HACCP per tipo apparecchio (usati per pre-compilare il
# form di creazione; l'admin può comunque personalizzarli per ogni apparecchio).
RANGE_RIFERIMENTO_TIPO = {
    'Frigorifero':  (0, 4),
    'Congelatore':  (-25, -18),
    'Abbattitore':  (0, 0),
}


def turno_e_data_riferimento(now=None):
    """
    Determina il turno di controllo corrente ('mattina' | 'sera') e la data
    a cui quel controllo si riferisce.

    Mattina: 08:00-20:59, riferita al giorno corrente.
    Sera: 21:00-07:59, riferita al giorno in cui la sera è iniziata (quindi
    tra mezzanotte e le 07:59 si fa ancora riferimento al giorno precedente,
    perché la sera "di oggi" comincia solo alle 21:00).

    Gli orari sono sempre riferiti al fuso orario italiano (Europe/Rome),
    indipendentemente dal fuso orario del server.
    """
    now = now or now_it()
    if 8 <= now.hour < 21:
        return 'mattina', now.date()
    if now.hour >= 21:
        return 'sera', now.date()
    return 'sera', now.date() - timedelta(days=1)


def _stato_controllo(row, now=None):
    turno, data_rif = turno_e_data_riferimento(now)
    ultima = row.get('ultima_mattina' if turno == 'mattina' else 'ultima_sera') or ''
    return 'OK' if ultima == data_rif.isoformat() else 'DA_CONTROLLARE'


def get_apparecchi(solo_attivi=True, reparto=None):
    with get_conn() as conn:
        sql = ("SELECT id, nome, tipo, reparto, marca, modello, seriale, "
               "temp_min, temp_max, attivo, ultima_mattina, ultima_sera FROM apparecchi")
        clauses, params = [], []
        if solo_attivi:
            clauses.append("attivo = 1")
        if reparto:
            clauses.append("reparto = ?")
            params.append(reparto)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY LOWER(nome)"
        cur = conn.execute(sql, tuple(params))
        rows = _rows(cur)
        for r in rows:
            r['stato_controllo'] = _stato_controllo(r)
        return rows


def get_apparecchio_by_id(apparecchio_id):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, nome, tipo, reparto, marca, modello, seriale, "
            "temp_min, temp_max, attivo, ultima_mattina, ultima_sera FROM apparecchi WHERE id = ?",
            (apparecchio_id,)
        )
        return _row(cur)


def get_apparecchio_by_nome(nome):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, nome, tipo, reparto, marca, modello, seriale, "
            "temp_min, temp_max, attivo, ultima_mattina, ultima_sera FROM apparecchi WHERE nome = ?",
            (nome,)
        )
        return _row(cur)


def registra_controllo_apparecchio(apparecchio_id, now=None):
    """Segna il turno corrente (mattina/sera) come controllato oggi per
    questo apparecchio. Chiamata dopo ogni registrazione di temperatura."""
    turno, data_rif = turno_e_data_riferimento(now)
    campo = 'ultima_mattina' if turno == 'mattina' else 'ultima_sera'
    with get_conn() as conn:
        conn.execute(f"UPDATE apparecchi SET {campo} = ? WHERE id = ?",
                     (data_rif.isoformat(), apparecchio_id))


def create_apparecchio(nome, tipo, temp_min, temp_max, reparto='Cucina', marca='', modello='', seriale=''):
    with get_conn() as conn:
        return conn.execute_insert(
            "INSERT INTO apparecchi (nome, tipo, reparto, marca, modello, seriale, temp_min, temp_max, attivo) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
            (nome, tipo, reparto, marca, modello, seriale, float(temp_min), float(temp_max))
        )


def update_apparecchio(apparecchio_id, nome, tipo, temp_min, temp_max, reparto='Cucina', marca='', modello='', seriale=''):
    with get_conn() as conn:
        conn.execute(
            "UPDATE apparecchi SET nome = ?, tipo = ?, reparto = ?, marca = ?, modello = ?, "
            "seriale = ?, temp_min = ?, temp_max = ? WHERE id = ?",
            (nome, tipo, reparto, marca, modello, seriale, float(temp_min), float(temp_max), apparecchio_id)
        )


def delete_apparecchio(apparecchio_id):
    """Soft delete: le rilevazioni storiche restano intatte."""
    with get_conn() as conn:
        conn.execute("UPDATE apparecchi SET attivo = 0 WHERE id = ?", (apparecchio_id,))


def insert_temperatura(row):
    with get_conn() as conn:
        return conn.execute_insert(
            """INSERT INTO temperature
               (apparecchio, tipo, data, ora, temperatura, temp_min, temp_max, esito, nota, operatore, synced)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (row['apparecchio'], row['tipo'], row['data'], row['ora'],
             row['temperatura'], row['temp_min'], row['temp_max'],
             row['esito'], row.get('nota', ''), row['operatore'],
             1 if row.get('synced', True) else 0)
        )


def get_unsynced_temperature():
    """Rilevazioni scritte in locale mentre Google Sheets non era
    raggiungibile, ancora da inviare a Sheets."""
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM temperature WHERE synced = 0")
        return _rows(cur)


def mark_temperatura_synced(temperatura_id):
    with get_conn() as conn:
        conn.execute("UPDATE temperature SET synced = 1 WHERE id = ?", (temperatura_id,))


def get_temperatura_by_id(temperatura_id):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, apparecchio, tipo, data, ora, temperatura, temp_min, temp_max, "
            "esito, nota, operatore FROM temperature WHERE id = ?",
            (temperatura_id,)
        )
        return _row(cur)


def elimina_temperatura(temperatura_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM temperature WHERE id = ?", (temperatura_id,))


def ricalcola_stato_apparecchio(apparecchio_id, apparecchio_nome):
    """Ricalcola ultima_mattina/ultima_sera scandendo le rilevazioni rimaste
    per questo apparecchio. Da chiamare dopo l'eliminazione di una rilevazione,
    così lo stato torna 'DA_CONTROLLARE' se non ce n'erano altre per il turno."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT data, ora FROM temperature WHERE apparecchio = ?",
            (apparecchio_nome,)
        )
        rows = _rows(cur)

    ultima_mattina, ultima_sera = '', ''
    for r in rows:
        try:
            y, m, d = (int(x) for x in r['data'].split('-'))
            hh, mm = (int(x) for x in (r['ora'] or '00:00').split(':'))
            dt = datetime(y, m, d, hh, mm)
        except (ValueError, TypeError):
            continue
        turno, data_rif = turno_e_data_riferimento(dt)
        rif_iso = data_rif.isoformat()
        if turno == 'mattina':
            ultima_mattina = max(ultima_mattina, rif_iso)
        else:
            ultima_sera = max(ultima_sera, rif_iso)

    with get_conn() as conn:
        conn.execute("UPDATE apparecchi SET ultima_mattina = ?, ultima_sera = ? WHERE id = ?",
                     (ultima_mattina, ultima_sera, apparecchio_id))


def log_eliminazione_temperatura(temp_row, eliminato_da):
    # Timestamp di log (metadato "quando è avvenuta l'azione"): salvato come
    # istante UTC esplicito (con offset), non come ora italiana ingenua, così
    # il frontend può convertirlo correttamente con
    # new Date(iso).toLocaleString('it-IT', {timeZone:'Europe/Rome'}).
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO log_eliminazioni_temperature
               (apparecchio, temperatura, temp_min, temp_max, esito,
                data_originale, ora_originale, operatore_originale, eliminato_da, eliminato_il)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (temp_row['apparecchio'], temp_row['temperatura'], temp_row['temp_min'], temp_row['temp_max'],
             temp_row['esito'], temp_row['data'], temp_row['ora'], temp_row['operatore'],
             eliminato_da, datetime.now(timezone.utc).isoformat(timespec='seconds'))
        )


def get_log_eliminazioni_temperature(limit=200):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM log_eliminazioni_temperature ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return _rows(cur)


def log_lista_spesa_azione(azione, prodotto, fornitore, utente):
    # Timestamp di log in UTC esplicito, vedi nota in log_eliminazione_temperatura.
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO log_lista_spesa (azione, prodotto, fornitore, utente, quando) "
            "VALUES (?, ?, ?, ?, ?)",
            (azione, prodotto, fornitore, utente, datetime.now(timezone.utc).isoformat(timespec='seconds'))
        )


def get_log_lista_spesa(limit=200):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM log_lista_spesa ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return _rows(cur)


def update_registro_lotto(row_id, lotto, scadenza, rimanenza):
    with get_conn() as conn:
        conn.execute(
            "UPDATE registro SET lotto = ?, scadenza = ?, rimanenza = ? WHERE id = ?",
            (lotto, scadenza, float(rimanenza), row_id)
        )


def log_modifica_registro(prodotto, lotto, modifiche, utente):
    # Timestamp di log in UTC esplicito, vedi nota in log_eliminazione_temperatura.
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO log_modifiche_registro (prodotto, lotto, modifiche, utente, quando) "
            "VALUES (?, ?, ?, ?, ?)",
            (prodotto, lotto, modifiche, utente, datetime.now(timezone.utc).isoformat(timespec='seconds'))
        )


def get_log_modifiche_registro(limit=200):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM log_modifiche_registro ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return _rows(cur)


def delete_registro_row(row_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM registro WHERE id = ?", (row_id,))


def log_eliminazione_carico(row, eliminato_da):
    # Timestamp di log in UTC esplicito, vedi nota in log_eliminazione_temperatura.
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO log_eliminazioni_registro
               (prodotto, lotto, scadenza, carico, scarico, unita, tipo,
                operatore_originale, data_originale, eliminato_da, eliminato_il)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (row['prodotto'], row['lotto'], row['scadenza'], row['carico'], row['scarico'],
             row['unita'], row.get('tipo', ''), row.get('operatore', ''), row['data'],
             eliminato_da, datetime.now(timezone.utc).isoformat(timespec='seconds'))
        )


def get_log_eliminazioni_registro(limit=200):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM log_eliminazioni_registro ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return _rows(cur)


def get_temperature_storico(giorni=30, apparecchio=None):
    limite = (today_it() - timedelta(days=giorni)).isoformat()
    with get_conn() as conn:
        sql = ("SELECT id, apparecchio, tipo, data, ora, temperatura, temp_min, temp_max, "
               "esito, nota, operatore FROM temperature WHERE data >= ?")
        params = [limite]
        if apparecchio:
            sql += " AND apparecchio = ?"
            params.append(apparecchio)
        sql += " ORDER BY data DESC, ora DESC, id DESC"
        cur = conn.execute(sql, tuple(params))
        return _rows(cur)


def replace_temperatura(rows):
    """Ricarica lo storico temperature dal foglio Google (usato da /api/sync)."""
    with get_conn() as conn:
        conn.execute("DELETE FROM temperature")
        if rows:
            conn.executemany(
                """INSERT INTO temperature
                   (apparecchio, tipo, data, ora, temperatura, temp_min, temp_max, esito, nota, operatore)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(r['apparecchio'], r.get('tipo', ''), r['data'], r.get('ora', ''),
                  r['temperatura'], r['temp_min'], r['temp_max'],
                  r['esito'], r.get('nota', ''), r['operatore']) for r in rows]
            )


def update_user_apparecchi_permesso(user_id, permesso):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET gestisce_apparecchi = ? WHERE id = ?",
            (1 if permesso else 0, user_id)
        )


# ─── EXPORT PDF ───────────────────────────────────────────────────────────────

def get_registro_by_mese(anno, mese):
    """Righe di registro del mese/anno indicato, per l'export PDF del
    Registro movimenti."""
    pattern = f"{anno:04d}-{mese:02d}-%"
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT data, prodotto, lotto, scadenza, carico, scarico, unita, "
            "operatore, reparto, tipo FROM registro WHERE data LIKE ? "
            "ORDER BY data, id",
            (pattern,)
        )
        return _rows(cur)


def get_temperature_by_mese(anno, mese):
    """Rilevazioni temperature del mese/anno indicato, per l'export PDF del
    Registro temperature."""
    pattern = f"{anno:04d}-{mese:02d}-%"
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT data, ora, apparecchio, temperatura, esito, operatore "
            "FROM temperature WHERE data LIKE ? ORDER BY data, ora",
            (pattern,)
        )
        return _rows(cur)


def get_report_mensile_anno(anno):
    """Quantità scaricata (consumata) per prodotto e mese nell'anno indicato,
    per l'export PDF del Report mensile — stesso calcolo del foglio
    REPORT MENSILE di Google Sheets, ma dal DB locale."""
    pattern = f"{anno:04d}-%"
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT prodotto, substr(data, 6, 2) AS mese, SUM(scarico) AS tot "
            "FROM registro WHERE data LIKE ? GROUP BY prodotto, mese",
            (pattern,)
        )
        rows = _rows(cur)
        prodotti_rows = _rows(conn.execute(
            "SELECT prodotto FROM listino ORDER BY LOWER(prodotto)"
        ))

    per_prodotto = {r['prodotto']: {m: 0.0 for m in range(1, 13)} for r in prodotti_rows}
    for r in rows:
        p = r['prodotto']
        if p not in per_prodotto:
            per_prodotto[p] = {m: 0.0 for m in range(1, 13)}
        try:
            mese = int(r['mese'])
        except (TypeError, ValueError):
            continue
        if 1 <= mese <= 12:
            per_prodotto[p][mese] = round(float(r['tot'] or 0), 3)

    return [{'prodotto': p, 'mesi': mesi} for p, mesi in per_prodotto.items()]


# ─── PREPARAZIONI ─────────────────────────────────────────────────────────────

def insert_preparazione(nome, reparto, data_preparazione, scadenza, quantita, unita, operatore_id, note):
    with get_conn() as conn:
        return conn.execute_insert(
            "INSERT INTO preparazioni (nome, reparto, data_preparazione, scadenza, "
            "quantita, unita, operatore_id, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (nome, reparto, data_preparazione, scadenza or '', float(quantita or 0),
             unita or '', operatore_id, note or '')
        )


def insert_preparazione_ingrediente(preparazione_id, prodotto, lotto, quantita, unita):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO preparazione_ingredienti (preparazione_id, prodotto, lotto, quantita, unita) "
            "VALUES (?, ?, ?, ?, ?)",
            (preparazione_id, prodotto, lotto or '', float(quantita or 0), unita or '')
        )


def get_preparazioni(limit=200):
    """Elenco preparazioni (senza ingredienti), più recenti prima, con il
    nome dell'operatore risolto da users.id."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT p.id, p.nome, p.reparto, p.data_preparazione, p.scadenza, "
            "p.quantita, p.unita, p.note, p.operatore_id, p.completata, "
            "COALESCE(u.nome, '—') AS operatore "
            "FROM preparazioni p LEFT JOIN users u ON u.id = p.operatore_id "
            "ORDER BY p.id DESC LIMIT ?",
            (limit,)
        )
        return _rows(cur)


def get_preparazione_by_id(prep_id):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT p.id, p.nome, p.reparto, p.data_preparazione, p.scadenza, "
            "p.quantita, p.unita, p.note, p.operatore_id, p.completata, "
            "COALESCE(u.nome, '—') AS operatore "
            "FROM preparazioni p LEFT JOIN users u ON u.id = p.operatore_id "
            "WHERE p.id = ?",
            (prep_id,)
        )
        return _row(cur)


def completa_preparazione(prep_id):
    """Segna una preparazione come completata (consumata interamente)."""
    with get_conn() as conn:
        conn.execute("UPDATE preparazioni SET completata = 1 WHERE id = ?", (prep_id,))


def get_preparazione_ingredienti(preparazione_id):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, prodotto, lotto, quantita, unita FROM preparazione_ingredienti "
            "WHERE preparazione_id = ? ORDER BY id",
            (preparazione_id,)
        )
        return _rows(cur)


# ─── SALE ──────────────────────────────────────────────────────────────────

def get_sale():
    with get_conn() as conn:
        cur = conn.execute("SELECT id, nome FROM sale ORDER BY LOWER(nome)")
        return _rows(cur)


def create_sala(nome):
    with get_conn() as conn:
        return conn.execute_insert("INSERT INTO sale (nome) VALUES (?)", (nome,))


def update_sala(sala_id, nome):
    with get_conn() as conn:
        conn.execute("UPDATE sale SET nome = ? WHERE id = ?", (nome, sala_id))


def delete_sala(sala_id):
    """Ritorna False (senza eliminare nulla) se la sala ha ancora tavoli
    attivi — il chiamante deve bloccare l'operazione in quel caso."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) AS n FROM tavoli WHERE sala_id = ? AND attivo = 1", (sala_id,)
        )
        if _row(cur)['n'] > 0:
            return False
        conn.execute("DELETE FROM sale WHERE id = ?", (sala_id,))
        return True


# ─── TAVOLI ────────────────────────────────────────────────────────────────

def get_tavoli(sala_id=None):
    """Elenco tavoli attivi con stato calcolato in tempo reale:
    'occupato' (toggle manuale del cameriere) > 'prenotato' (c'è una
    prenotazione oggi non cancellata collegata) > 'libero'."""
    with get_conn() as conn:
        sql = ("SELECT t.id, t.numero, t.forma, t.posti, t.sala_id, "
               "t.posizione_x, t.posizione_y, t.occupato, s.nome AS sala_nome "
               "FROM tavoli t JOIN sale s ON s.id = t.sala_id WHERE t.attivo = 1")
        params = []
        if sala_id:
            sql += " AND t.sala_id = ?"
            params.append(sala_id)
        sql += " ORDER BY t.numero"
        tavoli = _rows(conn.execute(sql, tuple(params)))

        oggi = today_it().isoformat()
        prenotati_rows = _rows(conn.execute(
            "SELECT DISTINCT ct.tavolo_id FROM combinazioni_tavoli ct "
            "JOIN prenotazioni p ON p.id = ct.prenotazione_id "
            "WHERE p.data = ? AND p.stato != 'cancellata'",
            (oggi,)
        ))
    prenotati_ids = {r['tavolo_id'] for r in prenotati_rows}

    for t in tavoli:
        if t['occupato']:
            t['stato'] = 'occupato'
        elif t['id'] in prenotati_ids:
            t['stato'] = 'prenotato'
        else:
            t['stato'] = 'libero'
    return tavoli


def get_tavolo_by_id(tavolo_id):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, numero, forma, posti, sala_id, posizione_x, posizione_y, "
            "occupato, attivo FROM tavoli WHERE id = ?",
            (tavolo_id,)
        )
        return _row(cur)


def create_tavolo(numero, forma, posti, sala_id, posizione_x=40, posizione_y=40):
    with get_conn() as conn:
        return conn.execute_insert(
            "INSERT INTO tavoli (numero, forma, posti, sala_id, posizione_x, posizione_y) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (numero, forma, posti, sala_id, posizione_x, posizione_y)
        )


_TAVOLO_CAMPI = ('numero', 'forma', 'posti', 'sala_id', 'posizione_x', 'posizione_y', 'occupato')


def update_tavolo(tavolo_id, **campi):
    """Update parziale: passa solo i campi da modificare, es.
    update_tavolo(5, posizione_x=120, posizione_y=80) oppure
    update_tavolo(5, occupato=1)."""
    sets, params = [], []
    for k, v in campi.items():
        if k not in _TAVOLO_CAMPI:
            continue
        sets.append(f"{k} = ?")
        params.append(v)
    if not sets:
        return
    params.append(tavolo_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE tavoli SET {', '.join(sets)} WHERE id = ?", tuple(params))


def delete_tavolo(tavolo_id):
    """Soft delete (le prenotazioni storiche restano intatte) + rimuove le
    combinazioni tavolo↔prenotazione collegate a questo tavolo."""
    with get_conn() as conn:
        conn.execute("UPDATE tavoli SET attivo = 0 WHERE id = ?", (tavolo_id,))
        conn.execute("DELETE FROM combinazioni_tavoli WHERE tavolo_id = ?", (tavolo_id,))


# ─── PRENOTAZIONI ──────────────────────────────────────────────────────────

def get_prenotazioni(data):
    with get_conn() as conn:
        prenotazioni = _rows(conn.execute(
            "SELECT p.id, p.nome, p.persone, p.data, p.ora, p.telefono, p.note, "
            "p.stato, p.operatore_id, COALESCE(u.nome, '—') AS operatore "
            "FROM prenotazioni p LEFT JOIN users u ON u.id = p.operatore_id "
            "WHERE p.data = ? ORDER BY p.ora, p.id",
            (data,)
        ))
        if prenotazioni:
            ids = [p['id'] for p in prenotazioni]
            placeholders = ','.join('?' * len(ids))
            tavoli_rows = _rows(conn.execute(
                f"SELECT ct.prenotazione_id, t.id, t.numero, t.sala_id "
                f"FROM combinazioni_tavoli ct JOIN tavoli t ON t.id = ct.tavolo_id "
                f"WHERE ct.prenotazione_id IN ({placeholders})",
                tuple(ids)
            ))
        else:
            tavoli_rows = []

    tavoli_per_prenot = {}
    for r in tavoli_rows:
        tavoli_per_prenot.setdefault(r['prenotazione_id'], []).append(
            {'id': r['id'], 'numero': r['numero'], 'sala_id': r['sala_id']}
        )
    for p in prenotazioni:
        p['tavoli'] = tavoli_per_prenot.get(p['id'], [])
    return prenotazioni


def get_prenotazione_by_id(prenotazione_id):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT p.id, p.nome, p.persone, p.data, p.ora, p.telefono, p.note, "
            "p.stato, p.operatore_id, COALESCE(u.nome, '—') AS operatore "
            "FROM prenotazioni p LEFT JOIN users u ON u.id = p.operatore_id "
            "WHERE p.id = ?",
            (prenotazione_id,)
        )
        prenotazione = _row(cur)
        if not prenotazione:
            return None
        tavoli = _rows(conn.execute(
            "SELECT t.id, t.numero, t.sala_id FROM combinazioni_tavoli ct "
            "JOIN tavoli t ON t.id = ct.tavolo_id WHERE ct.prenotazione_id = ?",
            (prenotazione_id,)
        ))
    prenotazione['tavoli'] = tavoli
    return prenotazione


def create_prenotazione(nome, persone, data, ora, telefono, note, operatore_id, tavoli_ids):
    with get_conn() as conn:
        prenotazione_id = conn.execute_insert(
            "INSERT INTO prenotazioni (nome, persone, data, ora, telefono, note, stato, operatore_id) "
            "VALUES (?, ?, ?, ?, ?, ?, 'confermata', ?)",
            (nome, persone, data, ora, telefono, note, operatore_id)
        )
        for tavolo_id in tavoli_ids:
            conn.execute(
                "INSERT INTO combinazioni_tavoli (prenotazione_id, tavolo_id) VALUES (?, ?)",
                (prenotazione_id, tavolo_id)
            )
    return prenotazione_id


def update_prenotazione(prenotazione_id, nome, persone, data, ora, telefono, note, stato, tavoli_ids):
    with get_conn() as conn:
        conn.execute(
            "UPDATE prenotazioni SET nome = ?, persone = ?, data = ?, ora = ?, "
            "telefono = ?, note = ?, stato = ? WHERE id = ?",
            (nome, persone, data, ora, telefono, note, stato, prenotazione_id)
        )
        conn.execute("DELETE FROM combinazioni_tavoli WHERE prenotazione_id = ?", (prenotazione_id,))
        for tavolo_id in tavoli_ids:
            conn.execute(
                "INSERT INTO combinazioni_tavoli (prenotazione_id, tavolo_id) VALUES (?, ?)",
                (prenotazione_id, tavolo_id)
            )


def delete_prenotazione(prenotazione_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM combinazioni_tavoli WHERE prenotazione_id = ?", (prenotazione_id,))
        conn.execute("DELETE FROM prenotazioni WHERE id = ?", (prenotazione_id,))


def get_coperti_giorno(data):
    """Coperti totali/pranzo/cena per la data indicata (esclude le
    prenotazioni cancellate) e coperti rimasti rispetto alla capienza
    totale della sala (somma posti di tutti i tavoli attivi)."""
    with get_conn() as conn:
        rows = _rows(conn.execute(
            "SELECT persone, ora FROM prenotazioni WHERE data = ? AND stato != 'cancellata'",
            (data,)
        ))
        capienza_row = _row(conn.execute(
            "SELECT COALESCE(SUM(posti), 0) AS tot FROM tavoli WHERE attivo = 1"
        ))

    pranzo, cena = 0, 0
    for r in rows:
        if (r['ora'] or '') < PRANZO_CENA_CUTOFF:
            pranzo += r['persone']
        else:
            cena += r['persone']

    capienza = capienza_row['tot'] if capienza_row else 0
    return {
        'totale':          pranzo + cena,
        'pranzo':          {'prenotati': pranzo, 'rimasti': max(0, capienza - pranzo)},
        'cena':            {'prenotati': cena,   'rimasti': max(0, capienza - cena)},
        'capienza_totale': capienza,
    }


def log_eliminazione_prenotazione(prenotazione, eliminato_da):
    # Timestamp di log in UTC esplicito, vedi nota in log_eliminazione_temperatura.
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO log_eliminazioni_prenotazioni
               (nome, persone, data_prenot, ora, operatore_orig, eliminato_da, eliminato_il)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (prenotazione['nome'], prenotazione['persone'], prenotazione['data'], prenotazione['ora'],
             prenotazione.get('operatore', ''), eliminato_da,
             datetime.now(timezone.utc).isoformat(timespec='seconds'))
        )


def get_log_eliminazioni_prenotazioni(limit=200):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM log_eliminazioni_prenotazioni ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return _rows(cur)


# ─── LOCK MOVIMENTI ─────────────────────────────────────────────────────────
# Lock "soft" (avviso, non blocco rigido) su un prodotto+lotto o un
# apparecchio: quando un utente apre un form di movimento (Carico, Scarico,
# In uso, Preparazioni, Temperature) la risorsa viene bloccata per
# LOCK_DURATA_MINUTI, così un collega che prova ad aprire lo stesso form
# sulla stessa risorsa viene avvisato di chi ci sta già lavorando.

def _purge_lock_scaduti():
    with get_conn() as conn:
        conn.execute("DELETE FROM lock_movimenti WHERE scade_il < ?", (now_it().isoformat(),))


def get_lock_attivo(tipo, prodotto='', lotto='', apparecchio_id=0):
    """Lock attivo per questa risorsa, di qualunque utente (None se libera)."""
    _purge_lock_scaduti()
    with get_conn() as conn:
        if tipo == 'apparecchio':
            cur = conn.execute(
                "SELECT * FROM lock_movimenti WHERE tipo = 'apparecchio' AND apparecchio_id = ?",
                (apparecchio_id,)
            )
        else:
            cur = conn.execute(
                "SELECT * FROM lock_movimenti WHERE tipo = 'prodotto_lotto' AND prodotto = ? AND lotto = ?",
                (prodotto, lotto)
            )
        return _row(cur)


def acquire_lock(tipo, user_id, user_nome, form, prodotto='', lotto='', apparecchio_id=0, force=False):
    """Prova ad acquisire il lock. Se già bloccato da un altro utente e non
    force, ritorna il lock esistente senza modificarlo. Altrimenti (risorsa
    libera, scaduta, già propria, o force) sostituisce il lock con uno nuovo
    per l'utente corrente."""
    esistente = get_lock_attivo(tipo, prodotto, lotto, apparecchio_id)
    if esistente and int(esistente['user_id']) != int(user_id) and not force:
        return {'acquired': False, 'lock': esistente}

    with get_conn() as conn:
        if tipo == 'apparecchio':
            conn.execute(
                "DELETE FROM lock_movimenti WHERE tipo = 'apparecchio' AND apparecchio_id = ?",
                (apparecchio_id,)
            )
        else:
            conn.execute(
                "DELETE FROM lock_movimenti WHERE tipo = 'prodotto_lotto' AND prodotto = ? AND lotto = ?",
                (prodotto, lotto)
            )
        ora    = now_it()
        scade  = ora + timedelta(minutes=LOCK_DURATA_MINUTI)
        conn.execute(
            """INSERT INTO lock_movimenti
               (tipo, prodotto, lotto, apparecchio_id, user_id, user_nome, form, creato_il, scade_il)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (tipo, prodotto, lotto, apparecchio_id, user_id, user_nome, form,
             ora.isoformat(), scade.isoformat())
        )
    return {'acquired': True}


def release_lock(tipo, user_id, prodotto='', lotto='', apparecchio_id=0):
    """Rilascia il lock solo se posseduto dallo stesso user_id (idempotente —
    un browser 'in ritardo' non può cancellare per sbaglio il lock fresco di
    qualcun altro)."""
    with get_conn() as conn:
        if tipo == 'apparecchio':
            conn.execute(
                "DELETE FROM lock_movimenti WHERE tipo = 'apparecchio' AND apparecchio_id = ? AND user_id = ?",
                (apparecchio_id, user_id)
            )
        else:
            conn.execute(
                "DELETE FROM lock_movimenti WHERE tipo = 'prodotto_lotto' AND prodotto = ? AND lotto = ? AND user_id = ?",
                (prodotto, lotto, user_id)
            )


def get_locks_attivi():
    """Tutti i lock non scaduti, per il polling del frontend."""
    _purge_lock_scaduti()
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM lock_movimenti")
        return _rows(cur)
