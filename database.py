import os
import re
import sqlite3
from datetime import date, timedelta
from collections import OrderedDict
from urllib.parse import unquote

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
                reparto      TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE TABLE IF NOT EXISTS users (
                id       {pk},
                nome     TEXT NOT NULL,
                email    TEXT,
                telefono TEXT,
                password TEXT NOT NULL,
                reparto  TEXT NOT NULL DEFAULT '',
                role     TEXT NOT NULL DEFAULT 'staff'
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
                quantita   REAL NOT NULL DEFAULT 0,
                unita      TEXT NOT NULL DEFAULT '',
                completato INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )""",
            "CREATE INDEX IF NOT EXISTS idx_reg_prod_lotto ON registro(prodotto, lotto)",
            "CREATE INDEX IF NOT EXISTS idx_reg_mov_id     ON registro(movimento_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email    ON users(email)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_telefono ON users(telefono)",
        ]:
            conn.execute(stmt)

    # ── Step 2: migrations — each in its own connection/transaction ────────────
    # On PostgreSQL a failed statement aborts the whole transaction, so we use
    # ADD COLUMN IF NOT EXISTS (PG 9.6+) which never fails.
    # On SQLite we keep the try/except approach.
    migrations = [
        ('registro', 'operatore', "TEXT NOT NULL DEFAULT ''"),
        ('registro', 'reparto',   "TEXT NOT NULL DEFAULT ''"),
        ('listino',  'categoria', "TEXT NOT NULL DEFAULT ''"),
        ('users',    'username',  "TEXT NOT NULL DEFAULT ''"),
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
                "INSERT OR IGNORE INTO listino (prodotto, fornitore, unita, scorta_min, categoria) "
                "VALUES (?, ?, ?, ?, ?)",
                [(r['prodotto'], r['fornitore'], r['unita'], r['scorta_min'], r.get('categoria', '')) for r in rows]
            )


def insert_listino_row(prodotto, fornitore, unita, scorta_min, categoria):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO listino (prodotto, fornitore, unita, scorta_min, categoria) "
            "VALUES (?, ?, ?, ?, ?)",
            (prodotto, fornitore, unita, float(scorta_min or 0), categoria or '')
        )


def get_fornitori():
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT DISTINCT fornitore FROM listino WHERE fornitore != '' ORDER BY fornitore"
        )
        return [r['fornitore'] for r in cur.fetchall()]


def get_all_prodotti():
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT prodotto, fornitore, unita, scorta_min, categoria FROM listino ORDER BY prodotto"
        )
        return _rows(cur)


def get_prodotti_by_fornitore(fornitore):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT prodotto, fornitore, unita, scorta_min, categoria FROM listino "
            "WHERE fornitore = ? ORDER BY prodotto",
            (fornitore,)
        )
        return _rows(cur)


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
                operatore, reparto)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                row.get('gs_row'), row['data'], row['fornitore'], row['prodotto'],
                row['lotto'], row['scadenza'], row['carico'], row['scarico'],
                row['unita'], row['etichetta'], row['movimento_id'], row['rimanenza'],
                row.get('operatore', ''), row.get('reparto', ''),
            )
        )


def get_lotti_attivi(prodotto):
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT r.lotto, r.scadenza, r.rimanenza, r.unita, r.fornitore, r.etichetta
               FROM registro r
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
            """SELECT r.prodotto, r.fornitore, r.lotto, r.scadenza,
                      r.rimanenza, r.unita, r.etichetta,
                      l.scorta_min, l.categoria
               FROM registro r
               LEFT JOIN listino l
                   ON l.prodotto = r.prodotto AND l.fornitore = r.fornitore
               WHERE r.id = (
                   SELECT MAX(r2.id) FROM registro r2
                   WHERE r2.prodotto = r.prodotto AND r2.lotto = r.lotto
               ) AND r.rimanenza > 0
               ORDER BY r.prodotto,
                 CASE WHEN r.scadenza = '' OR r.scadenza IS NULL THEN 1 ELSE 0 END,
                 r.scadenza ASC"""
        )
        rows = _rows(cur)

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
                'totale':            0.0,
                'prossima_scadenza': None,
                'lotti':             [],
            }
        g = grouped[p]
        g['totale'] = round(g['totale'] + r['rimanenza'], 4)
        scad = r['scadenza']
        if scad and (not g['prossima_scadenza'] or scad < g['prossima_scadenza']):
            g['prossima_scadenza'] = scad
        g['lotti'].append({
            'lotto':     r['lotto'],
            'scadenza':  r['scadenza'],
            'rimanenza': r['rimanenza'],
            'etichetta': r['etichetta'],
        })

    result = list(grouped.values())
    for g in result:
        g['sotto_scorta'] = g['scorta_min'] > 0 and g['totale'] < g['scorta_min']
    return result


def get_alerts():
    today      = date.today()
    limite_str = (today + timedelta(days=7)).isoformat()

    with get_conn() as conn:
        scad_rows = _rows(conn.execute(
            """SELECT r.prodotto, r.lotto, r.scadenza, r.rimanenza, r.unita, r.fornitore,
                      COALESCE(l.categoria, '') AS categoria
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
                      COALESCE(t.totale, 0) AS totale
               FROM listino l
               LEFT JOIN totali t ON t.prodotto = l.prodotto
               WHERE l.scorta_min > 0 AND COALESCE(t.totale, 0) < l.scorta_min
               ORDER BY l.prodotto"""
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
                    operatore, reparto)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(r.get('gs_row'), r['data'], r['fornitore'], r['prodotto'],
                  r['lotto'], r['scadenza'], r['carico'], r['scarico'],
                  r['unita'], r['etichetta'], r['movimento_id'], r['rimanenza'],
                  r.get('operatore', ''), r.get('reparto', ''))
                 for r in rows]
            )


# ─── USERS ───────────────────────────────────────────────────────────────────

def create_user(nome, username, password_hash, reparto, role='staff'):
    params = (nome, username.strip().lower(), password_hash, reparto, role)
    with get_conn() as conn:
        if _USE_PG:
            conn.execute(
                "INSERT INTO users (nome, username, password, reparto, role) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT (username) DO NOTHING",
                params
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO users (nome, username, password, reparto, role) "
                "VALUES (?, ?, ?, ?, ?)",
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
            "SELECT id, nome, email, telefono, reparto, role FROM users ORDER BY nome"
        )
        return _rows(cur)


def update_user_role(user_id, role):
    with get_conn() as conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))


def delete_user(user_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))


# ─── RESET TOKEN ──────────────────────────────────────────────────────────────

def create_reset_token(user_id, token, expires_at):
    """Invalida eventuali token precedenti e ne crea uno nuovo."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE reset_tokens SET used = 1 WHERE user_id = ?", (user_id,)
        )
        conn.execute(
            "INSERT INTO reset_tokens (user_id, token, expires_at, used) VALUES (?, ?, ?, 0)",
            (user_id, token, expires_at)
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


def add_lista_spesa_item(prodotto, fornitore, categoria, quantita, unita):
    from datetime import datetime
    with get_conn() as conn:
        return conn.execute_insert(
            "INSERT INTO lista_spesa (prodotto, fornitore, categoria, quantita, unita, completato, created_at) "
            "VALUES (?, ?, ?, ?, ?, 0, ?)",
            (prodotto, fornitore or '', categoria or '', float(quantita or 0),
             unita or '', datetime.now().isoformat())
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


def is_contact_taken(identifier, exclude_user_id):
    """Restituisce True se email/telefono è già usata da un altro utente."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id FROM users WHERE (email = ? OR telefono = ?) AND id != ?",
            (identifier, identifier, exclude_user_id)
        )
        return _row(cur) is not None


def get_feed(limit=40):
    """Ultimi movimenti per il feed home."""
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT id, data, fornitore, prodotto, lotto, scadenza,
                      carico, scarico, unita, operatore, reparto, movimento_id
               FROM registro
               ORDER BY id DESC
               LIMIT ?""",
            (limit,)
        )
        return _rows(cur)
