import sqlite3
import os
from datetime import date, timedelta
from collections import OrderedDict

# Su Render il disco persistente è montato in /data
_data_dir = '/data' if os.path.isdir('/data') else os.path.dirname(__file__)
DB_PATH = os.path.join(_data_dir, 'haccp.db')


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def db_init():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS listino (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            prodotto   TEXT NOT NULL,
            fornitore  TEXT NOT NULL DEFAULT '',
            unita      TEXT NOT NULL DEFAULT 'kg',
            scorta_min REAL NOT NULL DEFAULT 0,
            categoria  TEXT NOT NULL DEFAULT '',
            UNIQUE(prodotto, fornitore)
        );

        CREATE TABLE IF NOT EXISTS registro (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
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
        );

        CREATE TABLE IF NOT EXISTS users (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            nome     TEXT NOT NULL,
            email    TEXT,
            telefono TEXT,
            password TEXT NOT NULL,
            reparto  TEXT NOT NULL DEFAULT '',
            role     TEXT NOT NULL DEFAULT 'staff'
        );

        CREATE INDEX IF NOT EXISTS idx_reg_prod_lotto ON registro(prodotto, lotto);
        CREATE INDEX IF NOT EXISTS idx_reg_mov_id     ON registro(movimento_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email    ON users(email);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_telefono ON users(telefono);

        CREATE TABLE IF NOT EXISTS reset_tokens (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            token      TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used       INTEGER NOT NULL DEFAULT 0
        );
        """)

        # Migration: add columns to existing tables
        for table, col, defn in [
            ('registro', 'operatore', "TEXT NOT NULL DEFAULT ''"),
            ('registro', 'reparto',   "TEXT NOT NULL DEFAULT ''"),
            ('listino',  'categoria', "TEXT NOT NULL DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
            except Exception:
                pass  # Column already exists


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


def get_fornitori():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT fornitore FROM listino WHERE fornitore != '' ORDER BY fornitore"
        ).fetchall()
        return [r['fornitore'] for r in rows]


def get_all_prodotti():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT prodotto, fornitore, unita, scorta_min, categoria FROM listino ORDER BY prodotto"
        ).fetchall()
        return [dict(r) for r in rows]


def get_prodotti_by_fornitore(fornitore):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT prodotto, fornitore, unita, scorta_min, categoria FROM listino "
            "WHERE fornitore = ? ORDER BY prodotto",
            (fornitore,)
        ).fetchall()
        return [dict(r) for r in rows]


# ─── REGISTRO ────────────────────────────────────────────────────────────────

def get_ultima_rimanenza(prodotto, lotto):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT rimanenza FROM registro WHERE prodotto = ? AND lotto = ? "
            "ORDER BY id DESC LIMIT 1",
            (prodotto, lotto)
        ).fetchone()
        return float(row['rimanenza']) if row else 0.0


def insert_movimento(row):
    with get_conn() as conn:
        cur = conn.execute(
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
        return cur.lastrowid


def get_lotti_attivi(prodotto):
    with get_conn() as conn:
        rows = conn.execute(
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
        ).fetchall()
        return [dict(r) for r in rows]


def get_giacenze():
    with get_conn() as conn:
        rows = conn.execute(
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
        ).fetchall()

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
        scad_rows = conn.execute(
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
        ).fetchall()

        scorte_rows = conn.execute(
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
        ).fetchall()

    scadenze = []
    for r in scad_rows:
        try:
            giorni = (date.fromisoformat(r['scadenza']) - today).days
        except ValueError:
            giorni = None
        scadenze.append({
            **dict(r),
            'giorni': giorni,
            'tipo':   'scaduto' if (giorni is not None and giorni < 0) else 'in_scadenza',
        })

    return {
        'scadenze': scadenze,
        'scorte':   [dict(r) for r in scorte_rows],
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

def create_user(nome, email, telefono, password_hash, reparto, role='staff'):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO users (nome, email, telefono, password, reparto, role) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (nome, email or None, telefono or None, password_hash, reparto, role)
        )


def get_user_by_login(identifier):
    """Find user by email or telefono."""
    identifier = (identifier or '').strip()
    if not identifier:
        return None
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE email = ? OR telefono = ? LIMIT 1",
            (identifier, identifier)
        ).fetchone()
        return dict(row) if row else None


def get_user_by_id(user_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE id = ?", (user_id,)
        ).fetchone()
        return dict(row) if row else None


def get_all_users():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, nome, email, telefono, reparto, role FROM users ORDER BY nome"
        ).fetchall()
        return [dict(r) for r in rows]


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
        row = conn.execute(
            "SELECT * FROM reset_tokens WHERE token = ? AND used = 0 LIMIT 1",
            (token,)
        ).fetchone()
        return dict(row) if row else None


def use_reset_token(token_id):
    with get_conn() as conn:
        conn.execute("UPDATE reset_tokens SET used = 1 WHERE id = ?", (token_id,))


def update_user_password(user_id, password_hash):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET password = ? WHERE id = ?", (password_hash, user_id)
        )


def update_user_profile(user_id, nome, email, telefono):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET nome = ?, email = ?, telefono = ? WHERE id = ?",
            (nome, email or None, telefono or None, user_id)
        )


def is_contact_taken(identifier, exclude_user_id):
    """Restituisce True se email/telefono è già usata da un altro utente."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM users WHERE (email = ? OR telefono = ?) AND id != ?",
            (identifier, identifier, exclude_user_id)
        ).fetchone()
        return row is not None


def get_feed(limit=40):
    """Ultimi movimenti per il feed home."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, data, fornitore, prodotto, lotto, scadenza,
                   carico, scarico, unita, operatore, reparto, movimento_id
            FROM registro
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
