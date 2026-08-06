import os
import re
import sys
import json
import random
import string
import base64
import uuid
import threading
from datetime import datetime, timedelta
from functools import wraps

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

# stdout non è un TTY quando il processo gira dietro gunicorn/gestori di
# processo (Render compreso): per default Python lo bufferizza a blocchi
# interi, quindi i print() di diagnostica (push, scheduler, meteo...)
# possono restare invisibili nei log per minuti o non comparire affatto
# prima che il buffer si riempia. line_buffering li rende visibili subito.
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)

from timezone_utils import now_it, today_it, TZ_ITALIA

from flask import (
    Flask, render_template, request, jsonify,
    send_from_directory, send_file, redirect, url_for, abort,
)
from flask_login import (
    LoginManager, UserMixin,
    login_user, logout_user, login_required, current_user,
)
from werkzeug.security import generate_password_hash, check_password_hash
from pywebpush import webpush, WebPushException
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from database import (
    db_init,
    replace_listino, replace_registro,
    insert_listino_row,
    get_fornitori, get_categorie, CATEGORIE_FISSE, get_all_prodotti, get_prodotti_by_fornitore, get_reparto_prodotto,
    get_ultima_rimanenza, insert_movimento,
    get_lotti_attivi, get_giacenze, get_alerts,
    get_in_uso_attivi, finalizza_in_uso, get_movimento_by_id,
    create_user, get_user_by_login, get_user_by_nome, get_user_by_id,
    get_all_users, update_user_role, delete_user,
    approva_utente, rifiuta_utente, update_user_controllo_permesso,
    create_reset_token, get_reset_token, use_reset_token, update_user_password,
    get_active_reset_tokens, confirm_reset_token,
    update_user_profile, update_user_reparto, update_user_tema, update_user_home_card, is_username_taken,
    update_user_dash_cards, get_ultimo_carico, update_user_tutorial_visto,
    CHAT_CANALI,
    get_chat_messaggi, get_chat_preview, get_chat_messaggio_by_id, insert_chat_messaggio,
    update_chat_messaggio, elimina_chat_messaggio,
    set_chat_canale_letto, get_chat_unread_counts,
    log_chat_azione, get_log_chat_messaggi,
    RIFIUTI_ZONE_INFO, get_impostazione, set_impostazione, get_raccolta_calendario,
    zona_rifiuti_da_coordinate, update_user_rifiuti_posizione, get_user_rifiuti_posizione,
    get_or_create_vapid_keys, save_push_subscription, delete_push_subscription,
    get_push_subscriptions_by_users, get_all_push_subscriptions,
    log_meteo_lettura,
    get_lista_spesa, add_lista_spesa_item, update_lista_spesa_completato,
    update_lista_spesa_fornitore, delete_lista_spesa_item, clear_lista_spesa,
    get_lista_spesa_item_by_id,
    RANGE_RIFERIMENTO_TIPO, get_apparecchi, get_apparecchio_by_id,
    create_apparecchio, update_apparecchio, delete_apparecchio,
    insert_temperatura, get_temperature_storico, replace_temperatura, registra_controllo_apparecchio,
    update_user_apparecchi_permesso,
    get_temperatura_by_id, elimina_temperatura, ricalcola_stato_apparecchio, get_apparecchio_by_nome,
    log_eliminazione_temperatura, get_log_eliminazioni_temperature,
    log_lista_spesa_azione, get_log_lista_spesa,
    update_registro_lotto, log_modifica_registro, get_log_modifiche_registro,
    get_registro_by_mese, get_temperature_by_mese, get_report_mensile_anno,
    update_user_eliminazione_carichi_permesso,
    delete_registro_row, log_eliminazione_carico, get_log_eliminazioni_registro,
    insert_preparazione, insert_preparazione_ingrediente,
    get_preparazioni, get_preparazione_by_id, get_preparazione_ingredienti,
    completa_preparazione,
    get_sale, create_sala, update_sala, delete_sala, get_or_create_sala_default,
    get_tavoli, get_tavolo_by_id, create_tavolo, update_tavolo, delete_tavolo,
    update_tavolo_occupato,
    get_prenotazioni, get_prenotazione_by_id, create_prenotazione, update_prenotazione,
    update_stato_prenotazione,
    delete_prenotazione, get_coperti_giorno, get_calendario_prenotazioni,
    get_sala_mura, insert_sala_mura,
    get_disposizioni_giornaliere, upsert_disposizione_giornaliera,
    log_eliminazione_prenotazione, get_log_eliminazioni_prenotazioni,
    get_unsynced_registro, mark_registro_synced,
    get_unsynced_listino, mark_listino_synced,
    get_unsynced_temperature, mark_temperatura_synced,
    get_carico_recente,
    acquire_lock, release_lock, get_locks_attivi,
)
from sheets import (
    load_listino, load_registro, append_registro, append_listino, aggiorna_tipo_movimento,
    load_temperatura, append_temperatura, elimina_temperatura_foglio,
    aggiorna_riga_registro, elimina_riga_registro,
    SheetsUnavailableError,
)
from pdf_export import (
    genera_pdf_registro, genera_pdf_temperature, genera_pdf_report_mensile,
)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'haccp-brigade-2024')

# ─── FLASK-LOGIN ──────────────────────────────────────────────────────────────

login_manager = LoginManager(app)
login_manager.login_view = 'login_page'


class User(UserMixin):
    def __init__(self, data):
        self.id       = str(data['id'])
        self.nome     = data['nome']
        self.username = data.get('username') or ''
        self.reparto  = data.get('reparto') or ''
        self.role     = data.get('role', 'staff')
        self.gestisce_apparecchi = bool(data.get('gestisce_apparecchi'))
        self.tema     = data.get('tema') or 'chiaro'
        self.stato    = data.get('stato') or 'attivo'
        self.home_card = data.get('home_card') or 'preparazioni'
        self.dash_card1 = data.get('dash_card1') or 'alert'
        self.dash_card2 = data.get('dash_card2') or 'temperature'
        self.dash_card3 = data.get('dash_card3') or 'scadenza'
        self.tutorial_visto = bool(data.get('tutorial_visto'))
        self.puo_vedere_controllo = bool(data.get('puo_vedere_controllo'))
        self.puo_eliminare_carichi_raw = bool(data.get('puo_eliminare_carichi'))

    @property
    def puo_gestire_apparecchi(self):
        return self.role == 'admin' or self.gestisce_apparecchi

    @property
    def puo_accedere_controllo(self):
        return self.role == 'admin' or self.puo_vedere_controllo

    @property
    def puo_eliminare_carichi(self):
        return self.role == 'admin' or self.puo_eliminare_carichi_raw

    @property
    def puo_gestire_sala(self):
        return self.role == 'admin' or self.reparto == 'Sala'

    @property
    def puo_accedere_chat(self):
        return True

    @property
    def canali_chat(self):
        """Canali chat visibili/accessibili: #generale per tutti, il canale
        del proprio reparto per lo staff, tutti per l'admin."""
        if self.role == 'admin':
            return list(CHAT_CANALI)
        canali = ['generale']
        canali += [c for c, reparto in CHAT_CANALE_REPARTO.items() if reparto == self.reparto]
        return canali


@login_manager.user_loader
def load_user(user_id):
    data = get_user_by_id(int(user_id))
    return User(data) if data else None


@login_manager.unauthorized_handler
def unauthorized():
    if request.path.startswith('/api/'):
        return jsonify({'success': False, 'error': 'Non autenticato'}), 401
    return redirect(url_for('login_page'))


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != 'admin':
            abort(403)
        return f(*args, **kwargs)
    return decorated


def gestione_apparecchi_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.puo_gestire_apparecchi:
            abort(403)
        return f(*args, **kwargs)
    return decorated


def controllo_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.puo_accedere_controllo:
            abort(403)
        return f(*args, **kwargs)
    return decorated


def eliminazione_carichi_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.puo_eliminare_carichi:
            abort(403)
        return f(*args, **kwargs)
    return decorated


def gestione_sala_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.puo_gestire_sala:
            abort(403)
        return f(*args, **kwargs)
    return decorated


def chat_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.puo_accedere_chat:
            abort(403)
        return f(*args, **kwargs)
    return decorated


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _gen_id():
    rand = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{now_it().strftime('%Y%m%d')}-{rand}"


def _gen_etichetta(prodotto, lotto, scadenza):
    return f"PRODOTTO: {prodotto}  LOTTO: {lotto}  SCADENZA: {scadenza or '—'}"


def _fmt_data_it(iso):
    """Converte una data ISO yyyy-mm-dd in dd/mm/yyyy per i messaggi di log."""
    if not iso:
        return '—'
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', iso)
    return f"{m.group(3)}/{m.group(2)}/{m.group(1)}" if m else iso


# ─── AVVIO ────────────────────────────────────────────────────────────────────

db_init()

# Crea utente admin di default se non esiste
if not get_user_by_login('admin'):
    create_user(
        nome='Admin',
        username='admin',
        password_hash=generate_password_hash('admin123', method='pbkdf2:sha256'),
        reparto='',
        role='admin',
    )

# Le chiavi VAPID possono essere fissate da variabili d'ambiente (utile su
# Render per garantire che restino identiche tra un deploy e l'altro anche
# se il disco persistente venisse mai ricreato); in assenza delle env var
# si usano quelle generate una tantum e salvate in impostazioni_app.
_VAPID_PUB_ENV  = os.environ.get('VAPID_PUBLIC_KEY', '')
_VAPID_PRIV_ENV = os.environ.get('VAPID_PRIVATE_KEY', '')
if _VAPID_PUB_ENV and _VAPID_PRIV_ENV:
    VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY = _VAPID_PUB_ENV, _VAPID_PRIV_ENV
    VAPID_SOURCE = 'variabili d\'ambiente'
    print('[Push] chiavi VAPID caricate dalle variabili d\'ambiente')
else:
    VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY = get_or_create_vapid_keys()
    VAPID_SOURCE = 'database (impostazioni_app)'
    print('[Push] chiavi VAPID caricate dal database (impostazioni_app)')
# Il claim "sub" del JWT VAPID deve essere un mailto: o https: con un
# dominio REALE: Apple (web.push.apple.com, usato da Safari/iOS) valida
# questo dominio in modo più severo di Google/Mozilla e rifiuta con
# "BadJwtToken" domini fittizi o riservati come .local/.example/.test o
# @localhost, anche con chiavi e formato perfettamente corretti — è la causa
# reale del BadJwtToken persistente riscontrato su iOS/Safari, non un
# problema di formato o abbinamento delle chiavi (già esclusi).
VAPID_CLAIMS_SUB = os.environ.get('VAPID_SUBJECT', 'mailto:caparrotta99@gmail.com')

try:
    import importlib.metadata as _importlib_metadata
    PYWEBPUSH_VERSION = _importlib_metadata.version('pywebpush')
    print(f"[Push] pywebpush importato correttamente (versione {PYWEBPUSH_VERSION})")
except Exception as _e:
    PYWEBPUSH_VERSION = None
    print(f"[Push] ERRORE: pywebpush non importabile: {_e}")


def _invia_push(subscriptions, title, body, url='/', tag=''):
    """Invia una notifica push a una lista di subscription (righe di
    push_subscriptions). Rimuove automaticamente le subscription scadute
    (endpoint non più valido, es. utente disinstallato/notifiche revocate)."""
    payload = json.dumps({'title': title, 'body': body, 'url': url, 'tag': tag})
    print(f"[Push] invio '{title}' a {len(subscriptions)} subscription...")
    inviate, fallite = 0, 0
    for sub in subscriptions:
        endpoint_short = sub['endpoint'][-24:]
        try:
            webpush(
                subscription_info=json.loads(sub['subscription_json']),
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={'sub': VAPID_CLAIMS_SUB},
            )
            inviate += 1
            print(f"[Push]  ok -> ...{endpoint_short} (utente {sub['utente_id']})")
        except WebPushException as e:
            status = getattr(e.response, 'status_code', None)
            body_text = None
            try:
                body_text = e.response.text
            except Exception:
                pass
            print(f"[Push]  FALLITA -> ...{endpoint_short} (utente {sub['utente_id']}): status={status} {e}"
                  + (f" | risposta push service: {body_text}" if body_text else ""))
            fallite += 1
            if status in (404, 410):
                delete_push_subscription(sub['endpoint'])
                print(f"[Push]  subscription rimossa (endpoint non più valido, status {status})")
            elif status in (401, 403):
                # Il push service rifiuta il token VAPID (es. "BadJwtToken"):
                # quasi sempre perché la chiave pubblica con cui il browser si
                # è iscritto non corrisponde più alla chiave privata con cui
                # firmiamo ora — tipicamente dopo una rotazione delle VAPID
                # key. La subscription resterà rotta per sempre finché non
                # viene ricreata, quindi la rimuoviamo per forzare un nuovo
                # abbonamento dal client con la chiave pubblica corrente.
                delete_push_subscription(sub['endpoint'])
                print(f"[Push]  subscription rimossa (VAPID non valida per status {status} — "
                      f"probabile mismatch chiave pubblica/privata dopo rotazione VAPID)")
        except Exception as e:
            print(f"[Push]  ERRORE invio -> ...{endpoint_short} (utente {sub['utente_id']}): {e}")
            fallite += 1
    print(f"[Push] completato: {inviate} inviate, {fallite} fallite")
    return inviate, fallite


def push_to_users(user_ids, title, body, url='/', tag=''):
    subs = get_push_subscriptions_by_users(user_ids)
    if not subs:
        print(f"[Push] nessuna subscription trovata per utenti {list(user_ids)}, notifica '{title}' non inviata")
        return 0, 0
    return _invia_push(subs, title, body, url, tag)


def push_to_all(title, body, url='/', tag=''):
    subs = get_all_push_subscriptions()
    if not subs:
        print(f"[Push] nessuna subscription salvata nel database, notifica '{title}' non inviata")
        return 0, 0
    return _invia_push(subs, title, body, url, tag)


def _push_pending_to_sheets():
    """Invia a Google Sheets i movimenti/prodotti/temperature salvati solo in
    locale perché Sheets non era raggiungibile al momento della scrittura
    originale (vedi fallback nelle route /api/carico, /api/in-uso,
    /api/temperature, /api/prodotti, /api/preparazioni/<id>/completa).

    Va chiamata SEMPRE prima di un refresh da Sheets (manuale o
    automatico): quel refresh sovrascrive il DB locale con quello che trova
    su Sheets, quindi qualunque riga non ancora inviata andrebbe persa se
    non la spingessimo prima."""
    inviati = 0
    for row in get_unsynced_registro():
        try:
            append_registro(row)
            mark_registro_synced(row['id'])
            inviati += 1
        except Exception:
            pass  # resta 'da sincronizzare', ci riproverà il prossimo sync
    for row in get_unsynced_listino():
        try:
            append_listino(row)
            mark_listino_synced(row['prodotto'], row['fornitore'])
            inviati += 1
        except Exception:
            pass
    for row in get_unsynced_temperature():
        try:
            append_temperatura(row)
            mark_temperatura_synced(row['id'])
            inviati += 1
        except Exception:
            pass
    return inviati


def _sync_sheets_background():
    try:
        inviati = _push_pending_to_sheets()
        listino  = load_listino()
        replace_listino(listino)
        registro = load_registro()
        replace_registro(registro)
        temperature = load_temperatura()
        replace_temperatura(temperature)
        extra = f" | {inviati} in sospeso sincronizzati" if inviati else ""
        print(f"[SYNC] Listino: {len(listino)} prodotti | Registro: {len(registro)} movimenti | "
              f"Temperature: {len(temperature)} rilevazioni{extra}")
    except Exception as e:
        print(f"[SYNC] Sheets non raggiungibile: {e}")

threading.Thread(target=_sync_sheets_background, daemon=True).start()


# ─── AUTH ─────────────────────────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    error = None
    if request.method == 'POST':
        nome      = (request.form.get('nome') or '').strip()
        password  = request.form.get('password') or ''
        user_data = get_user_by_nome(nome)
        if user_data and check_password_hash(user_data['password'], password):
            if user_data.get('stato') == 'in_attesa':
                error = 'Il tuo account è in attesa di approvazione da un amministratore.'
            else:
                login_user(User(user_data), remember=True)
                return redirect(url_for('index'))
        else:
            error = 'Credenziali non valide'
    return render_template('login.html', error=error)


@app.route('/register', methods=['GET', 'POST'])
def register_page():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    error = None
    inviata = False
    if request.method == 'POST':
        nome     = (request.form.get('nome')     or '').strip()
        username = (request.form.get('username') or '').strip().lower()
        password = (request.form.get('password') or '')
        reparto  = (request.form.get('reparto')  or '').strip()

        if not nome:
            error = 'Inserisci il tuo nome'
        elif len(nome) < 2:
            error = 'Nome troppo corto'
        elif not username:
            error = 'Inserisci un username'
        elif len(username) < 3:
            error = 'Username troppo corto (minimo 3 caratteri)'
        elif len(password) < 6:
            error = 'Password troppo corta (minimo 6 caratteri)'
        elif not reparto:
            error = 'Seleziona il reparto'
        elif get_user_by_login(username):
            error = 'Username già in uso'
        else:
            try:
                create_user(nome, username,
                            generate_password_hash(password, method='pbkdf2:sha256'),
                            reparto, stato='in_attesa')
                inviata = True
            except Exception:
                error = 'Errore durante la registrazione'

    return render_template('register.html', error=error, inviata=inviata)


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login_page'))


# ─── RECUPERO PASSWORD ───────────────────────────────────────────────────────

@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    code  = None
    error = None
    if request.method == 'POST':
        nome      = (request.form.get('nome') or '').strip()
        user_data = get_user_by_nome(nome)
        if not user_data:
            error = 'Nessun account trovato con questo nome e cognome'
        else:
            code       = str(random.randint(100000, 999999))
            expires_at = (now_it() + timedelta(minutes=15)).isoformat()
            create_reset_token(user_data['id'], code, expires_at)
            print(f"[Reset] codice generato per utente {user_data['id']} ({user_data['nome']}), scade {expires_at}")
    return render_template('forgot.html', code=code, error=error)


@app.route('/reset-password', methods=['GET', 'POST'])
def reset_password():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    error   = None
    success = False
    if request.method == 'POST':
        token     = (request.form.get('token') or '').strip()
        password  = request.form.get('password') or ''
        password2 = request.form.get('password2') or ''
        if not token:
            error = 'Inserisci il codice di recupero'
        elif len(password) < 6:
            error = 'Password troppo corta (minimo 6 caratteri)'
        elif password != password2:
            error = 'Le password non coincidono'
        else:
            token_data = get_reset_token(token)
            if not token_data:
                error = 'Codice non valido o già utilizzato'
            elif datetime.fromisoformat(token_data['expires_at']) < now_it():
                use_reset_token(token_data['id'])
                error = 'Codice scaduto — richiedine uno nuovo'
            elif not token_data['confermato']:
                error = 'Il codice non è ancora stato confermato da un amministratore. Mostra il codice a un amministratore e riprova.'
            else:
                update_user_password(
                    token_data['user_id'],
                    generate_password_hash(password, method='pbkdf2:sha256'),
                )
                use_reset_token(token_data['id'])
                print(f"[Reset] password reimpostata per utente {token_data['user_id']} (codice invalidato)")
                success = True
    return render_template('reset.html', error=error, success=success)


# ─── PROFILO ─────────────────────────────────────────────────────────────────

@app.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    error   = None
    success = None
    user    = get_user_by_id(int(current_user.id))

    if request.method == 'POST':
        nome     = (request.form.get('nome')     or '').strip()
        username = (request.form.get('username') or '').strip().lower()
        cur_pw   =  request.form.get('current_password') or ''
        new_pw   =  request.form.get('new_password')     or ''
        new_pw2  =  request.form.get('new_password2')    or ''
        home_card = (request.form.get('home_card') or 'preparazioni').strip()
        if home_card not in ('nessuna', 'preparazioni', 'apparecchi', 'prenotazioni'):
            home_card = 'preparazioni'

        uid      = int(current_user.id)
        is_admin = current_user.role == 'admin'

        if not nome:
            error = 'Il nome non può essere vuoto'
        elif not username:
            error = 'L\'username non può essere vuoto'
        elif len(username) < 3:
            error = 'Username troppo corto (minimo 3 caratteri)'
        elif is_username_taken(username, uid):
            error = 'Username già in uso da un altro account'
        elif new_pw:
            if not check_password_hash(user['password'], cur_pw):
                error = 'Password attuale non corretta'
            elif len(new_pw) < 6:
                error = 'Nuova password troppo corta (minimo 6 caratteri)'
            elif new_pw != new_pw2:
                error = 'Le nuove password non coincidono'
            else:
                update_user_profile(uid, nome, username)
                if is_admin:
                    update_user_reparto(uid, (request.form.get('reparto') or '').strip())
                update_user_home_card(uid, home_card)
                update_user_password(uid, generate_password_hash(new_pw, method='pbkdf2:sha256'))
                success = 'Profilo e password aggiornati con successo'
                user = get_user_by_id(uid)
        else:
            update_user_profile(uid, nome, username)
            if is_admin:
                update_user_reparto(uid, (request.form.get('reparto') or '').strip())
            update_user_home_card(uid, home_card)
            success = 'Profilo aggiornato con successo'
            user = get_user_by_id(uid)

    return render_template('profile.html', user=user, error=error, success=success, vapid_public_key=VAPID_PUBLIC_KEY)


# ─── ADMIN ────────────────────────────────────────────────────────────────────

def _render_admin_page(**extra):
    reset_tokens = get_active_reset_tokens()
    for t in reset_tokens:
        t['scade_alle']     = datetime.fromisoformat(t['expires_at']).strftime('%H:%M')
        t['richiesto_alle'] = datetime.fromisoformat(t['created_at']).strftime('%H:%M') if t['created_at'] else '—'
    return render_template(
        'admin.html', users=get_all_users(), reset_tokens=reset_tokens, **extra
    )


@app.route('/admin')
@login_required
@admin_required
def admin_page():
    return _render_admin_page(test_notifica=request.args.get('test_notifica'))


@app.route('/admin/role', methods=['POST'])
@login_required
@admin_required
def admin_set_role():
    user_id = request.form.get('user_id', type=int)
    role    = (request.form.get('role') or '').strip()
    if not user_id or role not in ('admin', 'staff'):
        abort(400)
    if user_id == int(current_user.id) and role != 'admin':
        abort(400)  # Non puoi rimuovere il tuo stesso ruolo admin
    update_user_role(user_id, role)
    return redirect(url_for('admin_page'))


@app.route('/admin/delete', methods=['POST'])
@login_required
@admin_required
def admin_delete_user():
    user_id = request.form.get('user_id', type=int)
    if not user_id or user_id == int(current_user.id):
        abort(400)  # Non puoi eliminare te stesso
    delete_user(user_id)
    return redirect(url_for('admin_page'))


@app.route('/admin/apparecchi-permesso', methods=['POST'])
@login_required
@admin_required
def admin_set_apparecchi_permesso():
    user_id  = request.form.get('user_id', type=int)
    permesso = request.form.get('permesso') == '1'
    if not user_id:
        abort(400)
    update_user_apparecchi_permesso(user_id, permesso)
    return redirect(url_for('admin_page'))


@app.route('/admin/controllo-permesso', methods=['POST'])
@login_required
@admin_required
def admin_set_controllo_permesso():
    user_id  = request.form.get('user_id', type=int)
    permesso = request.form.get('permesso') == '1'
    if not user_id:
        abort(400)
    update_user_controllo_permesso(user_id, permesso)
    return redirect(url_for('admin_page'))


@app.route('/admin/eliminazione-carichi-permesso', methods=['POST'])
@login_required
@admin_required
def admin_set_eliminazione_carichi_permesso():
    user_id  = request.form.get('user_id', type=int)
    permesso = request.form.get('permesso') == '1'
    if not user_id:
        abort(400)
    update_user_eliminazione_carichi_permesso(user_id, permesso)
    return redirect(url_for('admin_page'))


@app.route('/admin/approva', methods=['POST'])
@login_required
@admin_required
def admin_approva_utente():
    user_id = request.form.get('user_id', type=int)
    if not user_id:
        abort(400)
    approva_utente(user_id)
    return redirect(url_for('admin_page'))


@app.route('/admin/rifiuta', methods=['POST'])
@login_required
@admin_required
def admin_rifiuta_utente():
    user_id = request.form.get('user_id', type=int)
    if not user_id:
        abort(400)
    rifiuta_utente(user_id)
    return redirect(url_for('admin_page'))


@app.route('/admin/reset-conferma', methods=['POST'])
@login_required
@admin_required
def admin_conferma_reset():
    token_id = request.form.get('token_id', type=int)
    if not token_id:
        abort(400)
    confirm_reset_token(token_id)
    print(f"[Reset] codice id={token_id} confermato dall'admin {current_user.id}")
    return redirect(url_for('admin_page'))


@app.route('/admin/test-notifica', methods=['POST'])
@login_required
@admin_required
def admin_test_notifica():
    inviate, fallite = push_to_users(
        [int(current_user.id)], 'Brigade', 'Test notifica Brigade - funziona!',
        url='/', tag='test-notifica',
    )
    if inviate:
        print(f"[Push] Test notifica inviata con successo all'admin {current_user.id}")
        esito = 'ok'
    else:
        print(f"[Push] Test notifica NON inviata all'admin {current_user.id} "
              f"(nessuna subscription attiva o invio fallito, fallite={fallite})")
        esito = 'fail'
    return redirect(url_for('admin_page', test_notifica=esito))


@app.route('/admin/debug-notifiche', methods=['POST'])
@login_required
@admin_required
def admin_debug_notifiche():
    """Diagnostica completa delle push notifications: quante subscription
    esistono, cosa contiene quella dell'admin, se le VAPID key sono
    configurate, se pywebpush è importabile, e un invio di prova con
    l'errore esatto restituito da webpush() (non solo un booleano)."""
    admin_id = int(current_user.id)
    print(f"[Push][Debug] avviato da admin {admin_id}")

    debug = {}

    tutte_le_subs = get_all_push_subscriptions()
    debug['totale_subscription'] = len(tutte_le_subs)
    print(f"[Push][Debug] subscription totali nel database: {len(tutte_le_subs)}")

    admin_subs = get_push_subscriptions_by_users([admin_id])
    debug['subscription_admin'] = []
    for s in admin_subs:
        try:
            parsed = json.loads(s['subscription_json'])
        except Exception as e:
            parsed = {}
            print(f"[Push][Debug] subscription id={s['id']} non è JSON valido: {e}")
        keys = parsed.get('keys') or {}
        debug['subscription_admin'].append({
            'id': s['id'],
            'endpoint': s['endpoint'],
            'p256dh_len': len(keys.get('p256dh', '')),
            'auth_len': len(keys.get('auth', '')),
        })
    print(f"[Push][Debug] subscription dell'admin {admin_id}: {len(admin_subs)}")

    def _caratteri_non_validi_b64url(s):
        # Alfabeto base64url: A-Z a-z 0-9 - _  (nessun '=', spazio, virgolette...)
        return sorted(set(c for c in s if not (c.isalnum() or c in '-_')))

    debug['vapid_public_presente']  = bool(VAPID_PUBLIC_KEY)
    debug['vapid_private_presente'] = bool(VAPID_PRIVATE_KEY)
    debug['vapid_source']           = VAPID_SOURCE
    # La chiave pubblica non è segreta (il browser la riceve comunque per
    # iscriversi): la mostriamo per intero così si può confrontare byte per
    # byte con quella messa su Render, invece di indovinare da un'anteprima.
    debug['vapid_public_full']      = VAPID_PUBLIC_KEY or '(mancante)'
    debug['vapid_public_len']       = len(VAPID_PUBLIC_KEY)
    debug['vapid_public_non_validi']  = _caratteri_non_validi_b64url(VAPID_PUBLIC_KEY)
    debug['vapid_private_len']       = len(VAPID_PRIVATE_KEY)
    debug['vapid_private_non_validi'] = _caratteri_non_validi_b64url(VAPID_PRIVATE_KEY)
    print(f"[Push][Debug] VAPID pubblica presente={debug['vapid_public_presente']} "
          f"len={debug['vapid_public_len']} (atteso 87) caratteri_non_validi={debug['vapid_public_non_validi']} "
          f"| privata presente={debug['vapid_private_presente']} len={debug['vapid_private_len']} "
          f"(atteso 43) caratteri_non_validi={debug['vapid_private_non_validi']} fonte={VAPID_SOURCE}")

    # Verifica decisiva: pywebpush/py_vapid non usa MAI VAPID_PUBLIC_KEY per
    # firmare — la chiave pubblica di firma viene ricalcolata matematicamente
    # dalla sola VAPID_PRIVATE_KEY. Se le due variabili d'ambiente non sono
    # una coppia coerente (es. incollate da generazioni diverse in momenti
    # diversi), il browser si iscrive con una chiave pubblica e il server
    # firma con una chiave privata che corrisponde a un'ALTRA chiave pubblica
    # — firma sempre non valida per il push service, qualsiasi subscription,
    # sempre "BadJwtToken". Lo controlliamo direttamente qui.
    debug['vapid_coppia_combacia'] = None
    debug['vapid_public_da_privata'] = None
    if VAPID_PRIVATE_KEY and not debug['vapid_private_non_validi']:
        try:
            priv_padded = VAPID_PRIVATE_KEY + '=' * (-len(VAPID_PRIVATE_KEY) % 4)
            priv_raw = base64.urlsafe_b64decode(priv_padded)
            priv_key = ec.derive_private_key(int.from_bytes(priv_raw, 'big'), ec.SECP256R1())
            pub_raw = priv_key.public_key().public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
            pub_derivata = base64.urlsafe_b64encode(pub_raw).rstrip(b'=').decode()
            debug['vapid_public_da_privata'] = pub_derivata
            debug['vapid_coppia_combacia'] = (pub_derivata == VAPID_PUBLIC_KEY)
            print(f"[Push][Debug] chiave pubblica derivata dalla privata: {pub_derivata}")
            print(f"[Push][Debug] combacia con VAPID_PUBLIC_KEY? {debug['vapid_coppia_combacia']}")
        except Exception as e:
            print(f"[Push][Debug] impossibile derivare la chiave pubblica dalla privata: {e}")

    # Apple (web.push.apple.com, cioè Safari/iOS) valida il dominio del claim
    # "sub" del JWT più severamente di Google/Mozilla e rifiuta con
    # "BadJwtToken" domini fittizi/riservati (.local, .example, .test,
    # .invalid, localhost) — anche con chiavi corrette e coppia coerente.
    # È una causa di BadJwtToken indipendente dal formato o dall'abbinamento
    # delle chiavi (controllati sopra), quindi va verificata a parte.
    debug['vapid_sub'] = VAPID_CLAIMS_SUB
    debug['vapid_sub_sospetto'] = any(
        d in VAPID_CLAIMS_SUB.lower() for d in ('.local', '.example', '.test', '.invalid', 'localhost')
    )
    print(f"[Push][Debug] VAPID sub={VAPID_CLAIMS_SUB!r} dominio_sospetto={debug['vapid_sub_sospetto']}")

    debug['pywebpush_ok']      = PYWEBPUSH_VERSION is not None
    debug['pywebpush_versione'] = PYWEBPUSH_VERSION or 'non importabile'
    print(f"[Push][Debug] pywebpush importabile={debug['pywebpush_ok']} versione={debug['pywebpush_versione']}")

    debug['invio_risultati'] = []
    if not admin_subs:
        print(f"[Push][Debug] nessuna subscription per l'admin {admin_id}: invio di prova saltato")
    else:
        payload = json.dumps({
            'title': 'Brigade', 'body': 'Test notifica Brigade - funziona!',
            'url': '/', 'tag': 'debug-notifiche',
        })
        for s in admin_subs:
            endpoint_short = s['endpoint'][-24:]
            entry = {'endpoint': s['endpoint']}
            print(f"[Push][Debug] tentativo invio -> ...{endpoint_short}")
            try:
                webpush(
                    subscription_info=json.loads(s['subscription_json']),
                    data=payload,
                    vapid_private_key=VAPID_PRIVATE_KEY,
                    vapid_claims={'sub': VAPID_CLAIMS_SUB},
                )
                entry['esito'] = 'ok'
                print(f"[Push][Debug] invio riuscito -> ...{endpoint_short}")
            except WebPushException as e:
                status = getattr(e.response, 'status_code', None)
                body_text = None
                try:
                    body_text = e.response.text
                except Exception:
                    pass
                entry['esito']  = 'errore'
                entry['errore'] = f"WebPushException status={status}: {e}" + (f" | risposta server push: {body_text}" if body_text else '')
                print(f"[Push][Debug] invio FALLITO -> ...{endpoint_short}: {entry['errore']}")
                if status in (404, 410, 401, 403):
                    delete_push_subscription(s['endpoint'])
                    entry['rimossa'] = True
                    print(f"[Push][Debug] subscription rimossa (status {status} — "
                          f"{'endpoint non più valido' if status in (404, 410) else 'VAPID non valida, probabile mismatch dopo rotazione chiavi'})")
            except Exception as e:
                entry['esito']  = 'errore'
                entry['errore'] = f"{type(e).__name__}: {e}"
                print(f"[Push][Debug] invio FALLITO (errore inatteso) -> ...{endpoint_short}: {entry['errore']}")
            debug['invio_risultati'].append(entry)

    print(f"[Push][Debug] completato per admin {admin_id}")
    return _render_admin_page(debug_notifiche=debug)


# ─── PAGINE ──────────────────────────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    return render_template('index.html', vapid_public_key=VAPID_PUBLIC_KEY)


@app.route('/static/sw.js')
def service_worker():
    resp = send_from_directory('static', 'sw.js')
    resp.headers['Content-Type'] = 'application/javascript'
    resp.headers['Service-Worker-Allowed'] = '/'
    return resp


@app.route('/landing')
def landing_page():
    """Landing page pubblica di marketing (file standalone nella root del
    progetto, non in templates/: nessun rendering Jinja, nessun login richiesto)."""
    return send_from_directory(app.root_path, 'landing.html')


@app.route('/privacy')
def privacy_page():
    """Informativa privacy pubblica (file standalone, stesso pattern di /landing)."""
    return send_from_directory(app.root_path, 'privacy.html')


# ─── API LISTINO ─────────────────────────────────────────────────────────────

@app.route('/api/fornitori')
@login_required
def api_fornitori():
    return jsonify({'success': True, 'fornitori': get_fornitori()})


@app.route('/api/categorie')
@login_required
def api_categorie():
    return jsonify({'success': True, 'categorie': get_categorie()})


@app.route('/api/tema', methods=['POST'])
@login_required
def api_tema():
    tema = (request.get_json(force=True).get('tema') or '').strip()
    if tema not in ('chiaro', 'scuro'):
        return jsonify({'success': False, 'error': 'Tema non valido'}), 400
    update_user_tema(int(current_user.id), tema)
    return jsonify({'success': True, 'tema': tema})


DASH_CARD_TYPES = (
    'alert', 'temperature', 'scadenza', 'ultimo_carico',
    'in_uso', 'preparazioni', 'coperti', 'sotto_scorta',
)


@app.route('/api/dashboard-cards', methods=['POST'])
@login_required
def api_dashboard_cards():
    d = request.get_json(force=True)
    cards = [(d.get('card1') or '').strip(), (d.get('card2') or '').strip(), (d.get('card3') or '').strip()]
    if any(c not in DASH_CARD_TYPES for c in cards):
        return jsonify({'success': False, 'error': 'Tipo di card non valido'}), 400
    update_user_dash_cards(int(current_user.id), *cards)
    return jsonify({'success': True})


@app.route('/api/tutorial-visto', methods=['POST'])
@login_required
def api_tutorial_visto():
    update_user_tutorial_visto(int(current_user.id))
    return jsonify({'success': True})


@app.route('/api/ultimo-carico')
@login_required
def api_ultimo_carico():
    row = get_ultimo_carico()
    if not row:
        return jsonify({'success': True, 'prodotto': None, 'quando': None})
    quando = ''
    try:
        quando = datetime.fromisoformat(row.get('creato_il') or '').strftime('%H:%M')
    except (ValueError, TypeError):
        # righe storiche senza creato_il: mostriamo almeno la data del movimento
        quando = row.get('data') or ''
    return jsonify({'success': True, 'prodotto': row.get('prodotto'), 'quando': quando})


@app.route('/api/prodotti')
@login_required
def api_prodotti():
    fornitore = request.args.get('fornitore', '').strip()
    prodotti  = get_prodotti_by_fornitore(fornitore) if fornitore else get_all_prodotti()
    return jsonify({'success': True, 'prodotti': prodotti})


@app.route('/api/prodotti', methods=['POST'])
@login_required
def api_aggiungi_prodotto():
    d          = request.get_json(force=True)
    prodotto   = (d.get('prodotto')  or '').strip()
    fornitore  = (d.get('fornitore') or '').strip()
    unita      = (d.get('unita')     or 'kg').strip()
    categoria  = (d.get('categoria') or '').strip()
    reparto    = (d.get('reparto')   or '').strip()
    try:
        scorta_min = float(d.get('scorta_min', 0) or 0)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Scorta minima non valida'}), 400

    if not prodotto:
        return jsonify({'success': False, 'error': 'Nome prodotto obbligatorio'}), 400
    if not fornitore:
        return jsonify({'success': False, 'error': 'Fornitore obbligatorio'}), 400
    if reparto not in ('Cucina', 'Sala', 'Pizzeria'):
        return jsonify({'success': False, 'error': 'Seleziona il reparto'}), 400
    if categoria not in CATEGORIE_FISSE:
        return jsonify({'success': False, 'error': 'Seleziona la categoria'}), 400
    if scorta_min < 0:
        return jsonify({'success': False, 'error': 'Scorta minima non valida'}), 400

    warning = None
    try:
        append_listino({'prodotto': prodotto, 'fornitore': fornitore, 'unita': unita,
                        'scorta_min': scorta_min, 'categoria': categoria, 'reparto': reparto})
    except SheetsUnavailableError:
        warning = 'Salvato localmente — Google Sheets non raggiungibile. Verrà sincronizzato automaticamente.'

    insert_listino_row(prodotto, fornitore, unita, scorta_min, categoria, reparto,
                        synced=(warning is None))
    return jsonify({'success': True, 'prodotto': prodotto, 'fornitore': fornitore,
                    'unita': unita, 'scorta_min': float(scorta_min or 0),
                    'categoria': categoria, 'reparto': reparto, 'warning': warning})


# ─── API REGISTRO ─────────────────────────────────────────────────────────────

@app.route('/api/lotti')
@login_required
def api_lotti():
    prodotto = request.args.get('prodotto', '').strip()
    if not prodotto:
        return jsonify({'success': True, 'lotti': []})
    return jsonify({'success': True, 'lotti': get_lotti_attivi(prodotto)})


@app.route('/api/giacenze')
@login_required
def api_giacenze():
    return jsonify({'success': True, 'giacenze': get_giacenze()})


@app.route('/api/alerts')
@login_required
def api_alerts():
    return jsonify({'success': True, **get_alerts()})


# ─── LISTA SPESA ──────────────────────────────────────────────────────────────

@app.route('/api/lista-spesa', methods=['GET'])
@login_required
def api_lista_spesa_get():
    return jsonify({'items': get_lista_spesa()})


@app.route('/api/lista-spesa', methods=['POST'])
@login_required
def api_lista_spesa_add():
    data     = request.get_json() or {}
    prodotto = (data.get('prodotto') or '').strip()
    if not prodotto:
        return jsonify({'error': 'prodotto richiesto'}), 400
    fornitore = (data.get('fornitore')  or '').strip()
    try:
        quantita = float(data.get('quantita', 0) or 0)
    except (TypeError, ValueError):
        return jsonify({'error': 'quantità non valida'}), 400
    new_id = add_lista_spesa_item(
        prodotto,
        fornitore,
        (data.get('categoria')  or '').strip(),
        quantita,
        (data.get('unita') or '').strip(),
        (data.get('reparto') or '').strip(),
    )
    log_lista_spesa_azione('aggiunto', prodotto, fornitore, current_user.nome)
    return jsonify({'id': new_id, 'success': True})


@app.route('/api/lista-spesa/<int:item_id>', methods=['PATCH'])
@login_required
def api_lista_spesa_patch(item_id):
    data = request.get_json() or {}
    if 'completato' in data:
        update_lista_spesa_completato(item_id, bool(data['completato']))
    if 'fornitore' in data:
        update_lista_spesa_fornitore(item_id, data['fornitore'] or '')
    return jsonify({'success': True})


@app.route('/api/lista-spesa/<int:item_id>', methods=['DELETE'])
@login_required
def api_lista_spesa_delete_one(item_id):
    item = get_lista_spesa_item_by_id(item_id)
    delete_lista_spesa_item(item_id)
    if item:
        log_lista_spesa_azione('eliminato', item['prodotto'], item['fornitore'], current_user.nome)
    return jsonify({'success': True})


@app.route('/api/lista-spesa', methods=['DELETE'])
@login_required
def api_lista_spesa_clear():
    items = get_lista_spesa()
    clear_lista_spesa()
    for item in items:
        log_lista_spesa_azione('eliminato', item['prodotto'], item['fornitore'], current_user.nome)
    return jsonify({'success': True})


# ─── OPERAZIONI ──────────────────────────────────────────────────────────────

@app.route('/api/carico', methods=['POST'])
@login_required
def api_carico():
    d         = request.get_json(force=True)
    prodotto  = (d.get('prodotto')  or '').strip()
    fornitore = (d.get('fornitore') or '').strip()
    lotto     = (d.get('lotto')     or '').strip()
    scadenza  = (d.get('scadenza')  or '').strip()
    unita     = (d.get('unita')     or 'kg').strip()
    force     = bool(d.get('force'))
    try:
        qty = float(d.get('qty', 0))
    except (TypeError, ValueError):
        qty = 0.0

    if not prodotto or not fornitore or not lotto or qty <= 0:
        return jsonify({'success': False, 'error': 'Compila tutti i campi obbligatori'}), 400

    if not force:
        recente = get_carico_recente(prodotto, lotto)
        if recente:
            return jsonify({
                'success':   False,
                'duplicate': True,
                'operatore': recente['operatore'],
                'ora':       recente['creato_il'][11:16],
            })

    rimanenza_prec  = get_ultima_rimanenza(prodotto, lotto)
    nuova_rimanenza = round(rimanenza_prec + qty, 4)
    movimento_id    = _gen_id()
    etichetta       = _gen_etichetta(prodotto, lotto, scadenza)
    oggi            = now_it().strftime('%Y-%m-%d')

    row = {
        'data':         oggi,
        'fornitore':    fornitore,
        'prodotto':     prodotto,
        'lotto':        lotto,
        'scadenza':     scadenza,
        'carico':       qty,
        'scarico':      0,
        'unita':        unita,
        'etichetta':    etichetta,
        'movimento_id': movimento_id,
        'rimanenza':    nuova_rimanenza,
        'operatore':    current_user.nome,
        'reparto':      get_reparto_prodotto(prodotto, fornitore),
    }

    warning = None
    try:
        append_registro(row)
    except SheetsUnavailableError:
        row['synced'] = False
        warning = 'Salvato localmente — Google Sheets non raggiungibile. Verrà sincronizzato automaticamente.'

    insert_movimento(row)

    return jsonify({
        'success':      True,
        'movimento_id': movimento_id,
        'etichetta':    etichetta,
        'rimanenza':    nuova_rimanenza,
        'prodotto':     prodotto,
        'fornitore':    fornitore,
        'lotto':        lotto,
        'scadenza':     scadenza,
        'qty':          qty,
        'unita':        unita,
        'warning':      warning,
    })


@app.route('/api/in-uso', methods=['GET'])
@login_required
def api_in_uso_list():
    prodotto = request.args.get('prodotto', '').strip()
    return jsonify({'success': True, 'in_uso': get_in_uso_attivi(prodotto or None)})


@app.route('/api/in-uso', methods=['POST'])
@login_required
def api_in_uso_crea():
    d        = request.get_json(force=True)
    prodotto = (d.get('prodotto') or '').strip()
    lotto    = (d.get('lotto')    or '').strip()
    try:
        qty = float(d.get('qty', 0))
    except (TypeError, ValueError):
        qty = 0.0

    if not prodotto or not lotto or qty <= 0:
        return jsonify({'success': False, 'error': 'Compila tutti i campi obbligatori'}), 400

    rimanenza_prec = get_ultima_rimanenza(prodotto, lotto)
    if qty > rimanenza_prec:
        return jsonify({
            'success': False,
            'error':   f'Non puoi prelevare più di {rimanenza_prec} disponibili',
        }), 400

    lotti_info = get_lotti_attivi(prodotto)
    info       = next((l for l in lotti_info if l['lotto'] == lotto), {})

    nuova_rimanenza = round(rimanenza_prec - qty, 4)
    movimento_id    = _gen_id()
    oggi            = now_it().strftime('%Y-%m-%d')

    row = {
        'data':         oggi,
        'fornitore':    info.get('fornitore', ''),
        'prodotto':     prodotto,
        'lotto':        lotto,
        'scadenza':     info.get('scadenza', ''),
        'carico':       0,
        'scarico':      qty,
        'unita':        info.get('unita', 'kg'),
        'etichetta':    '',
        'movimento_id': movimento_id,
        'rimanenza':    nuova_rimanenza,
        'operatore':    current_user.nome,
        'reparto':      info.get('reparto', 'Cucina'),
        'tipo':         'IN_USO',
    }

    warning = None
    try:
        append_registro(row)
    except SheetsUnavailableError:
        row['synced'] = False
        warning = 'Salvato localmente — Google Sheets non raggiungibile. Verrà sincronizzato automaticamente.'

    row_id = insert_movimento(row)

    return jsonify({
        'success':      True,
        'id':           row_id,
        'movimento_id': movimento_id,
        'rimanenza':    nuova_rimanenza,
        'prodotto':     prodotto,
        'lotto':        lotto,
        'qty':          qty,
        'unita':        row['unita'],
        'warning':      warning,
    })


@app.route('/api/scarico', methods=['POST'])
@login_required
def api_scarico():
    """Finalizza (in tutto o in parte) una riga IN_USO esistente. Se la
    quantità scaricata è minore di quella in uso, il residuo resta "in uso"
    in una nuova riga con lo stesso lotto e la stessa data originale."""
    d = request.get_json(force=True)
    try:
        row_id = int(d.get('id', 0))
    except (TypeError, ValueError):
        row_id = 0

    if not row_id:
        return jsonify({'success': False, 'error': 'Seleziona un elemento da scaricare'}), 400

    row = get_movimento_by_id(row_id)
    if not row or row.get('tipo') != 'IN_USO':
        return jsonify({'success': False, 'error': 'Elemento non trovato o già scaricato'}), 400

    try:
        qty = float(d.get('qty', row['scarico']))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Quantità non valida'}), 400

    if qty <= 0:
        return jsonify({'success': False, 'error': 'Inserisci una quantità valida'}), 400
    if qty > row['scarico'] + 1e-9:
        return jsonify({'success': False, 'error': f"Non puoi scaricare più di {row['scarico']} {row['unita']} in uso"}), 400

    qty       = round(qty, 4)
    residuo   = round(row['scarico'] - qty, 4)
    nuovo_mov_id = _gen_id() if residuo > 0 else None

    try:
        aggiorna_tipo_movimento(row['movimento_id'], 'SCARICO', nuovo_scarico=qty)
        if residuo > 0:
            append_registro({
                'data': row['data'], 'fornitore': row['fornitore'], 'prodotto': row['prodotto'],
                'lotto': row['lotto'], 'scadenza': row['scadenza'], 'carico': 0, 'scarico': residuo,
                'unita': row['unita'], 'etichetta': '', 'movimento_id': nuovo_mov_id,
                'rimanenza': row['rimanenza'], 'operatore': row['operatore'], 'reparto': row['reparto'],
                'tipo': 'IN_USO',
            })
    except Exception as e:
        return jsonify({'success': False, 'error': f'Errore Google Sheets: {e}'}), 500

    oggi = now_it().strftime('%Y-%m-%d')
    finalized, residua = finalizza_in_uso(row_id, oggi, qty, nuovo_mov_id)

    if not finalized:
        return jsonify({'success': False, 'error': 'Elemento non trovato o già scaricato'}), 400

    return jsonify({
        'success':      True,
        'prodotto':     finalized['prodotto'],
        'lotto':        finalized['lotto'],
        'qty':          finalized['scarico'],
        'unita':        finalized['unita'],
        'data_scarico': finalized['data_scarico'],
        'residuo':      residua['scarico'] if residua else 0,
    })


@app.route('/api/registro/<int:row_id>', methods=['PUT'])
@login_required
def api_registro_modifica(row_id):
    """Corregge lotto/scadenza/quantità di un lotto già registrato (sezione
    'In magazzino' di Giacenze). Aggiorna DB e REGISTRO Google Sheets, e
    registra la modifica nel log visibile dal pannello Controllo."""
    row = get_movimento_by_id(row_id)
    if not row:
        return jsonify({'success': False, 'error': 'Movimento non trovato'}), 404

    d        = request.get_json(force=True)
    lotto    = (d.get('lotto') or '').strip()
    scadenza = (d.get('scadenza') or '').strip()
    try:
        rimanenza = round(float(d.get('rimanenza', -1)), 4)
    except (TypeError, ValueError):
        rimanenza = -1

    if not lotto:
        return jsonify({'success': False, 'error': 'Il lotto è obbligatorio'}), 400
    if rimanenza < 0:
        return jsonify({'success': False, 'error': 'Quantità non valida'}), 400

    try:
        aggiorna_riga_registro(row['movimento_id'], lotto, scadenza, rimanenza)
    except Exception as e:
        return jsonify({'success': False, 'error': f'Errore Google Sheets: {e}'}), 500

    update_registro_lotto(row_id, lotto, scadenza, rimanenza)

    modifiche = []
    if round(row['rimanenza'], 4) != rimanenza:
        modifiche.append(f"Quantità: {row['rimanenza']} → {rimanenza} {row['unita']}")
    if row['lotto'] != lotto:
        modifiche.append(f"Lotto: {row['lotto'] or '—'} → {lotto}")
    if (row['scadenza'] or '') != scadenza:
        modifiche.append(f"Scadenza: {_fmt_data_it(row['scadenza'])} → {_fmt_data_it(scadenza)}")

    if modifiche:
        log_modifica_registro(row['prodotto'], lotto, '; '.join(modifiche), current_user.nome)

    return jsonify({'success': True, 'lotto': lotto, 'scadenza': scadenza, 'rimanenza': rimanenza})


@app.route('/api/registro/<int:row_id>', methods=['DELETE'])
@login_required
@eliminazione_carichi_required
def api_registro_elimina(row_id):
    """Elimina un carico registrato per errore (sezione 'In magazzino' di
    Giacenze). Rimuove la riga da DB e REGISTRO Google Sheets, e registra
    l'eliminazione nel log visibile dal pannello Controllo."""
    row = get_movimento_by_id(row_id)
    if not row:
        return jsonify({'success': False, 'error': 'Movimento non trovato'}), 404

    try:
        elimina_riga_registro(row['movimento_id'])
    except Exception as e:
        return jsonify({'success': False, 'error': f'Errore Google Sheets: {e}'}), 500

    delete_registro_row(row_id)
    log_eliminazione_carico(row, current_user.nome)

    return jsonify({'success': True})


# ─── TEMPERATURE ──────────────────────────────────────────────────────────────

@app.route('/api/apparecchi', methods=['GET'])
@login_required
def api_apparecchi_list():
    return jsonify({
        'success':    True,
        'apparecchi': get_apparecchi(),
        'puo_gestire': current_user.puo_gestire_apparecchi,
        'range_riferimento': RANGE_RIFERIMENTO_TIPO,
    })


@app.route('/api/apparecchi', methods=['POST'])
@login_required
@gestione_apparecchi_required
def api_apparecchi_crea():
    d       = request.get_json(force=True)
    nome    = (d.get('nome') or '').strip()
    tipo    = (d.get('tipo') or '').strip()
    reparto = (d.get('reparto') or '').strip()
    marca   = (d.get('marca') or '').strip()
    modello = (d.get('modello') or '').strip()
    seriale = (d.get('seriale') or '').strip()
    try:
        temp_min = float(d.get('temp_min'))
        temp_max = float(d.get('temp_max'))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Temperatura minima/massima non valida'}), 400

    if not nome:
        return jsonify({'success': False, 'error': 'Nome apparecchio obbligatorio'}), 400
    if tipo not in ('Frigorifero', 'Congelatore', 'Abbattitore'):
        return jsonify({'success': False, 'error': 'Seleziona il tipo apparecchio'}), 400
    if reparto not in ('Cucina', 'Sala', 'Pizzeria'):
        return jsonify({'success': False, 'error': 'Seleziona il reparto'}), 400
    if temp_min > temp_max:
        return jsonify({'success': False, 'error': 'La temperatura minima non può superare la massima'}), 400

    apparecchio_id = create_apparecchio(nome, tipo, temp_min, temp_max, reparto, marca, modello, seriale)
    return jsonify({'success': True, 'id': apparecchio_id})


@app.route('/api/apparecchi/<int:apparecchio_id>', methods=['PUT'])
@login_required
@gestione_apparecchi_required
def api_apparecchi_modifica(apparecchio_id):
    if not get_apparecchio_by_id(apparecchio_id):
        return jsonify({'success': False, 'error': 'Apparecchio non trovato'}), 404

    d       = request.get_json(force=True)
    nome    = (d.get('nome') or '').strip()
    tipo    = (d.get('tipo') or '').strip()
    reparto = (d.get('reparto') or '').strip()
    marca   = (d.get('marca') or '').strip()
    modello = (d.get('modello') or '').strip()
    seriale = (d.get('seriale') or '').strip()
    try:
        temp_min = float(d.get('temp_min'))
        temp_max = float(d.get('temp_max'))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Temperatura minima/massima non valida'}), 400

    if not nome:
        return jsonify({'success': False, 'error': 'Nome apparecchio obbligatorio'}), 400
    if tipo not in ('Frigorifero', 'Congelatore', 'Abbattitore'):
        return jsonify({'success': False, 'error': 'Seleziona il tipo apparecchio'}), 400
    if reparto not in ('Cucina', 'Sala', 'Pizzeria'):
        return jsonify({'success': False, 'error': 'Seleziona il reparto'}), 400
    if temp_min > temp_max:
        return jsonify({'success': False, 'error': 'La temperatura minima non può superare la massima'}), 400

    update_apparecchio(apparecchio_id, nome, tipo, temp_min, temp_max, reparto, marca, modello, seriale)
    return jsonify({'success': True})


@app.route('/api/apparecchi/<int:apparecchio_id>', methods=['DELETE'])
@login_required
@gestione_apparecchi_required
def api_apparecchi_elimina(apparecchio_id):
    if not get_apparecchio_by_id(apparecchio_id):
        return jsonify({'success': False, 'error': 'Apparecchio non trovato'}), 404
    delete_apparecchio(apparecchio_id)
    return jsonify({'success': True})


@app.route('/api/temperature', methods=['GET'])
@login_required
def api_temperature_storico():
    giorni      = request.args.get('giorni', default=30, type=int)
    apparecchio = (request.args.get('apparecchio') or '').strip() or None
    return jsonify({'success': True, 'rilevazioni': get_temperature_storico(giorni, apparecchio)})


@app.route('/api/temperature', methods=['POST'])
@login_required
def api_temperature_registra():
    d = request.get_json(force=True)
    try:
        apparecchio_id = int(d.get('apparecchio_id'))
        temperatura    = float(d.get('temperatura'))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Dati non validi'}), 400

    nota = (d.get('nota') or '').strip()
    data = (d.get('data') or now_it().strftime('%Y-%m-%d')).strip()
    ora  = (d.get('ora')  or now_it().strftime('%H:%M')).strip()

    app_row = get_apparecchio_by_id(apparecchio_id)
    if not app_row:
        return jsonify({'success': False, 'error': 'Apparecchio non trovato'}), 404

    fuori_soglia = temperatura < app_row['temp_min'] or temperatura > app_row['temp_max']
    if fuori_soglia and not nota:
        return jsonify({'success': False, 'error': 'Temperatura fuori soglia: la nota è obbligatoria'}), 400
    esito = 'FUORI_SOGLIA' if fuori_soglia else 'OK'

    row = {
        'apparecchio': app_row['nome'],
        'tipo':        app_row['tipo'],
        'data':        data,
        'ora':         ora,
        'temperatura': temperatura,
        'temp_min':    app_row['temp_min'],
        'temp_max':    app_row['temp_max'],
        'esito':       esito,
        'nota':        nota,
        'operatore':   current_user.nome,
    }

    warning = None
    try:
        append_temperatura(row)
    except SheetsUnavailableError:
        row['synced'] = False
        warning = 'Salvato localmente — Google Sheets non raggiungibile. Verrà sincronizzato automaticamente.'

    insert_temperatura(row)
    registra_controllo_apparecchio(apparecchio_id)

    return jsonify({'success': True, 'esito': esito, 'warning': warning})


@app.route('/api/temperature/<int:temperatura_id>', methods=['DELETE'])
@login_required
def api_temperature_elimina(temperatura_id):
    temp_row = get_temperatura_by_id(temperatura_id)
    if not temp_row:
        return jsonify({'success': False, 'error': 'Rilevazione non trovata'}), 404

    try:
        elimina_temperatura_foglio(temp_row['apparecchio'], temp_row['data'], temp_row['ora'])
    except Exception as e:
        return jsonify({'success': False, 'error': f'Errore Google Sheets: {e}'}), 500

    elimina_temperatura(temperatura_id)
    log_eliminazione_temperatura(temp_row, current_user.nome)

    app_row = get_apparecchio_by_nome(temp_row['apparecchio'])
    if app_row:
        ricalcola_stato_apparecchio(app_row['id'], app_row['nome'])

    return jsonify({'success': True})


@app.route('/api/controllo/log-temperature')
@login_required
@controllo_required
def api_controllo_log_temperature():
    return jsonify({'success': True, 'log': get_log_eliminazioni_temperature()})


@app.route('/api/controllo/log-lista-spesa')
@login_required
@controllo_required
def api_controllo_log_lista_spesa():
    return jsonify({'success': True, 'log': get_log_lista_spesa()})


@app.route('/api/controllo/log-modifiche-registro')
@login_required
@controllo_required
def api_controllo_log_modifiche_registro():
    return jsonify({'success': True, 'log': get_log_modifiche_registro()})


@app.route('/api/controllo/log-eliminazioni-registro')
@login_required
@controllo_required
def api_controllo_log_eliminazioni_registro():
    return jsonify({'success': True, 'log': get_log_eliminazioni_registro()})


@app.route('/api/sync', methods=['POST'])
@login_required
def api_sync():
    try:
        inviati = _push_pending_to_sheets()
        listino  = load_listino()
        replace_listino(listino)
        registro = load_registro()
        replace_registro(registro)
        temperature = load_temperatura()
        replace_temperatura(temperature)
        return jsonify({
            'success':     True,
            'listino':     len(listino),
            'registro':    len(registro),
            'temperature': len(temperature),
            'inviati':     inviati,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── PREPARAZIONI ─────────────────────────────────────────────────────────────

@app.route('/api/preparazioni', methods=['GET'])
@login_required
def api_preparazioni_list():
    oggi = now_it().strftime('%Y-%m-%d')
    preparazioni = get_preparazioni()
    for p in preparazioni:
        scad = (p.get('scadenza') or '')[:10]
        scaduta = bool(scad and scad < oggi)
        p['stato'] = 'completata' if (p.get('completata') or scaduta) else 'in_corso'
    return jsonify({'success': True, 'preparazioni': preparazioni})


@app.route('/api/preparazioni/<int:prep_id>', methods=['GET'])
@login_required
def api_preparazione_detail(prep_id):
    prep = get_preparazione_by_id(prep_id)
    if not prep:
        return jsonify({'success': False, 'error': 'Preparazione non trovata'}), 404
    ingredienti = get_preparazione_ingredienti(prep_id)
    return jsonify({'success': True, 'preparazione': prep, 'ingredienti': ingredienti})


@app.route('/api/preparazioni', methods=['POST'])
@login_required
def api_preparazione_crea():
    """Registra una nuova preparazione: scala ogni ingrediente dalle
    giacenze (una riga REGISTRO con tipo 'PREPARAZIONE' per ingrediente,
    come un 'in uso' ma non reversibile) e salva la ricetta con i lotti
    usati per l'etichetta."""
    d        = request.get_json(force=True)
    nome     = (d.get('nome') or '').strip()
    reparto  = (d.get('reparto') or '').strip()
    scadenza = (d.get('scadenza') or '').strip()
    unita    = (d.get('unita') or '').strip()
    note     = (d.get('note') or '').strip()
    ingredienti_in = d.get('ingredienti') or []

    try:
        quantita = float(d.get('quantita', 0))
    except (TypeError, ValueError):
        quantita = 0

    if not nome:
        return jsonify({'success': False, 'error': 'Nome preparazione obbligatorio'}), 400
    if reparto not in ('Cucina', 'Sala', 'Pizzeria'):
        return jsonify({'success': False, 'error': 'Seleziona il reparto'}), 400
    if quantita <= 0:
        return jsonify({'success': False, 'error': 'Inserisci una quantità prodotta valida'}), 400
    if not unita:
        return jsonify({'success': False, 'error': "Seleziona l'unità di misura"}), 400
    if not ingredienti_in:
        return jsonify({'success': False, 'error': 'Aggiungi almeno un ingrediente'}), 400

    # Valida tutti gli ingredienti PRIMA di scrivere qualsiasi cosa. Ogni
    # ingrediente può arrivare da due pool distinti (fonte 'magazzino' o
    # 'in_uso'), tenendo conto di eventuali usi ripetuti dello stesso
    # lotto/riga in-uso nella stessa richiesta.
    allocato        = {}   # (prodotto, lotto) -> qty allocata, per fonte magazzino
    allocato_in_uso = {}   # id riga in_uso    -> qty allocata, per fonte in_uso
    ingredienti = []
    for ing in ingredienti_in:
        prodotto = (ing.get('prodotto') or '').strip()
        fonte    = (ing.get('fonte') or 'magazzino').strip()
        try:
            ing_qty = round(float(ing.get('quantita', 0)), 4)
        except (TypeError, ValueError):
            ing_qty = 0

        if not prodotto:
            return jsonify({'success': False, 'error': 'Seleziona il prodotto per ogni ingrediente'}), 400
        if ing_qty <= 0:
            return jsonify({'success': False, 'error': f'Quantità non valida per {prodotto}'}), 400

        if fonte == 'in_uso':
            try:
                in_uso_id = int(ing.get('in_uso_id', 0))
            except (TypeError, ValueError):
                in_uso_id = 0
            if not in_uso_id:
                return jsonify({'success': False, 'error': f'Seleziona un elemento in uso per {prodotto}'}), 400

            righe_in_uso = get_in_uso_attivi(prodotto)
            info = next((r for r in righe_in_uso if r['id'] == in_uso_id), None)
            if not info:
                return jsonify({'success': False, 'error': f"Elemento in uso non trovato per {prodotto}"}), 400

            disponibile = round(float(info['qty']) - allocato_in_uso.get(in_uso_id, 0), 4)
            if ing_qty > disponibile + 1e-9:
                return jsonify({
                    'success': False,
                    'error': f"Non puoi usare più di {disponibile} {info['unita']} di {prodotto} (in uso) disponibili",
                }), 400
            allocato_in_uso[in_uso_id] = allocato_in_uso.get(in_uso_id, 0) + ing_qty

            ingredienti.append({
                'prodotto': prodotto, 'lotto': info['lotto'], 'qty': ing_qty, 'unita': info['unita'],
                'fonte': 'in_uso', 'in_uso_id': in_uso_id,
            })
        else:
            lotto = (ing.get('lotto') or '').strip()
            if not lotto:
                return jsonify({'success': False, 'error': 'Seleziona prodotto e lotto per ogni ingrediente'}), 400

            lotti_info = get_lotti_attivi(prodotto)
            info = next((l for l in lotti_info if l['lotto'] == lotto), None)
            if not info:
                return jsonify({'success': False, 'error': f"Lotto '{lotto}' non trovato per {prodotto}"}), 400

            key = (prodotto, lotto)
            disponibile = round(float(info['rimanenza']) - allocato.get(key, 0), 4)
            if ing_qty > disponibile + 1e-9:
                return jsonify({
                    'success': False,
                    'error': f"Non puoi usare più di {disponibile} {info['unita']} di {prodotto} (lotto {lotto}) disponibili",
                }), 400
            allocato[key] = allocato.get(key, 0) + ing_qty

            ingredienti.append({
                'prodotto': prodotto, 'lotto': lotto, 'qty': ing_qty, 'unita': info['unita'],
                'fornitore': info.get('fornitore', ''), 'scadenza': info.get('scadenza', ''),
                'nuova_rimanenza': round(disponibile - ing_qty, 4),
                'fonte': 'magazzino',
            })

    data_val = (d.get('data') or now_it().strftime('%Y-%m-%d')).strip()
    ora_val  = (d.get('ora')  or now_it().strftime('%H:%M')).strip()
    oggi      = data_val
    data_prep = f'{data_val} {ora_val}'

    # Ingredienti da magazzino: una nuova riga REGISTRO (tipo PREPARAZIONE)
    # per ingrediente, che scala la rimanenza del lotto — come oggi.
    righe_registro = []
    for ing in ingredienti:
        if ing['fonte'] != 'magazzino':
            continue
        righe_registro.append({
            'data': oggi, 'fornitore': ing['fornitore'], 'prodotto': ing['prodotto'],
            'lotto': ing['lotto'], 'scadenza': ing['scadenza'], 'carico': 0, 'scarico': ing['qty'],
            'unita': ing['unita'], 'etichetta': '', 'movimento_id': _gen_id(),
            'rimanenza': ing['nuova_rimanenza'], 'operatore': current_user.nome,
            'reparto': reparto, 'tipo': 'PREPARAZIONE',
        })

    # Ingredienti da "in uso": la quantità era già stata tolta dal magazzino
    # al momento del prelievo, quindi qui si finalizza (in tutto o in parte)
    # la riga IN_USO esistente invece di scalare di nuovo il magazzino —
    # altrimenti la stessa quantità verrebbe contata due volte.
    in_uso_da_finalizzare = []  # (in_uso_id, qty_totale, nuovo_movimento_id_o_None)
    for in_uso_id, tot_qty in allocato_in_uso.items():
        original = get_movimento_by_id(in_uso_id)
        if not original or original.get('tipo') != 'IN_USO':
            return jsonify({'success': False, 'error': 'Elemento in uso non più disponibile'}), 400
        residuo  = round(original['scarico'] - tot_qty, 4)
        nuovo_mov_id = _gen_id() if residuo > 0 else None
        in_uso_da_finalizzare.append((original, tot_qty, residuo, nuovo_mov_id))

    try:
        for row in righe_registro:
            append_registro(row)
        for original, tot_qty, residuo, nuovo_mov_id in in_uso_da_finalizzare:
            aggiorna_tipo_movimento(original['movimento_id'], 'PREPARAZIONE', nuovo_scarico=tot_qty)
            if residuo > 0:
                append_registro({
                    'data': original['data'], 'fornitore': original['fornitore'], 'prodotto': original['prodotto'],
                    'lotto': original['lotto'], 'scadenza': original['scadenza'], 'carico': 0, 'scarico': residuo,
                    'unita': original['unita'], 'etichetta': '', 'movimento_id': nuovo_mov_id,
                    'rimanenza': original['rimanenza'], 'operatore': original['operatore'],
                    'reparto': original['reparto'], 'tipo': 'IN_USO',
                })
    except Exception as e:
        return jsonify({'success': False, 'error': f'Errore Google Sheets: {e}'}), 500

    for row in righe_registro:
        insert_movimento(row)
    for original, tot_qty, residuo, nuovo_mov_id in in_uso_da_finalizzare:
        finalizza_in_uso(original['id'], oggi, tot_qty, nuovo_mov_id, tipo_finale='PREPARAZIONE')

    prep_id = insert_preparazione(nome, reparto, data_prep, scadenza, quantita, unita,
                                   int(current_user.id), note)
    for ing in ingredienti:
        insert_preparazione_ingrediente(prep_id, ing['prodotto'], ing['lotto'], ing['qty'], ing['unita'])

    return jsonify({
        'success':           True,
        'id':                prep_id,
        'nome':              nome,
        'reparto':           reparto,
        'data_preparazione': data_prep,
        'scadenza':          scadenza,
        'quantita':          quantita,
        'unita':             unita,
        'operatore':         current_user.nome,
        'ingredienti': [
            {'prodotto': i['prodotto'], 'lotto': i['lotto'], 'quantita': i['qty'], 'unita': i['unita']}
            for i in ingredienti
        ],
    })


@app.route('/api/preparazioni/<int:prep_id>/completa', methods=['POST'])
@login_required
def api_preparazione_completa(prep_id):
    """Segna una preparazione come completamente consumata: registra una
    riga di scarico nel REGISTRO (tipo 'SCARICO PREPARAZIONE', quantità
    prodotta) e marca la preparazione come completata."""
    prep = get_preparazione_by_id(prep_id)
    if not prep:
        return jsonify({'success': False, 'error': 'Preparazione non trovata'}), 404
    if prep.get('completata'):
        return jsonify({'success': False, 'error': 'Preparazione già completata'}), 400

    oggi = now_it().strftime('%Y-%m-%d')
    ora  = now_it().strftime('%H:%M')

    row = {
        'data':         oggi,
        'fornitore':    '',
        'prodotto':     prep['nome'],
        'lotto':        '',
        'scadenza':     '',
        'carico':       0,
        'scarico':      prep['quantita'],
        'unita':        prep['unita'],
        'etichetta':    f'Preparazione completata — ore {ora}',
        'movimento_id': _gen_id(),
        'rimanenza':    0,
        'operatore':    current_user.nome,
        'reparto':      prep['reparto'],
        'tipo':         'SCARICO_PREPARAZIONE',
    }

    warning = None
    try:
        append_registro(row)
    except SheetsUnavailableError:
        row['synced'] = False
        warning = 'Salvato localmente — Google Sheets non raggiungibile. Verrà sincronizzato automaticamente.'

    insert_movimento(row)
    completa_preparazione(prep_id)

    return jsonify({'success': True, 'warning': warning})


# ─── SALE ──────────────────────────────────────────────────────────────────

@app.route('/api/sale', methods=['GET'])
@login_required
def api_sale_list():
    return jsonify({'success': True, 'sale': get_sale(), 'puo_gestire': current_user.puo_gestire_sala})


@app.route('/api/sale', methods=['POST'])
@login_required
@admin_required
def api_sale_crea():
    d    = request.get_json(force=True)
    nome = (d.get('nome') or '').strip()
    if not nome:
        return jsonify({'success': False, 'error': 'Nome sala obbligatorio'}), 400
    sala_id = create_sala(nome)
    return jsonify({'success': True, 'id': sala_id, 'nome': nome})


@app.route('/api/sale/<int:sala_id>', methods=['PUT'])
@login_required
@admin_required
def api_sala_modifica(sala_id):
    d    = request.get_json(force=True)
    nome = (d.get('nome') or '').strip()
    if not nome:
        return jsonify({'success': False, 'error': 'Nome sala obbligatorio'}), 400
    update_sala(sala_id, nome)
    return jsonify({'success': True})


@app.route('/api/sale/<int:sala_id>', methods=['DELETE'])
@login_required
@admin_required
def api_sala_elimina(sala_id):
    if not delete_sala(sala_id):
        return jsonify({'success': False, 'error': 'Sposta o elimina prima i tavoli di questa sala'}), 400
    return jsonify({'success': True})


# ─── TAVOLI ────────────────────────────────────────────────────────────────

@app.route('/api/tavoli', methods=['GET'])
@login_required
def api_tavoli_list():
    sala_id  = request.args.get('sala_id', type=int)
    data     = (request.args.get('data') or '').strip() or None
    servizio = (request.args.get('servizio') or '').strip() or None
    return jsonify({
        'success':     True,
        'tavoli':      get_tavoli(sala_id, data=data, servizio=servizio),
        'puo_gestire': current_user.puo_gestire_sala,
    })


@app.route('/api/tavoli', methods=['POST'])
@login_required
@admin_required
def api_tavoli_crea():
    d      = request.get_json(force=True)
    numero = (d.get('numero') or '').strip()
    forma  = (d.get('forma') or '').strip()
    try:
        posti = int(d.get('posti', 0))
    except (TypeError, ValueError):
        posti = 0
    try:
        posizione_x = float(d.get('posizione_x', 40))
        posizione_y = float(d.get('posizione_y', 40))
    except (TypeError, ValueError):
        posizione_x, posizione_y = 40, 40

    if not numero:
        return jsonify({'success': False, 'error': 'Numero tavolo obbligatorio'}), 400
    if forma not in ('rettangolare', 'rotondo'):
        return jsonify({'success': False, 'error': 'Seleziona la forma del tavolo'}), 400
    if posti <= 0:
        return jsonify({'success': False, 'error': 'Numero posti non valido'}), 400

    # La UI ha un'unica planimetria condivisa (niente più scelta della
    # sala): assegna il tavolo a una sala di default, creata al volo se
    # non esiste ancora, per non toccare la FK tavoli.sala_id.
    sala_id = get_or_create_sala_default()

    tavolo_id = create_tavolo(numero, forma, posti, sala_id, posizione_x, posizione_y)
    return jsonify({'success': True, 'id': tavolo_id})


@app.route('/api/tavoli/<int:tavolo_id>', methods=['PATCH'])
@login_required
@admin_required
def api_tavoli_modifica(tavolo_id):
    """Update parziale: posizione (drag), numero/forma/posti/sala (modifica
    rapida) o toggle occupato — solo i campi presenti nel body vengono
    aggiornati."""
    if not get_tavolo_by_id(tavolo_id):
        return jsonify({'success': False, 'error': 'Tavolo non trovato'}), 404

    d     = request.get_json(force=True)
    campi = {}

    if 'numero' in d:
        numero = (d.get('numero') or '').strip()
        if not numero:
            return jsonify({'success': False, 'error': 'Numero tavolo obbligatorio'}), 400
        campi['numero'] = numero
    if 'forma' in d:
        forma = (d.get('forma') or '').strip()
        if forma not in ('rettangolare', 'rotondo'):
            return jsonify({'success': False, 'error': 'Seleziona la forma del tavolo'}), 400
        campi['forma'] = forma
    if 'posti' in d:
        try:
            posti = int(d.get('posti'))
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': 'Numero posti non valido'}), 400
        if posti <= 0:
            return jsonify({'success': False, 'error': 'Numero posti non valido'}), 400
        campi['posti'] = posti
    if 'sala_id' in d:
        try:
            campi['sala_id'] = int(d.get('sala_id'))
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': 'Sala non valida'}), 400
    if 'posizione_x' in d:
        try:
            campi['posizione_x'] = float(d.get('posizione_x'))
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': 'Posizione non valida'}), 400
    if 'posizione_y' in d:
        try:
            campi['posizione_y'] = float(d.get('posizione_y'))
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': 'Posizione non valida'}), 400
    if 'occupato' in d:
        campi['occupato'] = 1 if d.get('occupato') else 0
    if 'rotazione' in d:
        try:
            campi['rotazione'] = int(d.get('rotazione')) % 360
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': 'Rotazione non valida'}), 400

    if campi:
        update_tavolo(tavolo_id, **campi)
    return jsonify({'success': True})


@app.route('/api/tavoli/<int:tavolo_id>', methods=['DELETE'])
@login_required
@admin_required
def api_tavoli_elimina(tavolo_id):
    if not get_tavolo_by_id(tavolo_id):
        return jsonify({'success': False, 'error': 'Tavolo non trovato'}), 404
    delete_tavolo(tavolo_id)
    return jsonify({'success': True})


@app.route('/api/tavoli/<int:tavolo_id>/occupato', methods=['POST'])
@login_required
@gestione_sala_required
def api_tavolo_occupato(tavolo_id):
    """Toggle manuale walk-in (cliente senza prenotazione) — a differenza
    del PATCH sopra è disponibile allo staff di sala, non solo admin,
    perché non tocca il layout permanente."""
    if not get_tavolo_by_id(tavolo_id):
        return jsonify({'success': False, 'error': 'Tavolo non trovato'}), 404
    d = request.get_json(force=True)
    update_tavolo_occupato(tavolo_id, bool(d.get('occupato')))
    return jsonify({'success': True})


# ─── PRENOTAZIONI ──────────────────────────────────────────────────────────

@app.route('/api/prenotazioni', methods=['GET'])
@login_required
def api_prenotazioni_list():
    data = (request.args.get('data') or now_it().strftime('%Y-%m-%d')).strip()
    return jsonify({
        'success':       True,
        'prenotazioni':  get_prenotazioni(data),
        'coperti':       get_coperti_giorno(data),
        'puo_gestire':   current_user.puo_gestire_sala,
    })


@app.route('/api/prenotazioni', methods=['POST'])
@login_required
@gestione_sala_required
def api_prenotazioni_crea():
    d          = request.get_json(force=True)
    nome       = (d.get('nome') or '').strip()
    telefono   = (d.get('telefono') or '').strip()
    note       = (d.get('note') or '').strip()
    data_val   = (d.get('data') or now_it().strftime('%Y-%m-%d')).strip()
    ora        = (d.get('ora') or '').strip()
    tavoli_ids = d.get('tavoli') or []
    try:
        persone = int(d.get('persone', 0))
    except (TypeError, ValueError):
        persone = 0
    try:
        tavoli_ids = [int(t) for t in tavoli_ids]
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Tavoli selezionati non validi'}), 400

    if not nome:
        return jsonify({'success': False, 'error': 'Nome obbligatorio'}), 400
    if persone <= 0:
        return jsonify({'success': False, 'error': 'Numero persone non valido'}), 400
    if not ora:
        return jsonify({'success': False, 'error': "Seleziona l'ora"}), 400

    prenotazione_id = create_prenotazione(
        nome, persone, data_val, ora, telefono, note, int(current_user.id), tavoli_ids
    )
    return jsonify({'success': True, 'id': prenotazione_id})


@app.route('/api/prenotazioni/<int:prenotazione_id>', methods=['PUT'])
@login_required
@gestione_sala_required
def api_prenotazioni_modifica(prenotazione_id):
    esistente = get_prenotazione_by_id(prenotazione_id)
    if not esistente:
        return jsonify({'success': False, 'error': 'Prenotazione non trovata'}), 404

    d          = request.get_json(force=True)
    nome       = (d.get('nome') or '').strip()
    telefono   = (d.get('telefono') or '').strip()
    note       = (d.get('note') or '').strip()
    data_val   = (d.get('data') or '').strip()
    ora        = (d.get('ora') or '').strip()
    tavoli_ids = d.get('tavoli') or []
    try:
        persone = int(d.get('persone', 0))
    except (TypeError, ValueError):
        persone = 0
    try:
        tavoli_ids = [int(t) for t in tavoli_ids]
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Tavoli selezionati non validi'}), 400

    if not nome:
        return jsonify({'success': False, 'error': 'Nome obbligatorio'}), 400
    if persone <= 0:
        return jsonify({'success': False, 'error': 'Numero persone non valido'}), 400
    if not data_val or not ora:
        return jsonify({'success': False, 'error': 'Data e ora obbligatorie'}), 400

    # Lo stato di servizio (prenotato/arrivato/conto/libero) avanza solo dai
    # pulsanti dedicati (vedi /stato sotto): l'edit del form non può
    # resettarlo, quindi qui si ripassa sempre quello già in DB.
    update_prenotazione(
        prenotazione_id, nome, persone, data_val, ora, telefono, note,
        esistente['stato'], tavoli_ids
    )
    return jsonify({'success': True})


@app.route('/api/prenotazioni/<int:prenotazione_id>', methods=['DELETE'])
@login_required
@gestione_sala_required
def api_prenotazioni_elimina(prenotazione_id):
    prenotazione = get_prenotazione_by_id(prenotazione_id)
    if not prenotazione:
        return jsonify({'success': False, 'error': 'Prenotazione non trovata'}), 404
    delete_prenotazione(prenotazione_id)
    log_eliminazione_prenotazione(prenotazione, current_user.nome)
    return jsonify({'success': True})


@app.route('/api/prenotazioni/<int:prenotazione_id>/stato', methods=['POST'])
@login_required
@gestione_sala_required
def api_prenotazioni_stato(prenotazione_id):
    """Avanza lo stato di servizio: prenotato -> arrivato -> conto -> libero.
    Non validiamo la sequenza lato server (la UI offre solo il passo
    successivo corretto) — accettiamo qualunque valore tra i tre raggiungibili."""
    if not get_prenotazione_by_id(prenotazione_id):
        return jsonify({'success': False, 'error': 'Prenotazione non trovata'}), 404
    stato = (request.get_json(force=True).get('stato') or '').strip()
    if stato not in ('arrivato', 'conto', 'libero'):
        return jsonify({'success': False, 'error': 'Stato non valido'}), 400
    update_stato_prenotazione(prenotazione_id, stato)
    return jsonify({'success': True})


@app.route('/api/sala-disposizione', methods=['POST'])
@login_required
@gestione_sala_required
def api_sala_disposizione():
    """Salva uno spostamento temporaneo di un tavolo per una data+servizio
    specifici — non tocca mai la posizione base (quella si cambia solo in
    Setup Sala, admin-only)."""
    d = request.get_json(force=True)
    servizio = (d.get('servizio') or '').strip()
    data_val = (d.get('data') or '').strip()
    if servizio not in ('pranzo', 'cena'):
        return jsonify({'success': False, 'error': 'Servizio non valido'}), 400
    if not data_val:
        return jsonify({'success': False, 'error': 'Data obbligatoria'}), 400
    try:
        tavolo_id   = int(d.get('tavolo_id'))
        posizione_x = float(d.get('posizione_x'))
        posizione_y = float(d.get('posizione_y'))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Dati non validi'}), 400
    if not get_tavolo_by_id(tavolo_id):
        return jsonify({'success': False, 'error': 'Tavolo non trovato'}), 404
    upsert_disposizione_giornaliera(tavolo_id, data_val, servizio, posizione_x, posizione_y)
    return jsonify({'success': True})


@app.route('/api/prenotazioni/calendario', methods=['GET'])
@login_required
def api_prenotazioni_calendario():
    anno = request.args.get('anno', type=int) or now_it().year
    mese = request.args.get('mese', type=int) or now_it().month
    if not (1 <= mese <= 12):
        return jsonify({'success': False, 'error': 'Mese non valido'}), 400
    return jsonify({'success': True, **get_calendario_prenotazioni(anno, mese)})


@app.route('/api/sala-mura', methods=['GET'])
@login_required
def api_sala_mura_get():
    mura = get_sala_mura()
    return jsonify({'success': True, 'percorso_svg': mura['percorso_svg'] if mura else ''})


@app.route('/api/sala-mura', methods=['POST'])
@login_required
@admin_required
def api_sala_mura_salva():
    d = request.get_json(force=True)
    percorso = (d.get('percorso_svg') or '').strip()
    insert_sala_mura(percorso)  # stringa vuota ammessa: "Cancella" + "Salva" azzera la planimetria
    return jsonify({'success': True})


@app.route('/api/sala-scan', methods=['POST'])
@login_required
@admin_required
def api_sala_scan():
    """Analizza una foto della sala con Claude Vision e ritorna mura (percorsi
    SVG) e tavoli già convertiti nel sistema di coordinate della mappa
    (0-1000 x, 0-700 y) — le coordinate normalizzate 0-100 restituite dal
    modello sono relative all'inquadratura, non alla planimetria reale."""
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        return jsonify({'success': False, 'error': 'ANTHROPIC_API_KEY non configurata'}), 500

    d          = request.get_json(force=True)
    image_b64  = d.get('image', '')
    media_type = d.get('media_type', 'image/jpeg')

    if not image_b64:
        return jsonify({'success': False, 'error': 'Immagine mancante'}), 400

    if ',' in image_b64:
        image_b64 = image_b64.split(',', 1)[1]

    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=2000,
            messages=[{
                'role': 'user',
                'content': [
                    {
                        'type': 'image',
                        'source': {
                            'type':       'base64',
                            'media_type': media_type,
                            'data':       image_b64,
                        },
                    },
                    {
                        'type': 'text',
                        'text': (
                            "Analizza questa immagine di una sala ristorante. Identifica: "
                            "1) i muri perimetrali della sala, "
                            "2) i tavoli presenti con la loro forma (rotondo/rettangolare) e posizione. "
                            "Restituisci SOLO un JSON: "
                            '{muri: [{x1,y1,x2,y2}], tavoli: [{id, forma: \'rotondo/rettangolare\', x, y, larghezza, altezza}]} '
                            "dove tutte le coordinate sono normalizzate da 0 a 100 "
                            "(0,0 in alto a sinistra dell'immagine, 100,100 in basso a destra)."
                        ),
                    },
                ],
            }],
        )
        text = resp.content[0].text
        try:
            dati = _extract_json_object(text)
        except json.JSONDecodeError:
            dati = None
        if not dati:
            return jsonify({'success': False, 'error': 'Nessun muro o tavolo riconosciuto nella foto'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

    def _pct(v, asse_max):
        try:
            return round(max(0.0, min(1.0, float(v) / 100)) * asse_max, 1)
        except (TypeError, ValueError):
            return None

    muri = []
    for m in (dati.get('muri') or [])[:200]:
        x1, y1 = _pct(m.get('x1'), 1000), _pct(m.get('y1'), 700)
        x2, y2 = _pct(m.get('x2'), 1000), _pct(m.get('y2'), 700)
        if None in (x1, y1, x2, y2):
            continue
        muri.append(f"M{x1},{y1} L{x2},{y2}")

    tavoli = []
    for t in (dati.get('tavoli') or [])[:100]:
        x, y = _pct(t.get('x'), 1000), _pct(t.get('y'), 700)
        if x is None or y is None:
            continue
        forma_raw = str(t.get('forma') or '').strip().lower()
        forma = 'rotondo' if forma_raw in ('rotondo', 'round', 'circolare', 'circle', 'tondo') else 'rettangolare'
        tavoli.append({
            'forma':       forma,
            'posizione_x': max(20.0, min(980.0, x)),
            'posizione_y': max(20.0, min(680.0, y)),
        })

    return jsonify({'success': True, 'muri': muri, 'tavoli': tavoli})


@app.route('/api/controllo/log-eliminazioni-prenotazioni')
@login_required
@controllo_required
def api_controllo_log_eliminazioni_prenotazioni():
    return jsonify({'success': True, 'log': get_log_eliminazioni_prenotazioni()})


# ─── EXPORT PDF ───────────────────────────────────────────────────────────────

@app.route('/api/export/pdf')
@login_required
def api_export_pdf():
    tipo = (request.args.get('tipo') or '').strip()
    if tipo not in ('registro', 'temperature', 'report_mensile'):
        return jsonify({'success': False, 'error': 'Tipo report non valido'}), 400

    try:
        anno = int(request.args.get('anno', 0))
    except (TypeError, ValueError):
        anno = 0
    if not (2000 <= anno <= 2100):
        return jsonify({'success': False, 'error': 'Anno non valido'}), 400

    mese = None
    if tipo in ('registro', 'temperature'):
        try:
            mese = int(request.args.get('mese', 0))
        except (TypeError, ValueError):
            mese = 0
        if not (1 <= mese <= 12):
            return jsonify({'success': False, 'error': 'Mese non valido'}), 400

    if tipo == 'registro':
        buf = genera_pdf_registro(get_registro_by_mese(anno, mese), anno, mese)
        filename = f'registro-movimenti_{anno}-{mese:02d}.pdf'
    elif tipo == 'temperature':
        buf = genera_pdf_temperature(get_temperature_by_mese(anno, mese), anno, mese)
        filename = f'registro-temperature_{anno}-{mese:02d}.pdf'
    else:
        buf = genera_pdf_report_mensile(get_report_mensile_anno(anno), anno)
        filename = f'report-mensile_{anno}.pdf'

    return send_file(buf, mimetype='application/pdf', as_attachment=True, download_name=filename)


# ─── SCAN ETICHETTA (Claude Vision) ──────────────────────────────────────────

def _extract_json_object(text):
    """Estrae un oggetto JSON dalla risposta del modello, tollerando eventuale
    testo introduttivo o code fence markdown (```json ... ```) attorno al JSON."""
    cleaned = re.sub(r'^```(?:json)?\s*|\s*```$', '', text.strip(), flags=re.IGNORECASE)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    start = cleaned.find('{')
    end   = cleaned.rfind('}')
    if start == -1 or end == -1 or end <= start:
        return None
    return json.loads(cleaned[start:end + 1])


@app.route('/api/scan-label', methods=['POST'])
@login_required
def api_scan_label():
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        return jsonify({'success': False, 'error': 'ANTHROPIC_API_KEY non configurata'}), 500

    d          = request.get_json(force=True)
    image_b64  = d.get('image', '')
    media_type = d.get('media_type', 'image/jpeg')

    if not image_b64:
        return jsonify({'success': False, 'error': 'Immagine mancante'}), 400

    # Rimuove il prefisso data URL se presente
    if ',' in image_b64:
        image_b64 = image_b64.split(',', 1)[1]

    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=500,
            messages=[{
                'role': 'user',
                'content': [
                    {
                        'type': 'image',
                        'source': {
                            'type':       'base64',
                            'media_type': media_type,
                            'data':       image_b64,
                        },
                    },
                    {
                        'type': 'text',
                        'text': (
                            'Guarda questa etichetta di un prodotto alimentare ed estrai: '
                            'numero di lotto e data di scadenza. '
                            'Rispondi SOLO in JSON: {"lotto": "...", "scadenza": "DD/MM/YYYY"}. '
                            'Se non leggi un campo metti null.'
                        ),
                    },
                ],
            }],
        )
        text = resp.content[0].text
        try:
            result = _extract_json_object(text)
        except json.JSONDecodeError:
            result = None
        if not result:
            return jsonify({'success': False, 'error': 'Nessun dato trovato nell\'etichetta'})
        return jsonify({'success': True,
                        'lotto':   result.get('lotto'),
                        'scadenza': result.get('scadenza')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


_TIPO_APPARECCHIO_MAP = {
    'frigorifero': 'Frigorifero', 'congelatore': 'Congelatore', 'abbattitore': 'Abbattitore',
}


@app.route('/api/scan-apparecchio', methods=['POST'])
@login_required
def api_scan_apparecchio():
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        return jsonify({'success': False, 'error': 'ANTHROPIC_API_KEY non configurata'}), 500

    d          = request.get_json(force=True)
    image_b64  = d.get('image', '')
    media_type = d.get('media_type', 'image/jpeg')

    if not image_b64:
        return jsonify({'success': False, 'error': 'Immagine mancante'}), 400

    if ',' in image_b64:
        image_b64 = image_b64.split(',', 1)[1]

    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=500,
            messages=[{
                'role': 'user',
                'content': [
                    {
                        'type': 'image',
                        'source': {
                            'type':       'base64',
                            'media_type': media_type,
                            'data':       image_b64,
                        },
                    },
                    {
                        'type': 'text',
                        'text': (
                            "Guarda questa targhetta di un apparecchio di refrigerazione e dimmi: "
                            "marca, modello, numero seriale, tipo (frigorifero/congelatore/abbattitore). "
                            "Rispondi SOLO in JSON: {marca: '', modello: '', seriale: '', tipo: ''}. "
                            "Se non leggi un campo metti null."
                        ),
                    },
                ],
            }],
        )
        text = resp.content[0].text
        try:
            result = _extract_json_object(text)
        except json.JSONDecodeError:
            result = None
        if not result:
            return jsonify({'success': False, 'error': 'Nessun dato trovato nella targhetta'})

        tipo_raw = (result.get('tipo') or '').strip().lower()
        tipo = _TIPO_APPARECCHIO_MAP.get(tipo_raw)

        return jsonify({'success': True,
                        'marca':   result.get('marca'),
                        'modello': result.get('modello'),
                        'seriale': result.get('seriale'),
                        'tipo':    tipo})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── LOCK MOVIMENTI ───────────────────────────────────────────────────────────

@app.route('/api/locks/acquire', methods=['POST'])
@login_required
def api_locks_acquire():
    d = request.get_json(force=True)
    tipo = (d.get('tipo') or '').strip()
    if tipo not in ('prodotto_lotto', 'apparecchio'):
        return jsonify({'success': False, 'error': 'Tipo lock non valido'}), 400
    prodotto = (d.get('prodotto') or '').strip()
    lotto    = (d.get('lotto')    or '').strip()
    try:
        apparecchio_id = int(d.get('apparecchio_id', 0) or 0)
    except (TypeError, ValueError):
        apparecchio_id = 0
    form  = (d.get('form') or '').strip()
    force = bool(d.get('force'))

    result = acquire_lock(tipo, int(current_user.id), current_user.nome, form,
                           prodotto=prodotto, lotto=lotto, apparecchio_id=apparecchio_id,
                           force=force)
    if result['acquired']:
        return jsonify({'success': True})
    lock = result['lock']
    return jsonify({'success': False, 'locked_by': lock['user_nome'], 'form': lock['form']})


@app.route('/api/locks/release', methods=['POST'])
@login_required
def api_locks_release():
    d = request.get_json(force=True)
    tipo     = (d.get('tipo')     or '').strip()
    prodotto = (d.get('prodotto') or '').strip()
    lotto    = (d.get('lotto')    or '').strip()
    try:
        apparecchio_id = int(d.get('apparecchio_id', 0) or 0)
    except (TypeError, ValueError):
        apparecchio_id = 0
    release_lock(tipo, int(current_user.id), prodotto=prodotto, lotto=lotto,
                 apparecchio_id=apparecchio_id)
    return jsonify({'success': True})


@app.route('/api/locks')
@login_required
def api_locks_list():
    return jsonify({'success': True, 'locks': get_locks_attivi()})


# ─── CHAT ─────────────────────────────────────────────────────────────────────
CHAT_UPLOAD_DIR = os.path.join(app.root_path, 'static', 'chat_uploads')
CHAT_CANALE_REPARTO = {'cucina': 'Cucina', 'sala': 'Sala', 'pizzeria': 'Pizzeria'}


def _salva_foto_chat(foto_b64):
    """Decodifica un'immagine base64 (eventualmente con prefisso data URL) e la
    salva su disco, restituendo l'URL relativo da salvare nel messaggio."""
    if not foto_b64:
        return ''
    if ',' in foto_b64:
        foto_b64 = foto_b64.split(',', 1)[1]
    os.makedirs(CHAT_UPLOAD_DIR, exist_ok=True)
    nome_file = f"{uuid.uuid4().hex}.jpg"
    with open(os.path.join(CHAT_UPLOAD_DIR, nome_file), 'wb') as f:
        f.write(base64.b64decode(foto_b64))
    return f"/static/chat_uploads/{nome_file}"


@app.route('/api/chat')
@login_required
@chat_required
def api_chat_list():
    canale = (request.args.get('canale') or 'generale').strip()
    if canale not in current_user.canali_chat:
        return jsonify({'success': False, 'error': 'Canale non valido'}), 400
    return jsonify({'success': True, 'messaggi': get_chat_messaggi(canale)})


@app.route('/api/chat', methods=['POST'])
@login_required
@chat_required
def api_chat_invia():
    d      = request.get_json(force=True)
    canale = (d.get('canale') or 'generale').strip()
    testo  = (d.get('testo') or '').strip()
    foto_b64 = d.get('foto') or ''
    if canale not in current_user.canali_chat:
        return jsonify({'success': False, 'error': 'Canale non valido'}), 400
    if not testo and not foto_b64:
        return jsonify({'success': False, 'error': 'Messaggio vuoto'}), 400

    foto_url = _salva_foto_chat(foto_b64)
    autore_badge = 'Admin' if current_user.role == 'admin' else current_user.reparto
    msg_id = insert_chat_messaggio(
        canale, int(current_user.id), current_user.nome, testo, foto_url, autore_badge
    )

    # Notifica solo gli utenti che vedono davvero questo canale (stesso
    # reparto o admin): #generale è visibile a tutti, gli altri canali solo
    # al reparto corrispondente.
    dest = [u['id'] for u in get_all_users()
            if u['id'] != int(current_user.id) and u['stato'] == 'attivo'
            and (u['role'] == 'admin' or canale == 'generale' or CHAT_CANALE_REPARTO.get(canale) == u['reparto'])]
    push_to_users(
        dest, f'{current_user.nome} in #{canale}',
        testo if testo else 'Ha inviato una foto',
        url='/?goto=chat', tag='chat'
    )

    return jsonify({'success': True, 'id': msg_id})


@app.route('/api/chat/<int:msg_id>', methods=['PUT'])
@login_required
@chat_required
def api_chat_modifica(msg_id):
    msg = get_chat_messaggio_by_id(msg_id)
    if not msg or msg['eliminato']:
        return jsonify({'success': False, 'error': 'Messaggio non trovato'}), 404
    if msg['utente_id'] != int(current_user.id) and current_user.role != 'admin':
        return jsonify({'success': False, 'error': 'Non autorizzato'}), 403

    testo = (request.get_json(force=True).get('testo') or '').strip()
    if not testo:
        return jsonify({'success': False, 'error': 'Testo vuoto'}), 400

    log_chat_azione('modificato', msg['canale'], msg['utente_nome'], msg['testo'], current_user.nome)
    update_chat_messaggio(msg_id, testo)
    return jsonify({'success': True})


@app.route('/api/chat/<int:msg_id>', methods=['DELETE'])
@login_required
@chat_required
def api_chat_elimina(msg_id):
    msg = get_chat_messaggio_by_id(msg_id)
    if not msg or msg['eliminato']:
        return jsonify({'success': False, 'error': 'Messaggio non trovato'}), 404
    if msg['utente_id'] != int(current_user.id) and current_user.role != 'admin':
        return jsonify({'success': False, 'error': 'Non autorizzato'}), 403

    log_chat_azione('eliminato', msg['canale'], msg['utente_nome'], msg['testo'], current_user.nome)
    elimina_chat_messaggio(msg_id)
    return jsonify({'success': True})


@app.route('/api/chat/preview')
@login_required
@chat_required
def api_chat_preview():
    return jsonify({'success': True, 'messaggi': get_chat_preview(current_user.canali_chat)})


@app.route('/api/chat/unread-count')
@login_required
@chat_required
def api_chat_unread_count():
    counts = get_chat_unread_counts(int(current_user.id), current_user.canali_chat)
    return jsonify({'success': True, 'counts': counts, 'count': sum(counts.values())})


@app.route('/api/chat/letto', methods=['POST'])
@login_required
@chat_required
def api_chat_letto():
    canale = (request.get_json(silent=True) or {}).get('canale', '')
    if canale not in current_user.canali_chat:
        return jsonify({'success': False, 'error': 'Canale non valido'}), 400
    set_chat_canale_letto(int(current_user.id), canale)
    return jsonify({'success': True})


@app.route('/api/controllo/log-chat')
@login_required
@controllo_required
def api_controllo_log_chat():
    return jsonify({'success': True, 'log': get_log_chat_messaggi()})


# ─── RACCOLTA RIFIUTI ───────────────────────────────────────────────────────

RIFIUTI_TIPI_LABEL = {
    'organico': 'Organico', 'carta': 'Carta', 'plastica': 'Plastica',
    'secco': 'Indifferenziato', 'vetro': 'Vetro e lattine', 'verde': 'Verde e ramaglie',
}

RIFIUTI_GIORNI_PROSSIMI = 7  # quante sere mostrare oltre a "stasera" e "domani"


def _tipi_per_sera(sera, zona, info, calendario):
    """Tipi di rifiuto da esporre la sera di 'sera' (un date), secondo la
    regola della zona: il centro raccoglie la mattina dopo la stessa sera
    dell'esposizione, la Città raccoglie la mattina della sera precedente
    (quindi si espone la sera prima del giorno di raccolta)."""
    giorno_raccolta = sera + timedelta(days=1) if info['espone_sera_precedente'] else sera
    return [r['tipo_rifiuto'] for r in calendario if r['giorno_settimana'] == giorno_raccolta.weekday()]


def _calcola_rifiuti_oggi():
    """Zona configurata, sue info e lista dei tipi di rifiuto da esporre
    stasera. Usata dal job dello scheduler (promemoria delle 18:00)."""
    zona = get_impostazione('zona_rifiuti', '')
    info = RIFIUTI_ZONE_INFO.get(zona)
    if not info:
        return zona, info, []
    calendario = get_raccolta_calendario(zona)
    tipi = _tipi_per_sera(today_it(), zona, info, calendario)
    return zona, info, tipi


def _calcola_rifiuti_esteso():
    """Come _calcola_rifiuti_oggi ma con l'anteprima di domani e dei
    prossimi RIFIUTI_GIORNI_PROSSIMI giorni, per la card della dashboard."""
    zona = get_impostazione('zona_rifiuti', '')
    info = RIFIUTI_ZONE_INFO.get(zona)
    if not info:
        return zona, info, [], None, []
    calendario = get_raccolta_calendario(zona)
    oggi = today_it()
    stasera = _tipi_per_sera(oggi, zona, info, calendario)
    domani = {
        'data': (oggi + timedelta(days=1)).isoformat(),
        'tipi': _tipi_per_sera(oggi + timedelta(days=1), zona, info, calendario),
    }
    prossimi = []
    for i in range(2, 2 + RIFIUTI_GIORNI_PROSSIMI):
        giorno = oggi + timedelta(days=i)
        prossimi.append({'data': giorno.isoformat(), 'tipi': _tipi_per_sera(giorno, zona, info, calendario)})
    return zona, info, stasera, domani, prossimi


@app.route('/api/rifiuti/oggi')
@login_required
def api_rifiuti_oggi():
    zona, info, stasera, domani, prossimi = _calcola_rifiuti_esteso()
    if not info:
        return jsonify({'success': True, 'zona': '', 'zona_label': '', 'tipi': [], 'domani': None, 'prossimi': []})
    return jsonify({
        'success': True,
        'zona': zona,
        'zona_label': info['label'],
        'tipi': stasera,
        'orario_esposizione': info.get('orario_esposizione', ''),
        'vetro_scadenza': info.get('vetro_scadenza', ''),
        'domani': domani,
        'prossimi': prossimi,
    })


@app.route('/api/rifiuti/posizione', methods=['POST'])
@login_required
def api_rifiuti_set_posizione():
    """Riceve una lettura GPS (da watchPosition, quindi potenzialmente
    frequente): salva l'ultima posizione nota per QUESTO utente — da usare
    come fallback quando la geolocalizzazione del browser è momentaneamente
    irraggiungibile — e ricalcola/aggiorna la zona condivisa del locale."""
    data = request.get_json(silent=True) or {}
    try:
        lat = float(data.get('lat'))
        lon = float(data.get('lon'))
    except (TypeError, ValueError):
        print(f"[Rifiuti] posizione non valida ricevuta da utente {current_user.id}: {data}")
        return jsonify({'success': False, 'error': 'Coordinate non valide'}), 400
    update_user_rifiuti_posizione(int(current_user.id), lat, lon)
    zona = zona_rifiuti_da_coordinate(lat, lon)
    set_impostazione('zona_rifiuti', zona)
    print(f"[Rifiuti] posizione utente {current_user.id}: lat={lat:.5f} lon={lon:.5f} -> zona={zona}")
    return jsonify({'success': True, 'zona': zona})


@app.route('/api/rifiuti/usa-ultima-posizione', methods=['POST'])
@login_required
def api_rifiuti_usa_ultima_posizione():
    """Fallback quando il browser non riesce a fornire una posizione fresca
    (non un diniego permanente, es. timeout/GPS momentaneamente assente):
    riusa l'ultima posizione nota salvata per questo utente, se esiste."""
    pos = get_user_rifiuti_posizione(int(current_user.id))
    if not pos or (not pos['rifiuti_lat'] and not pos['rifiuti_lon']):
        print(f"[Rifiuti] nessuna posizione precedente salvata per utente {current_user.id}")
        return jsonify({'success': False, 'error': 'Nessuna posizione precedente salvata'}), 404
    zona = zona_rifiuti_da_coordinate(pos['rifiuti_lat'], pos['rifiuti_lon'])
    set_impostazione('zona_rifiuti', zona)
    print(f"[Rifiuti] fallback su ultima posizione nota (utente {current_user.id}, "
          f"aggiornata il {pos['rifiuti_geo_aggiornato_il']}): zona={zona}")
    return jsonify({'success': True, 'zona': zona})


@app.route('/api/rifiuti/zona', methods=['POST'])
@login_required
def api_rifiuti_set_zona():
    # Nessun @admin_required: la zona è un dato di posizione del locale (non
    # un'impostazione sensibile) e il rilevamento automatico via
    # geolocalizzazione parte per qualunque utente che apre la dashboard —
    # con l'admin_required qui, il salvataggio falliva (403) per tutto lo
    # staff non-admin e la zona non veniva mai impostata.
    data = request.get_json(silent=True) or {}
    zona = data.get('zona', '')
    if zona not in RIFIUTI_ZONE_INFO:
        print(f"[Rifiuti] zona non valida ricevuta: {zona!r}")
        return jsonify({'success': False, 'error': 'Zona non valida'}), 400
    set_impostazione('zona_rifiuti', zona)
    print(f"[Rifiuti] zona impostata: {zona} (utente {current_user.id})")
    return jsonify({'success': True})


# ─── PUSH NOTIFICATIONS ─────────────────────────────────────────────────────

@app.route('/api/push/vapid-public-key')
@login_required
def api_push_vapid_key():
    return jsonify({'success': True, 'publicKey': VAPID_PUBLIC_KEY})


@app.route('/api/push/subscribe', methods=['POST'])
@login_required
def api_push_subscribe():
    data = request.get_json(silent=True)
    endpoint = (data or {}).get('endpoint', '')
    print(f"[Push] subscribe ricevuta da utente {current_user.id}: "
          f"endpoint=...{endpoint[-24:] if endpoint else '(mancante)'}")
    if not data or not endpoint:
        print(f"[Push] subscribe fallito: dati non validi o body non JSON (utente {current_user.id})")
        return jsonify({'success': False, 'error': 'Subscription non valida'}), 400
    try:
        save_push_subscription(int(current_user.id), json.dumps(data), endpoint)
    except Exception as e:
        print(f"[Push] ERRORE salvataggio subscription per utente {current_user.id}: {e}")
        return jsonify({'success': False, 'error': 'Errore nel salvataggio della subscription'}), 500
    print(f"[Push] subscription salvata e confermata nel database per utente {current_user.id}: ...{endpoint[-24:]}")
    return jsonify({'success': True})


@app.route('/api/push/unsubscribe', methods=['POST'])
@login_required
def api_push_unsubscribe():
    endpoint = (request.get_json(silent=True) or {}).get('endpoint', '')
    if endpoint:
        delete_push_subscription(endpoint)
        print(f"[Push] subscription rimossa per utente {current_user.id}: ...{endpoint[-24:]}")
    return jsonify({'success': True})


# ─── METEO ──────────────────────────────────────────────────────────────────

@app.route('/api/meteo/log', methods=['POST'])
@login_required
def api_meteo_log():
    d = request.get_json(force=True)
    print(f"[Meteo] valore grezzo ricevuto dal client: {d}")
    try:
        temperatura  = float(d.get('temperatura'))
        weather_code = int(d.get('weather_code'))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Dati non validi'}), 400
    log_meteo_lettura(temperatura, weather_code)
    return jsonify({'success': True})


# ─── NOTIFICHE AUTOMATICHE GIORNALIERE ──────────────────────────────────────

def _job_promemoria_rifiuti():
    print('[Scheduler] job promemoria_rifiuti in esecuzione...')
    zona, info, tipi = _calcola_rifiuti_oggi()
    if not info or not tipi:
        print(f"[Scheduler] promemoria_rifiuti: nessuna raccolta stasera (zona={zona!r}, tipi={tipi})")
        return
    nomi = ', '.join(RIFIUTI_TIPI_LABEL.get(t, t) for t in tipi)
    print(f"[Scheduler] promemoria_rifiuti: invio notifica per {nomi} (zona={zona})")
    push_to_all('Raccolta rifiuti', f'Stasera esponi: {nomi}', url='/?goto=home', tag='promemoria-rifiuti')


_scheduler = BackgroundScheduler(timezone=TZ_ITALIA)
_scheduler.add_job(_job_promemoria_rifiuti,
                    CronTrigger(hour=18, minute=0, timezone=TZ_ITALIA), id='promemoria_rifiuti')
_scheduler.start()
_job_rifiuti_ref = _scheduler.get_job('promemoria_rifiuti')
print(f"[Scheduler] avviato, prossima esecuzione promemoria_rifiuti: "
      f"{_job_rifiuti_ref.next_run_time if _job_rifiuti_ref else 'N/D'}")


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    # use_reloader=False: con il reloader di Werkzeug l'intero modulo (quindi
    # anche l'avvio di _scheduler sopra) verrebbe eseguito due volte,
    # duplicando ogni notifica push programmata.
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=port)
