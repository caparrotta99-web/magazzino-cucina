import os
import re
import json
import random
import string
import threading
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Flask, render_template, request, jsonify,
    send_from_directory, redirect, url_for, abort,
)
from flask_login import (
    LoginManager, UserMixin,
    login_user, logout_user, login_required, current_user,
)
from werkzeug.security import generate_password_hash, check_password_hash

from database import (
    db_init,
    replace_listino, replace_registro,
    insert_listino_row,
    get_fornitori, get_categorie, get_all_prodotti, get_prodotti_by_fornitore,
    get_ultima_rimanenza, insert_movimento,
    get_lotti_attivi, get_giacenze, get_alerts,
    create_user, get_user_by_login, get_user_by_id,
    get_all_users, update_user_role, delete_user,
    create_reset_token, get_reset_token, use_reset_token, update_user_password,
    update_user_profile, update_user_reparto, is_username_taken, get_feed,
    get_lista_spesa, add_lista_spesa_item, update_lista_spesa_completato,
    update_lista_spesa_fornitore, delete_lista_spesa_item, clear_lista_spesa,
)
from sheets import load_listino, load_registro, append_registro, append_listino

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


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _gen_id():
    rand = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{datetime.now().strftime('%Y%m%d')}-{rand}"


def _gen_etichetta(prodotto, lotto, scadenza):
    return f"PRODOTTO: {prodotto}  LOTTO: {lotto}  SCADENZA: {scadenza or '—'}"


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

def _sync_sheets_background():
    try:
        listino  = load_listino()
        replace_listino(listino)
        registro = load_registro()
        replace_registro(registro)
        print(f"[SYNC] Listino: {len(listino)} prodotti | Registro: {len(registro)} movimenti")
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
        username  = (request.form.get('username') or '').strip()
        password  = request.form.get('password') or ''
        user_data = get_user_by_login(username)
        if user_data and check_password_hash(user_data['password'], password):
            login_user(User(user_data), remember=True)
            return redirect(url_for('index'))
        error = 'Username o password non corretti'
    return render_template('login.html', error=error)


@app.route('/register', methods=['GET', 'POST'])
def register_page():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    error = None
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
                            generate_password_hash(password, method='pbkdf2:sha256'), reparto)
                login_user(User(get_user_by_login(username)), remember=True)
                return redirect(url_for('index'))
            except Exception:
                error = 'Errore durante la registrazione'

    return render_template('register.html', error=error)


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
        identifier = (request.form.get('identifier') or '').strip()
        user_data  = get_user_by_login(identifier)
        if not user_data:
            error = 'Nessun account trovato con questi dati'
        else:
            code       = str(random.randint(100000, 999999))
            expires_at = (datetime.now() + timedelta(minutes=15)).isoformat()
            create_reset_token(user_data['id'], code, expires_at)
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
            elif datetime.fromisoformat(token_data['expires_at']) < datetime.now():
                use_reset_token(token_data['id'])
                error = 'Codice scaduto — richiedine uno nuovo'
            else:
                update_user_password(
                    token_data['user_id'],
                    generate_password_hash(password, method='pbkdf2:sha256'),
                )
                use_reset_token(token_data['id'])
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
                update_user_password(uid, generate_password_hash(new_pw, method='pbkdf2:sha256'))
                success = 'Profilo e password aggiornati con successo'
                user = get_user_by_id(uid)
        else:
            update_user_profile(uid, nome, username)
            if is_admin:
                update_user_reparto(uid, (request.form.get('reparto') or '').strip())
            success = 'Profilo aggiornato con successo'
            user = get_user_by_id(uid)

    return render_template('profile.html', user=user, error=error, success=success)


# ─── ADMIN ────────────────────────────────────────────────────────────────────

@app.route('/admin')
@login_required
@admin_required
def admin_page():
    return render_template('admin.html', users=get_all_users())


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


# ─── PAGINE ──────────────────────────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    return render_template('index.html')


@app.route('/static/sw.js')
def service_worker():
    resp = send_from_directory('static', 'sw.js')
    resp.headers['Content-Type'] = 'application/javascript'
    resp.headers['Service-Worker-Allowed'] = '/'
    return resp


# ─── API LISTINO ─────────────────────────────────────────────────────────────

@app.route('/api/fornitori')
@login_required
def api_fornitori():
    return jsonify({'success': True, 'fornitori': get_fornitori()})


@app.route('/api/categorie')
@login_required
def api_categorie():
    return jsonify({'success': True, 'categorie': get_categorie()})


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
    scorta_min = d.get('scorta_min', 0)
    categoria  = (d.get('categoria') or '').strip()

    if not prodotto:
        return jsonify({'success': False, 'error': 'Nome prodotto obbligatorio'}), 400
    if not fornitore:
        return jsonify({'success': False, 'error': 'Fornitore obbligatorio'}), 400

    try:
        append_listino({'prodotto': prodotto, 'fornitore': fornitore,
                        'unita': unita, 'scorta_min': scorta_min, 'categoria': categoria})
    except Exception as e:
        return jsonify({'success': False, 'error': f'Errore Google Sheets: {e}'}), 500

    insert_listino_row(prodotto, fornitore, unita, scorta_min, categoria)
    return jsonify({'success': True, 'prodotto': prodotto, 'fornitore': fornitore,
                    'unita': unita, 'scorta_min': float(scorta_min or 0), 'categoria': categoria})


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
    new_id = add_lista_spesa_item(
        prodotto,
        (data.get('fornitore')  or '').strip(),
        (data.get('categoria')  or '').strip(),
        data.get('quantita', 0),
        (data.get('unita') or '').strip(),
    )
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
    delete_lista_spesa_item(item_id)
    return jsonify({'success': True})


@app.route('/api/lista-spesa', methods=['DELETE'])
@login_required
def api_lista_spesa_clear():
    clear_lista_spesa()
    return jsonify({'success': True})


@app.route('/api/feed')
@login_required
def api_feed():
    return jsonify({'success': True, 'movements': get_feed()})


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
    try:
        qty = float(d.get('qty', 0))
    except (TypeError, ValueError):
        qty = 0.0

    if not prodotto or not fornitore or not lotto or qty <= 0:
        return jsonify({'success': False, 'error': 'Compila tutti i campi obbligatori'}), 400

    rimanenza_prec  = get_ultima_rimanenza(prodotto, lotto)
    nuova_rimanenza = round(rimanenza_prec + qty, 4)
    movimento_id    = _gen_id()
    etichetta       = _gen_etichetta(prodotto, lotto, scadenza)
    oggi            = datetime.now().strftime('%Y-%m-%d')

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
        'reparto':      current_user.reparto,
    }

    try:
        append_registro(row)
    except Exception as e:
        return jsonify({'success': False, 'error': f'Errore Google Sheets: {e}'}), 500

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
    })


@app.route('/api/scarico', methods=['POST'])
@login_required
def api_scarico():
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
            'error':   f'Non puoi scaricare più di {rimanenza_prec} disponibili',
        }), 400

    lotti_info = get_lotti_attivi(prodotto)
    info       = next((l for l in lotti_info if l['lotto'] == lotto), {})

    nuova_rimanenza = round(rimanenza_prec - qty, 4)
    movimento_id    = _gen_id()
    oggi            = datetime.now().strftime('%Y-%m-%d')

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
        'reparto':      current_user.reparto,
    }

    try:
        append_registro(row)
    except Exception as e:
        return jsonify({'success': False, 'error': f'Errore Google Sheets: {e}'}), 500

    insert_movimento(row)

    return jsonify({
        'success':      True,
        'movimento_id': movimento_id,
        'rimanenza':    nuova_rimanenza,
    })


@app.route('/api/sync', methods=['POST'])
@login_required
def api_sync():
    try:
        listino  = load_listino()
        replace_listino(listino)
        registro = load_registro()
        replace_registro(registro)
        return jsonify({
            'success':  True,
            'listino':  len(listino),
            'registro': len(registro),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── SCAN ETICHETTA (Claude Vision) ──────────────────────────────────────────

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
            model='claude-sonnet-4-20250514',
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
        text  = resp.content[0].text
        match = re.search(r'\{.*?\}', text, re.DOTALL)
        if not match:
            return jsonify({'success': False, 'error': 'Nessun dato trovato nell\'etichetta'})
        result = json.loads(match.group())
        return jsonify({'success': True,
                        'lotto':   result.get('lotto'),
                        'scadenza': result.get('scadenza')})
    except json.JSONDecodeError:
        return jsonify({'success': False, 'error': 'Risposta non valida'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(debug=True, host='0.0.0.0', port=port)
