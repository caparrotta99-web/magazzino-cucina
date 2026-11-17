"""Spesa programmata: impara dagli ordini (i prodotti spuntati nella lista
spesa) e, quando un prodotto viene comprato lo stesso giorno della
settimana per 4 settimane di fila, lo suggerisce per quel giorno.

Esempio: se l'orata è stata spuntata martedì 27/10, 3/11, 10/11 e 17/11,
martedì 24/11 (e già lunedì, come "domani") viene suggerita.

Guarda solo gli ordini: non guarda il magazzino né i carichi registrati.
Non aggiunge niente alla lista spesa da solo.
Questo file non tocca il database e non parla con i fogli Google: riceve
gli ordini già letti e restituisce i suggerimenti.
"""
from datetime import date, datetime, timedelta
from statistics import median

from timezone_utils import TZ_ITALIA

SETTIMANE = 4   # quante settimane di fila, stesso giorno, servono
GIORNI_SETTIMANA = ['lunedì', 'martedì', 'mercoledì', 'giovedì', 'venerdì', 'sabato', 'domenica']


def _giorno(valore):
    """Giorno italiano da '2026-11-17', '2026-11-17T23:30:00' (ora italiana)
    o '2026-11-17T22:30:00+00:00' (UTC: va convertito, a mezzanotte cambia
    giorno)."""
    if isinstance(valore, datetime):
        dt = valore
    elif isinstance(valore, date):
        return valore
    else:
        s = str(valore or '').strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            try:
                return datetime.strptime(s[:10], '%d/%m/%Y').date()
            except ValueError:
                return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ_ITALIA)
    return dt.date()


def _chiave(prodotto):
    return ' '.join(str(prodotto or '').lower().split())


def calcola_suggeriti(ordini, gia_in_lista=(), oggi=None):
    """ordini: dizionari con prodotto, fornitore, data, e se ci sono
    quantita, unita, categoria, reparto.
    gia_in_lista: prodotti già in lista spesa o già spuntati oggi.
    Restituisce i suggerimenti per oggi e per domani."""
    oggi = oggi or date.today()
    esclusi = {_chiave(p) for p in gia_in_lista}

    per_prodotto = {}
    for o in ordini:
        k = _chiave(o.get('prodotto'))
        g = _giorno(o.get('data'))
        if k and g and g <= oggi:
            per_prodotto.setdefault(k, []).append((g, o))

    suggeriti = []
    for quando, giorno in (('oggi', oggi), ('domani', oggi + timedelta(days=1))):
        settimane_prima = [giorno - timedelta(days=7 * i) for i in range(1, SETTIMANE + 1)]
        for k, righe in per_prodotto.items():
            if k in esclusi:
                continue
            giorni = {g for g, _ in righe}
            if giorno in giorni:          # già comprato quel giorno
                continue
            if not all(g in giorni for g in settimane_prima):
                continue
            righe.sort(key=lambda r: r[0])
            con_fornitore = [o for _, o in righe if (o.get('fornitore') or '').strip()]
            quantita = [float(o.get('quantita') or 0) for g, o in righe
                        if g in settimane_prima and float(o.get('quantita') or 0) > 0]

            def ultimo_valore(campo):
                for _, o in reversed(righe):
                    if (o.get(campo) or '').strip():
                        return o[campo].strip()
                return ''

            suggeriti.append({
                'quando':    quando,
                'giorno':    giorno.isoformat(),
                'giorno_settimana': GIORNI_SETTIMANA[giorno.weekday()],
                'prodotto':  (righe[-1][1].get('prodotto') or '').strip(),
                'fornitore': (con_fornitore[-1].get('fornitore') or '').strip() if con_fornitore else '',
                'categoria': ultimo_valore('categoria'),
                'reparto':   ultimo_valore('reparto'),
                'unita':     ultimo_valore('unita'),
                'quantita':  round(median(quantita), 2) if quantita else 0,
                'settimane': SETTIMANE,
            })
        # un prodotto suggerito per oggi non si ripete per domani
        esclusi |= {_chiave(s['prodotto']) for s in suggeriti}

    suggeriti.sort(key=lambda s: (s['quando'] != 'oggi', s['fornitore'].lower(), s['prodotto'].lower()))
    return suggeriti
