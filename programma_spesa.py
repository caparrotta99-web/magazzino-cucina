"""Programma della spesa: impara da cosa avete comprato e propone cosa
ricomprare.

Come funziona, in parole semplici:
- ogni volta che un prodotto viene spuntato nella lista spesa (messo nel
  carrello) l'app si segna il giorno e il fornitore scelto su quella riga;
- per ogni prodotto guarda ogni quanti giorni lo comprate di solito
  (la "mediana" degli intervalli, così una settimana strana non sballa tutto);
- se il prossimo acquisto previsto è oggi, domani o dopodomani (o è già
  passato), il prodotto finisce nella lista "Da ricomprare";
- il fornitore proposto è quello dell'ultimo acquisto.

Non aggiunge niente alla lista spesa da solo: propone, e decidete voi.
Questo file non tocca il database e non parla con i fogli Google: riceve
gli acquisti già letti e restituisce i suggerimenti.
"""
from datetime import date, datetime, timedelta
from statistics import median

ACQUISTI_MINIMI = 3        # servono almeno 3 giorni di acquisto per capire il ritmo
GIORNI_ANTICIPO = 2        # propone i prodotti che servono entro 2 giorni
ABBANDONATO_DOPO = 30      # se non lo comprate da tanto (e oltre 3 giri), non lo propone più
GIORNI_SETTIMANA = ['lun', 'mar', 'mer', 'gio', 'ven', 'sab', 'dom']


def _giorno(valore):
    """Da '2026-11-17', '2026-11-17T18:41:50+00:00' o '17/11/2026' a date."""
    if isinstance(valore, datetime):
        return valore.date()
    if isinstance(valore, date):
        return valore
    s = str(valore or '').strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        pass
    try:
        return datetime.strptime(s[:10], '%d/%m/%Y').date()
    except ValueError:
        return None


def _chiave(prodotto):
    return ' '.join(str(prodotto or '').lower().split())


def giorni_abituali(acquisti):
    """Per ogni fornitore, i giorni della settimana in cui di solito si va
    (almeno 2 volte e almeno un quinto delle uscite da quel fornitore)."""
    uscite = {}
    for a in acquisti:
        f = (a.get('fornitore') or '').strip()
        g = _giorno(a.get('data'))
        if f and g:
            uscite.setdefault(f, set()).add(g)
    risultato = {}
    for f, giorni in uscite.items():
        conta = [0] * 7
        for g in giorni:
            conta[g.weekday()] += 1
        totale = len(giorni)
        risultato[f] = [GIORNI_SETTIMANA[i] for i in range(7)
                        if conta[i] >= 2 and conta[i] >= totale / 5]
    return risultato


def calcola_suggeriti(acquisti, gia_in_lista=(), oggi=None):
    """acquisti: dizionari con prodotto, fornitore, data, e se ci sono
    quantita, unita, categoria, reparto.
    gia_in_lista: nomi dei prodotti già in lista spesa o appena comprati
    (non vanno riproposti)."""
    oggi = oggi or date.today()
    esclusi = {_chiave(p) for p in gia_in_lista}

    per_prodotto = {}
    for a in acquisti:
        k = _chiave(a.get('prodotto'))
        g = _giorno(a.get('data'))
        if not k or not g or g > oggi:
            continue
        per_prodotto.setdefault(k, []).append((g, a))

    abituali = giorni_abituali(acquisti)
    suggeriti = []
    for k, righe in per_prodotto.items():
        if k in esclusi:
            continue
        righe.sort(key=lambda r: r[0])
        giorni = sorted({g for g, _ in righe})
        if len(giorni) < ACQUISTI_MINIMI:
            continue
        intervalli = [(b - a).days for a, b in zip(giorni, giorni[1:])]
        ogni = max(1, round(median(intervalli)))
        ultimo = giorni[-1]
        prossimo = ultimo + timedelta(days=ogni)
        mancano = (prossimo - oggi).days
        da_quanto = (oggi - ultimo).days
        if mancano > GIORNI_ANTICIPO:
            continue
        if da_quanto > max(3 * ogni, ABBANDONATO_DOPO):
            continue

        # l'ultima riga con un fornitore decide il fornitore proposto
        ultima = righe[-1][1]
        con_fornitore = [a for _, a in righe if (a.get('fornitore') or '').strip()]
        fornitore = (con_fornitore[-1].get('fornitore') if con_fornitore else '') or ''
        quantita = [float(a.get('quantita') or 0) for _, a in righe if float(a.get('quantita') or 0) > 0]

        def ultimo_valore(campo):
            for _, a in reversed(righe):
                if (a.get(campo) or '').strip():
                    return a[campo].strip()
            return ''

        suggeriti.append({
            'prodotto':   (ultima.get('prodotto') or '').strip(),
            'fornitore':  fornitore.strip(),
            'categoria':  ultimo_valore('categoria'),
            'reparto':    ultimo_valore('reparto'),
            'unita':      ultimo_valore('unita'),
            'quantita':   round(median(quantita), 2) if quantita else 0,
            'ogni_giorni': ogni,
            'acquisti':   len(giorni),
            'ultimo':     ultimo.isoformat(),
            'prossimo':   prossimo.isoformat(),
            'mancano':    mancano,          # negativo = in ritardo di N giorni
            'giorni_fornitore': abituali.get(fornitore.strip(), []),
        })

    suggeriti.sort(key=lambda s: (s['mancano'], s['fornitore'].lower(), s['prodotto'].lower()))
    return suggeriti
