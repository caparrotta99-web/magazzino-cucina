"""Spesa programmata: impara il ritmo con cui ordinate ogni prodotto e ve lo
consiglia il giorno prima che serva.

Come funziona, in parole semplici:
- guarda solo gli ordini: i prodotti spuntati nella lista spesa, con il
  giorno e il fornitore della riga (non il magazzino, non i carichi);
- per ogni prodotto misura i giorni tra un ordine e l'altro e prende il
  valore tipico (la mediana: una volta strana non sballa il ritmo).
  Mele 2 volte al mese → ogni ~15 giorni; roast-beef 4 volte → ogni ~7;
- prossimo ordine previsto = ultimo ordine + ritmo;
- il giorno prima ve lo consiglia ("domani"); se il giorno è arrivato e non
  è ancora stato ordinato, resta consigliato anche quel giorno ("oggi");
- se il giorno è passato da più di un giorno, smette di proporlo: vuol dire
  che il ritmo è cambiato. Riprende da solo al prossimo ordine;
- consiglia solo i prodotti con un ritmo REGOLARE: almeno 2 intervalli su 3
  devono essere vicini al ritmo tipico. Un prodotto ordinato a caso (una
  volta dopo 2 giorni, poi dopo 10, poi dopo 4) non viene consigliato;
- il ritmo si misura sugli ordini più recenti, così se cambia l'app si adegua;
- nelle righe vecchie (prima che esistesse "comprati") ignora gli "svuota
  lista": il pulsante toglie tutto nello stesso secondo. Spuntare tutta la
  lista insieme (ordine su WhatsApp) invece prende qualche secondo per
  prodotto, e conta come ordine.

Non aggiunge niente alla lista spesa da solo.
Questo file non tocca il database e non parla con i fogli Google: riceve
gli ordini già letti e restituisce i consigli.
"""
from datetime import date, datetime, timedelta
from statistics import median

from timezone_utils import TZ_ITALIA

ORDINI_MINIMI = 4      # servono almeno 4 ordini (3 intervalli) per capire il ritmo
STORICO_GIORNI = 120   # si guardano gli ultimi 4 mesi...
INTERVALLI_RECENTI = 8 # ...ma il ritmo si misura sugli ultimi 8 intervalli
REGOLARITA_MINIMA = 2 / 3   # quota di intervalli vicini al ritmo tipico
SVUOTA_MINIMO = 3      # 3+ prodotti tolti dalla stessa persona nello stesso SECONDO = svuota


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
    gia_in_lista: prodotti già nella lista spesa (non vanno riproposti)."""
    oggi = oggi or date.today()
    esclusi = {_chiave(p) for p in gia_in_lista}

    # righe vecchie tolte in blocco (svuota lista): non sono acquisti
    blocchi = {}
    for o in ordini:
        if o.get('fonte') == 'log':
            chiave_sec = (o.get('utente') or '', str(o.get('data') or '')[:19])
            blocchi[chiave_sec] = blocchi.get(chiave_sec, 0) + 1
    ordini = [o for o in ordini if not (
        o.get('fonte') == 'log'
        and blocchi[(o.get('utente') or '', str(o.get('data') or '')[:19])] >= SVUOTA_MINIMO)]

    per_prodotto = {}
    for o in ordini:
        k = _chiave(o.get('prodotto'))
        g = _giorno(o.get('data'))
        if k and g and g <= oggi and (oggi - g).days <= STORICO_GIORNI:
            per_prodotto.setdefault(k, []).append((g, o))

    suggeriti = []
    for k, righe in per_prodotto.items():
        if k in esclusi:
            continue
        righe.sort(key=lambda r: r[0])
        giorni = sorted({g for g, _ in righe})
        if len(giorni) < ORDINI_MINIMI:
            continue
        intervalli = [(b - a).days for a, b in zip(giorni, giorni[1:])][-INTERVALLI_RECENTI:]
        ogni = max(1, round(median(intervalli)))
        # vicino al ritmo = entro un terzo del ritmo (almeno 1 giorno)
        tolleranza = max(1, round(ogni / 3))
        vicini = sum(1 for i in intervalli if abs(i - ogni) <= tolleranza)
        if vicini / len(intervalli) < REGOLARITA_MINIMA:
            continue
        ultimo = giorni[-1]
        prossimo = ultimo + timedelta(days=ogni)
        mancano = (prossimo - oggi).days
        if mancano == 1:
            quando = 'domani'
        elif mancano == 0:
            quando = 'oggi'
        else:
            continue

        con_fornitore = [o for _, o in righe if (o.get('fornitore') or '').strip()]
        quantita = [float(o.get('quantita') or 0) for _, o in righe if float(o.get('quantita') or 0) > 0]

        def ultimo_valore(campo):
            for _, o in reversed(righe):
                if (o.get(campo) or '').strip():
                    return o[campo].strip()
            return ''

        suggeriti.append({
            'quando':      quando,
            'giorno':      prossimo.isoformat(),
            'prodotto':    (righe[-1][1].get('prodotto') or '').strip(),
            'fornitore':   (con_fornitore[-1].get('fornitore') or '').strip() if con_fornitore else '',
            'categoria':   ultimo_valore('categoria'),
            'reparto':     ultimo_valore('reparto'),
            'unita':       ultimo_valore('unita'),
            'quantita':    round(median(quantita), 2) if quantita else 0,
            'ogni_giorni': ogni,
            'ordini':      len(giorni),
            'regolari':    vicini,
            'intervalli':  len(intervalli),
            'ultimo':      ultimo.isoformat(),
        })

    suggeriti.sort(key=lambda s: (s['quando'] != 'oggi', s['fornitore'].lower(), s['prodotto'].lower()))
    return suggeriti
