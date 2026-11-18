"""Proposta del menu del giorno per la settimana (pranzo: 4 primi + 4 secondi).

Regole, in parole semplici:
- i piatti si prendono dall'archivio (si riempie da solo con i menu scritti);
- un piatto non torna prima di 14 giorni;
- tra i piatti possibili sceglie quello che non fate da più tempo, così
  l'archivio gira tutto e non si ripetono sempre gli stessi;
- nei secondi prova a mettere ogni giorno almeno un pesce e una carne;
- mai due primi della stessa "famiglia" lo stesso giorno (due risotti,
  due gnocchi...): la famiglia è la prima parola del nome;
- i posti già compilati a mano non si toccano, e i giorni chiusi restano vuoti;
- il menu del giorno c'è da martedì a venerdì.

Questo file non tocca il database: riceve archivio e menu già letti e
restituisce i piatti da aggiungere.
"""
from datetime import date, timedelta

GIORNI_SENZA_RIPETERE = 14
POSTI = 4
GIORNI_MENU = (1, 2, 3, 4)   # menu del giorno da martedì a venerdì (lunedì chiuso,
                             # sabato e domenica niente menu del giorno)
_PAROLE_VUOTE = {'di', 'del', 'della', 'delle', 'dei', 'al', 'alla', 'alle', 'ai', 'con', 'e', 'in', 'il', 'la', 'lo', 'le', 'gli'}


def _chiave(nome):
    return ' '.join(str(nome or '').lower().split())


def famiglia(nome):
    """'Risotto ai funghi' → 'risotto'; 'Gnocchi al pesto' → 'gnocchi'."""
    for parola in _chiave(nome).split():
        if parola not in _PAROLE_VUOTE:
            return parola
    return ''


def giorni_settimana(lunedi):
    """I giorni con il menu del giorno: da martedì a venerdì."""
    return [lunedi + timedelta(days=i) for i in GIORNI_MENU]


def proponi_settimana(lunedi, archivio, menu, giorni_chiusi=()):
    """lunedi: date. archivio: [{nome, tipo, categoria}].
    menu: piatti già nel menu (anche delle settimane prima) [{data, tipo, posizione, piatto, categoria}].
    Restituisce i piatti da aggiungere: [{data, tipo, posizione, piatto, categoria}]."""
    chiusi = {str(g) for g in giorni_chiusi}
    # ultima volta in cui ogni piatto è stato messo nel menu
    ultima = {}
    occupati = {}
    for m in menu:
        g = date.fromisoformat(str(m['data'])[:10])
        k = (_chiave(m['piatto']), m['tipo'])
        if k not in ultima or g > ultima[k]:
            ultima[k] = g
        occupati.setdefault((g, m['tipo']), {})[int(m['posizione'])] = m

    proposte = []
    for giorno in giorni_settimana(lunedi):
        if giorno.isoformat() in chiusi:
            continue
        for tipo in ('primo', 'secondo'):
            del_giorno = dict(occupati.get((giorno, tipo), {}))
            liberi = [p for p in range(1, POSTI + 1) if p not in del_giorno]
            if not liberi:
                continue

            def usabile(piatto):
                k = (_chiave(piatto['nome']), tipo)
                if any(_chiave(m['piatto']) == k[0] for m in del_giorno.values()):
                    return False
                u = ultima.get(k)
                if u and abs((giorno - u).days) < GIORNI_SENZA_RIPETERE:
                    return False
                if tipo == 'primo':
                    fam = famiglia(piatto['nome'])
                    if fam and any(famiglia(m['piatto']) == fam for m in del_giorno.values()):
                        return False
                return True

            for posto in liberi:
                candidati = [p for p in archivio if p['tipo'] == tipo and usabile(p)]
                if not candidati:
                    break
                if tipo == 'secondo':
                    presenti = {m.get('categoria') for m in del_giorno.values()}
                    for cat in ('pesce', 'carne'):
                        if cat not in presenti and any(p.get('categoria') == cat for p in candidati):
                            candidati = [p for p in candidati if p.get('categoria') == cat]
                            break
                # quello che non fate da più tempo (mai fatto = prima di tutti)
                candidati.sort(key=lambda p: (ultima.get((_chiave(p['nome']), tipo), date.min), _chiave(p['nome'])))
                scelto = candidati[0]
                nuovo = {'data': giorno.isoformat(), 'tipo': tipo, 'posizione': posto,
                         'piatto': scelto['nome'], 'categoria': scelto.get('categoria') or ''}
                proposte.append(nuovo)
                del_giorno[posto] = nuovo
                ultima[(_chiave(scelto['nome']), tipo)] = giorno
    return proposte


def alternative(giorno, tipo, archivio, menu, quante=6):
    """Piatti consigliati per un posto: quelli usabili, dal meno recente."""
    ultima = {}
    del_giorno = set()
    for m in menu:
        g = date.fromisoformat(str(m['data'])[:10])
        k = _chiave(m['piatto'])
        if m['tipo'] == tipo:
            if k not in ultima or g > ultima[k]:
                ultima[k] = g
            if g == giorno:
                del_giorno.add(k)
    ok = [p for p in archivio if p['tipo'] == tipo and _chiave(p['nome']) not in del_giorno
          and not (ultima.get(_chiave(p['nome'])) and abs((giorno - ultima[_chiave(p['nome'])]).days) < GIORNI_SENZA_RIPETERE)]
    ok.sort(key=lambda p: (ultima.get(_chiave(p['nome']), date.min), _chiave(p['nome'])))
    return [{'nome': p['nome'], 'categoria': p.get('categoria') or '',
             'ultima_volta': ultima[_chiave(p['nome'])].isoformat() if _chiave(p['nome']) in ultima else ''}
            for p in ok[:quante]]
