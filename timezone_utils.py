from datetime import datetime
from zoneinfo import ZoneInfo

TZ_ITALIA = ZoneInfo('Europe/Rome')


def now_it():
    """Data e ora correnti nel fuso orario italiano (Europe/Rome), con
    passaggio automatico ora solare/legale. Il server gira in UTC: questa
    funzione va usata al posto di datetime.now() ovunque nell'app.
    Restituisce un datetime naive (senza tzinfo) per restare compatibile
    con confronti e stringhe ISO già usati in tutto il resto del codice."""
    return datetime.now(TZ_ITALIA).replace(tzinfo=None)


def today_it():
    """Data odierna (solo giorno) nel fuso orario italiano."""
    return now_it().date()
