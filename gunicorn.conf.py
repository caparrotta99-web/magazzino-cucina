# Gunicorn lo legge da solo all'avvio (anche se il comando di Render ha già
# --workers 1). Un solo processo, perché lo scheduler e l'invio ai fogli Google
# girano dentro l'app e non devono partire due volte; ma con più thread:
# così se qualcuno sta scansionando un'etichetta o sincronizzando con i fogli
# (operazioni lente), gli altri possono aprire la lista spesa senza aspettare.
workers = 1
threads = 8


def post_worker_init(worker):
    """Avvia invio ai fogli, ricarica e promemoria nel processo che risponde
    alle richieste, non in quello che lo crea (vedi app.avvia_lavori_in_sottofondo)."""
    import sys
    modulo = sys.modules.get('app')
    if modulo is not None and hasattr(modulo, 'avvia_lavori_in_sottofondo'):
        modulo.avvia_lavori_in_sottofondo()
