# Gunicorn lo legge da solo all'avvio (anche se il comando di Render ha già
# --workers 1). Un solo processo, perché lo scheduler e l'invio ai fogli Google
# girano dentro l'app e non devono partire due volte; ma con più thread:
# così se qualcuno sta scansionando un'etichetta o sincronizzando con i fogli
# (operazioni lente), gli altri possono aprire la lista spesa senza aspettare.
workers = 1
threads = 8
