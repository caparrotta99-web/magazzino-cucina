"""Un finto Google Sheets che vive in memoria.

Serve a provare l'invio in sottofondo senza toccare il foglio vero del
ristorante: le funzioni di sheets.py girano per intero (stesse colonne,
stessa formattazione di date e numeri), ma invece di parlare con Google
scrivono in una lista di liste che il test puo' leggere e confrontare.
"""

# Intestazioni come sul foglio REGISTRO vero.
HEADER_REGISTRO = [
    'Data', 'Fornitore', 'Prodotto', 'Lotto', 'Scadenza',
    'Carico', 'Scarico', 'Unità', 'Etichetta', 'ID', 'Rimanenza lotto',
    'Operatore', 'Reparto', 'Tipo movimento',
]

HEADER_TEMPERATURE = [
    'Data', 'Ora', 'Fascia oraria', 'Apparecchio', 'Temp. rilevata (°C)',
    'Temp. limite (°C)', 'Esito', 'Operatore', 'Note',
]

HEADER_LISTINO = ['Prodotto', 'Fornitore', 'Unità', 'Scorta minima', 'Categoria', 'Reparto']


class FintoWorksheet:
    def __init__(self, header):
        self.valori = [list(header)]

    # --- metodi usati da sheets.py ---
    def row_values(self, n):
        return list(self.valori[n - 1]) if n <= len(self.valori) else []

    def get_all_values(self):
        return [list(r) for r in self.valori]

    def append_row(self, row, value_input_option=None):
        self.valori.append(list(row))

    def update_cell(self, riga, colonna, valore):
        self.valori[riga - 1][colonna - 1] = valore

    def delete_rows(self, riga):
        del self.valori[riga - 1]

    # --- comodita' per i test ---
    @property
    def righe_dati(self):
        """Le righe senza l'intestazione."""
        return [list(r) for r in self.valori[1:]]


class FintoSpreadsheet:
    def __init__(self):
        self.fogli = {
            'REGISTRO': FintoWorksheet(HEADER_REGISTRO),
            'TEMPERATURE': FintoWorksheet(HEADER_TEMPERATURE),
            'LISTINO': FintoWorksheet(HEADER_LISTINO),
        }

    def worksheet(self, nome):
        return self.fogli[nome]


class FintoClient:
    def __init__(self, spreadsheet):
        self._ss = spreadsheet

    def open_by_key(self, key):
        return self._ss


def installa(monkeypatch, sheets_mod):
    """Sostituisce il client di sheets.py con il finto foglio e lo restituisce."""
    ss = FintoSpreadsheet()
    monkeypatch.setattr(sheets_mod, '_get_client', lambda: FintoClient(ss))
    return ss
