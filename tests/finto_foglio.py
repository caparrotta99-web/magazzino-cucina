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
    """Un foglio in memoria.

    Le celle conservano il valore come lo scrive sheets.py (i numeri restano
    numeri). In lettura si comporta come Google Sheets, che ha due modi
    diversi di restituire la stessa cella:

      * di base (`FORMATTED_VALUE`) torna il testo COME SI VEDE sul foglio,
        quindi gia' arrotondato dal formato della colonna: se la colonna mostra
        un decimale, 0.75 si legge "0.8";
      * con `value_render_option='UNFORMATTED_VALUE'` torna il valore vero,
        numeri compresi.

    `decimali_mostrati` imita il formato della colonna, cosi' nei test si puo'
    verificare che la rilettura non arrotondi le giacenze.
    """

    def __init__(self, header, decimali_mostrati=1):
        self.valori = [list(header)]
        self.decimali_mostrati = decimali_mostrati

    def _formatta(self, val):
        """Il testo che si vedrebbe nella cella."""
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            return str(val)
        return f"{float(val):.{self.decimali_mostrati}f}"

    # --- metodi usati da sheets.py ---
    def row_values(self, n):
        if n > len(self.valori):
            return []
        return [self._formatta(v) for v in self.valori[n - 1]]

    def get_all_values(self, value_render_option=None):
        if value_render_option == 'UNFORMATTED_VALUE':
            return [list(r) for r in self.valori]
        return [[self._formatta(v) for v in r] for r in self.valori]

    def append_row(self, row, value_input_option=None):
        self.valori.append(list(row))

    def update_cell(self, riga, colonna, valore):
        self.valori[riga - 1][colonna - 1] = valore

    def delete_rows(self, riga):
        del self.valori[riga - 1]

    # --- comodita' per i test ---
    @property
    def righe_dati(self):
        """Le righe senza l'intestazione, coi valori veri (non formattati)."""
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
