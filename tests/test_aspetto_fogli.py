"""Lo script dell'aspetto dei fogli deve cambiare solo il formato, mai i valori."""
import os, sys, json
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'strumenti'))
import sistema_aspetto_fogli as saf

HEADER = ['Data', 'Fornitore', 'Prodotto', 'Lotto', 'Scadenza', 'Carico', 'Scarico', 'unità di misura',
          'etichetta', 'ID', 'Rimanenza lotto', 'Operatore', 'Reparto', 'Tipo movimento']


def _req():
    return saf.costruisci_richieste(7, HEADER, 500, 3, [11, 12], 13, saf.COLORI_TIPO)


def test_nessuna_scrittura_di_valori():
    testo = json.dumps(_req())
    for vietato in ('updateCells', 'appendCells', 'deleteDimension', 'insertDimension',
                    'deleteRange', 'userEnteredValue": "0', 'pasteData', 'findReplace'):
        assert vietato not in testo or vietato == 'userEnteredValue": "0'
    tipi = {list(r)[0] for r in _req()}
    assert tipi <= {'deleteConditionalFormatRule', 'deleteBanding', 'repeatCell', 'setDataValidation',
                    'updateSheetProperties', 'updateDimensionProperties', 'addBanding',
                    'addConditionalFormatRule'}


def test_toglie_prima_le_vecchie_regole():
    r = _req()
    assert [list(x)[0] for x in r[:5]] == ['deleteConditionalFormatRule'] * 3 + ['deleteBanding'] * 2
    assert [x['deleteConditionalFormatRule']['index'] for x in r[:3]] == [2, 1, 0]


def test_colonna_id_nascosta_e_tipo_colorato():
    r = _req()
    dims = [x['updateDimensionProperties'] for x in r if 'updateDimensionProperties' in x
            and x['updateDimensionProperties']['range']['dimension'] == 'COLUMNS']
    nascoste = [d['range']['startIndex'] for d in dims if d['properties'].get('hiddenByUser')]
    assert nascoste == [HEADER.index('ID')]
    regole = [x['addConditionalFormatRule']['rule'] for x in r if 'addConditionalFormatRule' in x]
    assert {g['booleanRule']['condition']['values'][0]['userEnteredValue'] for g in regole} == set(saf.COLORI_TIPO)
    assert all(g['ranges'][0]['startColumnIndex'] == 13 for g in regole)


def test_riconosce_le_colonne():
    assert saf._tipo_colonna('Rimanenza lotto') == 'rimanenza'
    assert saf._tipo_colonna('unità di misura') in ('unità', 'misura')
    assert saf._tipo_colonna('Scarico') == 'scarico' and saf._tipo_colonna('Carico') == 'carico'
    assert saf._tipo_colonna('ID') == 'id'
    assert saf._tipo_colonna('Tipo movimento') == 'tipo movimento'
    assert saf._tipo_colonna('Temp. rilevata') == 'rilevata'


def test_niente_filtro_che_permetterebbe_di_riordinare():
    assert not any('setBasicFilter' in r or 'sortRange' in r for r in _req())


def test_numeri_in_formato_automatico_senza_arrotondare():
    for r in _req():
        rc = r.get('repeatCell')
        if rc and 'numberFormat' in rc['fields']:
            nf = rc['cell']['userEnteredFormat'].get('numberFormat')
            assert nf is None or nf.get('type') == 'DATE'
