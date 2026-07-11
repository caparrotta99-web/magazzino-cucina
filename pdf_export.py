import os
from io import BytesIO

from timezone_utils import now_it

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image,
)

RISTORANTE = 'Ristorante Pizzeria Primavera'
LOGO_PATH  = os.path.join(os.path.dirname(__file__), 'static', 'icon-512.png')

MESI_IT = [
    'Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno',
    'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre',
]

_TIPO_LABEL = {'CARICO': 'Carico', 'IN_USO': 'In uso', 'SCARICO': 'Scarico'}

_styles = getSampleStyleSheet()
_stile_brand = ParagraphStyle(
    'Brand', parent=_styles['Heading1'], fontSize=22, leading=26, spaceAfter=0,
)
_stile_sub = ParagraphStyle(
    'Sub', parent=_styles['Normal'], fontSize=12, textColor=colors.HexColor('#444444'),
    spaceAfter=2,
)
_stile_titolo_report = ParagraphStyle(
    'TitoloReport', parent=_styles['Heading2'], fontSize=14, spaceBefore=10, spaceAfter=2,
)
_stile_meta = ParagraphStyle(
    'Meta', parent=_styles['Normal'], fontSize=9, textColor=colors.HexColor('#666666'),
    spaceAfter=12,
)


def _fascia_oraria(ora):
    try:
        h = int((ora or '').split(':')[0])
    except (ValueError, IndexError):
        return ''
    return 'Sera' if h >= 21 else 'Mattina'


def _fmt_data(iso):
    if not iso:
        return '—'
    try:
        y, m, d = iso.split('-')
        return f'{d}/{m}/{y}'
    except ValueError:
        return iso


def _fmt_num(val):
    if val in (None, ''):
        return ''
    try:
        f = float(val)
    except (TypeError, ValueError):
        return str(val)
    if f == int(f):
        return str(int(f))
    return f'{f:.2f}'.rstrip('0').rstrip('.')


def _intestazione(titolo_report, sottotitolo):
    """Blocco comune a tutti i PDF: logo, nome app, ristorante, titolo del
    report e data/ora di generazione."""
    elementi = []

    if os.path.isfile(LOGO_PATH):
        logo = Image(LOGO_PATH, width=1.6 * cm, height=1.6 * cm)
        header_tbl = Table(
            [[logo, Paragraph('Brigade', _stile_brand)]],
            colWidths=[1.9 * cm, None],
        )
        header_tbl.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ]))
        elementi.append(header_tbl)
    else:
        elementi.append(Paragraph('Brigade', _stile_brand))

    elementi.append(Paragraph(RISTORANTE, _stile_sub))
    elementi.append(Paragraph(titolo_report, _stile_titolo_report))
    generato_il = now_it().strftime('%d/%m/%Y %H:%M')
    meta = sottotitolo + (' — ' if sottotitolo else '') + f'Generato il {generato_il}'
    elementi.append(Paragraph(meta, _stile_meta))
    return elementi


def _tabella(headers, rows, col_widths, font_size=8):
    if not rows:
        data = [headers, ['Nessun dato disponibile per il periodo selezionato'] + [''] * (len(headers) - 1)]
    else:
        data = [headers] + rows

    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    style = [
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#111827')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), font_size),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cccccc')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')]),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]
    if not rows:
        style.append(('SPAN', (0, 1), (-1, 1)))
        style.append(('ALIGN', (0, 1), (-1, 1), 'CENTER'))
    tbl.setStyle(TableStyle(style))
    return tbl


def _doc(buffer):
    return SimpleDocTemplate(
        buffer, pagesize=landscape(A4),
        topMargin=1.3 * cm, bottomMargin=1.3 * cm,
        leftMargin=1.3 * cm, rightMargin=1.3 * cm,
        title='Brigade — Export HACCP',
    )


def genera_pdf_registro(rows, anno, mese):
    buffer = BytesIO()
    elementi = _intestazione(
        'Registro movimenti', f'{MESI_IT[mese - 1]} {anno}'
    )
    elementi.append(Spacer(1, 6))

    headers = ['Data', 'Prodotto', 'Lotto', 'Scadenza', 'Tipo', 'Quantità', 'Operatore', 'Reparto']
    col_widths = [2.2 * cm, 6.0 * cm, 3.0 * cm, 2.2 * cm, 2.3 * cm, 2.3 * cm, 3.8 * cm, 2.5 * cm]

    data_rows = []
    for r in rows:
        tipo = r.get('tipo') or 'CARICO'
        qty = r['carico'] if tipo == 'CARICO' else r['scarico']
        data_rows.append([
            _fmt_data(r['data']),
            r['prodotto'],
            r['lotto'] or '—',
            _fmt_data(r['scadenza']) if r['scadenza'] else '—',
            _TIPO_LABEL.get(tipo, tipo),
            f"{_fmt_num(qty)} {r['unita']}",
            r['operatore'] or '—',
            r['reparto'] or '—',
        ])

    elementi.append(_tabella(headers, data_rows, col_widths))
    doc = _doc(buffer)
    doc.build(elementi)
    buffer.seek(0)
    return buffer


def genera_pdf_temperature(rows, anno, mese):
    buffer = BytesIO()
    elementi = _intestazione(
        'Registro temperature', f'{MESI_IT[mese - 1]} {anno}'
    )
    elementi.append(Spacer(1, 6))

    headers = ['Data', 'Fascia oraria', 'Apparecchio', 'Temperatura', 'Stato', 'Operatore']
    col_widths = [2.5 * cm, 3.0 * cm, 6.5 * cm, 3.0 * cm, 3.5 * cm, 4.5 * cm]

    data_rows = []
    for r in rows:
        stato = 'OK' if r['esito'] == 'OK' else 'FUORI SOGLIA'
        data_rows.append([
            _fmt_data(r['data']),
            _fascia_oraria(r['ora']),
            r['apparecchio'],
            f"{_fmt_num(r['temperatura'])} °C",
            stato,
            r['operatore'] or '—',
        ])

    elementi.append(_tabella(headers, data_rows, col_widths))
    doc = _doc(buffer)
    doc.build(elementi)
    buffer.seek(0)
    return buffer


def genera_pdf_report_mensile(rows, anno):
    buffer = BytesIO()
    elementi = _intestazione('Report mensile', str(anno))
    elementi.append(Spacer(1, 6))

    headers = ['Prodotto'] + [m[:3] for m in MESI_IT] + ['Tot.']
    col_widths = [5.2 * cm] + [1.55 * cm] * 12 + [1.6 * cm]

    data_rows = []
    for r in rows:
        mesi = r['mesi']
        tot = sum(mesi.values())
        data_rows.append(
            [r['prodotto']] + [_fmt_num(mesi[m]) for m in range(1, 13)] + [_fmt_num(tot)]
        )

    elementi.append(_tabella(headers, data_rows, col_widths, font_size=7))
    doc = _doc(buffer)
    doc.build(elementi)
    buffer.seek(0)
    return buffer
