const CHAT_CANALE_DESC = {
  generale: 'Canale generale per tutto lo staff',
  cucina:   'Solo per il reparto Cucina',
  sala:     'Solo per il reparto Sala',
  pizzeria: 'Solo per il reparto Pizzeria',
};
const CHAT_BADGE_COLORS = { Admin: '#eab308', Cucina: '#f97316', Sala: '#8b5cf6', Pizzeria: '#0d9488' };
const CHAT_AVATAR_PALETTE = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#0d9488', '#3b82f6', '#8b5cf6', '#ec4899'];

// ── STATE ────────────────────────────────────────────────────────────────
const S = {
  prodotti:     [],
  categorie:    [],
  lottiScarico: [],
  lottiInUso:   [],
  inUso:        [],
  giacenze:     [],
  alerts:       { scadenze: [], scorte: [], count: 0 },
  npOrigin:     'carico',   // 'carico' | 'spesa' — da dove è stato aperto il modal Nuovo prodotto
  activePage:   'home',
  giacFilter:   'all',
  giacRepartoFilter:  '',      // '' | 'cucina' | 'sala' | 'pizzeria'
  giacPageIdx:  0,             // 0 = In magazzino, 1 = In uso (swipe pages)
  homePageIdx:  1,             // 0 = Dashboard, 1 = Azioni rapide
  chatCanale:      'generale',
  chatMessaggi:    [],
  listaSpesa:      [],
  spesaTimers:     {},   // id → { timeout, interval, startTime }
  spesaFornFilter: 'all',
  alertTypeFilter: 'all',   // 'all' | 'scadenza' | 'scorta'
  alertCatFilter:  'all',   // 'all' | category name
  alertRepartoFilter: 'all',   // 'all' | 'cucina' | 'sala' | 'pizzeria'
  caricoData: { prodotto:'', fornitore:'', unita:'', reparto:'', categoria:'', lotto:'', scadenza:'', qty:null, _scanPending:false },
  // Nome letto sull'etichetta che aspetta di essere abbinato a un prodotto
  // del listino: quando l'utente sceglie, l'abbinamento viene salvato.
  caricoScanTesto: '',
  caricoScanNota: '',
  // Fornitore dell'ultimo carico: i suoi prodotti vengono proposti per primi.
  ultimoFornitore: '',
  inUsoReparto:    '', inUsoCategoria:    '',
  prepIngReparto:  '',
  apparecchi:            [],
  locks:                 [],
  puoGestireApparecchi:  false,
  rangeRiferimento:      {},
  temperatureStorico:    [],
  preparazioni:          [],
  prenotazioni:          [],
  coperti:               { totale: 0, pranzo: { rimasti: 0 }, cena: { rimasti: 0 } },
  prenData:              '',
  prenEditId:            null,      // null = nuova prenotazione, altrimenti id in modifica
  prenTavoliSelezionati: [],        // id tavoli selezionati nel form prenotazione
  prenCalMese:           null,      // mese visualizzato nel calendario (1-based)
  prenCalAnno:           null,
  prenCalGiorniPieni:    [],        // date 'YYYY-MM-DD' con sala al completo
  sale:                  [],
  tavoli:                [],
  tavoloEditId:          null,      // null = nuovo tavolo, altrimenti id in modifica
  setupSalaTab:          'mura',    // 'mura' | 'tavoli' (solo dentro Setup Sala)
  salaMuraPercorsoSalvato: '',      // d-string già persistito lato server
  salaMuraSottopercorsi:  [],       // sottopercorsi disegnati nella sessione corrente (non ancora salvati)
  muraTool:              'linea',
  pren4Servizio:         'pranzo', // servizio selezionato nella mappa ('pranzo' | 'cena')
  prenTavoloPreselezionato: null,  // id tavolo preselezionato aprendo "Nuova" dal pannello
  prenCalTavoliPerGiorno: {},      // {iso: [tavolo_id,...]} per i pallini delle mini-mappe
  // Contesto della mappa tavoli condivisa da Setup Sala e Prenotazioni:
  // mode='setup' → planimetria base (Setup Sala, admin); mode='servizio' →
  // stato/posizioni per una data+servizio specifici (mappa Prenotazioni).
  mappaContext: {
    mode: 'servizio', data: null, servizio: 'pranzo', origine: 'home',
    canvasId: 'sala-map-canvas', svgId: 'sala-mura-svg',
  },
};

// ── FILTRI REPARTO + CATEGORIA (Carico / In uso / Scarico) ────────────────
function filterProdotti(items, reparto, categoria) {
  return items.filter(p => {
    const rOk = !reparto   || (p.reparto   || 'Cucina').toLowerCase() === reparto.toLowerCase();
    const cOk = !categoria || (p.categoria || '') === categoria;
    return rOk && cOk;
  });
}

function onInUsoFilterChange(sel) {
  sel.classList.toggle('active', !!sel.value);
  S.inUsoReparto   = document.getElementById('u-filter-reparto').value;
  S.inUsoCategoria = document.getElementById('u-filter-categoria').value;
  resetInUsoProdotto();
  renderAperti();
}

// ── CATEGORIE ────────────────────────────────────────────────────────────
async function loadCategorie() {
  const data = await api('/api/categorie');
  S.categorie = data.categorie || [];
  _updateCategorieUI();
}

function _updateCategorieUI() {
  // Aggiorna select np-categoria nel modal Nuovo Prodotto
  const sel = document.getElementById('np-categoria');
  if (sel) {
    const cur = sel.value;
    sel.innerHTML = '<option value="" disabled' + (cur ? '' : ' selected') + '>Seleziona categoria…</option>' +
      S.categorie.map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('');
    if (cur) sel.value = cur;
  }
  // Aggiorna i filtri categoria di Giacenze, Carico, In uso
  ['filter-categoria', 'c-filter-categoria', 'u-filter-categoria'].forEach(id => {
    const fsel = document.getElementById(id);
    if (!fsel) return;
    const cur = fsel.value;
    fsel.innerHTML = '<option value="">Categoria</option>' +
      S.categorie.map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('');
    if (cur) fsel.value = cur;
  });
}

// ── INIT ─────────────────────────────────────────────────────────────────
async function init() {
  startHdrClock();
  startHomeDashClock();
  animHomeCards();
  _initGiacSwipe();
  _initHomeSwipe();
  _setHomePage(0, false);
  renderNotifBanner();
  initRifiutiGeoWatch();
  await Promise.all([loadCategorie(), loadProdottiCarico(), loadGiacenze(), loadAlerts(), loadListaSpesa(), loadInUso(), loadApparecchi(), loadStoricoTemperature(), loadLocks()]);
  setInterval(loadLocks, 20000);
  renderInUsoProdotti();
  if (CAN_CHAT) loadChatUnreadBadge();

  // Al primo login apri automaticamente il tutorial (menu principale) e
  // segna subito come visto, così non si ripresenta ai login successivi.
  if (!TUTORIAL_VISTO) {
    navTo('help');
    api('/api/tutorial-visto', { method: 'POST' });
  }
}

function setHdrDate() {
  const el = document.getElementById('hdr-date');
  if (!el) return;
  el.textContent = new Date().toLocaleDateString('it-IT', {
    weekday: 'short', day: 'numeric', month: 'short', timeZone: 'Europe/Rome',
  });
}

function setHdrTime() {
  const el = document.getElementById('hdr-time');
  if (!el) return;
  el.textContent = new Date().toLocaleTimeString('it-IT', {
    hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Rome',
  });
}

function startHdrClock() {
  setHdrDate();
  setHdrTime();
  // Allinea il primo aggiornamento al prossimo minuto esatto, poi ripete ogni 60s.
  const msAlProssimoMinuto = 60000 - (Date.now() % 60000);
  setTimeout(() => {
    setHdrDate();
    setHdrTime();
    setInterval(() => { setHdrDate(); setHdrTime(); }, 60000);
  }, msAlProssimoMinuto);
}

// ── HOME — DASHBOARD (Pagina 1) ──────────────────────────────────────────
function _romeHour() {
  return parseInt(new Date().toLocaleString('en-US', { hour: 'numeric', hour12: false, timeZone: 'Europe/Rome' }), 10);
}

function startHomeDashClock() {
  // Aggiorna sia la Dashboard (Pagina 1) sia la topbar integrata di
  // Azioni rapide (Pagina 2) — ogni pagina ha i propri id, quindi si
  // aggiorna solo ciò che esiste.
  const tick = () => {
    const greetTxt = _romeHour() < 18 ? 'Buongiorno' : 'Buonasera';
    const dateTxt  = new Date().toLocaleDateString('it-IT', { weekday: 'long', day: 'numeric', month: 'long', timeZone: 'Europe/Rome' });
    const timeTxt  = new Date().toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Rome' });
    ['home-dash-greeting', 'home-menu-greeting'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = greetTxt;
    });
    ['home-dash-date', 'home-menu-date'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = dateTxt;
    });
    ['home-dash-time', 'home-menu-time'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = timeTxt;
    });
  };
  tick();
  const msAlProssimoMinuto = 60000 - (Date.now() % 60000);
  setTimeout(() => { tick(); setInterval(tick, 60000); }, msAlProssimoMinuto);
}

function renderHomeDashboard() {
  if (!document.getElementById('home-dash-meteo')) return;

  const docks = S.listaSpesa.filter(i => i.fornitore === 'DOCKS' && !i.completato);
  document.getElementById('home-dash-docks-num').textContent = docks.length;
  document.getElementById('home-dash-docks-list').innerHTML = docks.length
    ? docks.slice(0, 8).map(i => `<li>${esc(i.prodotto)}</li>`).join('')
    : '<li class="home-dash-simple-empty">Nessun prodotto da ordinare</li>';

  loadMeteoVigevano();
  loadRifiutiRaccolta();
  if (CAN_CHAT) loadChatPreview();
}

// ── METEO VIGEVANO (Open-Meteo, nessuna chiave richiesta) ────────────────
const METEO_LAT = 45.3167, METEO_LON = 8.8500;

// Icone SVG minimal, mappate dai weather_code WMO restituiti da Open-Meteo
const METEO_ICONS = {
  sereno: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><path d="M12 2v2M12 20v2M4.2 4.2l1.4 1.4M18.4 18.4l1.4 1.4M2 12h2M20 12h2M4.2 19.8l1.4-1.4M18.4 5.6l1.4-1.4"/></svg>',
  nuvoloso: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6.5 19a4.5 4.5 0 0 1-.7-8.94 5.5 5.5 0 0 1 10.6-2.02A4.5 4.5 0 0 1 17.5 19h-11Z"/></svg>',
  nebbia: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6.5 15a4.5 4.5 0 0 1-.7-8.94 5.5 5.5 0 0 1 10.6-2.02A4.5 4.5 0 0 1 17.5 12"/><line x1="3" y1="18" x2="21" y2="18"/><line x1="5" y1="21" x2="19" y2="21"/></svg>',
  pioggia: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6.5 13a4.5 4.5 0 0 1-.7-8.94 5.5 5.5 0 0 1 10.6-2.02A4.5 4.5 0 0 1 17.5 13h-11Z"/><line x1="8" y1="16" x2="7" y2="20"/><line x1="12" y1="16" x2="11" y2="20"/><line x1="16" y1="16" x2="15" y2="20"/></svg>',
  neve: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6.5 13a4.5 4.5 0 0 1-.7-8.94 5.5 5.5 0 0 1 10.6-2.02A4.5 4.5 0 0 1 17.5 13h-11Z"/><line x1="8" y1="17" x2="8" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/><line x1="16" y1="17" x2="16" y2="21"/><line x1="6.5" y1="19" x2="9.5" y2="19"/><line x1="10.5" y1="19" x2="13.5" y2="19"/><line x1="14.5" y1="19" x2="17.5" y2="19"/></svg>',
  temporale: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6.5 13a4.5 4.5 0 0 1-.7-8.94 5.5 5.5 0 0 1 10.6-2.02A4.5 4.5 0 0 1 17.5 13h-11Z"/><polyline points="13 15 10 20 13 20 11 24"/></svg>',
};
function _meteoInfo(code) {
  if (code === 0) return { label: 'Sereno', icon: 'sereno' };
  if ([1, 2, 3].includes(code)) return { label: 'Nuvoloso', icon: 'nuvoloso' };
  if ([45, 48].includes(code)) return { label: 'Nebbia', icon: 'nebbia' };
  if ([51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82].includes(code)) return { label: 'Pioggia', icon: 'pioggia' };
  if ([71, 73, 75, 77, 85, 86].includes(code)) return { label: 'Neve', icon: 'neve' };
  if ([95, 96, 99].includes(code)) return { label: 'Temporale', icon: 'temporale' };
  return { label: 'Variabile', icon: 'nuvoloso' };
}
async function loadMeteoVigevano() {
  // NB: niente flag "già caricato una volta" qui — questa funzione viene
  // richiamata ogni volta che si torna sulla Dashboard (renderHomeDashboard),
  // ed è la cache sessionStorage con TTL 10 minuti qui sotto a decidere se
  // serve un fetch nuovo. Un flag one-shot bloccava ogni fetch successivo
  // al primo, quindi la temperatura restava congelata per l'intera sessione
  // anche ore dopo, pur cambiando l'orario mostrato altrove in pagina.
  try {
    const cached = sessionStorage.getItem('meteoVigevanoV3');
    if (cached) {
      const { t, data } = JSON.parse(cached);
      if (Date.now() - t < 10 * 60 * 1000) { _renderMeteo(data, t); return; }
    }
    // cache: 'no-store' + timestamp in query string: evita che una cache
    // intermedia (browser, proxy, service worker) restituisca una risposta
    // vecchia invece di interrogare davvero Open-Meteo.
    const res = await fetch(`https://api.open-meteo.com/v1/forecast?latitude=${METEO_LAT}&longitude=${METEO_LON}&current=temperature_2m,weather_code,apparent_temperature&daily=temperature_2m_max,temperature_2m_min&timezone=Europe%2FRome&forecast_days=1&_=${Date.now()}`, { cache: 'no-store' });
    const data = await res.json();
    console.log('[Meteo Vigevano] risposta grezza API Open-Meteo:', JSON.stringify(data));
    console.log('[Meteo Vigevano] campi estratti:', {
      rilevato_alle:      data.current && data.current.time,
      temperatura_attuale: data.current && data.current.temperature_2m,
      percepita:           data.current && data.current.apparent_temperature,
      weather_code:        data.current && data.current.weather_code,
      max_oggi:            data.daily && data.daily.temperature_2m_max && data.daily.temperature_2m_max[0],
      min_oggi:            data.daily && data.daily.temperature_2m_min && data.daily.temperature_2m_min[0],
    });
    const t = Date.now();
    sessionStorage.setItem('meteoVigevanoV3', JSON.stringify({ t, data }));
    // Log lato server (solo sulle letture fresche, non su quelle da cache):
    // serve a confrontare nei prossimi giorni la temperatura mostrata con
    // quella reale di Vigevano e decidere se applicare una correzione.
    if (data.current) {
      api('/api/meteo/log', {
        method: 'POST',
        body: { temperatura: data.current.temperature_2m, weather_code: data.current.weather_code },
      }).catch(() => {});
    }
    _renderMeteo(data, t);
  } catch (e) {
    const cond = document.getElementById('home-dash-meteo-cond');
    if (cond) cond.textContent = 'Meteo non disponibile';
  }
}
function _renderMeteo(data, updatedAt) {
  const cur = data && data.current;
  if (!cur) return;
  const info = _meteoInfo(cur.weather_code);
  document.getElementById('home-dash-meteo-temp').textContent = Math.round(cur.temperature_2m) + '°';
  document.getElementById('home-dash-meteo-cond').textContent = info.label;
  document.getElementById('home-dash-meteo-icon').innerHTML = METEO_ICONS[info.icon];

  const daily = data && data.daily;
  const minmaxEl = document.getElementById('home-dash-meteo-minmax');
  if (minmaxEl) {
    minmaxEl.textContent = (daily && daily.temperature_2m_min && daily.temperature_2m_max)
      ? `min ${Math.round(daily.temperature_2m_min[0])}° · max ${Math.round(daily.temperature_2m_max[0])}°`
      : '';
  }

  const updEl = document.getElementById('home-dash-meteo-updated');
  if (updEl && updatedAt) {
    const ora = new Date(updatedAt).toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' });
    updEl.textContent = ` · Aggiornato alle ${ora}`;
  }
}

// ── RACCOLTA RIFIUTI (Open-Meteo per il meteo, geolocalizzazione per la zona) ──
const RIFIUTI_TIPI = {
  organico: {
    label: 'Organico', color: '#4caf50', bg: 'rgba(76,175,80,.22)',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 3.7 18 2c1 4.5-1.5 8-4.5 8-1.5 0-2.7-.3-3.5-1M11 20c1-2.5 2.5-5 4.5-6.5"/></svg>',
  },
  carta: {
    label: 'Carta', color: '#2196f3', bg: 'rgba(33,150,243,.22)',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
  },
  plastica: {
    label: 'Plastica', color: '#fbc02d', bg: 'rgba(251,192,45,.22)',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 2h6v3.5l1.5 2.5v11a2 2 0 0 1-2 2h-5a2 2 0 0 1-2-2V8l1.5-2.5V2Z"/><line x1="9" y1="2" x2="15" y2="2"/></svg>',
  },
  secco: {
    label: 'Indifferenziato', color: '#757575', bg: 'rgba(117,117,117,.22)',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg>',
  },
  vetro: {
    label: 'Vetro e lattine', color: '#26a69a', bg: 'rgba(38,166,154,.22)',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 2h8l-1 9a3 3 0 0 1-3 3 3 3 0 0 1-3-3L8 2Z"/><path d="M9 14v6M15 14v6M7 22h10"/></svg>',
  },
  verde: {
    label: 'Verde e ramaglie', color: '#8d6e63', bg: 'rgba(141,110,99,.22)',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22V12M12 12c-4-1-6-4-6-8 5 0 8 2 8 6M12 12c3-1 5-3 5-6-4 0-6 1.5-6 4"/></svg>',
  },
};

async function loadRifiutiRaccolta() {
  const data = await api('/api/rifiuti/oggi');
  _renderRifiutiRaccolta(data);
}

// ── ZONA RIFIUTI ──────────────────────────────────────────────────────────
// Ognuno sceglie la propria zona (salvata sul suo utente); chi non l'ha mai
// scelta vede quella del ristorante. La posizione si chiede SOLO se non c'è
// nessuna zona, oppure quando si tocca "Rileva automaticamente".
// Prima l'app avviava watchPosition a ogni apertura: su iPhone (app sulla
// Home) il permesso posizione veniva richiesto ogni volta.
let _rifiutiUltimaZonaNota = null;

async function initRifiutiGeoWatch() {
  if (!document.getElementById('home-dash-rifiuti-zona')) return; // card non presente
  const data = await api('/api/rifiuti/oggi');
  if (data && data.zona) {
    _rifiutiUltimaZonaNota = data.zona;
    return; // zona già nota: nessuna richiesta di posizione
  }
  rilevaZonaRifiutiAutomatica();
}

function rilevaZonaRifiutiAutomatica() {
  // Una sola lettura (non un rilevamento continuo).
  if (!navigator.geolocation) {
    console.warn('[Rifiuti] navigator.geolocation non disponibile su questo browser');
    openRifiutiZonaPicker();
    return;
  }
  navigator.geolocation.getCurrentPosition(
    pos => {
      api('/api/rifiuti/posizione', {
        method: 'POST',
        body: { lat: pos.coords.latitude, lon: pos.coords.longitude },
      }).then(res => {
        if (!res || !res.success) { console.warn('[Rifiuti] salvataggio posizione fallito', res); openRifiutiZonaPicker(); return; }
        closeRifiutiZonaPicker();
        _rifiutiUltimaZonaNota = res.zona;
        loadRifiutiRaccolta();
      });
    },
    err => {
      console.warn('[Rifiuti] posizione non disponibile', err.code, err.message);
      openRifiutiZonaPicker(); // scelta manuale della zona
    },
    { enableHighAccuracy: false, maximumAge: 10 * 60 * 1000, timeout: 10000 }
  );
}

async function salvaZonaRifiuti(zona) {
  const res = await api('/api/rifiuti/zona', { method: 'POST', body: { zona } });
  if (!res.success) {
    console.warn('[Rifiuti] salvataggio zona fallito', res);
    toast(res.error || 'Errore', 'error');
    openRifiutiZonaPicker();
    return;
  }
  _rifiutiUltimaZonaNota = zona;
  closeRifiutiZonaPicker();
  loadRifiutiRaccolta();
}

function openRifiutiZonaPicker() { document.getElementById('modal-rifiuti-zona').classList.add('open'); }
function closeRifiutiZonaPicker() { document.getElementById('modal-rifiuti-zona').classList.remove('open'); }
function bgCloseRifiutiZonaPicker(e) { if (e.target === document.getElementById('modal-rifiuti-zona')) closeRifiutiZonaPicker(); }

function _rifiutiChip(t, prefissoLabel) {
  const info = RIFIUTI_TIPI[t];
  if (!info) return '';
  return `<div class="home-dash-rifiuti-chip" style="--rifiuto-color:${info.color};--rifiuto-bg:${info.bg}">
    <span class="home-dash-rifiuti-icon">${info.icon}</span>
    <span class="home-dash-rifiuti-label">${prefissoLabel ? prefissoLabel + ': ' : ''}${info.label}</span>
  </div>`;
}

function _rifiutiOrarioNota(tipi, data) {
  const parti = [];
  if (data.orario_esposizione) parti.push(data.orario_esposizione);
  if (tipi.includes('vetro') && data.vetro_scadenza) parti.push(data.vetro_scadenza);
  return parti.length ? `<div class="home-dash-rifiuti-orario">${esc(parti.join(' · '))}</div>` : '';
}

function _fmtGiornoRifiuti(iso) {
  const d = new Date(iso + 'T00:00:00');
  const s = d.toLocaleDateString('it-IT', { weekday: 'long', day: 'numeric', month: 'long' });
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function toggleRifiutiExpand() {
  document.getElementById('home-dash-rifiuti-card').classList.toggle('expanded');
}

function _renderRifiutiRaccolta(data) {
  const zonaEl = document.getElementById('home-dash-rifiuti-zona');
  const oggiEl = document.getElementById('home-dash-rifiuti-oggi');
  const bodyEl = document.getElementById('home-dash-rifiuti-body');
  if (!zonaEl || !oggiEl || !bodyEl) return;

  document.querySelectorAll('#modal-rifiuti-zona .chat-action-opt[data-zona]').forEach(b =>
    b.classList.toggle('selected', b.dataset.zona === data.zona));

  if (!data.zona) {
    zonaEl.textContent = 'Zona non impostata';
    oggiEl.textContent = 'Imposta la zona per vedere il calendario';
    bodyEl.innerHTML = '';
    return;
  }
  zonaEl.textContent = data.zona_label;

  // Riga sempre visibile: solo lo stato di stasera, in una riga.
  oggiEl.textContent = data.tipi.length
    ? 'Stasera esponi: ' + data.tipi.map(t => RIFIUTI_TIPI[t] ? RIFIUTI_TIPI[t].label : t).join(', ')
    : 'Nessuna raccolta stasera';

  // Pannello espandibile (aperto al tap): dettaglio di stasera + calendario
  // completo della settimana.
  let html = '<div class="home-dash-rifiuti-section-title">Stasera esponi</div>';
  html += data.tipi.length
    ? data.tipi.map(t => _rifiutiChip(t)).join('') + _rifiutiOrarioNota(data.tipi, data)
    : '<div class="home-dash-rifiuti-prossimo-none">Nessuna raccolta stasera</div>';

  if (data.domani) {
    html += '<div class="home-dash-rifiuti-section-title">Domani sera</div>';
    html += data.domani.tipi.length
      ? data.domani.tipi.map(t => _rifiutiChip(t)).join('') + _rifiutiOrarioNota(data.domani.tipi, data)
      : '<div class="home-dash-rifiuti-prossimo-none">Nessuna raccolta domani</div>';
  }

  if (data.prossimi && data.prossimi.length) {
    html += '<div class="home-dash-rifiuti-section-title">Prossime sere</div><div class="home-dash-rifiuti-prossimi">';
    html += data.prossimi.map(p => `
      <div class="home-dash-rifiuti-prossimo-row">
        <span class="home-dash-rifiuti-prossimo-giorno">${esc(_fmtGiornoRifiuti(p.data))}</span>
        <span class="home-dash-rifiuti-prossimo-tipi">${
          p.tipi.length
            ? p.tipi.map(t => `<span class="home-dash-rifiuti-prossimo-badge" style="--rifiuto-color:${RIFIUTI_TIPI[t] ? RIFIUTI_TIPI[t].color : '#999'};--rifiuto-bg:${RIFIUTI_TIPI[t] ? RIFIUTI_TIPI[t].bg : 'transparent'}">${RIFIUTI_TIPI[t] ? esc(RIFIUTI_TIPI[t].label) : esc(t)}</span>`).join('')
            : '<span class="home-dash-rifiuti-prossimo-none">—</span>'
        }</span>
      </div>`).join('');
    html += '</div>';
  }

  bodyEl.innerHTML = html;
}

// ── ANTEPRIMA CHAT (dashboard) ────────────────────────────────────────────
function _fmtChatOra(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  const oggi = new Date();
  const stessoGiorno = d.toDateString() === oggi.toDateString();
  return stessoGiorno
    ? d.toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' })
    : d.toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit' });
}
async function loadChatPreview() {
  const wrap = document.getElementById('home-dash-chat-wrap');
  if (!wrap) return;
  const [dataMsg, dataUnread] = await Promise.all([
    api('/api/chat/preview'),
    api('/api/chat/unread-count'),
  ]);
  const msgs  = dataMsg.messaggi || [];
  const count = dataUnread.count || 0;

  // Nessun messaggio: un link piccolo invece della card grande — non ha
  // senso occupare tutto quello spazio per dire "non c'è niente".
  if (!msgs.length) {
    wrap.innerHTML = `
      <a class="home-dash-chat-link" onclick="navTo('chat')">
        Chat <span class="home-dash-chat-link-arrow">→</span>
      </a>`;
    return;
  }

  // Con messaggi: solo l'ultimo, su una riga compatta (niente più elenco
  // di 4 righe né pulsante "Apri chat" separato — la card è già tappabile).
  // get_chat_preview ritorna i messaggi in ordine cronologico crescente
  // (come get_chat_messaggi): il più recente è l'ULTIMO elemento, non il
  // primo — indipendentemente da chi lo ha inviato.
  const m = msgs[msgs.length - 1];
  wrap.innerHTML = `
    <div class="home-dash-card home-dash-card-chat" onclick="navTo('chat')">
      <div class="home-dash-chat-compact">
        <div class="home-dash-chat-compact-info">
          <span class="home-dash-chat-nome">${esc(m.utente_nome)}</span>
          <span class="home-dash-chat-testo">${m.eliminato ? '<em>Messaggio eliminato</em>' : esc(m.testo || (m.foto ? 'Foto' : ''))}</span>
        </div>
        <div class="home-dash-chat-compact-meta">
          ${count > 0 ? `<span class="home-dash-badge" style="display:inline-block">${count}</span>` : ''}
          <span class="home-dash-chat-ora">${_fmtChatOra(m.creato_il)}</span>
        </div>
      </div>
    </div>`;
}

// ── TEMA ─────────────────────────────────────────────────────────────────
async function toggleTema() {
  const html   = document.documentElement;
  const scuro  = html.getAttribute('data-theme') === 'dark';
  const nuovo  = scuro ? 'light' : 'dark';
  html.setAttribute('data-theme', nuovo);

  document.querySelectorAll('.js-theme-icon-on').forEach(el  => el.style.display = nuovo === 'dark' ? 'none'  : 'block');
  document.querySelectorAll('.js-theme-icon-off').forEach(el => el.style.display = nuovo === 'dark' ? 'block' : 'none');

  const metaTheme = document.querySelector('meta[name="theme-color"]');
  if (metaTheme) metaTheme.setAttribute('content', nuovo === 'dark' ? '#0f2c35' : '#ffffff');

  await api('/api/tema', { method: 'POST', body: { tema: nuovo === 'dark' ? 'scuro' : 'chiaro' } });
}

// ── DRAWER ───────────────────────────────────────────────────────────────
function openDrawer() {
  document.getElementById('drawer').classList.add('open');
  document.getElementById('drawer-overlay').classList.add('open');
  document.getElementById('drawer').setAttribute('aria-hidden', 'false');
  document.querySelectorAll('.js-btn-hamburger').forEach(b => b.setAttribute('aria-expanded', 'true'));
}
function closeDrawer() {
  document.getElementById('drawer').classList.remove('open');
  document.getElementById('drawer-overlay').classList.remove('open');
  document.getElementById('drawer').setAttribute('aria-hidden', 'true');
  document.querySelectorAll('.js-btn-hamburger').forEach(b => b.setAttribute('aria-expanded', 'false'));
}

// ── NAVIGATION ───────────────────────────────────────────────────────────
function navTo(page) {
  closeDrawer();
  S.activePage = page;

  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  const pg = document.getElementById(`page-${page}`);
  if (pg) pg.classList.add('active');

  document.querySelectorAll('.drw-item[data-page]').forEach(b =>
    b.classList.toggle('active', b.dataset.page === page));

  window.scrollTo({ top: 0, behavior: 'smooth' });

  document.body.classList.toggle('home-active', page === 'home');
  if (page === 'home') { animHomeCards(); _setHomePage(1, false); }
  else                 { document.body.classList.remove('home-p1'); }
  if (page === 'carico')     resetCaricoWizard();
  if (page === 'giacenze')   { updateFornitoriFilter(); renderGiacenze(); _setGiacPage(0, false); }
  if (page === 'alert')      renderAlerts();
  if (page === 'lista-spesa') loadListaSpesa();
  if (page === 'temperature') { loadApparecchi(); loadStoricoTemperature(); }
  if (page === 'controllo')  loadControlloLogs();
  if (page === 'export')    initExportPage();
  if (page === 'preparazioni') loadPreparazioni();
  if (page === 'prenotazioni') initPrenotazioniPage();
  if (page === 'setup-sala') initSetupSalaPage();
  if (page === 'help')      closeTutorialSection(); // riparti sempre dal menu principale del tutorial

  document.body.classList.toggle('chat-active', page === 'chat');
  // <html> (non <body>) è il vero elemento di scroll del documento: senza
  // bloccarlo qui, il margin-top di <main> "collassa" verso l'alto e rende
  // l'intera pagina scrollabile di quel tanto, scoprendo lo sfondo sotto la
  // barra di input quando si scrolla (vedi regola html.chat-no-scroll).
  document.documentElement.classList.toggle('chat-no-scroll', page === 'chat');
  if (page === 'chat') initChatPage(); else stopChatPolling();
}
// alias for backward compat

// ── HOME ANIMATION ───────────────────────────────────────────────────────
function animHomeCards() {
  document.querySelectorAll('.hcard').forEach((c, i) => {
    c.classList.remove('visible');
    setTimeout(() => c.classList.add('visible'), 80 + i * 110);
  });
}

// ── GIACENZE FILTERS ─────────────────────────────────────────────────────
function setGiacFilter(name, btn) {
  S.giacFilter = name;
  document.querySelectorAll('.fpill').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  renderGiacenze();
}
function onFilterSelectChange(sel) {
  sel.classList.toggle('active', !!sel.value);
}
function updateFornitoriFilter() {
  const sel = document.getElementById('filter-fornitore');
  const cur = sel.value;
  const fornitori = [...new Set(S.giacenze.map(g => g.fornitore).filter(Boolean))].sort();
  sel.innerHTML = '<option value="">Fornitore</option>' +
    fornitori.map(f => `<option value="${esc(f)}">${esc(f)}</option>`).join('');
  if (cur) sel.value = cur;
  sel.classList.toggle('active', !!sel.value);
}

// ── LOCK MOVIMENTI ───────────────────────────────────────────────────────
// Lock "soft" condiviso da Carico/Scarico/In uso/Preparazioni/Temperature:
// avvisa se un collega sta già lavorando sulla stessa risorsa, ma non
// impedisce di procedere se l'utente conferma comunque (non ruba il lock).
async function tryAcquireLock({ tipo, prodotto = '', lotto = '', apparecchio_id = 0, form }) {
  const res = await api('/api/locks/acquire', {
    method: 'POST',
    body: { tipo, prodotto, lotto, apparecchio_id, form },
  });
  if (res.success) return true;
  const cosa = tipo === 'apparecchio' ? 'apparecchio' : 'prodotto';
  return confirm(`Attenzione! ${res.locked_by} sta già lavorando su questo ${cosa}\n\nVuoi continuare comunque?`);
}
function releaseLock({ tipo, prodotto = '', lotto = '', apparecchio_id = 0 }) {
  api('/api/locks/release', { method: 'POST', body: { tipo, prodotto, lotto, apparecchio_id } });
}

// ── CARICO ───────────────────────────────────────────────────────────────
async function loadProdottiCarico() {
  const data = await api('/api/prodotti');
  S.prodotti = data.prodotti || [];
}

function resetCaricoWizard() {
  releaseCaricoLock();
  S.caricoData = { prodotto:'', fornitore:'', unita:'', reparto:'', categoria:'', lotto:'', scadenza:'', qty:null, _scanPending:false };
  S.caricoScanTesto = '';
  const searchEl = document.getElementById('carico-search-input');
  if (searchEl) searchEl.value = '';
  document.getElementById('carico-search-dropdown')?.classList.remove('open');
  document.getElementById('carico-step-1').style.display = '';
  document.getElementById('carico-step-2').style.display = 'none';
  document.getElementById('carico-success').classList.remove('show');
}

function closeCaricoWizard() {
  resetCaricoWizard();
  S.ultimoFornitore = '';   // la consegna in corso finisce qui
  navTo('home');
}

function releaseCaricoLock() {
  if (!S.caricoData?.prodotto) return;
  releaseLock({ tipo: 'prodotto_lotto', prodotto: S.caricoData.prodotto, lotto: '' });
}

function onCaricoSearchInput() {
  const q  = document.getElementById('carico-search-input').value.trim().toLowerCase();
  const dd = document.getElementById('carico-search-dropdown');
  const forn = S.ultimoFornitore || '';

  // A campo vuoto, subito dopo un carico, si propongono gli altri prodotti
  // dello stesso fornitore: e' il caso di una consegna da registrare tutta.
  if (!q) {
    if (!forn) { dd.classList.remove('open'); dd.innerHTML = ''; return; }
    const stesso = S.prodotti.filter(p => (p.fornitore || '') === forn).slice(0, 30);
    if (!stesso.length) { dd.classList.remove('open'); dd.innerHTML = ''; return; }
    dd.innerHTML = `<div class="spesa-search-empty">Altri prodotti di ${esc(forn)}</div>` +
      stesso.map(p => `
      <div class="spesa-search-item" onclick="pickCaricoSearchResult(${esc(JSON.stringify(p))})">
        <span class="spesa-search-item-nome">${esc(p.prodotto)}</span>
        <span class="spesa-search-item-forn">${esc(p.fornitore || '—')}${p.reparto ? ' · ' + esc(p.reparto) : ''}</span>
      </div>`).join('');
    dd.classList.add('open');
    return;
  }

  const matches = S.prodotti.filter(p =>
    p.prodotto.toLowerCase().includes(q) || (p.fornitore || '').toLowerCase().includes(q)
  ).sort((a, b) => {
    // stesso fornitore dell'ultimo carico in cima, poi ordine alfabetico
    const sa = (a.fornitore || '') === forn ? 0 : 1;
    const sb = (b.fornitore || '') === forn ? 0 : 1;
    return sa - sb || a.prodotto.localeCompare(b.prodotto);
  }).slice(0, 30);

  dd.innerHTML = matches.length
    ? matches.map(p => `
      <div class="spesa-search-item" onclick="pickCaricoSearchResult(${esc(JSON.stringify(p))})">
        <span class="spesa-search-item-nome">${esc(p.prodotto)}</span>
        <span class="spesa-search-item-forn">${esc(p.fornitore || '—')}${p.reparto ? ' · ' + esc(p.reparto) : ''}</span>
      </div>`).join('')
    : (S.caricoScanTesto
        // Dopo una scansione il prodotto esiste gia' a listino, solo con un
        // altro nome: va cercato e abbinato, non creato da capo.
        ? `<div class="spesa-search-empty">Nessun prodotto trovato con questo nome. Cerca come si chiama nel listino.</div>`
        : `<div class="spesa-search-empty">Nessun prodotto trovato</div>
       <div class="spesa-search-addnew" onclick="openNuovoProdotto('carico')">
         <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
         Aggiungi nuovo prodotto
       </div>`);
  dd.classList.add('open');
}

function _closeCaricoDropdownDelayed() {
  setTimeout(() => {
    document.getElementById('carico-search-dropdown')?.classList.remove('open');
  }, 150);
}

async function pickCaricoSearchResult(p) {
  document.getElementById('carico-search-dropdown').classList.remove('open');

  // Veniamo da una scansione che non aveva riconosciuto il prodotto: ora che
  // l'utente ha scelto, l'app impara e la prossima volta lo trova da sola.
  if (S.caricoScanTesto) {
    api('/api/etichette/abbina', {
      method: 'POST',
      body: { testo: S.caricoScanTesto, prodotto: p.prodotto, fornitore: p.fornitore || '' },
    }).catch(() => {});
    S.caricoScanTesto = '';
  }

  S.caricoData.prodotto  = p.prodotto;
  S.caricoData.fornitore = p.fornitore || '';
  S.caricoData.unita     = p.unita || '';
  S.caricoData.reparto   = p.reparto || '';
  S.caricoData.categoria = p.categoria || '';
  await goToCaricoStep2();
}

async function goToCaricoStep2() {
  const ok = await tryAcquireLock({
    tipo: 'prodotto_lotto', prodotto: S.caricoData.prodotto, lotto: '', form: 'carico',
  });
  if (!ok) return;

  document.getElementById('c2-prodotto').textContent  = S.caricoData.prodotto;
  document.getElementById('c2-fornitore').textContent = S.caricoData.fornitore || '—';
  document.getElementById('c2-reparto').textContent   = S.caricoData.reparto || '—';
  document.getElementById('c2-qty-unita').textContent = S.caricoData.unita || '';

  const nota = document.getElementById('c2-qty-nota');
  if (S.caricoScanNota) {
    nota.textContent = S.caricoScanNota;
    nota.classList.add('show');
    S.caricoScanNota = '';
  } else {
    nota.textContent = '';
    nota.classList.remove('show');
  }

  const lottoEl = document.getElementById('c2-lotto');
  const scadEl  = document.getElementById('c2-scadenza');
  if (S.caricoData._scanPending) {
    _typeIntoField(lottoEl, S.caricoData.lotto || '');
    scadEl.value = S.caricoData.scadenza || '';
    S.caricoData._scanPending = false;
  } else {
    lottoEl.value = S.caricoData.lotto || '';
    scadEl.value  = S.caricoData.scadenza || '';
  }
  document.getElementById('c2-qty').value = S.caricoData.qty ?? '';

  document.getElementById('carico-step-1').style.display = 'none';
  document.getElementById('carico-step-2').style.display = '';
}

function adjustCaricoQty(delta) {
  const el = document.getElementById('c2-qty');
  const unitaMinuta = ['kg', 'lt', 'l'].includes((S.caricoData.unita || '').toLowerCase());
  const step = unitaMinuta ? 0.1 : 1;
  let val = parseFloat(el.value) || 0;
  val = Math.max(0, Math.round((val + delta * step) * 100) / 100);
  el.value = val;
}

async function confermaCarico() {
  // Una schermata sola: i campi si leggono e si controllano qui, senza il
  // passaggio di riepilogo che prima stava in mezzo.
  const lotto    = document.getElementById('c2-lotto').value.trim();
  const scadenza = document.getElementById('c2-scadenza').value;
  const qty      = parseFloat(document.getElementById('c2-qty').value);

  if (!qty || qty <= 0) {
    toast('Inserisci il peso o la quantità', 'error');
    document.getElementById('c2-qty').focus();
    return;
  }
  if (!lotto) {
    toast('Inserisci il numero di lotto', 'error');
    document.getElementById('c2-lotto').focus();
    return;
  }

  S.caricoData.lotto    = lotto;
  S.caricoData.scadenza = scadenza;
  S.caricoData.qty      = qty;

  const btn = document.getElementById('btn-carico-confirm');
  btn.classList.add('loading'); btn.disabled = true;

  const payload = {
    fornitore: S.caricoData.fornitore, prodotto: S.caricoData.prodotto,
    lotto: S.caricoData.lotto, scadenza: S.caricoData.scadenza,
    qty: S.caricoData.qty, unita: S.caricoData.unita,
  };

  let data = await api('/api/carico', { method: 'POST', body: payload });

  if (data.duplicate) {
    btn.classList.remove('loading'); btn.disabled = false;
    const msg = `Attenzione! Questo prodotto è già stato caricato oggi alle ${data.ora} da ${data.operatore}. Vuoi continuare comunque?`;
    if (!confirm(msg)) return;
    btn.classList.add('loading'); btn.disabled = true;
    data = await api('/api/carico', { method: 'POST', body: { ...payload, force: true } });
  }

  btn.classList.remove('loading'); btn.disabled = false;

  if (!data.success) { toast(data.error || 'Errore durante il carico', 'error'); return; }

  releaseCaricoLock();
  await Promise.all([loadGiacenze(), loadAlerts()]);
  renderInUsoProdotti();
  if (data.warning) toast(data.warning, 'warning');
  showCaricoSuccess();
}

function showCaricoSuccess() {
  document.getElementById('carico-step-2').style.display = 'none';
  // Si scarica quasi sempre una consegna intera: il fornitore appena usato
  // resta in memoria e i suoi prodotti vengono proposti per primi.
  S.ultimoFornitore = S.caricoData.fornitore || '';
  const el = document.getElementById('carico-success');
  el.classList.add('show');
  setTimeout(() => {
    el.classList.remove('show');
    resetCaricoWizard();
    const searchEl = document.getElementById('carico-search-input');
    if (searchEl) {
      onCaricoSearchInput();   // mostra subito i prodotti dello stesso fornitore
      searchEl.focus();
    }
  }, 1300);
}

function triggerCamera() { document.getElementById('camera-input').click(); }

function _typeIntoField(el, value) {
  // Effetto "scrittura" progressiva: popola il campo carattere per carattere
  el.value = '';
  const chars = String(value).split('');
  chars.forEach((ch, i) => setTimeout(() => { el.value += ch; }, i * 35));
}

function compressImage(file, maxSize = 1200, quality = 0.8) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    const url = URL.createObjectURL(file);
    img.onload = () => {
      URL.revokeObjectURL(url);
      let { width, height } = img;
      if (width > maxSize || height > maxSize) {
        if (width > height) { height = Math.round(height * maxSize / width); width = maxSize; }
        else { width = Math.round(width * maxSize / height); height = maxSize; }
      }
      const canvas = document.createElement('canvas');
      canvas.width = width; canvas.height = height;
      canvas.getContext('2d').drawImage(img, 0, 0, width, height);
      resolve(canvas.toDataURL('image/jpeg', quality));
    };
    img.onerror = () => { URL.revokeObjectURL(url); reject(new Error('Impossibile leggere l\'immagine')); };
    img.src = url;
  });
}

function readFileAsDataURL(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => resolve(e.target.result);
    reader.onerror = () => reject(new Error('Impossibile leggere il file'));
    reader.readAsDataURL(file);
  });
}

async function onCameraCapture(input) {
  const file = input.files[0];
  if (!file) return;

  const btn    = document.getElementById('btn-scan');
  const banner = document.getElementById('scan-banner');

  // — Stato loading —
  btn.disabled = true;
  btn.classList.add('scanning');
  btn.innerHTML = '<div class="btn-scan-spinner"></div> Analisi in corso...';
  banner.classList.remove('hiding');
  banner.classList.add('active');

  let image, mediaType = 'image/jpeg';
  try {
    image = await compressImage(file, 1200, 0.8);
  } catch {
    // Fallback: invia il file originale se la compressione fallisce
    image = await readFileAsDataURL(file);
    mediaType = file.type || 'image/jpeg';
  }

  const data = await api('/api/scan-label', {
    method: 'POST',
    body: { image, media_type: mediaType },
  });

  // — Nascondi banner con animazione —
  banner.classList.remove('active');
  banner.classList.add('hiding');
  setTimeout(() => { banner.classList.remove('hiding'); }, 260);

  // — Ripristina bottone —
  btn.disabled = false;
  btn.classList.remove('scanning');
  btn.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/><circle cx="12" cy="13" r="4"/></svg> Scansiona etichetta';

  if (data.success) {
    let filled = false;
    if (data.lotto) {
      S.caricoData.lotto = data.lotto;
      filled = true;
    }
    if (data.scadenza) {
      const parts = String(data.scadenza).split('/');
      if (parts.length === 3) {
        S.caricoData.scadenza = `${parts[2]}-${parts[1].padStart(2,'0')}-${parts[0].padStart(2,'0')}`;
        filled = true;
      }
    }
    if (data.quantita != null && data.quantita > 0) {
      S.caricoData.qty = data.quantita;
      filled = true;
    }
    if (filled) S.caricoData._scanPending = true;

    if (data.prodotto) {
      // Prodotto riconosciuto: si va dritti alla schermata del carico, gia'
      // compilata con lotto, scadenza e peso letti dall'etichetta.
      S.caricoScanTesto = '';
      S.caricoScanNota = data.convertita
        ? `In etichetta ${data.quantita_letta} ${data.unita_letta}, convertito in ${data.quantita} ${data.unita}. Puoi correggerlo.`
        : '';
      S.caricoData.prodotto  = data.prodotto;
      S.caricoData.fornitore = data.fornitore || '';
      S.caricoData.unita     = data.unita || '';
      const inListino = (S.prodotti || []).find(
        p => p.prodotto === data.prodotto && p.fornitore === (data.fornitore || ''));
      S.caricoData.reparto   = inListino?.reparto || '';
      S.caricoData.categoria = inListino?.categoria || '';
      toast(`Etichetta letta: ${data.prodotto}`, 'success');
      await goToCaricoStep2();
    } else if (data.nome_letto) {
      // Nome diverso da quello del listino: si apre la ricerca gia' compilata
      // col nome letto, e la scelta dell'utente diventa un abbinamento salvato.
      S.caricoScanTesto = data.nome_letto;
      const searchEl = document.getElementById('carico-search-input');
      if (searchEl) {
        searchEl.value = data.nome_letto;
        onCaricoSearchInput();
        searchEl.focus();
      }
      toast(`Letto "${data.nome_letto}" — scegli il prodotto nel listino`, 'success');
    } else {
      toast(filled ? 'Etichetta letta — ora seleziona il prodotto' : 'Nessun dato trovato',
            filled ? 'success' : 'error');
    }
  } else {
    toast(data.error || 'Non riesco a leggere l\'etichetta, inserisci manualmente', 'error');
  }
  input.value = '';
}

// ── IN USO ───────────────────────────────────────────────────────────────
// Il prodotto si cerca invece di scorrere un menu lungo come il listino.
function renderInUsoProdotti() {
  const scelto = document.getElementById('u-prodotto')?.value;
  if (!scelto) return;
  // Se il prodotto scelto non ha piu' giacenza, si riparte dalla ricerca.
  const filtered = filterProdotti(S.giacenze, S.inUsoReparto, S.inUsoCategoria);
  if (!filtered.find(g => g.prodotto === scelto)) resetInUsoProdotto();
}

function resetInUsoProdotto() {
  document.getElementById('u-prodotto').value = '';
  document.getElementById('u-lotto').value = '';
  S.lottiInUso = [];
  const scelto = document.getElementById('u-prodotto-scelto');
  scelto.classList.remove('show');
  scelto.innerHTML = '';
  document.getElementById('u-lotti-group').style.display = 'none';
  document.getElementById('u-lotti-cards').innerHTML = '';
  document.getElementById('u-qty').value = '';
  const searchEl = document.getElementById('u-search-input');
  if (searchEl) { searchEl.value = ''; searchEl.style.display = ''; }
  document.getElementById('u-qty-error').classList.remove('show');
}

function onInUsoSearchInput() {
  const q  = document.getElementById('u-search-input').value.trim().toLowerCase();
  const dd = document.getElementById('u-search-dropdown');
  const disponibili = filterProdotti(S.giacenze, S.inUsoReparto, S.inUsoCategoria);

  const matches = (q
    ? disponibili.filter(g => g.prodotto.toLowerCase().includes(q)
        || (g.fornitore || '').toLowerCase().includes(q))
    : disponibili).slice(0, 30);

  dd.innerHTML = matches.length
    ? matches.map(g => `
      <div class="spesa-search-item" onclick="pickInUsoProdotto(${esc(JSON.stringify(g.prodotto))})">
        <span class="spesa-search-item-nome">${esc(g.prodotto)}</span>
        <span class="spesa-search-item-forn">${g.totale != null ? g.totale + ' ' + esc(g.unita || '') : ''}</span>
      </div>`).join('')
    : `<div class="spesa-search-empty">Nessun prodotto in magazzino con questo nome</div>`;
  dd.classList.add('open');
}

function _closeInUsoDropdownDelayed() {
  setTimeout(() => {
    document.getElementById('u-search-dropdown')?.classList.remove('open');
  }, 150);
}

async function pickInUsoProdotto(prodotto) {
  document.getElementById('u-search-dropdown').classList.remove('open');
  document.getElementById('u-prodotto').value = prodotto;

  const scelto = document.getElementById('u-prodotto-scelto');
  scelto.innerHTML = `<span>${esc(prodotto)}</span>`
    + `<span class="u-cambia" onclick="resetInUsoProdotto()">Cambia</span>`;
  scelto.classList.add('show');
  document.getElementById('u-search-input').style.display = 'none';

  await onProdottoInUso();
}

async function onProdottoInUso() {
  const prodotto = document.getElementById('u-prodotto').value;
  const gruppo   = document.getElementById('u-lotti-group');
  document.getElementById('u-lotto').value = '';
  if (!prodotto) { gruppo.style.display = 'none'; return; }

  const data = await api(`/api/lotti?prodotto=${encodeURIComponent(prodotto)}`);
  // Primo chi scade prima: e' l'ordine in cui vanno consumati.
  S.lottiInUso = (data.lotti || []).slice().sort((a, b) => {
    if (!a.scadenza && !b.scadenza) return 0;
    if (!a.scadenza) return 1;
    if (!b.scadenza) return -1;
    return a.scadenza < b.scadenza ? -1 : a.scadenza > b.scadenza ? 1 : 0;
  });

  renderLottiInUso();
  gruppo.style.display = S.lottiInUso.length ? '' : 'none';

  // Il primo lotto buono e' gia' scelto: quasi sempre e' quello giusto e non
  // serve nemmeno toccarlo. Un lotto scaduto invece non si sceglie mai da
  // solo, altrimenti si userebbe roba scaduta senza accorgersene: va toccato
  // a mano, e l'app chiede conferma.
  const i = _primoLottoNonScaduto();
  if (i !== -1) await selezionaLottoInUso(S.lottiInUso[i].lotto, true);
}

function _primoLottoNonScaduto() {
  return S.lottiInUso.findIndex(l => !(l.scadenza && daysTo(l.scadenza) < 0));
}

function renderLottiInUso() {
  const cont     = document.getElementById('u-lotti-cards');
  const scelto   = document.getElementById('u-lotto').value;
  const primoBuono = _primoLottoNonScaduto();
  cont.innerHTML = S.lottiInUso.map((l, i) => {
    const giorni  = l.scadenza ? daysTo(l.scadenza) : null;
    const scaduto = giorni !== null && giorni < 0;
    const scadTxt = l.scadenza
      ? `scade ${fmtDate(l.scadenza)}${giorni !== null ? ` (${giorni}gg)` : ''}`
      : 'senza scadenza';
    const badge = scaduto
      ? '<div class="lotto-card-badge scaduto">Scaduto</div>'
      : (i === primoBuono ? '<div class="lotto-card-badge">Usa prima questo</div>' : '');
    return `
      <div class="lotto-card ${l.lotto === scelto ? 'selected' : ''} ${scaduto ? 'scaduto' : ''}"
           onclick="selezionaLottoInUso(${esc(JSON.stringify(l.lotto))})">
        <div class="lotto-card-top">
          <span class="lotto-card-nome">${esc(l.lotto)}</span>
          <span class="lotto-card-qty">${l.rimanenza} ${esc(l.unita)}</span>
        </div>
        <div class="lotto-card-sub">${scadTxt} · carico ${fmtDate(l.data_carico)}${l.fornitore ? ' · ' + esc(l.fornitore) : ''}</div>
        ${badge}
      </div>`;
  }).join('');
}

async function selezionaLottoInUso(lotto, automatico = false) {
  const prodotto = document.getElementById('u-prodotto').value;
  const l = S.lottiInUso.find(x => x.lotto === lotto);
  if (!l) return;

  // Un lotto scaduto si puo' ancora usare (succede), ma mai per sbaglio.
  const giorni = l.scadenza ? daysTo(l.scadenza) : null;
  if (!automatico && giorni !== null && giorni < 0) {
    if (!confirm(`Il lotto ${l.lotto} è scaduto il ${fmtDate(l.scadenza)}. Vuoi usarlo lo stesso?`)) return;
  }

  if (_inUsoLockKey) { releaseLock({ tipo: 'prodotto_lotto', ..._inUsoLockKey }); _inUsoLockKey = null; }
  const ok = await tryAcquireLock({ tipo: 'prodotto_lotto', prodotto, lotto, form: 'in_uso' });
  if (!ok) return;
  _inUsoLockKey = { prodotto, lotto };

  document.getElementById('u-lotto').value = lotto;
  document.getElementById('u-qty-error').classList.remove('show');
  document.getElementById('u-qty').classList.remove('error');
  renderLottiInUso();
}

function inUsoTutto() {
  const lotto = document.getElementById('u-lotto').value;
  const l = S.lottiInUso.find(x => x.lotto === lotto);
  if (!l) { toast('Scegli prima il lotto', 'error'); return; }
  document.getElementById('u-qty').value = l.rimanenza;
  validateQtyInUso();
}

let _inUsoLockKey = null;

function validateQtyInUso() {
  const lotto = document.getElementById('u-lotto').value;
  const qtyEl = document.getElementById('u-qty');
  const errEl = document.getElementById('u-qty-error');
  const qty   = parseFloat(qtyEl.value);
  const l     = S.lottiInUso.find(x => x.lotto === lotto);
  if (l && qty > 0 && qty > l.rimanenza) {
    errEl.textContent = `Non puoi prelevare più di ${l.rimanenza} ${l.unita} disponibili`;
    errEl.classList.add('show'); qtyEl.classList.add('error'); return false;
  }
  errEl.textContent = ''; errEl.classList.remove('show'); qtyEl.classList.remove('error');
  return true;
}

async function submitInUso() {
  const prodotto = document.getElementById('u-prodotto').value;
  const lotto    = document.getElementById('u-lotto').value;
  const qty      = parseFloat(document.getElementById('u-qty').value);
  if (!prodotto)        { toast('Seleziona il prodotto', 'error'); return; }
  if (!lotto)           { toast('Seleziona il lotto', 'error'); return; }
  if (!qty || qty <= 0) { toast('Inserisci una quantità valida', 'error'); return; }
  if (!validateQtyInUso()) return;

  const btn = document.getElementById('btn-in-uso');
  btn.classList.add('loading'); btn.disabled = true;

  const data = await api('/api/in-uso', { method: 'POST', body: { prodotto, lotto, qty } });

  btn.classList.remove('loading'); btn.disabled = false;

  // Il lotto appena prelevato va liberato PRIMA di ricaricare: subito dopo
  // onProdottoInUso() riseleziona il primo lotto e ne prende uno nuovo.
  if (_inUsoLockKey) { releaseLock({ tipo: 'prodotto_lotto', ..._inUsoLockKey }); _inUsoLockKey = null; }

  if (data.success) {
    toast(`Messo in uso — rimasto in magazzino: ${data.rimanenza}`, 'success');
    document.getElementById('u-qty').value = '';
    await Promise.all([loadGiacenze(), loadAlerts(), loadInUso()]);
    renderInUsoProdotti();
    if (document.getElementById('u-prodotto').value) await onProdottoInUso();
    if (data.warning) setTimeout(() => toast(data.warning, 'warning'), 1800);
  } else { toast(data.error || 'Errore durante il prelievo', 'error'); }
}

// ── SCARICO (finalizza un elemento IN USO, dal pulsante "Scarica" in Giacenze) ─
async function loadInUso() {
  const data = await api('/api/in-uso');
  S.inUso = data.in_uso || [];
  renderAperti();
}

// ── APERTI ADESSO ────────────────────────────────────────────────────────
// Quello che e' gia' stato tirato fuori dal magazzino, in cima alla pagina:
// e' la domanda che ci si fa piu' spesso ("cosa abbiamo aperto?") e da qui si
// chiude in un tocco, senza passare dalle Giacenze.
function renderAperti() {
  const wrap = document.getElementById('aperti-wrap');
  const list = document.getElementById('aperti-list');
  if (!wrap || !list) return;

  const aperti = filterProdotti(S.inUso, S.inUsoReparto, S.inUsoCategoria);
  if (!aperti.length) { wrap.style.display = 'none'; list.innerHTML = ''; return; }

  list.innerHTML = aperti.map(r => {
    const giorni  = r.scadenza ? daysTo(r.scadenza) : null;
    const scaduto = giorni !== null && giorni < 0;
    const scadTxt = r.scadenza
      ? `scade ${fmtDate(r.scadenza)}${giorni !== null ? ` (${giorni}gg)` : ''}`
      : 'senza scadenza';
    return `
      <div class="aperto-card ${scaduto ? 'scaduto' : ''}">
        <div class="aperto-top">
          <span class="aperto-nome">${esc(r.prodotto)}</span>
          <span class="aperto-qty">${r.qty} ${esc(r.unita)}</span>
        </div>
        <div class="aperto-sub">${r.lotto ? 'lotto ' + esc(r.lotto) + ' · ' : ''}aperto il ${fmtDate(r.data)} · ${scadTxt}</div>
        <div class="aperto-azioni">
          <button type="button" class="az-finito"  onclick="apertoFinito(${r.id})">Finito</button>
          <button type="button" class="az-parziale" onclick="openScaricoModal(${r.id})">Parziale</button>
          <button type="button" class="az-buttato" onclick="apertoButtato(${r.id})">Buttato</button>
        </div>
      </div>`;
  }).join('');
  wrap.style.display = '';
}

async function apertoFinito(id) {
  const r = S.inUso.find(x => x.id === id);
  if (!r) return;
  await _chiudiAperto(r, r.qty, 'SCARICO', `${r.prodotto} finito`);
}

async function apertoButtato(id) {
  const r = S.inUso.find(x => x.id === id);
  if (!r) return;
  // Buttare roba e' un movimento che resta nel registro HACCP: va confermato.
  if (!confirm(`Buttare ${r.qty} ${r.unita} di ${r.prodotto}${r.lotto ? ' (lotto ' + r.lotto + ')' : ''}?`)) return;
  await _chiudiAperto(r, r.qty, 'SCARTO', `${r.prodotto} buttato`);
}

async function _chiudiAperto(r, qty, tipo, messaggio) {
  const ok = await tryAcquireLock({ tipo: 'prodotto_lotto', prodotto: r.prodotto, lotto: r.lotto, form: 'scarico' });
  if (!ok) return;
  const data = await api('/api/scarico', { method: 'POST', body: { id: r.id, qty, tipo } });
  releaseLock({ tipo: 'prodotto_lotto', prodotto: r.prodotto, lotto: r.lotto });

  if (data.success) {
    toast(messaggio, 'success');
    await Promise.all([loadInUso(), loadGiacenze(), loadAlerts()]);
    renderInUsoProdotti();
    if (S.activePage === 'giacenze') renderGiacenze();
  } else {
    toast(data.error || 'Errore durante lo scarico', 'error');
  }
}

let _scaricoModalRowId = null;

let _scaricoLockKey = null;

async function openScaricoModal(id) {
  const r = S.inUso.find(x => x.id === id);
  if (!r) return;
  const ok = await tryAcquireLock({ tipo: 'prodotto_lotto', prodotto: r.prodotto, lotto: r.lotto, form: 'scarico' });
  if (!ok) return;
  _scaricoLockKey = { prodotto: r.prodotto, lotto: r.lotto };

  _scaricoModalRowId = id;
  document.getElementById('scarico-modal-info').innerHTML = `
    <div class="lot-info-row"><span>Prodotto</span><strong>${esc(r.prodotto)}</strong></div>
    <div class="lot-info-row"><span>Quantità in uso</span><strong>${r.qty} ${esc(r.unita)}</strong></div>
    <div class="lot-info-row"><span>In uso dal</span><strong>${fmtDate(r.data)}</strong></div>
    <div class="lot-info-row"><span>Data carico</span><strong>${fmtDate(r.data_carico)}</strong></div>
    ${r.lotto     ? `<div class="lot-info-row"><span>Lotto</span><strong>${esc(r.lotto)}</strong></div>`     : ''}
    ${r.fornitore ? `<div class="lot-info-row"><span>Fornitore</span><strong>${esc(r.fornitore)}</strong></div>` : ''}
  `;
  const qtyEl = document.getElementById('scarico-modal-qty');
  qtyEl.value = r.qty;
  qtyEl.max   = r.qty;
  document.getElementById('scarico-modal-qty-error').textContent = '';
  document.getElementById('scarico-modal-qty-error').classList.remove('show');
  qtyEl.classList.remove('error');
  document.getElementById('modal-scarico').classList.add('open');
}
function closeScaricoModal() {
  document.getElementById('modal-scarico').classList.remove('open');
  _scaricoModalRowId = null;
  if (_scaricoLockKey) { releaseLock({ tipo: 'prodotto_lotto', ..._scaricoLockKey }); _scaricoLockKey = null; }
}
function bgCloseScaricoModal(e) { if (e.target === document.getElementById('modal-scarico')) closeScaricoModal(); }

function validateQtyScaricoModal() {
  const qtyEl = document.getElementById('scarico-modal-qty');
  const errEl = document.getElementById('scarico-modal-qty-error');
  const qty   = parseFloat(qtyEl.value);
  const r     = S.inUso.find(x => x.id === _scaricoModalRowId);
  if (r && qty > 0 && qty > r.qty) {
    errEl.textContent = `Non puoi scaricare più di ${r.qty} ${r.unita} in uso`;
    errEl.classList.add('show'); qtyEl.classList.add('error'); return false;
  }
  errEl.textContent = ''; errEl.classList.remove('show'); qtyEl.classList.remove('error');
  return true;
}

async function submitScaricoModal() {
  if (!_scaricoModalRowId) return;
  const qty = parseFloat(document.getElementById('scarico-modal-qty').value);
  if (!qty || qty <= 0) { toast('Inserisci una quantità valida', 'error'); return; }
  if (!validateQtyScaricoModal()) return;

  const btn = document.getElementById('btn-scarico-modal-confirm');
  btn.disabled = true;

  const data = await api('/api/scarico', { method: 'POST', body: { id: _scaricoModalRowId, qty } });

  btn.disabled = false;

  if (data.success) {
    const msg = data.residuo > 0
      ? `Scarico registrato — ${data.prodotto} (${data.residuo} ancora in uso)`
      : `Scarico registrato — ${data.prodotto}`;
    toast(msg, 'success');
    closeScaricoModal();
    await Promise.all([loadInUso(), loadGiacenze(), loadAlerts()]);
    renderInUsoProdotti();
    if (S.activePage === 'giacenze') renderGiacenze();
  } else { toast(data.error || 'Errore durante lo scarico', 'error'); }
}

// ── GIACENZE ─────────────────────────────────────────────────────────────
async function loadGiacenze() {
  const data = await api('/api/giacenze');
  S.giacenze = data.giacenze || [];
  if (S.activePage === 'giacenze') { updateFornitoriFilter(); renderGiacenze(); }
}

function renderGiacenze() {
  const q       = (document.getElementById('search-giacenze').value || '').toLowerCase();
  const forn    = document.getElementById('filter-fornitore').value;
  const cat     = document.getElementById('filter-categoria').value;
  const reparto = document.getElementById('filter-reparto').value;
  const elMag   = document.getElementById('giacenze-list-magazzino');
  const elUso   = document.getElementById('giacenze-list-inuso');

  let list = [...S.giacenze];
  if (q)                           list = list.filter(g => g.prodotto.toLowerCase().includes(q));
  if (forn)                        list = list.filter(g => g.fornitore === forn);
  if (cat)                         list = list.filter(g => g.categoria === cat);
  if (reparto)                     list = list.filter(g => (g.reparto || 'Cucina') === reparto);
  if (S.giacFilter === 'scadenza') list = list.filter(g => g.prossima_scadenza && daysTo(g.prossima_scadenza) <= 7);
  if (S.giacFilter === 'scorta')   list = list.filter(g => g.sotto_scorta);

  list.sort((a, b) => {
    if (!a.prossima_scadenza && !b.prossima_scadenza) return 0;
    if (!a.prossima_scadenza) return 1;
    if (!b.prossima_scadenza) return -1;
    return new Date(a.prossima_scadenza) - new Date(b.prossima_scadenza);
  });

  // Le due sezioni sono indipendenti: un prodotto può comparire in entrambe.
  // "In magazzino" mostra la rimanenza residua (carico - in uso, già netta
  // lato backend); "In uso" mostra la quantità attualmente prelevata.
  const magazzino = list.filter(g => g.totale > 0);
  const inUso     = list.filter(g => g.in_uso > 0);

  const emptyState = (msg) => `<div class="empty-state">
    <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/></svg>
    <p>${msg}</p>
  </div>`;

  const scadCls = (scadenza) => {
    const giorni = scadenza ? daysTo(scadenza) : null;
    return giorni === null ? '' : giorni < 0 ? 'scad-danger' : giorni <= 7 ? 'scad-warn' : 'scad-ok';
  };

  const magazzinoCard = (g) => {
    const idx      = S.giacenze.indexOf(g);
    const lotsHtml = g.lotti.map(l => {
      const etData = esc(JSON.stringify({ prodotto: g.prodotto, lotto: l.lotto, scadenza: l.scadenza }));
      const lock   = _lockFor(g.prodotto, l.lotto);
      return `<div class="giac-lot-row">
        <div class="giac-lot-info">
          <span class="giac-lot-code">${esc(l.lotto)}</span>
          <span class="giac-lot-carico">Carico ${fmtDate(l.data_carico)}</span>
        </div>
        ${l.scadenza ? `<span class="scad-badge ${scadCls(l.scadenza)}">${fmtDate(l.scadenza)}</span>` : ''}
        <span class="giac-lot-qty">${l.rimanenza}&nbsp;${esc(g.unita)}</span>
        ${lock ? `<span class="lock-badge" onclick="event.stopPropagation(); toast('In lavorazione da ${esc(lock.user_nome)}', 'info')" title="In lavorazione da ${esc(lock.user_nome)}">${_ICON_LOCK}</span>` : ''}
        <button type="button" class="lot-print-btn" onclick="event.stopPropagation(); openModificaLotto(${l.id}, ${etData})" aria-label="Modifica lotto">${_ICON_MATITA}</button>
      </div>`;
    }).join('');
    return `<div class="giac-card" onclick="showLotDetail(S.giacenze[${idx}])">
      <div class="giac-top">
        <div>
          <div class="giac-name">${esc(g.prodotto)}</div>
          <div class="giac-fornitore">${esc(g.fornitore||'—')}${g.categoria?`&nbsp;·&nbsp;<span class="cat-badge">${esc(g.categoria)}</span>`:''}</div>
        </div>
        <div>
          <div class="giac-qty-big">${g.totale}</div>
          <div class="giac-unit">${esc(g.unita)}</div>
        </div>
      </div>
      <div class="giac-lots">${lotsHtml}</div>
    </div>`;
  };

  const inUsoCard = (g) => {
    const idx      = S.giacenze.indexOf(g);
    const lotsHtml = S.inUso.filter(r => r.prodotto === g.prodotto).map(l => {
      const lock = _lockFor(g.prodotto, l.lotto);
      return `<div class="giac-lot-row">
        <div class="giac-lot-info">
          <span class="giac-lot-code">${esc(l.lotto)}</span>
          <span class="giac-lot-carico">Carico ${fmtDate(l.data_carico)}</span>
        </div>
        ${l.scadenza ? `<span class="scad-badge ${scadCls(l.scadenza)}">${fmtDate(l.scadenza)}</span>` : ''}
        <span class="giac-lot-qty">${l.qty}&nbsp;${esc(l.unita)}</span>
        ${lock ? `<span class="lock-badge" onclick="event.stopPropagation(); toast('In lavorazione da ${esc(lock.user_nome)}', 'info')" title="In lavorazione da ${esc(lock.user_nome)}">${_ICON_LOCK}</span>` : ''}
        <button type="button" class="lot-print-btn" onclick="event.stopPropagation(); openScaricoModal(${l.id})" aria-label="Scarica">${_ICON_SCARICA}</button>
      </div>`;
    }).join('');
    return `<div class="giac-card" onclick="showLotDetail(S.giacenze[${idx}])">
      <div class="giac-top">
        <div>
          <div class="giac-name">${esc(g.prodotto)}</div>
          <div class="giac-fornitore">${esc(g.fornitore||'—')}${g.categoria?`&nbsp;·&nbsp;<span class="cat-badge">${esc(g.categoria)}</span>`:''}</div>
        </div>
        <div>
          <div class="giac-qty-big">${g.in_uso}</div>
          <div class="giac-unit">${esc(g.unita)}</div>
        </div>
      </div>
      <div class="giac-lots">${lotsHtml}</div>
    </div>`;
  };

  elMag.innerHTML = `<div class="giac-sec-title">In magazzino</div>` +
    (magazzino.length ? magazzino.map(magazzinoCard).join('') : emptyState(q ? 'Nessun prodotto trovato' : 'Nessun prodotto in magazzino'));
  elUso.innerHTML = `<div class="giac-sec-title">In uso</div>` +
    (inUso.length ? inUso.map(inUsoCard).join('') : emptyState(q ? 'Nessun prodotto trovato' : 'Nessun prodotto in uso'));
}

// ── GIACENZE — SWIPE TRA PAGINE ("In magazzino" / "In uso") ─────────────────
function _setGiacPage(idx, animate = true) {
  const viewport = document.getElementById('giac-swipe-viewport');
  const track    = document.getElementById('giac-swipe-track');
  if (!viewport || !track) return;
  idx = Math.max(0, Math.min(1, idx));
  S.giacPageIdx = idx;
  track.style.transition = animate ? '' : 'none';
  track.style.transform  = `translateX(${-idx * viewport.offsetWidth}px)`;
  if (!animate) track.offsetHeight; // forza il reflow prima di riabilitare la transizione
  track.style.transition = '';
  document.querySelectorAll('#giac-dots .giac-dot').forEach((d, i) => d.classList.toggle('active', i === idx));
}

function _initGiacSwipe() {
  const viewport = document.getElementById('giac-swipe-viewport');
  const track    = document.getElementById('giac-swipe-track');
  if (!viewport || !track || viewport.dataset.swipeInit) return;
  viewport.dataset.swipeInit = '1';

  let startX = 0, startY = 0, startTranslate = 0, dragging = false, horizontal = null;

  viewport.addEventListener('touchstart', e => {
    if (e.touches.length !== 1) return;
    startX         = e.touches[0].clientX;
    startY         = e.touches[0].clientY;
    startTranslate = -S.giacPageIdx * viewport.offsetWidth;
    dragging       = true;
    horizontal     = null;
    track.style.transition = 'none';
  }, { passive: true });

  viewport.addEventListener('touchmove', e => {
    if (!dragging) return;
    const dx = e.touches[0].clientX - startX;
    const dy = e.touches[0].clientY - startY;
    if (horizontal === null) {
      if (Math.abs(dx) < 6 && Math.abs(dy) < 6) return;
      horizontal = Math.abs(dx) > Math.abs(dy);
      if (!horizontal) { dragging = false; return; } // scroll verticale nativo
    }
    if (!horizontal) return;
    e.preventDefault();
    const w = viewport.offsetWidth;
    let next = startTranslate + dx;
    const min = -w, max = 0;
    if (next > max) next = max + (next - max) * 0.35;   // resistenza ai bordi
    if (next < min) next = min + (next - min) * 0.35;
    track.style.transform = `translateX(${next}px)`;
  }, { passive: false });

  const endDrag = e => {
    if (!dragging) return;
    dragging = false;
    if (!horizontal) return;
    const w  = viewport.offsetWidth;
    const cx = e.changedTouches ? e.changedTouches[0].clientX : startX;
    const dx = cx - startX;
    let idx  = S.giacPageIdx;
    if (Math.abs(dx) > w * 0.18) idx += dx < 0 ? 1 : -1;
    _setGiacPage(idx);
  };
  viewport.addEventListener('touchend', endDrag);
  viewport.addEventListener('touchcancel', endDrag);

  window.addEventListener('resize', () => _setGiacPage(S.giacPageIdx, false));
}

// ── HOME — SWIPE PAGES ───────────────────────────────────────────────────
function _setHomePage(idx, animate = true) {
  const viewport = document.getElementById('home-swipe-viewport');
  const track    = document.getElementById('home-swipe-track');
  if (!viewport || !track) return;
  const pages  = track.querySelectorAll('.home-swipe-page');
  const maxIdx = pages.length - 1;
  idx = Math.max(0, Math.min(maxIdx, idx));
  S.homePageIdx = idx;
  track.style.transition = animate ? '' : 'none';
  track.style.transform  = `translateX(${-idx * viewport.offsetWidth}px)`;
  if (!animate) track.offsetHeight; // forza il reflow prima di riabilitare la transizione
  track.style.transition = '';
  document.querySelectorAll('#home-switcher .home-switcher-btn').forEach((b, i) => b.classList.toggle('active', i === idx));
  document.body.classList.toggle('home-p1', pages.length > 1 && idx === 0);
  if (pages[idx]) pages[idx].scrollTop = 0;
  if (idx === 0 && pages.length > 1) renderHomeDashboard();
}

function _initHomeSwipe() {
  const viewport = document.getElementById('home-swipe-viewport');
  const track    = document.getElementById('home-swipe-track');
  if (!viewport || !track || viewport.dataset.swipeInit) return;
  viewport.dataset.swipeInit = '1';
  if (track.querySelectorAll('.home-swipe-page').length < 2) return;

  let startX = 0, startY = 0, startTranslate = 0, dragging = false, horizontal = null;

  viewport.addEventListener('touchstart', e => {
    if (e.touches.length !== 1) return;
    startX         = e.touches[0].clientX;
    startY         = e.touches[0].clientY;
    startTranslate = -S.homePageIdx * viewport.offsetWidth;
    dragging       = true;
    horizontal     = null;
    track.style.transition = 'none';
  }, { passive: true });

  viewport.addEventListener('touchmove', e => {
    if (!dragging) return;
    const dx = e.touches[0].clientX - startX;
    const dy = e.touches[0].clientY - startY;
    if (horizontal === null) {
      if (Math.abs(dx) < 6 && Math.abs(dy) < 6) return;
      horizontal = Math.abs(dx) > Math.abs(dy);
      if (!horizontal) { dragging = false; return; } // scroll verticale nativo
    }
    if (!horizontal) return;
    e.preventDefault();
    const w = viewport.offsetWidth;
    let next = startTranslate + dx;
    const min = -w, max = 0;
    if (next > max) next = max + (next - max) * 0.35;   // resistenza ai bordi
    if (next < min) next = min + (next - min) * 0.35;
    track.style.transform = `translateX(${next}px)`;
  }, { passive: false });

  const endDrag = e => {
    if (!dragging) return;
    dragging = false;
    if (!horizontal) return;
    const w  = viewport.offsetWidth;
    const cx = e.changedTouches ? e.changedTouches[0].clientX : startX;
    const dx = cx - startX;
    let idx  = S.homePageIdx;
    if (Math.abs(dx) > w * 0.18) idx += dx < 0 ? 1 : -1;
    _setHomePage(idx);
  };
  viewport.addEventListener('touchend', endDrag);
  viewport.addEventListener('touchcancel', endDrag);

  window.addEventListener('resize', () => _setHomePage(S.homePageIdx, false));
}

// ── TUTORIAL ("?") ───────────────────────────────────────────────────────
// Icone riusate identiche a quelle già presenti in Home/hcard, per coerenza visiva.
const TUT_ICO_CARICO = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>';
const TUT_ICO_INUSO   = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>';
const TUT_ICO_SCARICO = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/></svg>';
const TUT_ICO_GIACENZE = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>';
const TUT_ICO_ALERT   = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';
const TUT_ICO_TEMP    = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 4v10.54a4 4 0 1 1-4 0V4a2 2 0 0 1 4 0Z"/></svg>';
const TUT_ICO_PREP    = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 11h14l-1.2 7.2a2 2 0 0 1-2 1.8H8.2a2 2 0 0 1-2-1.8L5 11Z"/><path d="M7 11a5 5 0 0 1 10 0"/><path d="M12 3v3"/><path d="M8.5 3.8l1 2.6"/><path d="M15.5 3.8l-1 2.6"/></svg>';
const TUT_ICO_SPESA   = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/></svg>';
const TUT_ICO_SHEETS  = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="3" x2="9" y2="21"/><line x1="15" y1="3" x2="15" y2="21"/></svg>';

const TUTORIAL_SECTIONS = [
  {
    id: 'benvenuto', icon: 'img',
    steps: [
      { icon: 'img', title: 'Benvenuto in Brigade!',
        text: 'Brigade è il tuo assistente HACCP digitale. Ti aiuta a tracciare ogni prodotto dal fornitore alla cucina, registrare le temperature e gestire il magazzino in modo semplice e conforme alla normativa.' },
    ],
  },
  {
    id: 'carico', icon: TUT_ICO_CARICO,
    steps: [
      { icon: TUT_ICO_CARICO, title: 'Apri Carico', text: 'Quando arriva la merce dal fornitore, apri Carico dalla home' },
      { icon: TUT_ICO_CARICO, title: 'Cerca o scansiona', text: "Cerca il prodotto scrivendo le prime lettere oppure scansiona l'etichetta con la fotocamera" },
      { icon: TUT_ICO_CARICO, title: 'Lettura automatica AI', text: 'La fotocamera AI legge automaticamente lotto e scadenza dall\'etichetta' },
      { icon: TUT_ICO_CARICO, title: 'Inserisci la quantità', text: 'Inserisci la quantità ricevuta usando i pulsanti + e -' },
      { icon: TUT_ICO_CARICO, title: 'Conferma e genera etichetta', text: "Controlla il riepilogo e tocca Conferma — l'etichetta HACCP viene generata automaticamente" },
      { icon: TUT_ICO_CARICO, title: 'Prodotto non in listino?', text: 'Se il prodotto non è nel listino, usa il pulsante + per aggiungerlo' },
    ],
  },
  {
    id: 'in-uso', icon: TUT_ICO_INUSO,
    steps: [
      { icon: TUT_ICO_INUSO, title: 'Preleva dal magazzino', text: 'Quando prelevi un prodotto dal magazzino per usarlo in cucina, usa In uso' },
      { icon: TUT_ICO_INUSO, title: 'Scegli prodotto e lotto', text: 'Seleziona il prodotto e il lotto — Brigade mostra solo i lotti disponibili' },
      { icon: TUT_ICO_INUSO, title: 'Indica la quantità', text: 'Inserisci la quantità che stai prelevando' },
      { icon: TUT_ICO_INUSO, title: 'Aggiornamento automatico', text: 'Il prodotto passa da Magazzino a In uso nelle Giacenze' },
    ],
  },
  {
    id: 'scarico', icon: TUT_ICO_SCARICO,
    steps: [
      { icon: TUT_ICO_SCARICO, title: 'Prodotto finito?', text: 'Quando un prodotto è completamente finito, vai in Giacenze → tab In uso' },
      { icon: TUT_ICO_SCARICO, title: 'Tocca Scarica', text: 'Trova il prodotto e tocca Scarica' },
      { icon: TUT_ICO_SCARICO, title: 'Anche parziale', text: 'Inserisci la quantità consumata — puoi scaricare anche solo una parte' },
      { icon: TUT_ICO_SCARICO, title: 'Tutto tracciato', text: 'Il movimento viene registrato nel REGISTRO Google Sheets automaticamente' },
    ],
  },
  {
    id: 'giacenze', icon: TUT_ICO_GIACENZE,
    steps: [
      { icon: TUT_ICO_GIACENZE, title: 'Magazzino in tempo reale', text: 'In Giacenze trovi tutto il tuo magazzino in tempo reale' },
      { icon: TUT_ICO_GIACENZE, title: 'Magazzino e In uso', text: 'Swipe verso sinistra per vedere i prodotti In uso, verso destra per il Magazzino' },
      { icon: TUT_ICO_GIACENZE, title: 'Badge colorati', text: 'I badge colorati indicano lo stato: verde=OK, giallo=in scadenza, rosso=scaduto' },
      { icon: TUT_ICO_GIACENZE, title: 'Etichetta stampabile', text: "Tocca un prodotto per vedere l'etichetta stampabile con prodotto, lotto e scadenza" },
      { icon: TUT_ICO_GIACENZE, title: 'Filtra per trovare subito', text: 'Usa i filtri Reparto e Categoria per trovare subito quello che cerchi' },
    ],
  },
  {
    id: 'alert', icon: TUT_ICO_ALERT,
    steps: [
      { icon: TUT_ICO_ALERT, title: 'Scadenze sotto controllo', text: 'Brigade ti avvisa automaticamente quando un prodotto scade entro 7 giorni' },
      { icon: TUT_ICO_ALERT, title: 'Scorte minime', text: 'Ti avvisa anche quando le scorte scendono sotto il minimo impostato nel listino' },
      { icon: TUT_ICO_ALERT, title: 'Aggiungi alla lista spesa', text: 'Tocca il carrello su un prodotto per aggiungerlo direttamente alla Lista Spesa' },
      { icon: TUT_ICO_ALERT, title: 'Filtra gli alert', text: 'Usa i filtri per vedere gli alert per reparto o categoria' },
    ],
  },
  {
    id: 'temperature', icon: TUT_ICO_TEMP,
    steps: [
      { icon: TUT_ICO_TEMP, title: 'Promemoria automatico', text: 'Ogni giorno alle 08:00 e alle 21:00 Brigade ti ricorda di controllare le temperature' },
      { icon: TUT_ICO_TEMP, title: 'Da controllare', text: 'Gli apparecchi mostrano DA CONTROLLARE finché non registri la temperatura' },
      { icon: TUT_ICO_TEMP, title: 'Registra il valore', text: 'Tocca Registra temperatura, inserisci il valore rilevato e conferma' },
      { icon: TUT_ICO_TEMP, title: 'Fuori soglia', text: 'Se la temperatura è fuori range appare FUORI SOGLIA e devi inserire una nota obbligatoria' },
      { icon: TUT_ICO_TEMP, title: 'Storico su Google Sheets', text: 'Tutte le rilevazioni vengono salvate nel foglio TEMPERATURE di Google Sheets' },
      { icon: TUT_ICO_TEMP, title: 'Hai sbagliato?', text: 'Swipe verso sinistra su una rilevazione per eliminarla se hai sbagliato' },
    ],
  },
  {
    id: 'preparazioni', icon: TUT_ICO_PREP,
    steps: [
      { icon: TUT_ICO_PREP, title: 'Prepari in anticipo?', text: 'Quando prepari un piatto in anticipo, registralo in Preparazioni' },
      { icon: TUT_ICO_PREP, title: 'Nome, data, scadenza', text: 'Inserisci il nome della preparazione, la data e la scadenza' },
      { icon: TUT_ICO_PREP, title: 'Ingredienti e lotti', text: 'Aggiungi gli ingredienti usati con i relativi lotti — Brigade scala automaticamente le giacenze' },
      { icon: TUT_ICO_PREP, title: 'Etichetta preparazione', text: "Genera l'etichetta con nome preparazione, data e lista ingredienti" },
      { icon: TUT_ICO_PREP, title: 'Preparazione finita', text: 'Quando la preparazione è finita, tocca Finita per registrare lo scarico definitivo' },
    ],
  },
  {
    id: 'lista-spesa', icon: TUT_ICO_SPESA,
    steps: [
      { icon: TUT_ICO_SPESA, title: 'Aggiungi prodotti', text: 'Aggiungi i prodotti da ordinare manualmente o direttamente dagli Alert' },
      { icon: TUT_ICO_SPESA, title: 'Reparto, fornitore, prodotto', text: 'Scegli reparto, fornitore e prodotto — oppure scrivi direttamente il nome' },
      { icon: TUT_ICO_SPESA, title: 'Filtra per fornitore', text: 'Filtra per fornitore per vedere solo i prodotti da ordinare a quel fornitore' },
      { icon: TUT_ICO_SPESA, title: 'Copia e invia su WhatsApp', text: 'Tocca Copia ordine per copiare la lista e incollarla su WhatsApp' },
      { icon: TUT_ICO_SPESA, title: 'Spunta quando ordini', text: 'Spunta un prodotto quando lo ordini — sparisce automaticamente dopo 5 secondi' },
    ],
  },
  {
    id: 'google-sheets', icon: TUT_ICO_SHEETS,
    steps: [
      { icon: TUT_ICO_SHEETS, title: 'Tutto sincronizzato', text: 'Tutti i movimenti vengono salvati automaticamente nel tuo Google Sheets' },
      { icon: TUT_ICO_SHEETS, title: 'Foglio REGISTRO', text: 'Il foglio REGISTRO contiene ogni carico, scarico e movimento con lotto e operatore' },
      { icon: TUT_ICO_SHEETS, title: 'Foglio TEMPERATURE', text: 'Il foglio TEMPERATURE contiene tutte le rilevazioni con stato OK o FUORI SOGLIA' },
      { icon: TUT_ICO_SHEETS, title: 'Sincronizza manualmente', text: 'Tocca Sincronizza in alto per aggiornare i dati dal listino Google Sheets' },
      { icon: TUT_ICO_SHEETS, title: 'Pronto per un controllo', text: 'In caso di controllo ASL/NAS hai tutto tracciato e disponibile' },
    ],
  },
];

let _tutStepIdx = 0;

function _tutIconHtml(icon) {
  if (icon === 'img') return '<img src="/static/icon.svg" alt="Brigade">';
  if (icon.startsWith('<svg')) return icon;
  return esc(icon); // emoji
}
function _tutIconClass(icon) {
  return (icon !== 'img' && !icon.startsWith('<svg')) ? ' emoji' : '';
}

function openTutorialSection(id) {
  const sec = TUTORIAL_SECTIONS.find(s => s.id === id);
  if (!sec) return;

  const track = document.getElementById('tutorial-swipe-track');
  track.innerHTML = sec.steps.map(st => `
    <div class="tutorial-swipe-page">
      <div class="tutorial-step-icon${_tutIconClass(st.icon)}">${_tutIconHtml(st.icon)}</div>
      <div class="tutorial-step-title">${esc(st.title)}</div>
      <div class="tutorial-step-text">${esc(st.text)}</div>
    </div>`).join('');

  document.getElementById('tutorial-dots').innerHTML =
    sec.steps.map((_, i) => `<span class="tutorial-dot${i === 0 ? ' active' : ''}"></span>`).join('');

  document.getElementById('tutorial-menu').classList.add('hidden');
  document.getElementById('tutorial-section').classList.add('open');

  _initTutorialSwipe();
  _setTutorialStep(0, false);
}

function closeTutorialSection() {
  document.getElementById('tutorial-section').classList.remove('open');
  document.getElementById('tutorial-menu').classList.remove('hidden');
}

function _setTutorialStep(idx, animate = true) {
  const viewport = document.getElementById('tutorial-swipe-viewport');
  const track    = document.getElementById('tutorial-swipe-track');
  if (!viewport || !track) return;
  const pages  = track.querySelectorAll('.tutorial-swipe-page');
  const maxIdx = pages.length - 1;
  if (maxIdx < 0) return;
  idx = Math.max(0, Math.min(maxIdx, idx));
  _tutStepIdx = idx;
  track.style.transition = animate ? '' : 'none';
  track.style.transform  = `translateX(${-idx * viewport.offsetWidth}px)`;
  if (!animate) track.offsetHeight; // forza il reflow prima di riabilitare la transizione
  track.style.transition = '';
  document.querySelectorAll('#tutorial-dots .tutorial-dot').forEach((d, i) => d.classList.toggle('active', i === idx));
  document.getElementById('tutorial-btn-prev').classList.toggle('show', idx > 0);
  document.getElementById('tutorial-btn-next').textContent = idx === maxIdx ? 'Fine' : 'Avanti';
}

function tutorialStep(delta) {
  const track  = document.getElementById('tutorial-swipe-track');
  const maxIdx = track.querySelectorAll('.tutorial-swipe-page').length - 1;
  if (delta > 0 && _tutStepIdx >= maxIdx) { closeTutorialSection(); return; }
  _setTutorialStep(_tutStepIdx + delta);
}

function _initTutorialSwipe() {
  const viewport = document.getElementById('tutorial-swipe-viewport');
  const track    = document.getElementById('tutorial-swipe-track');
  if (!viewport || !track || viewport.dataset.swipeInit) return;
  viewport.dataset.swipeInit = '1';

  let startX = 0, startY = 0, startTranslate = 0, dragging = false, horizontal = null;

  viewport.addEventListener('touchstart', e => {
    if (e.touches.length !== 1) return;
    startX         = e.touches[0].clientX;
    startY         = e.touches[0].clientY;
    startTranslate = -_tutStepIdx * viewport.offsetWidth;
    dragging       = true;
    horizontal     = null;
    track.style.transition = 'none';
  }, { passive: true });

  viewport.addEventListener('touchmove', e => {
    if (!dragging) return;
    const dx = e.touches[0].clientX - startX;
    const dy = e.touches[0].clientY - startY;
    if (horizontal === null) {
      if (Math.abs(dx) < 6 && Math.abs(dy) < 6) return;
      horizontal = Math.abs(dx) > Math.abs(dy);
      if (!horizontal) { dragging = false; return; } // scroll verticale nativo
    }
    if (!horizontal) return;
    e.preventDefault();
    const w      = viewport.offsetWidth;
    const maxIdx = track.querySelectorAll('.tutorial-swipe-page').length - 1;
    let next = startTranslate + dx;
    const min = -maxIdx * w, max = 0;
    if (next > max) next = max + (next - max) * 0.35;   // resistenza ai bordi
    if (next < min) next = min + (next - min) * 0.35;
    track.style.transform = `translateX(${next}px)`;
  }, { passive: false });

  const endDrag = e => {
    if (!dragging) return;
    dragging = false;
    if (!horizontal) return;
    const w  = viewport.offsetWidth;
    const cx = e.changedTouches ? e.changedTouches[0].clientX : startX;
    const dx = cx - startX;
    let idx  = _tutStepIdx;
    if (Math.abs(dx) > w * 0.18) idx += dx < 0 ? 1 : -1;
    _setTutorialStep(idx);
  };
  viewport.addEventListener('touchend', endDrag);
  viewport.addEventListener('touchcancel', endDrag);

  window.addEventListener('resize', () => {
    if (document.getElementById('tutorial-section').classList.contains('open')) _setTutorialStep(_tutStepIdx, false);
  });
}

// ── ALERTS ───────────────────────────────────────────────────────────────
async function loadAlerts() {
  const data  = await api('/api/alerts');
  S.alerts    = data;
  const count = data.count || 0;

  const badge = document.getElementById('drw-alert-badge');
  badge.textContent = count > 0 ? count : '';
  badge.classList.toggle('show', count > 0);

  const num = document.getElementById('home-alert-num');
  if (num) num.textContent = count;

  if (S.activePage === 'alert') renderAlerts();
}

function setAlertTypeFilter(val) {
  S.alertTypeFilter = val;
  renderAlertFilters();
  renderAlerts();
}
function setAlertCatFilter(val) {
  S.alertCatFilter = val || 'all';
  renderAlertFilters();
  renderAlerts();
}
function setAlertRepartoFilter(val) {
  S.alertRepartoFilter = val || 'all';
  renderAlertFilters();
  renderAlerts();
}

function renderAlertFilters() {
  const row = document.getElementById('alert-filter-row');
  if (!row) return;
  const typeFilters = [
    { val: 'all',     lbl: 'Tutti' },
    { val: 'scadenza', lbl: 'In scadenza' },
    { val: 'scorta',   lbl: 'Sotto scorta' },
  ];
  const repValue = S.alertRepartoFilter === 'all' ? '' : S.alertRepartoFilter;
  const catValue = S.alertCatFilter === 'all' ? '' : S.alertCatFilter;
  row.innerHTML = [
    ...typeFilters.map(f =>
      `<button class="fpill ${S.alertTypeFilter === f.val ? 'active' : ''}" onclick="setAlertTypeFilter('${f.val}')">${f.lbl}</button>`
    ),
    `<select class="fselect-pill ${repValue ? 'active' : ''}" onchange="setAlertRepartoFilter(this.value)">
       <option value="">Reparto</option>
       ${REPARTI.map(r => `<option value="${r}" ${repValue===r?'selected':''}>${r.charAt(0).toUpperCase()+r.slice(1)}</option>`).join('')}
     </select>`,
    `<select class="fselect-pill ${catValue ? 'active' : ''}" onchange="setAlertCatFilter(this.value)">
       <option value="">Categoria</option>
       ${S.categorie.map(c => `<option value="${esc(c)}" ${catValue===c?'selected':''}>${esc(c)}</option>`).join('')}
     </select>`,
  ].join('');
}

function renderAlerts() {
  const el = document.getElementById('alerts-content');
  const { scadenze, scorte } = S.alerts;

  renderAlertFilters();

  if (!scadenze.length && !scorte.length) {
    el.innerHTML = `<div class="empty-state">
      <svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
      <p style="color:var(--success);font-weight:600">Nessun alert — tutto a posto</p>
    </div>`;
    return;
  }

  // Flatten all items with type tag
  let items = [
    ...scadenze.map(a => ({ ...a, _type: 'scadenza' })),
    ...scorte.map(a => ({ ...a, _type: 'scorta' })),
  ];

  // Apply type filter
  if (S.alertTypeFilter !== 'all') {
    items = items.filter(a => a._type === S.alertTypeFilter);
  }
  // Apply category filter
  if (S.alertCatFilter !== 'all') {
    items = items.filter(a => (a.categoria || '') === S.alertCatFilter);
  }
  // Apply reparto filter
  if (S.alertRepartoFilter !== 'all') {
    items = items.filter(a => (a.reparto || 'Cucina').toLowerCase() === S.alertRepartoFilter);
  }

  if (!items.length) {
    el.innerHTML = `<div class="empty-state"><p>Nessun alert per questo filtro</p></div>`;
    return;
  }

  // Group by categoria
  const groups = {};
  for (const item of items) {
    const cat = (item.categoria || '').trim() || 'Altro';
    if (!groups[cat]) groups[cat] = [];
    groups[cat].push(item);
  }
  // Sort within groups: scaduti first, then by urgency
  for (const g of Object.values(groups)) {
    g.sort((a, b) => {
      if (a._type !== b._type) return a._type === 'scadenza' ? -1 : 1;
      if (a._type === 'scadenza') return (a.giorni ?? 999) - (b.giorni ?? 999);
      return a.prodotto.localeCompare(b.prodotto, 'it');
    });
  }
  const orderedCats = [
    ...S.categorie.filter(c => groups[c]),
    ...Object.keys(groups).filter(c => !S.categorie.includes(c)).sort(),
  ];

  el.innerHTML = orderedCats.map(cat => {
    const count = groups[cat].length;
    const cardsHtml = groups[cat].map(a => _renderAlertCard(a)).join('');
    return `<div class="alert-cat-group">
      <div class="alert-cat-hdr">
        ${esc(cat)}
        <span class="alert-cat-count">${count}</span>
      </div>
      ${cardsHtml}
    </div>`;
  }).join('');
}

function _renderAlertCard(a) {
  if (a._type === 'scadenza') {
    const isScad = a.tipo === 'scaduto';
    const cls    = isScad ? 'danger' : 'warning';
    const ggTxt  = a.giorni === null ? '' : a.giorni < 0 ? 'Scaduto' : a.giorni === 0 ? 'Scade oggi' : `${a.giorni}gg`;
    const ggCls  = isScad ? 'red' : (a.giorni !== null && a.giorni <= 3 ? 'red' : 'orange');
    return `<div class="alert-card ${cls}">
      <div class="alert-ico ${cls}">
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
      </div>
      <div class="alert-info">
        <div class="alert-prodotto">${esc(a.prodotto)}</div>
        <div class="alert-sub">
          ${a.lotto ? `Lotto <strong>${esc(a.lotto)}</strong> · ` : ''}${a.scadenza ? fmtDate(a.scadenza) : '—'}${a.fornitore ? ` · ${esc(a.fornitore)}` : ''} · Carico ${fmtDate(a.data_carico)}
        </div>
      </div>
      <div class="alert-right">
        <div class="alert-qty">${a.rimanenza}&nbsp;${esc(a.unita)}</div>
        <div class="alert-days ${ggCls}">${ggTxt}</div>
      </div>
    </div>`;
  } else {
    const inLista = S.listaSpesa.some(i => i.prodotto === a.prodotto && i.fornitore === a.fornitore);
    const btnCls  = inLista ? 'btn-add-spesa in-lista' : 'btn-add-spesa';
    const btnEvt  = inLista ? '' : `onclick="addToListaFromAlert(${esc(JSON.stringify(a))})"`;
    const btnIco  = inLista
      ? `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>`
      : `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/></svg>`;
    return `<div class="alert-card scorta">
      <div class="alert-ico scorta">
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
      </div>
      <div class="alert-info">
        <div class="alert-prodotto">${esc(a.prodotto)}</div>
        <div class="alert-sub">${esc(a.fornitore || '—')}</div>
      </div>
      <div class="alert-right">
        <div class="alert-qty">${a.totale}&nbsp;${esc(a.unita)}</div>
        <div class="alert-days orange">min. ${a.scorta_min}</div>
      </div>
      <button class="${btnCls}" ${btnEvt} title="${inLista ? 'Già in lista' : 'Aggiungi alla lista spesa'}">${btnIco}</button>
    </div>`;
  }
}

async function addToListaFromAlert(a) {
  const exists = S.listaSpesa.find(i => i.prodotto === a.prodotto && i.fornitore === a.fornitore);
  if (exists) { toast('Prodotto già in lista', 'info'); return; }

  const quantita = Math.round(Math.max(0, (a.scorta_min || 0) - (a.totale || 0)) * 100) / 100;
  const result = await api('/api/lista-spesa', {
    method: 'POST',
    body: {
      prodotto:  a.prodotto,
      fornitore: a.fornitore || '',
      categoria: a.categoria || '',
      reparto:   a.reparto || '',
      quantita,
      unita:     a.unita || '',
    },
  });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }

  await loadListaSpesa();
  toast('Prodotto aggiunto alla lista spesa', 'success');
  renderAlerts();
}

// ── LOT DETAIL ────────────────────────────────────────────────────────────
const _ICON_STAMPA = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 6 2 18 2 18 9"/><path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"/><rect x="6" y="14" width="12" height="8"/></svg>`;
const _ICON_MATITA = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/></svg>`;
const _ICON_SCARICA = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/></svg>`;
const _ICON_LOCK    = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>`;

// ── LOCK MOVIMENTI (polling) ─────────────────────────────────────────────
async function loadLocks() {
  const data = await api('/api/locks');
  S.locks = data.locks || [];
  if (S.activePage === 'giacenze')    renderGiacenze();
  if (S.activePage === 'temperature') renderApparecchi();
}
function _lockFor(prodotto, lotto) {
  return S.locks.find(l => l.tipo === 'prodotto_lotto' && l.prodotto === prodotto && l.lotto === lotto);
}
function _lockForApparecchio(id) {
  return S.locks.find(l => l.tipo === 'apparecchio' && l.apparecchio_id === id);
}

function showLotDetail(g) {
  document.getElementById('lot-detail-title').textContent = g.prodotto;
  const scortaNote = g.scorta_min > 0
    ? `<div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Scorta minima</span><span style="font-weight:600">${g.scorta_min}&nbsp;${esc(g.unita)}</span></div>` : '';

  const magazzinoHtml = g.lotti.length ? g.lotti.map(l => {
    const giorni = l.scadenza ? daysTo(l.scadenza) : null;
    const cls    = giorni===null ? 'scad-ok' : giorni<0 ? 'scad-danger' : giorni<=7 ? 'scad-warn' : 'scad-ok';
    const etData = esc(JSON.stringify({ prodotto: g.prodotto, lotto: l.lotto, scadenza: l.scadenza }));
    const riga = `<div class="lot-detail-row">
      <div>
        <div class="lot-detail-lotto">${esc(l.lotto)}</div>
        <div class="lot-detail-meta">${l.rimanenza}&nbsp;${esc(g.unita)} · Carico ${fmtDate(l.data_carico)}</div>
      </div>
      ${l.scadenza ? `<span class="lot-scad-pill ${cls}">${fmtDate(l.scadenza)}</span>` : ''}
      <div style="display:flex;gap:6px;flex-shrink:0">
        <button type="button" class="lot-print-btn" onclick="event.stopPropagation(); openModificaLotto(${l.id}, ${etData})" aria-label="Modifica lotto">${_ICON_MATITA}</button>
        <button type="button" class="lot-print-btn" onclick="event.stopPropagation(); printLotLabel(${etData})" aria-label="Stampa etichetta">${_ICON_STAMPA}</button>
      </div>
    </div>`;
    if (!CAN_DELETE_CARICHI) return riga;
    return `<div class="lot-swipe">
      <div class="lot-swipe-delete" onclick="event.stopPropagation(); confermaEliminaCarico(${l.id})">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
        Elimina
      </div>
      ${riga}
    </div>`;
  }).join('') : `<div class="lot-detail-row"><span style="font-size:13px;color:var(--muted)">Nessun lotto in magazzino</span></div>`;

  const inUsoLotti = S.inUso.filter(r => r.prodotto === g.prodotto);
  const inUsoHtml = inUsoLotti.length ? inUsoLotti.map(l => {
    const giorni = l.scadenza ? daysTo(l.scadenza) : null;
    const cls    = giorni===null ? 'scad-ok' : giorni<0 ? 'scad-danger' : giorni<=7 ? 'scad-warn' : 'scad-ok';
    const etData = esc(JSON.stringify({ prodotto: g.prodotto, lotto: l.lotto, scadenza: l.scadenza }));
    return `<div class="lot-detail-row">
      <div>
        <div class="lot-detail-lotto">${esc(l.lotto)}</div>
        <div class="lot-detail-meta">${l.qty}&nbsp;${esc(l.unita)} · Carico ${fmtDate(l.data_carico)}</div>
      </div>
      ${l.scadenza ? `<span class="lot-scad-pill ${cls}">${fmtDate(l.scadenza)}</span>` : ''}
      <button type="button" class="lot-print-btn" onclick="event.stopPropagation(); printLotLabel(${etData})" aria-label="Stampa etichetta">${_ICON_STAMPA}</button>
    </div>`;
  }).join('') : `<div class="lot-detail-row"><span style="font-size:13px;color:var(--muted)">Nessun prodotto in uso</span></div>`;

  document.getElementById('lot-detail-body').innerHTML = scortaNote +
    `<div class="spesa-group-hdr" style="margin:14px 4px 8px">In magazzino</div>` + magazzinoHtml +
    `<div class="spesa-group-hdr" style="margin:18px 4px 8px">In uso</div>` + inUsoHtml;
  document.getElementById('lot-detail-overlay').classList.add('open');
  _initSwipeLotti();
}
function closeLotDetail() {
  document.getElementById('lot-detail-overlay').classList.remove('open');
}

// ── ELIMINAZIONE CARICO (swipe, sezione "In magazzino") ─────────────────────
function _initSwipeLotti() {
  const list = document.getElementById('lot-detail-body');
  if (!list || list.dataset.swipeInit) return;
  list.dataset.swipeInit = '1';
  let state = null;

  list.addEventListener('touchstart', e => {
    const row = e.target.closest('.lot-swipe .lot-detail-row');
    if (!row) return;
    if (state && state.row !== row) _closeSwipeLottoRow(state.row);
    list.querySelectorAll('.lot-swipe .lot-detail-row.open').forEach(r => { if (r !== row) _closeSwipeLottoRow(r); });
    const btn = row.parentElement.querySelector('.lot-swipe-delete');
    state = { row, btn, startX: e.touches[0].clientX, startedOpen: row.classList.contains('open') };
  }, { passive: true });

  list.addEventListener('touchmove', e => {
    if (!state) return;
    const dx = e.touches[0].clientX - state.startX;
    const base = state.startedOpen ? -84 : 0;
    state.lastX = Math.max(-84, Math.min(0, base + dx));
    state.row.style.transform = `translateX(${state.lastX}px)`;
    if (state.btn) state.btn.style.transform = `translateX(${84 + state.lastX}px)`;
  }, { passive: true });

  list.addEventListener('touchend', () => {
    if (!state) return;
    const open = (state.lastX ?? (state.startedOpen ? -84 : 0)) < -42;
    const finalX = open ? -84 : 0;
    state.row.style.transform = `translateX(${finalX}px)`;
    if (state.btn) state.btn.style.transform = `translateX(${84 + finalX}px)`;
    state.row.classList.toggle('open', open);
    state = null;
  });
}
function _closeSwipeLottoRow(row) {
  row.style.transform = 'translateX(0)';
  row.classList.remove('open');
  const btn = row.parentElement.querySelector('.lot-swipe-delete');
  if (btn) btn.style.transform = 'translateX(84px)';
}

async function confermaEliminaCarico(rowId) {
  if (!confirm('Sei sicuro di voler eliminare questo carico?')) return;
  const result = await api(`/api/registro/${rowId}`, { method: 'DELETE' });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  toast('Carico eliminato', 'success');
  closeLotDetail();
  await Promise.all([loadGiacenze(), loadAlerts()]);
}

// ── ETICHETTA LOTTO (Giacenze) ──────────────────────────────────────────────
function printLotLabel(d) {
  document.getElementById('eti-lotto-prodotto').textContent = d.prodotto;
  document.getElementById('eti-lotto-lotto').textContent    = d.lotto || '—';
  document.getElementById('eti-lotto-scadenza').textContent = d.scadenza ? fmtDate(d.scadenza) : '—';
  document.getElementById('modal-eti-lotto').classList.add('open');
}
function closeEtiLotto() { document.getElementById('modal-eti-lotto').classList.remove('open'); }
function bgCloseEtiLotto(e) { if (e.target === document.getElementById('modal-eti-lotto')) closeEtiLotto(); }

async function copiaEtichettaLotto() {
  const prodotto  = document.getElementById('eti-lotto-prodotto').textContent;
  const lotto     = document.getElementById('eti-lotto-lotto').textContent;
  const scadenza  = document.getElementById('eti-lotto-scadenza').textContent;
  const testo = `PRODOTTO: ${prodotto}\nLOTTO: ${lotto}\nSCADENZA: ${scadenza}`;
  try {
    await navigator.clipboard.writeText(testo);
    toast('Testo copiato!', 'success');
  } catch (e) {
    toast('Impossibile copiare il testo', 'error');
  }
}

// ── MODIFICA LOTTO (Giacenze — In magazzino) ────────────────────────────────
function openModificaLotto(rowId, d) {
  const g = S.giacenze.find(x => x.prodotto === d.prodotto);
  const l = g ? g.lotti.find(x => x.id === rowId) : null;
  document.getElementById('ml-row-id').value    = rowId;
  document.getElementById('ml-prodotto').textContent = d.prodotto;
  document.getElementById('ml-qty').value       = l ? l.rimanenza : '';
  document.getElementById('ml-lotto').value     = d.lotto || '';
  document.getElementById('ml-scadenza').value  = d.scadenza || '';
  document.getElementById('modal-modifica-lotto').classList.add('open');
}
function closeModificaLotto() { document.getElementById('modal-modifica-lotto').classList.remove('open'); }
function bgCloseML(e) { if (e.target === document.getElementById('modal-modifica-lotto')) closeModificaLotto(); }

async function submitModificaLotto() {
  const rowId    = parseInt(document.getElementById('ml-row-id').value) || 0;
  const qty      = parseFloat(document.getElementById('ml-qty').value);
  const lotto    = document.getElementById('ml-lotto').value.trim();
  const scadenza = document.getElementById('ml-scadenza').value;

  if (!rowId) return;
  if (!lotto) { toast('Il lotto è obbligatorio', 'error'); return; }
  if (isNaN(qty) || qty < 0) { toast('Quantità non valida', 'error'); return; }

  const result = await api(`/api/registro/${rowId}`, {
    method: 'PUT',
    body: { rimanenza: qty, lotto, scadenza },
  });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }

  closeModificaLotto();
  closeLotDetail();
  toast('Lotto aggiornato', 'success');
  await Promise.all([loadGiacenze(), loadAlerts()]);
}

// ── LISTA SPESA ───────────────────────────────────────────────────────────

function _updateSpesaBadge() {
  const n = S.listaSpesa.filter(i => !i.completato).length;
  const el = document.getElementById('home-spesa-num');
  if (el) el.textContent = n;
}

function _enrichItem(item) {
  // Lookup live stock data at render time
  const giac = S.giacenze.find(g => g.prodotto === item.prodotto);
  const prod = S.prodotti.find(p => p.prodotto === item.prodotto);
  return {
    ...item,
    checked:    !!item.completato,
    qtaAttuale: giac ? giac.totale : 0,
    scorta_min: prod ? prod.scorta_min : 0,
  };
}

async function loadListaSpesa() {
  const data = await api('/api/lista-spesa');
  S.listaSpesa = (data.items || []).map(_enrichItem);
  _updateSpesaBadge();
  if (S.activePage === 'lista-spesa') renderListaSpesa();
}

function _populateSpesaFornitori() {
  const selR = document.getElementById('spesa-sel-reparto');
  if (selR) selR.value = '';
  const selF = document.getElementById('spesa-sel-fornitore');
  if (selF) {
    selF.innerHTML = '<option value="">Seleziona prima il reparto</option>';
    selF.disabled = true;
  }
  const selP = document.getElementById('spesa-sel-prodotto');
  if (selP) { selP.innerHTML = '<option value="">Seleziona prima il reparto</option>'; selP.disabled = true; }
  const searchInput = document.getElementById('spesa-search-input');
  if (searchInput) searchInput.value = '';
  const dd = document.getElementById('spesa-search-dropdown');
  if (dd) { dd.classList.remove('open'); dd.innerHTML = ''; }
}

function _populateSpesaFornitoriForReparto(reparto) {
  const selF = document.getElementById('spesa-sel-fornitore');
  if (!reparto) {
    selF.innerHTML = '<option value="">Seleziona prima il reparto</option>';
    selF.disabled = true;
    return;
  }
  const fornitori = [...new Set(
    S.prodotti.filter(p => (p.reparto || 'Cucina') === reparto).map(p => p.fornitore).filter(Boolean)
  )].sort();
  selF.innerHTML = '<option value="">Tutti i fornitori</option>' +
    fornitori.map(f => `<option value="${esc(f)}">${esc(f)}</option>`).join('');
  selF.disabled = false;
}

function _populateSpesaProdotti(reparto, fornitore) {
  const selP = document.getElementById('spesa-sel-prodotto');
  if (!reparto) {
    selP.innerHTML = '<option value="">Seleziona prima il reparto</option>';
    selP.disabled = true;
    return;
  }
  const filtered = S.prodotti.filter(p =>
    (p.reparto || 'Cucina') === reparto && (!fornitore || p.fornitore === fornitore)
  );
  selP.innerHTML = '<option value="">Scegli prodotto…</option>' +
    filtered.map(p => `<option value="${esc(p.prodotto)}|${esc(p.unita)}">${esc(p.prodotto)}</option>`).join('');
  selP.disabled = false;
}

function onSpesaReparto() {
  const reparto = document.getElementById('spesa-sel-reparto').value;
  _populateSpesaFornitoriForReparto(reparto);
  _populateSpesaProdotti(reparto, '');
}

function onSpesaFornitore() {
  const reparto   = document.getElementById('spesa-sel-reparto').value;
  const fornitore = document.getElementById('spesa-sel-fornitore').value;
  _populateSpesaProdotti(reparto, fornitore);
}

function _setSpesaSelection(reparto, fornitore, prodotto, unita) {
  reparto = reparto || 'Cucina';
  const selR = document.getElementById('spesa-sel-reparto');
  const selF = document.getElementById('spesa-sel-fornitore');
  const selP = document.getElementById('spesa-sel-prodotto');
  selR.value = reparto;
  _populateSpesaFornitoriForReparto(reparto);
  selF.value = fornitore || '';
  _populateSpesaProdotti(reparto, fornitore || '');
  selP.value = `${prodotto}|${unita || ''}`;
}

function onSpesaSearchInput() {
  const q  = document.getElementById('spesa-search-input').value.trim().toLowerCase();
  const dd = document.getElementById('spesa-search-dropdown');
  if (!q) { dd.classList.remove('open'); dd.innerHTML = ''; return; }

  const matches = S.prodotti.filter(p =>
    p.prodotto.toLowerCase().includes(q) || (p.fornitore || '').toLowerCase().includes(q)
  ).slice(0, 30);

  dd.innerHTML = matches.length
    ? matches.map(p => `
      <div class="spesa-search-item" onclick="pickSpesaSearchResult(${esc(JSON.stringify(p))})">
        <span class="spesa-search-item-nome">${esc(p.prodotto)}</span>
        <span class="spesa-search-item-forn">${esc(p.fornitore || '—')}${p.reparto ? ' · ' + esc(p.reparto) : ''}</span>
      </div>`).join('')
    : `<div class="spesa-search-empty">Nessun prodotto trovato</div>
       <div class="spesa-search-addnew" onclick="openNuovoProdotto('spesa')">
         <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
         Aggiungi nuovo prodotto
       </div>`;
  dd.classList.add('open');
}

function pickSpesaSearchResult(p) {
  document.getElementById('spesa-search-input').value = p.prodotto;
  document.getElementById('spesa-search-dropdown').classList.remove('open');
  _setSpesaSelection(p.reparto || 'Cucina', p.fornitore || '', p.prodotto, p.unita || '');
}

function _closeSpesaDropdownDelayed() {
  setTimeout(() => {
    document.getElementById('spesa-search-dropdown')?.classList.remove('open');
  }, 150);
}


function _spesaItem(item) {
  const sottoScorta = item.scorta_min > 0 && item.qtaAttuale < item.scorta_min;

  // Costruisci lista fornitori: tutti dal listino + quello corrente se diverso
  const tuttiF = [...new Set(S.prodotti.map(p => p.fornitore).filter(Boolean))].sort();
  if (item.fornitore && !tuttiF.includes(item.fornitore)) tuttiF.unshift(item.fornitore);
  const fornitoriOpts = ['', ...tuttiF].map(f =>
    `<option value="${esc(f)}" ${f === (item.fornitore || '') ? 'selected' : ''}>${f || '— fornitore —'}</option>`
  ).join('');

  const extraInfo = [
    item.qtaAttuale > 0 ? `Att: ${item.qtaAttuale}${item.unita ? ' ' + esc(item.unita) : ''}` : '',
    item.scorta_min    ? `Min: ${item.scorta_min}${item.unita ? ' ' + esc(item.unita) : ''}`  : '',
  ].filter(Boolean).join(' · ');

  return `
    <div class="spesa-item ${item.checked ? 'checked' : ''}" data-spesa-id="${item.id}">
      <div class="spesa-countdown-bar" id="spesa-bar-${item.id}"></div>
      <div class="spesa-check" onclick="toggleSpesaCheck(${item.id})">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
      </div>
      <div class="spesa-info">
        <div class="spesa-name">
          ${esc(item.prodotto)}
          ${sottoScorta ? '<span class="spesa-auto-tag">SOTTO SCORTA</span>' : ''}
        </div>
        <div class="spesa-sub" style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:4px">
          <select class="spesa-forn-select" onchange="updateSpesaFornitore(${item.id}, this.value)">${fornitoriOpts}</select>
          ${extraInfo ? `<span>${extraInfo}</span>` : ''}
        </div>
      </div>
      <div class="spesa-right">
        <div class="spesa-qty">${item.quantita > 0 ? item.quantita : '—'}</div>
        <div class="spesa-qty-lbl">${esc(item.unita)}</div>
      </div>
      <button class="btn-spesa-del" onclick="removeSpesaItem(${item.id})" title="Rimuovi">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
      </button>
    </div>`;
}

function renderSpesaFilterPills() {
  const row = document.getElementById('spesa-filter-pills');
  if (!row) return;
  const fornitori = [...new Set(S.listaSpesa.map(i => i.fornitore).filter(Boolean))].sort();
  row.innerHTML =
    `<button class="fpill ${S.spesaFornFilter === 'all' ? 'active' : ''}" onclick="setSpesaFilter('all')">Tutti</button>` +
    fornitori.map(f =>
      `<button class="fpill ${S.spesaFornFilter === f ? 'active' : ''}" onclick="setSpesaFilter('${esc(f)}')">${esc(f)}</button>`
    ).join('');
}

function setSpesaFilter(forn) {
  S.spesaFornFilter = forn;
  renderListaSpesa();
}

function renderListaSpesa() {
  // Update counter (always across full list, not filtered)
  const pendenti = S.listaSpesa.filter(i => !i.completato).length;
  const ctr = document.getElementById('spesa-counter');
  if (ctr) ctr.innerHTML = `<strong>${pendenti}</strong> prodott${pendenti === 1 ? 'o' : 'i'} da ordinare`;

  renderSpesaFilterPills();

  const container = document.getElementById('spesa-list');
  if (!container) return;

  if (!S.listaSpesa.length) {
    container.innerHTML = `
      <div class="empty-state">
        <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/></svg>
        <p>Lista vuota — aggiungi prodotti manualmente o tramite il bottone carrello nella pagina Alert</p>
      </div>`;
    return;
  }

  // Apply fornitore filter
  let visible = S.spesaFornFilter === 'all'
    ? S.listaSpesa
    : S.listaSpesa.filter(i => i.fornitore === S.spesaFornFilter);

  if (!visible.length) {
    container.innerHTML = `<div class="empty-state"><p>Nessun prodotto per questo filtro</p></div>`;
    return;
  }

  // Group by categoria with fixed order; unknowns go to end
  const groups = {};
  for (const item of visible) {
    const cat = (item.categoria || '').trim() || 'Altro';
    if (!groups[cat]) groups[cat] = [];
    groups[cat].push(item);
  }

  // Sort within each group: unchecked first, then alphabetically
  for (const g of Object.values(groups)) {
    g.sort((a, b) => a.checked - b.checked || a.prodotto.localeCompare(b.prodotto, 'it'));
  }

  // Build ordered category keys
  const orderedCats = [
    ...S.categorie.filter(c => groups[c]),
    ...Object.keys(groups).filter(c => !S.categorie.includes(c)).sort(),
  ];

  container.innerHTML = orderedCats.map(cat => {
    const label = cat;
    const items = groups[cat];
    const count = items.length;
    return `
      <div class="spesa-group">
        <div class="spesa-group-hdr">
          ${esc(label)}
          <span class="spesa-cat-count">${count}</span>
        </div>
        ${items.map(item => _spesaItem(item)).join('')}
      </div>`;
  }).join('');

  // Riallinea le barre dei countdown attivi dopo il re-render DOM
  _restoreCountdownBars();
}

// ── LISTA SPESA — COUNTDOWN ───────────────────────────────────────────────
const _SPESA_COUNTDOWN_MS = 5000;

function _startSpesaCountdown(id) {
  _cancelSpesaCountdown(id); // evita doppi timer
  const startTime = Date.now();

  const interval = setInterval(() => {
    const bar = document.getElementById(`spesa-bar-${id}`);
    if (!bar) return;
    const elapsed  = Date.now() - startTime;
    const progress = Math.min(elapsed / _SPESA_COUNTDOWN_MS * 100, 100);
    bar.style.width = progress + '%';
  }, 50);

  const timeout = setTimeout(async () => {
    clearInterval(interval);
    delete S.spesaTimers[id];

    // Leggi l'altezza attuale prima di iniziare l'animazione di rimozione
    const el = document.querySelector(`[data-spesa-id="${id}"]`);
    if (el) {
      el.style.maxHeight = el.offsetHeight + 'px';
      // Lascia un frame per applicare max-height, poi aggiungi .removing
      requestAnimationFrame(() => {
        requestAnimationFrame(() => { el.classList.add('removing'); });
      });
      await new Promise(r => setTimeout(r, 450));
    }

    S.listaSpesa = S.listaSpesa.filter(i => i.id !== id);
    _updateSpesaBadge();
    renderListaSpesa();
    api(`/api/lista-spesa/${id}`, { method: 'DELETE' });
  }, _SPESA_COUNTDOWN_MS);

  S.spesaTimers[id] = { timeout, interval, startTime };
}

function _cancelSpesaCountdown(id) {
  const t = S.spesaTimers[id];
  if (!t) return;
  clearTimeout(t.timeout);
  clearInterval(t.interval);
  delete S.spesaTimers[id];
}

function _restoreCountdownBars() {
  const now = Date.now();
  for (const [idStr, timer] of Object.entries(S.spesaTimers)) {
    const bar = document.getElementById(`spesa-bar-${idStr}`);
    if (!bar) continue;
    const elapsed  = now - timer.startTime;
    const progress = Math.min(elapsed / _SPESA_COUNTDOWN_MS * 100, 100);
    bar.style.width = progress + '%';
  }
}

async function toggleSpesaCheck(id) {
  const item = S.listaSpesa.find(i => i.id === id);
  if (!item) return;
  const newVal = !item.completato;
  item.completato = newVal;
  item.checked    = newVal;

  if (newVal) {
    _startSpesaCountdown(id);
  } else {
    _cancelSpesaCountdown(id);
  }

  _updateSpesaBadge();
  renderListaSpesa(); // chiama _restoreCountdownBars() internamente
  api(`/api/lista-spesa/${id}`, { method: 'PATCH', body: { completato: newVal } });
}

function updateSpesaFornitore(id, fornitore) {
  const item = S.listaSpesa.find(i => i.id === id);
  if (!item) return;
  item.fornitore = fornitore;
  // Aggiorna i pill fornitore senza re-render completo
  renderSpesaFilterPills();
  api(`/api/lista-spesa/${id}`, { method: 'PATCH', body: { fornitore } });
}

async function removeSpesaItem(id) {
  S.listaSpesa = S.listaSpesa.filter(i => i.id !== id);
  _updateSpesaBadge();
  renderListaSpesa();
  await api(`/api/lista-spesa/${id}`, { method: 'DELETE' });
}

async function copiaOrdineSpesa() {
  const attivi = S.listaSpesa.filter(i => !i.completato);
  const items = (S.spesaFornFilter && S.spesaFornFilter !== 'all')
    ? attivi.filter(i => i.fornitore === S.spesaFornFilter)
    : attivi;

  if (!items.length) { toast('Nessun prodotto da ordinare', 'error'); return; }

  const testo = items.map(i => `- ${i.prodotto}${i.quantita ? ` x ${i.quantita}` : ''}`).join('\n');
  try {
    await navigator.clipboard.writeText(testo);
    toast('Ordine copiato! Incollalo su WhatsApp', 'success');
  } catch (e) {
    toast('Impossibile copiare l\'ordine', 'error');
  }
}

async function svuotaLista() {
  if (!S.listaSpesa.length) return;
  S.listaSpesa = [];
  _updateSpesaBadge();
  renderListaSpesa();
  await api('/api/lista-spesa', { method: 'DELETE' });
  toast('Lista svuotata', 'info');
}

function toggleAddPanel() {
  const panel = document.getElementById('spesa-add-panel');
  if (!panel) return;
  panel.classList.toggle('open');
  if (panel.classList.contains('open')) {
    _populateSpesaFornitori();
    document.getElementById('spesa-add-qty').value = '';
  }
}

async function confirmAddProdotto() {
  const selR = document.getElementById('spesa-sel-reparto');
  const selF = document.getElementById('spesa-sel-fornitore');
  const selP = document.getElementById('spesa-sel-prodotto');
  const qtyEl = document.getElementById('spesa-add-qty');
  if (!selR?.value) { toast('Seleziona il reparto', 'error'); return; }
  if (!selP?.value) { toast('Seleziona il prodotto', 'error'); return; }

  const fornitore = selF?.value || '';
  const [prodotto, unita] = selP.value.split('|');
  const qty = parseFloat(qtyEl.value) || 0;

  const giac = S.giacenze.find(g => g.prodotto === prodotto);
  const qtaAttuale = giac ? giac.totale : 0;
  const prod = S.prodotti.find(p => p.prodotto === prodotto);
  const scorta_min = prod ? prod.scorta_min : 0;

  const result = await api('/api/lista-spesa', {
    method: 'POST',
    body: {
      prodotto,
      fornitore,
      categoria: prod?.categoria || '',
      reparto:   prod?.reparto || '',
      quantita:  qty,
      unita:     unita || '',
    },
  });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }

  _populateSpesaFornitori();
  qtyEl.value = '';
  document.getElementById('spesa-add-panel').classList.remove('open');
  await loadListaSpesa();
  toast(`${prodotto} aggiunto alla lista`, 'success');
}

// ── NUOVO PRODOTTO ────────────────────────────────────────────────────────
function openNuovoProdotto(origin = 'carico') {
  S.npOrigin = origin;
  document.getElementById('spesa-search-dropdown')?.classList.remove('open');

  // Populate fornitore select from already-loaded prodotti
  const fornitori = [...new Set(S.prodotti.map(p => p.fornitore).filter(Boolean))].sort();
  const selF = document.getElementById('np-fornitore');
  selF.innerHTML = '<option value="">Seleziona fornitore…</option>' +
    fornitori.map(f => `<option value="${esc(f)}">${esc(f)}</option>`).join('') +
    '<option value="__altro__">+ Altro…</option>';

  // Reset fields
  document.getElementById('np-nome').value =
    origin === 'spesa'        ? (document.getElementById('spesa-search-input')?.value.trim() || '') :
    origin === 'carico-step1' ? (document.getElementById('carico-search-input')?.value.trim() || '') :
    '';
  document.getElementById('np-scorta').value  = '';
  document.getElementById('np-unita').value   = 'kg';
  document.getElementById('np-categoria').value = '';
  // Pre-seleziona il reparto già scelto nella pagina di provenienza, se c'è
  document.getElementById('np-reparto').value =
    origin === 'spesa' ? (document.getElementById('spesa-sel-reparto')?.value || '') : '';
  document.getElementById('np-fornitore-custom').value = '';
  document.getElementById('np-fornitore-custom-wrap').style.display = 'none';
  document.getElementById('btn-np-salva').disabled = false;
  document.getElementById('modal-nuovo-prodotto').classList.add('open');
}

function closeNuovoProdotto() {
  document.getElementById('modal-nuovo-prodotto').classList.remove('open');
}

function bgCloseNP(e) {
  if (e.target === document.getElementById('modal-nuovo-prodotto')) closeNuovoProdotto();
}

function onNpFornitore() {
  const val = document.getElementById('np-fornitore').value;
  document.getElementById('np-fornitore-custom-wrap').style.display =
    val === '__altro__' ? 'block' : 'none';
}

async function submitNuovoProdotto() {
  const nome     = document.getElementById('np-nome').value.trim();
  const fornSel  = document.getElementById('np-fornitore').value;
  const fornitore = fornSel === '__altro__'
    ? document.getElementById('np-fornitore-custom').value.trim()
    : fornSel;
  const unita     = document.getElementById('np-unita').value;
  const scorta    = parseFloat(document.getElementById('np-scorta').value) || 0;
  const categoria = document.getElementById('np-categoria').value;
  const reparto   = document.getElementById('np-reparto').value;

  if (!nome)      { toast('Inserisci il nome del prodotto', 'error'); return; }
  if (!fornitore) { toast('Seleziona o inserisci il fornitore', 'error'); return; }
  if (!reparto)   { toast('Seleziona il reparto', 'error'); return; }
  if (!categoria) { toast('Seleziona la categoria', 'error'); return; }

  const btn = document.getElementById('btn-np-salva');
  btn.disabled = true;

  const data = await api('/api/prodotti', {
    method: 'POST',
    body:   { prodotto: nome, fornitore, unita, scorta_min: scorta, categoria, reparto },
  });

  btn.disabled = false;
  if (!data.success) { toast(data.error || 'Errore', 'error'); return; }

  closeNuovoProdotto();
  await loadProdottiCarico();

  if (S.npOrigin === 'spesa') {
    _setSpesaSelection(reparto, fornitore, nome, unita);
    document.getElementById('spesa-search-input').value = nome;
  } else if (S.npOrigin === 'carico-step1') {
    // Resta sullo Step 1: campo di ricerca già compilato, pronto da confermare
    document.getElementById('carico-search-input').value = nome;
    onCaricoSearchInput();
  } else {
    // Porta il nuovo prodotto direttamente allo Step 2 del wizard Carico
    S.caricoData.prodotto  = nome;
    S.caricoData.fornitore = fornitore;
    S.caricoData.unita     = unita;
    S.caricoData.reparto   = reparto;
    S.caricoData.categoria = categoria;
    document.getElementById('carico-search-input').value = nome;
    await goToCaricoStep2();
  }
  if (data.warning) toast(data.warning, 'warning');
  else toast('Prodotto aggiunto al listino', 'success');
}

// ── SYNC ──────────────────────────────────────────────────────────────────
async function syncAll() {
  const btns = document.querySelectorAll('.js-btn-sync');
  btns.forEach(b => b.classList.add('spinning'));
  toast('Sincronizzazione…', 'info');
  const data = await api('/api/sync', { method: 'POST' });
  btns.forEach(b => b.classList.remove('spinning'));
  if (data.success) {
    await Promise.all([loadProdottiCarico(), loadGiacenze(), loadAlerts()]);
    const extra = data.inviati ? ` (${data.inviati} in sospeso inviati)` : '';
    toast(`Sincronizzato: ${data.listino} prodotti, ${data.registro} movimenti${extra}`, 'success');
  } else { toast(data.error || 'Errore sincronizzazione', 'error'); }
}

// ── UTILITIES ────────────────────────────────────────────────────────────
async function api(url, opts = {}) {
  try {
    const res = await fetch(url, {
      method:  opts.method || 'GET',
      headers: opts.body ? { 'Content-Type': 'application/json' } : {},
      body:    opts.body ? JSON.stringify(opts.body) : undefined,
    });
    if (res.status === 401) { window.location.href = '/login'; return { success: false }; }
    return await res.json();
  } catch (e) {
    toast('Errore di rete', 'error');
    return { success: false, error: e.message };
  }
}
function fmtDate(iso) {
  if (!iso) return '—';
  const [y, m, d] = iso.split('-');
  return `${d}/${m}/${y}`;
}
function daysTo(iso) {
  if (!iso) return null;
  try {
    const today = new Date(); today.setHours(0,0,0,0);
    return Math.round((new Date(iso + 'T00:00:00') - today) / 86400000);
  } catch { return null; }
}
// Data (YYYY-MM-DD) e ora (HH:MM) correnti nel fuso orario italiano,
// indipendentemente dal fuso orario impostato sul dispositivo.
function dataOraItaliana(date = new Date()) {
  const data = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Rome', year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(date);
  const ora = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Rome', hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(date);
  return { data, ora };
}
function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
let _toastT;
function toast(msg, type = 'info') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = `toast show ${type}`;
  clearTimeout(_toastT);
  _toastT = setTimeout(() => el.classList.remove('show'), 3600);
}

// ── TEMPERATURE ──────────────────────────────────────────────────────────
async function loadApparecchi() {
  const data = await api('/api/apparecchi');
  S.apparecchi = data.apparecchi || [];
  S.puoGestireApparecchi = !!data.puo_gestire;
  S.rangeRiferimento = data.range_riferimento || {};
  renderApparecchi();
}

function _fmtTemp(v) {
  const n = Number(v);
  const s = Number.isInteger(n) ? String(n) : n.toFixed(1);
  return (n > 0 ? '+' : '') + s;
}

function renderApparecchi() {
  const list = document.getElementById('temp-apparecchi-list');
  const btnNuovo = document.getElementById('btn-nuovo-apparecchio');
  if (btnNuovo) btnNuovo.style.display = S.puoGestireApparecchi ? 'flex' : 'none';
  if (!list) return;

  const repartoFiltro = document.getElementById('temp-filter-reparto')?.value || '';
  const apparecchiFiltrati = repartoFiltro
    ? S.apparecchi.filter(a => (a.reparto || 'Cucina') === repartoFiltro)
    : S.apparecchi;

  if (!apparecchiFiltrati.length) {
    list.innerHTML = `<div class="empty-state"><p>Nessun apparecchio${repartoFiltro ? ' per questo reparto' : ' configurato' + (S.puoGestireApparecchi ? ' — aggiungine uno' : '')}</p></div>`;
  } else {
    list.innerHTML = apparecchiFiltrati.map(a => {
      const ultima = S.temperatureStorico.find(t => t.apparecchio === a.nome);
      const ultimaHtml = ultima
        ? `<span class="temp-badge ${ultima.esito === 'OK' ? 'ok' : 'danger'}">${ultima.esito === 'OK' ? 'OK' : 'FUORI SOGLIA'}</span> ${_fmtTemp(ultima.temperatura)}°C — ${fmtDate(ultima.data)} ${esc(ultima.ora||'')}`
        : 'Nessuna rilevazione';
      const marcaModello = [a.marca, a.modello].filter(Boolean).join(' ');
      const daControllare = a.stato_controllo === 'DA_CONTROLLARE';
      const statoBadge = `<span class="temp-stato-badge ${daControllare ? 'da-controllare' : 'ok'}">${daControllare ? 'DA CONTROLLARE' : 'OK'}</span>`;
      const lock = _lockForApparecchio(a.id);
      return `<div class="temp-app-card">
        ${statoBadge}
        ${lock ? `<span class="lock-badge" onclick="toast('In lavorazione da ${esc(lock.user_nome)}', 'info')" title="In lavorazione da ${esc(lock.user_nome)}">${_ICON_LOCK}</span>` : ''}
        <div class="temp-app-top">
          <div>
            <div class="temp-app-nome">${esc(a.nome)}</div>
            <div class="temp-app-tipo">${esc(a.tipo)} · ${esc(a.reparto || 'Cucina')}${marcaModello ? ' · ' + esc(marcaModello) : ''}</div>
          </div>
          <div class="temp-app-range">${_fmtTemp(a.temp_min)}°C / ${_fmtTemp(a.temp_max)}°C</div>
        </div>
        <div class="temp-app-last">${ultimaHtml}</div>
        <div class="temp-app-actions">
          <button type="button" class="btn-temp-registra" onclick="openRegistraTempModal(${a.id})">Registra temperatura</button>
          ${S.puoGestireApparecchi ? `
          <button type="button" class="btn-temp-icon" onclick="openApparecchioModal(${a.id})" aria-label="Modifica">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5Z"/></svg>
          </button>
          <button type="button" class="btn-temp-icon" onclick="eliminaApparecchio(${a.id})" aria-label="Elimina">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
          </button>` : ''}
        </div>
      </div>`;
    }).join('');
  }

  const fsel = document.getElementById('temp-filter-apparecchio');
  if (fsel) {
    const cur = fsel.value;
    fsel.innerHTML = '<option value="">Tutti gli apparecchi</option>' +
      S.apparecchi.map(a => `<option value="${esc(a.nome)}">${esc(a.nome)}</option>`).join('');
    if (cur) fsel.value = cur;
  }
}

// ── APPARECCHIO: nuovo / modifica / elimina ────────────────────────────────
function openApparecchioModal(id = 0) {
  const a = id ? S.apparecchi.find(x => x.id === id) : null;
  document.getElementById('app-id').value = id || 0;
  document.getElementById('app-modal-title').textContent = a ? 'Modifica apparecchio' : 'Nuovo apparecchio';
  document.getElementById('app-nome').value = a ? a.nome : '';
  document.getElementById('app-tipo').value = a ? a.tipo : '';
  document.getElementById('app-reparto').value = a ? (a.reparto || 'Cucina') : '';
  document.getElementById('app-temp-min').value = a ? a.temp_min : '';
  document.getElementById('app-temp-max').value = a ? a.temp_max : '';
  document.getElementById('app-marca').value = a ? (a.marca || '') : '';
  document.getElementById('app-modello').value = a ? (a.modello || '') : '';
  document.getElementById('app-seriale').value = a ? (a.seriale || '') : '';
  document.getElementById('modal-apparecchio').classList.add('open');
}
function closeApparecchioModal() { document.getElementById('modal-apparecchio').classList.remove('open'); }
function bgCloseApparecchio(e) { if (e.target === document.getElementById('modal-apparecchio')) closeApparecchioModal(); }

function onApparecchioTipoChange() {
  const tipo  = document.getElementById('app-tipo').value;
  const range = S.rangeRiferimento[tipo];
  if (range) {
    document.getElementById('app-temp-min').value = range[0];
    document.getElementById('app-temp-max').value = range[1];
  }
}

function triggerApparecchioCamera() { document.getElementById('app-camera-input').click(); }

async function onApparecchioCameraCapture(input) {
  const file = input.files[0];
  if (!file) return;

  const btn = document.getElementById('btn-scan-apparecchio');
  const originalHtml = btn.innerHTML;
  btn.disabled = true;
  btn.classList.add('scanning');
  btn.innerHTML = '<div class="btn-scan-spinner"></div> Analisi in corso...';

  let image, mediaType = 'image/jpeg';
  try {
    image = await compressImage(file, 1200, 0.8);
  } catch {
    image = await readFileAsDataURL(file);
    mediaType = file.type || 'image/jpeg';
  }

  const data = await api('/api/scan-apparecchio', {
    method: 'POST',
    body: { image, media_type: mediaType },
  });

  btn.disabled = false;
  btn.classList.remove('scanning');
  btn.innerHTML = originalHtml;

  if (data.success) {
    let filled = false;
    if (data.marca)   { document.getElementById('app-marca').value   = data.marca;   filled = true; }
    if (data.modello) { document.getElementById('app-modello').value = data.modello; filled = true; }
    if (data.seriale) { document.getElementById('app-seriale').value = data.seriale; filled = true; }
    if (data.tipo)    { document.getElementById('app-tipo').value    = data.tipo; onApparecchioTipoChange(); filled = true; }
    const nomeEl = document.getElementById('app-nome');
    if (!nomeEl.value.trim() && (data.marca || data.modello)) {
      nomeEl.value = [data.marca, data.modello].filter(Boolean).join(' ');
    }
    toast(filled ? '✓ Targhetta letta' : 'Nessun dato trovato', filled ? 'success' : 'error');
  } else {
    toast(data.error || 'Non riesco a leggere la targhetta, inserisci manualmente', 'error');
  }
  input.value = '';
}

async function submitApparecchio() {
  const id      = parseInt(document.getElementById('app-id').value) || 0;
  const nome    = document.getElementById('app-nome').value.trim();
  const tipo    = document.getElementById('app-tipo').value;
  const reparto = document.getElementById('app-reparto').value;
  const tempMin = document.getElementById('app-temp-min').value;
  const tempMax = document.getElementById('app-temp-max').value;
  const marca   = document.getElementById('app-marca').value.trim();
  const modello = document.getElementById('app-modello').value.trim();
  const seriale = document.getElementById('app-seriale').value.trim();

  if (!nome) { toast('Inserisci il nome dell\'apparecchio', 'error'); return; }
  if (!tipo) { toast('Seleziona il tipo', 'error'); return; }
  if (!reparto) { toast('Seleziona il reparto', 'error'); return; }
  if (tempMin === '' || tempMax === '') { toast('Inserisci temperatura minima e massima', 'error'); return; }

  const body = { nome, tipo, reparto, temp_min: parseFloat(tempMin), temp_max: parseFloat(tempMax), marca, modello, seriale };
  const result = id
    ? await api(`/api/apparecchi/${id}`, { method: 'PUT', body })
    : await api('/api/apparecchi', { method: 'POST', body });

  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeApparecchioModal();
  await loadApparecchi();
  toast(id ? 'Apparecchio aggiornato' : 'Apparecchio aggiunto', 'success');
}

async function eliminaApparecchio(id) {
  const a = S.apparecchi.find(x => x.id === id);
  if (!a) return;
  if (!confirm(`Eliminare "${a.nome}"? Le rilevazioni storiche resteranno salvate.`)) return;
  const result = await api(`/api/apparecchi/${id}`, { method: 'DELETE' });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  await loadApparecchi();
  toast('Apparecchio eliminato', 'success');
}

// ── REGISTRA TEMPERATURA ───────────────────────────────────────────────────
let _tempLockApparecchioId = null;

async function openRegistraTempModal(apparecchioId) {
  const a = S.apparecchi.find(x => x.id === apparecchioId);
  if (!a) return;
  const ok = await tryAcquireLock({ tipo: 'apparecchio', apparecchio_id: apparecchioId, form: 'temperatura' });
  if (!ok) return;
  _tempLockApparecchioId = apparecchioId;

  const { data, ora } = dataOraItaliana();
  document.getElementById('rt-apparecchio-id').value = apparecchioId;
  document.getElementById('rt-titolo').textContent = `Registra temperatura — ${a.nome}`;
  document.getElementById('rt-data').value = data;
  document.getElementById('rt-ora').value  = ora;
  document.getElementById('rt-temperatura').value = '';
  document.getElementById('rt-nota').value = '';
  document.getElementById('rt-fuori-soglia').style.display = 'none';
  document.getElementById('rt-nota-label').textContent = 'Nota';
  document.getElementById('modal-registra-temp').classList.add('open');
}
function closeRegistraTempModal() {
  document.getElementById('modal-registra-temp').classList.remove('open');
  if (_tempLockApparecchioId !== null) {
    releaseLock({ tipo: 'apparecchio', apparecchio_id: _tempLockApparecchioId });
    _tempLockApparecchioId = null;
  }
}
function bgCloseRegistraTemp(e) { if (e.target === document.getElementById('modal-registra-temp')) closeRegistraTempModal(); }

function onRtTemperaturaInput() {
  const id  = parseInt(document.getElementById('rt-apparecchio-id').value);
  const a   = S.apparecchi.find(x => x.id === id);
  const val = parseFloat(document.getElementById('rt-temperatura').value);
  const banner    = document.getElementById('rt-fuori-soglia');
  const notaLabel = document.getElementById('rt-nota-label');
  if (!a || isNaN(val)) { banner.style.display = 'none'; notaLabel.textContent = 'Nota'; return; }
  const fuori = val < a.temp_min || val > a.temp_max;
  banner.style.display = fuori ? 'block' : 'none';
  notaLabel.textContent = fuori ? 'Nota *  (obbligatoria — temperatura fuori soglia)' : 'Nota';
}

function toggleSegnoTemperatura() {
  const el  = document.getElementById('rt-temperatura');
  const raw = el.value.trim();
  if (!raw || raw === '-')      el.value = raw === '-' ? '' : '-';
  else if (raw.startsWith('-')) el.value = raw.slice(1);
  else                          el.value = '-' + raw;
  onRtTemperaturaInput();
  el.focus();
}

async function submitRegistraTemp() {
  const apparecchioId = parseInt(document.getElementById('rt-apparecchio-id').value);
  const a    = S.apparecchi.find(x => x.id === apparecchioId);
  const data = document.getElementById('rt-data').value;
  const ora  = document.getElementById('rt-ora').value;
  const temperaturaStr = document.getElementById('rt-temperatura').value;
  const nota = document.getElementById('rt-nota').value.trim();

  if (temperaturaStr === '') { toast('Inserisci la temperatura rilevata', 'error'); return; }
  const temperatura = parseFloat(temperaturaStr);

  const fuori = a && (temperatura < a.temp_min || temperatura > a.temp_max);
  if (fuori && !nota) { toast('Temperatura fuori soglia: la nota è obbligatoria', 'error'); return; }

  const btn = document.getElementById('btn-rt-salva');
  btn.disabled = true;
  const result = await api('/api/temperature', {
    method: 'POST',
    body: { apparecchio_id: apparecchioId, data, ora, temperatura, nota },
  });
  btn.disabled = false;

  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeRegistraTempModal();
  await Promise.all([loadApparecchi(), loadStoricoTemperature()]);
  toast(result.esito === 'FUORI_SOGLIA' ? 'Registrato — FUORI SOGLIA' : 'Temperatura registrata',
        result.esito === 'FUORI_SOGLIA' ? 'error' : 'success');
  if (result.warning) setTimeout(() => toast(result.warning, 'warning'), 1800);
}

// ── STORICO TEMPERATURE ─────────────────────────────────────────────────────
async function loadStoricoTemperature() {
  const data = await api('/api/temperature?giorni=30');
  S.temperatureStorico = data.rilevazioni || [];
  renderStoricoTemperature();
  renderApparecchi();
}

function renderStoricoTemperature() {
  const list = document.getElementById('temp-storico-list');
  if (!list) return;
  const filtro = document.getElementById('temp-filter-apparecchio')?.value || '';
  const items = filtro ? S.temperatureStorico.filter(t => t.apparecchio === filtro) : S.temperatureStorico;

  if (!items.length) {
    list.innerHTML = `<div class="empty-state"><p>Nessuna rilevazione negli ultimi 30 giorni</p></div>`;
    return;
  }

  list.innerHTML = items.map(t => `
    <div class="temp-storico-swipe">
      <div class="temp-storico-delete" onclick="confermaEliminaTemperatura(${t.id})">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
        Elimina
      </div>
      <div class="temp-storico-row">
        <div class="temp-storico-info">
          <div class="temp-storico-app">${esc(t.apparecchio)}</div>
          <div class="temp-storico-meta">${fmtDate(t.data)} ${esc(t.ora||'')} · ${esc(t.operatore||'')}</div>
          ${t.nota ? `<div class="temp-storico-nota">${esc(t.nota)}</div>` : ''}
        </div>
        <div class="temp-storico-right">
          <div class="temp-storico-val">${_fmtTemp(t.temperatura)}°C</div>
          <span class="temp-badge ${t.esito === 'OK' ? 'ok' : 'danger'}">${t.esito === 'OK' ? 'OK' : 'FUORI SOGLIA'}</span>
        </div>
      </div>
    </div>
  `).join('');
  _initSwipeStoricoTemperature();
}

function _initSwipeStoricoTemperature() {
  const list = document.getElementById('temp-storico-list');
  if (!list || list.dataset.swipeInit) return;
  list.dataset.swipeInit = '1';
  let state = null;

  list.addEventListener('touchstart', e => {
    const row = e.target.closest('.temp-storico-row');
    if (!row) return;
    if (state && state.row !== row) _closeSwipeRow(state.row);
    list.querySelectorAll('.temp-storico-row.open').forEach(r => { if (r !== row) _closeSwipeRow(r); });
    const btn = row.parentElement.querySelector('.temp-storico-delete');
    state = { row, btn, startX: e.touches[0].clientX, startedOpen: row.classList.contains('open') };
  }, { passive: true });

  list.addEventListener('touchmove', e => {
    if (!state) return;
    const dx = e.touches[0].clientX - state.startX;
    const base = state.startedOpen ? -84 : 0;
    state.lastX = Math.max(-84, Math.min(0, base + dx));
    state.row.style.transform = `translateX(${state.lastX}px)`;
    if (state.btn) state.btn.style.transform = `translateX(${84 + state.lastX}px)`;
  }, { passive: true });

  list.addEventListener('touchend', () => {
    if (!state) return;
    const open = (state.lastX ?? (state.startedOpen ? -84 : 0)) < -42;
    const finalX = open ? -84 : 0;
    state.row.style.transform = `translateX(${finalX}px)`;
    if (state.btn) state.btn.style.transform = `translateX(${84 + finalX}px)`;
    state.row.classList.toggle('open', open);
    state = null;
  });
}
function _closeSwipeRow(row) {
  row.style.transform = 'translateX(0)';
  row.classList.remove('open');
  const btn = row.parentElement.querySelector('.temp-storico-delete');
  if (btn) btn.style.transform = 'translateX(84px)';
}

async function confermaEliminaTemperatura(id) {
  if (!confirm('Sei sicuro di voler eliminare questa rilevazione?')) return;
  const result = await api(`/api/temperature/${id}`, { method: 'DELETE' });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  toast('Rilevazione eliminata', 'success');
  await loadStoricoTemperature();
}

// ── CONTROLLO ────────────────────────────────────────────────────────────
function fmtDateTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleString('it-IT', {
    timeZone: 'Europe/Rome',
    day: '2-digit', month: '2-digit', year: 'numeric',
    hour: '2-digit', minute: '2-digit',
  }).replace(',', '');
}

async function loadControlloLogs() {
  const [dataTemp, dataSpesa, dataRegistro, dataElimReg, dataChat] = await Promise.all([
    api('/api/controllo/log-temperature'),
    api('/api/controllo/log-lista-spesa'),
    api('/api/controllo/log-modifiche-registro'),
    api('/api/controllo/log-eliminazioni-registro'),
    api('/api/controllo/log-chat'),
  ]);
  renderLogEliminazioniTemperature(dataTemp.log || []);
  renderLogListaSpesa(dataSpesa.log || []);
  renderLogModificheRegistro(dataRegistro.log || []);
  renderLogEliminazioniRegistro(dataElimReg.log || []);
  renderLogChat(dataChat.log || []);
}

function renderLogEliminazioniTemperature(log) {
  const el = document.getElementById('controllo-log-temperature');
  if (!el) return;
  if (!log.length) {
    el.innerHTML = `<div class="empty-state"><p>Nessuna eliminazione registrata</p></div>`;
    return;
  }
  el.innerHTML = log.map(l => `
    <div class="log-row">
      <div class="log-row-top">
        <div class="log-row-title">${esc(l.apparecchio)} — ${_fmtTemp(l.temperatura)}°C</div>
        <span class="log-row-tag eliminato">Eliminata</span>
      </div>
      <div class="log-row-meta">
        Rilevazione originale: ${fmtDate(l.data_originale)} ${esc(l.ora_originale||'')} · ${esc(l.esito === 'OK' ? 'OK' : 'FUORI SOGLIA')} · operatore ${esc(l.operatore_originale||'—')}
      </div>
      <div class="log-row-meta">
        Eliminata da <strong>${esc(l.eliminato_da)}</strong> il ${fmtDateTime(l.eliminato_il)}
      </div>
    </div>
  `).join('');
}

function renderLogListaSpesa(log) {
  const el = document.getElementById('controllo-log-lista-spesa');
  if (!el) return;
  if (!log.length) {
    el.innerHTML = `<div class="empty-state"><p>Nessuna attività registrata</p></div>`;
    return;
  }
  el.innerHTML = log.map(l => `
    <div class="log-row">
      <div class="log-row-top">
        <div class="log-row-title">${esc(l.prodotto)}${l.fornitore ? ' · ' + esc(l.fornitore) : ''}</div>
        <span class="log-row-tag ${l.azione}">${l.azione === 'aggiunto' ? 'Aggiunto' : 'Eliminato'}</span>
      </div>
      <div class="log-row-meta">
        <strong>${esc(l.utente)}</strong> il ${fmtDateTime(l.quando)}
      </div>
    </div>
  `).join('');
}

function renderLogModificheRegistro(log) {
  const el = document.getElementById('controllo-log-modifiche-registro');
  if (!el) return;
  if (!log.length) {
    el.innerHTML = `<div class="empty-state"><p>Nessuna modifica registrata</p></div>`;
    return;
  }
  el.innerHTML = log.map(l => `
    <div class="log-row">
      <div class="log-row-top">
        <div class="log-row-title">${esc(l.prodotto)}${l.lotto ? ' · lotto ' + esc(l.lotto) : ''}</div>
        <span class="log-row-tag modificato">Modificato</span>
      </div>
      <div class="log-row-meta">${esc(l.modifiche)}</div>
      <div class="log-row-meta">
        <strong>${esc(l.utente)}</strong> il ${fmtDateTime(l.quando)}
      </div>
    </div>
  `).join('');
}

function renderLogEliminazioniRegistro(log) {
  const el = document.getElementById('controllo-log-eliminazioni-registro');
  if (!el) return;
  if (!log.length) {
    el.innerHTML = `<div class="empty-state"><p>Nessuna eliminazione registrata</p></div>`;
    return;
  }
  const TIPO_LBL = { CARICO: 'Carico', IN_USO: 'In uso', SCARICO: 'Scarico', PREPARAZIONE: 'Preparazione' };
  el.innerHTML = log.map(l => {
    const tipo = l.tipo || 'CARICO';
    const qty  = tipo === 'CARICO' ? l.carico : l.scarico;
    return `
    <div class="log-row">
      <div class="log-row-top">
        <div class="log-row-title">${esc(l.prodotto)}${l.lotto ? ' · lotto ' + esc(l.lotto) : ''}</div>
        <span class="log-row-tag eliminato">Eliminato</span>
      </div>
      <div class="log-row-meta">
        ${TIPO_LBL[tipo] || tipo}: ${qty}&nbsp;${esc(l.unita||'')} · ${fmtDate(l.data_originale)} · operatore ${esc(l.operatore_originale||'—')}
      </div>
      <div class="log-row-meta">
        Eliminato da <strong>${esc(l.eliminato_da)}</strong> il ${fmtDateTime(l.eliminato_il)}
      </div>
    </div>
  `;
  }).join('');
}

function renderLogChat(log) {
  const el = document.getElementById('controllo-log-chat');
  if (!el) return;
  if (!log.length) {
    el.innerHTML = `<div class="empty-state"><p>Nessuna attività registrata</p></div>`;
    return;
  }
  const AZIONE_LBL = { modificato: 'Modificato', eliminato: 'Eliminato' };
  el.innerHTML = log.map(l => `
    <div class="log-row">
      <div class="log-row-top">
        <div class="log-row-title">${esc(l.canale)} · ${esc(l.autore_originale)}</div>
        <span class="log-row-tag ${l.azione}">${AZIONE_LBL[l.azione] || l.azione}</span>
      </div>
      <div class="log-row-meta">"${esc(l.testo_originale || '')}"</div>
      <div class="log-row-meta">
        <strong>${esc(l.operatore)}</strong> il ${fmtDateTime(l.quando)}
      </div>
    </div>
  `).join('');
}

// ── PREPARAZIONI ─────────────────────────────────────────────────────────
let prepIngredienti = [];
let prepIngredienteSeq = 0;

function fmtDataOra(str) {
  if (!str) return '—';
  const [datePart, timePart] = str.split(' ');
  return `${fmtDate(datePart)}${timePart ? ' ' + timePart : ''}`;
}

async function loadPreparazioni() {
  const data = await api('/api/preparazioni');
  S.preparazioni = data.preparazioni || [];
  renderPreparazioni();
}

function renderPreparazioni() {
  const el      = document.getElementById('prep-list');
  const reparto = document.getElementById('prep-filter-reparto').value;
  const stato   = document.getElementById('prep-filter-stato').value;
  let list = [...S.preparazioni];
  if (reparto) list = list.filter(p => p.reparto === reparto);
  if (stato)   list = list.filter(p => p.stato === stato);

  document.getElementById('prep-counter').innerHTML = `<strong>${list.length}</strong> preparazioni`;

  if (!list.length) {
    el.innerHTML = `<div class="empty-state">
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
      <p>Nessuna preparazione trovata</p>
    </div>`;
    return;
  }

  el.innerHTML = list.map(p => {
    const giorni = p.scadenza ? daysTo(p.scadenza) : null;
    const cls    = giorni === null ? '' : giorni < 0 ? 'scad-danger' : giorni <= 1 ? 'scad-warn' : 'scad-ok';
    return `<div class="prep-card">
      <div class="prep-top" onclick="showPreparazioneDetail(${p.id})">
        <div>
          <div class="prep-nome">${esc(p.nome)}</div>
          <div class="prep-meta">${esc(p.reparto)} · ${fmtDataOra(p.data_preparazione)} · ${esc(p.operatore)}</div>
        </div>
        <div style="text-align:right;flex-shrink:0">
          ${p.scadenza ? `<span class="scad-badge ${cls}">${fmtDate(p.scadenza)}</span>` : ''}
          <div class="prep-stato-badge ${p.stato}" style="margin-top:6px">${p.stato === 'in_corso' ? 'In corso' : 'Completata'}</div>
        </div>
      </div>
      ${p.stato === 'in_corso' ? `<button type="button" class="btn-prep-finita" onclick="event.stopPropagation(); confermaCompletaPreparazione(${p.id})">Finita</button>` : ''}
    </div>`;
  }).join('');
}

async function confermaCompletaPreparazione(id) {
  if (!confirm('La preparazione è stata completamente consumata?')) return;
  const result = await api(`/api/preparazioni/${id}/completa`, { method: 'POST' });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  if (result.warning) toast(result.warning, 'warning');
  else toast('Preparazione completata', 'success');
  await loadPreparazioni();
}

function openNuovaPreparazione() {
  document.getElementById('prep-nome').value     = '';
  document.getElementById('prep-reparto').value  = '';
  document.getElementById('prep-quantita').value = '';
  document.getElementById('prep-unita').value    = 'kg';
  document.getElementById('prep-scadenza').value = '';
  document.getElementById('prep-note').value     = '';
  const { data, ora } = dataOraItaliana();
  document.getElementById('prep-data').value = data;
  document.getElementById('prep-ora').value  = ora;
  S.prepIngReparto = '';
  document.getElementById('prep-ing-filter-reparto').value = '';
  prepIngredienti = [];
  prepIngredienteSeq = 0;
  addIngredienteRow();
  document.getElementById('modal-nuova-preparazione').classList.add('open');
}

function onPrepIngRepartoChange(sel) {
  S.prepIngReparto = sel.value;
  renderIngredientiRows();
}
function _releaseIngredienteLock(r) {
  if (r && r.lockKey) { releaseLock({ tipo: 'prodotto_lotto', ...r.lockKey }); r.lockKey = null; }
}
function closeNuovaPreparazione() {
  document.getElementById('modal-nuova-preparazione').classList.remove('open');
  prepIngredienti.forEach(_releaseIngredienteLock);
}
function bgClosePrepForm(e) { if (e.target === document.getElementById('modal-nuova-preparazione')) closeNuovaPreparazione(); }

function addIngredienteRow() {
  prepIngredienti.push({ seq: prepIngredienteSeq++, prodotto: '', fonte: 'magazzino', selected: null, qty: '', lotti: [], lockKey: null });
  renderIngredientiRows();
}
function removeIngredienteRow(seq) {
  if (prepIngredienti.length <= 1) { toast('Serve almeno un ingrediente', 'error'); return; }
  _releaseIngredienteLock(prepIngredienti.find(r => r.seq === seq));
  prepIngredienti = prepIngredienti.filter(r => r.seq !== seq);
  renderIngredientiRows();
}

function renderIngredientiRows() {
  const container = document.getElementById('prep-ingredienti-list');
  const prodottiFiltrati = filterProdotti(S.prodotti, S.prepIngReparto, '');
  container.innerHTML = prepIngredienti.map((r, idx) => `
    <div class="ingr-row">
      <div class="ingr-row-top">
        <span class="ingr-row-num">Ingrediente ${idx + 1}</span>
        ${prepIngredienti.length > 1 ? `<button type="button" class="ingr-remove-btn" onclick="removeIngredienteRow(${r.seq})">✕</button>` : ''}
      </div>
      <div class="fselect-wrap" style="margin-bottom:8px">
        <select class="fselect" onchange="onIngredienteProdotto(${r.seq}, this.value)">
          <option value="">Seleziona prodotto…</option>
          ${prodottiFiltrati.map(p => `<option value="${esc(p.prodotto)}" ${r.prodotto===p.prodotto?'selected':''}>${esc(p.prodotto)}</option>`).join('')}
        </select>
      </div>
      <div class="fselect-wrap" style="margin-bottom:8px">
        <select class="fselect" onchange="onIngredienteFonte(${r.seq}, this.value)">
          <option value="magazzino" ${r.fonte==='magazzino'?'selected':''}>In magazzino</option>
          <option value="in_uso" ${r.fonte==='in_uso'?'selected':''}>In uso</option>
        </select>
      </div>
      <div class="fselect-wrap" style="margin-bottom:8px">
        <select class="fselect" ${r.lotti.length ? '' : 'disabled'} onchange="onIngredienteLotto(${r.seq}, this.value)">
          <option value="">${r.prodotto ? 'Seleziona lotto…' : 'Seleziona prima il prodotto'}</option>
          ${r.lotti.map((l, i) => `<option value="${i}" ${r.selected===l?'selected':''}>${esc(l.lotto)} — ${l.disponibile}&nbsp;${esc(l.unita)}${l.scadenza?` · scade ${fmtDate(l.scadenza)}`:''} · carico ${fmtDate(l.data_carico)}</option>`).join('')}
        </select>
      </div>
      <input class="finput" type="number" min="0.01" step="0.01" inputmode="decimal"
             placeholder="Quantità usata" value="${r.qty}"
             oninput="onIngredienteQty(${r.seq}, this.value)">
    </div>
  `).join('');
}

async function _caricaLottiIngrediente(r) {
  if (r.fonte === 'in_uso') {
    const data = await api(`/api/in-uso?prodotto=${encodeURIComponent(r.prodotto)}`);
    r.lotti = (data.in_uso || []).map(x => ({
      lotto: x.lotto, disponibile: x.qty, unita: x.unita, scadenza: x.scadenza,
      data_carico: x.data_carico, in_uso_id: x.id,
    }));
  } else {
    const data = await api(`/api/lotti?prodotto=${encodeURIComponent(r.prodotto)}`);
    r.lotti = (data.lotti || []).map(x => ({
      lotto: x.lotto, disponibile: x.rimanenza, unita: x.unita, scadenza: x.scadenza,
      data_carico: x.data_carico, in_uso_id: null,
    }));
  }
}

async function onIngredienteProdotto(seq, prodotto) {
  const r = prepIngredienti.find(x => x.seq === seq);
  if (!r) return;
  _releaseIngredienteLock(r);
  r.prodotto = prodotto; r.selected = null; r.qty = ''; r.lotti = [];
  renderIngredientiRows();
  if (!prodotto) return;
  await _caricaLottiIngrediente(r);
  const r2 = prepIngredienti.find(x => x.seq === seq);
  if (!r2) return; // rimosso nel frattempo
  renderIngredientiRows();
}

async function onIngredienteFonte(seq, fonte) {
  const r = prepIngredienti.find(x => x.seq === seq);
  if (!r) return;
  _releaseIngredienteLock(r);
  r.fonte = fonte; r.selected = null; r.qty = ''; r.lotti = [];
  renderIngredientiRows();
  if (!r.prodotto) return;
  await _caricaLottiIngrediente(r);
  const r2 = prepIngredienti.find(x => x.seq === seq);
  if (!r2) return; // rimosso nel frattempo
  renderIngredientiRows();
}

async function onIngredienteLotto(seq, idxStr) {
  const r = prepIngredienti.find(x => x.seq === seq);
  if (!r) return;
  _releaseIngredienteLock(r);
  const idx = idxStr === '' ? -1 : Number(idxStr);
  const scelto = idx >= 0 ? r.lotti[idx] : null;
  if (!scelto) { r.selected = null; renderIngredientiRows(); return; }

  const ok = await tryAcquireLock({ tipo: 'prodotto_lotto', prodotto: r.prodotto, lotto: scelto.lotto, form: 'preparazione' });
  if (!ok) { r.selected = null; renderIngredientiRows(); return; }

  r.selected = scelto;
  r.lockKey  = { prodotto: r.prodotto, lotto: scelto.lotto };
  renderIngredientiRows();
}

function onIngredienteQty(seq, val) {
  const r = prepIngredienti.find(x => x.seq === seq);
  if (r) r.qty = val;
}

async function submitPreparazione() {
  const nome     = document.getElementById('prep-nome').value.trim();
  const reparto  = document.getElementById('prep-reparto').value;
  const quantita = parseFloat(document.getElementById('prep-quantita').value);
  const unita    = document.getElementById('prep-unita').value;
  const scadenza = document.getElementById('prep-scadenza').value;
  const note     = document.getElementById('prep-note').value.trim();
  const dataVal  = document.getElementById('prep-data').value;
  const oraVal   = document.getElementById('prep-ora').value;

  if (!nome)    { toast('Inserisci il nome della preparazione', 'error'); return; }
  if (!reparto) { toast('Seleziona il reparto', 'error'); return; }
  if (!quantita || quantita <= 0) { toast('Inserisci una quantità prodotta valida', 'error'); return; }

  const ingredienti = prepIngredienti
    .filter(r => r.prodotto && r.selected && r.qty)
    .map(r => r.fonte === 'in_uso'
      ? { prodotto: r.prodotto, fonte: 'in_uso', in_uso_id: r.selected.in_uso_id, quantita: parseFloat(r.qty) }
      : { prodotto: r.prodotto, fonte: 'magazzino', lotto: r.selected.lotto, quantita: parseFloat(r.qty) });

  if (!ingredienti.length) { toast('Aggiungi almeno un ingrediente completo', 'error'); return; }

  const btn = document.getElementById('btn-prep-salva');
  btn.disabled = true;

  const result = await api('/api/preparazioni', {
    method: 'POST',
    body: { nome, reparto, quantita, unita, scadenza, note, ingredienti, data: dataVal, ora: oraVal },
  });

  btn.disabled = false;

  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeNuovaPreparazione();
  toast('Preparazione registrata', 'success');
  await Promise.all([loadPreparazioni(), loadGiacenze(), loadAlerts()]);
}

let _prepEtichettaData = null;

async function showPreparazioneDetail(id) {
  const data = await api(`/api/preparazioni/${id}`);
  if (!data.success) { toast(data.error || 'Errore', 'error'); return; }
  const p = data.preparazione;
  const ingredienti = data.ingredienti || [];
  _prepEtichettaData = { nome: p.nome, data: fmtDataOra(p.data_preparazione), ingredienti };

  const content = `
    <div class="eti-lotto-label">
      <div class="eti-lotto-prodotto">${esc(p.nome)}</div>
      <div class="eti-lotto-row"><span>Data produzione</span><strong>${fmtDataOra(p.data_preparazione)}</strong></div>
    </div>
    <div style="margin-top:18px">
      <div class="spesa-group-hdr">Lotti ingredienti</div>
      ${ingredienti.map(i => `
        <div class="lot-detail-row">
          <div class="lot-detail-lotto">${esc(i.prodotto)}</div>
          <span style="font-size:13px;font-weight:600">${esc(i.lotto || '—')}</span>
        </div>
      `).join('')}
    </div>
  `;
  document.getElementById('prep-eti-content').innerHTML = content;
  document.getElementById('modal-eti-preparazione').classList.add('open');
}
function closePrepEtichetta() { document.getElementById('modal-eti-preparazione').classList.remove('open'); }
function bgClosePrepEti(e) { if (e.target === document.getElementById('modal-eti-preparazione')) closePrepEtichetta(); }

async function copiaEtichettaPreparazione() {
  if (!_prepEtichettaData) return;
  const { nome, data, ingredienti } = _prepEtichettaData;
  const righe = ingredienti.map(i => `- ${i.prodotto}: ${i.lotto || '—'}`).join('\n');
  const testo = `PREPARAZIONE: ${nome}\nDATA: ${data}\nINGREDIENTI:\n${righe}`;
  try {
    await navigator.clipboard.writeText(testo);
    toast('Testo copiato!', 'success');
  } catch (e) {
    toast('Impossibile copiare il testo', 'error');
  }
}

// ── PRENOTAZIONI ─────────────────────────────────────────────────────────
const PRANZO_CENA_CUTOFF = '17:00';
const PREN_STATO_LBL = { prenotato: 'Prenotato', arrivato: 'Arrivato', conto: 'Conto richiesto', libero: 'Libero' };
const PREN_STATO_AZIONE = {
  prenotato: { label: 'Arrivati',         next: 'arrivato' },
  arrivato:  { label: 'Conto richiesto',  next: 'conto' },
  conto:     { label: 'Libera tavolo',    next: 'libero' },
};

function initPrenotazioniPage() {
  if (!S.prenCalMese) {
    const oggi = dataOraItaliana().data.split('-');
    S.prenCalAnno = parseInt(oggi[0], 10);
    S.prenCalMese = parseInt(oggi[1], 10);
  }
  document.getElementById('pren-cal-view').style.display = 'none';
  document.getElementById('pren-day-view').style.display = 'none';
  document.getElementById('pren-mappa-view').style.display = '';
  document.getElementById('pren-mappa-torna-lista').style.display = 'none';

  const oggi = dataOraItaliana();
  S.pren4Servizio = oggi.ora < PRANZO_CENA_CUTOFF ? 'pranzo' : 'cena';
  S.mappaContext = {
    mode: 'servizio', data: oggi.data, servizio: S.pren4Servizio, origine: 'home',
    canvasId: 'sala-map-canvas', svgId: 'sala-mura-svg',
  };
  _renderPrenServizioSwitch();
  loadMappaServizio();
}

function _renderPrenServizioSwitch() {
  document.querySelectorAll('#pren-servizio-switch .pren-servizio-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.servizio === S.pren4Servizio)
  );
}

function setPren4Servizio(servizio) {
  S.pren4Servizio = servizio;
  S.mappaContext.servizio = servizio;
  _renderPrenServizioSwitch();
  loadMappaServizio();
}

async function loadMappaServizio() {
  const { data, servizio } = S.mappaContext;
  const [tavoliData] = await Promise.all([
    api(`/api/tavoli?data=${encodeURIComponent(data)}&servizio=${servizio}`),
    loadSalaMura(),
  ]);
  S.tavoli = tavoliData.tavoli || [];
  renderSalaMuraSvg();
  renderMappaSala();
  _avviaPollingMappa();
}

let _mappaPollTimer = null;
function _avviaPollingMappa() {
  clearInterval(_mappaPollTimer);
  _mappaPollTimer = setInterval(async () => {
    if (S.activePage !== 'prenotazioni' || document.getElementById('pren-mappa-view').style.display === 'none') {
      clearInterval(_mappaPollTimer);
      return;
    }
    const { data, servizio } = S.mappaContext;
    const tavoliData = await api(`/api/tavoli?data=${encodeURIComponent(data)}&servizio=${servizio}`);
    S.tavoli = tavoliData.tavoli || [];
    renderMappaSala();
  }, 20000);
}

function apriCalendarioDaMappa() {
  document.getElementById('pren-mappa-view').style.display = 'none';
  document.getElementById('pren-day-view').style.display = 'none';
  document.getElementById('pren-cal-view').style.display = '';
  loadCalendarioPrenotazioni();
}

function tornaAMappaDalCalendario() {
  document.getElementById('pren-cal-view').style.display = 'none';
  document.getElementById('pren-day-view').style.display = 'none';
  document.getElementById('pren-mappa-view').style.display = '';
  document.getElementById('pren-mappa-torna-lista').style.display = 'none';
  S.mappaContext = {
    mode: 'servizio', data: dataOraItaliana().data, servizio: S.pren4Servizio, origine: 'home',
    canvasId: 'sala-map-canvas', svgId: 'sala-mura-svg',
  };
  loadMappaServizio();
}

function apriMappaPerGiorno() {
  document.getElementById('pren-day-view').style.display = 'none';
  document.getElementById('pren-mappa-view').style.display = '';
  document.getElementById('pren-mappa-torna-lista').style.display = '';
  S.mappaContext = {
    mode: 'servizio', data: S.prenData, servizio: S.pren4Servizio, origine: 'day',
    canvasId: 'sala-map-canvas', svgId: 'sala-mura-svg',
  };
  loadMappaServizio();
}

function tornaAllaListaDaMappa() {
  document.getElementById('pren-mappa-view').style.display = 'none';
  document.getElementById('pren-mappa-torna-lista').style.display = 'none';
  document.getElementById('pren-day-view').style.display = '';
}

async function loadCalendarioPrenotazioni() {
  const data = await api(`/api/prenotazioni/calendario?anno=${S.prenCalAnno}&mese=${S.prenCalMese}`);
  S.prenCalGiorniPieni = data.giorni_pieni || [];
  S.prenCalTavoliPerGiorno = data.tavoli_per_giorno || {};
  renderCalendarioPrenotazioni();
}

const MESI_IT = ['Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno','Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre'];

function renderCalendarioPrenotazioni() {
  document.getElementById('pren-cal-label').textContent = `${MESI_IT[S.prenCalMese - 1]} ${S.prenCalAnno}`;

  const primoGiorno  = new Date(S.prenCalAnno, S.prenCalMese - 1, 1);
  const ultimoGiorno  = new Date(S.prenCalAnno, S.prenCalMese, 0).getDate();
  const offset       = (primoGiorno.getDay() + 6) % 7;   // Lun=0 .. Dom=6
  const oggiISO       = dataOraItaliana().data;

  const celle = [];
  for (let i = 0; i < offset; i++) celle.push(null);
  for (let d = 1; d <= ultimoGiorno; d++) {
    const iso = `${S.prenCalAnno}-${String(S.prenCalMese).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
    celle.push({ giorno: d, iso, oggi: iso === oggiISO, pieno: S.prenCalGiorniPieni.includes(iso) });
  }
  while (celle.length % 7 !== 0) celle.push(null);

  document.getElementById('pren-cal-grid').innerHTML = celle.map(c => c
    ? `<div class="pren-cal-day ${c.pieno ? 'pieno' : ''} ${c.oggi ? 'oggi' : ''}" onclick="prenCalApriGiorno('${c.iso}')">
         <span class="pren-cal-day-num">${c.giorno}</span>
         ${_miniMappaCella(c.iso)}
       </div>`
    : `<div class="pren-cal-day fuori-mese"></div>`
  ).join('');
}

function _miniMappaCella(iso) {
  if (!S.tavoli.length) return '';
  const prenotati = new Set(S.prenCalTavoliPerGiorno[iso] || []);
  const muri = S.salaMuraPercorsoSalvato
    ? `<path d="${S.salaMuraPercorsoSalvato}" fill="none" stroke="currentColor" stroke-width="10" opacity="0.25"/>`
    : '';
  const punti = S.tavoli.map(t => {
    const cx = (t.posizione_x || 0) + 28;
    const cy = (t.posizione_y || 0) + 28;
    const colore = prenotati.has(t.id) ? 'var(--yellow)' : 'var(--green)';
    return `<circle cx="${cx}" cy="${cy}" r="22" fill="${colore}"/>`;
  }).join('');
  return `<svg class="pren-cal-mini-mappa" viewBox="0 0 1000 700" preserveAspectRatio="xMidYMid meet">${muri}${punti}</svg>`;
}

function prenCalNav(delta) {
  S.prenCalMese += delta;
  if (S.prenCalMese < 1)  { S.prenCalMese = 12; S.prenCalAnno--; }
  if (S.prenCalMese > 12) { S.prenCalMese = 1;  S.prenCalAnno++; }
  loadCalendarioPrenotazioni();
}

function prenCalApriGiorno(iso) {
  S.prenData = iso;
  document.getElementById('pren-cal-view').style.display = 'none';
  document.getElementById('pren-day-view').style.display = '';
  document.getElementById('pren-day-label').textContent = fmtDate(iso);
  loadPrenotazioni();
}

function prenCalTornaAlMese() {
  document.getElementById('pren-cal-view').style.display = '';
  document.getElementById('pren-day-view').style.display = 'none';
  loadCalendarioPrenotazioni();
}

async function loadPrenotazioni() {
  if (!S.prenData) S.prenData = dataOraItaliana().data;
  const data = await api(`/api/prenotazioni?data=${encodeURIComponent(S.prenData)}`);
  S.prenotazioni = data.prenotazioni || [];
  S.coperti = data.coperti || S.coperti;
  renderCoperti();
  renderPrenotazioni();
  if (!S.sale.length) await loadSaleETavoli();
}

function renderCoperti() {
  document.getElementById('coperti-totale').textContent = S.coperti.totale ?? 0;
  document.getElementById('coperti-pranzo').textContent = S.coperti.pranzo ? S.coperti.pranzo.rimasti : 0;
  document.getElementById('coperti-cena').textContent   = S.coperti.cena   ? S.coperti.cena.rimasti   : 0;
}

function renderPrenotazioni() {
  const el = document.getElementById('pren-list');
  if (!S.prenotazioni.length) {
    el.innerHTML = `<div class="empty-state">
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
      <p>Nessuna prenotazione per questo giorno</p>
    </div>`;
    return;
  }

  const pranzo = S.prenotazioni.filter(p => p.ora < PRANZO_CENA_CUTOFF);
  const cena   = S.prenotazioni.filter(p => p.ora >= PRANZO_CENA_CUTOFF);

  const rigaHtml = (p) => {
    const tavoliTxt = p.tavoli && p.tavoli.length ? p.tavoli.map(t => t.numero).join(', ') : '—';
    const riga = `<div class="pren-row" onclick="showPrenDetail(${p.id})">
      <div>
        <div class="pren-nome">${esc(p.nome)}</div>
        <div class="pren-meta">${esc(p.ora)} · ${p.persone}&nbsp;persone${p.telefono ? ' · ' + esc(p.telefono) : ''} · Tavolo ${esc(tavoliTxt)}</div>
      </div>
      <span class="pren-badge ${p.stato}">${PREN_STATO_LBL[p.stato] || p.stato}</span>
    </div>`;
    if (!CAN_GESTIRE_SALA) return riga;
    return `<div class="pren-swipe">
      <div class="pren-delete" onclick="event.stopPropagation(); confermaEliminaPrenotazione(${p.id})">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
        Elimina
      </div>
      ${riga}
    </div>`;
  };

  const section = (titolo, list) => list.length
    ? `<div class="spesa-group-hdr">${titolo}</div>${list.map(rigaHtml).join('')}`
    : '';

  el.innerHTML = section('Pranzo', pranzo) + section('Cena', cena);
  _initSwipePrenotazioni();
}

function _initSwipePrenotazioni() {
  const list = document.getElementById('pren-list');
  if (!list || list.dataset.swipeInit) return;
  list.dataset.swipeInit = '1';
  let state = null;

  list.addEventListener('touchstart', e => {
    const row = e.target.closest('.pren-swipe .pren-row');
    if (!row) return;
    if (state && state.row !== row) _closeSwipePren(state.row);
    list.querySelectorAll('.pren-swipe .pren-row.open').forEach(r => { if (r !== row) _closeSwipePren(r); });
    const btn = row.parentElement.querySelector('.pren-delete');
    state = { row, btn, startX: e.touches[0].clientX, startedOpen: row.classList.contains('open') };
  }, { passive: true });

  list.addEventListener('touchmove', e => {
    if (!state) return;
    const dx = e.touches[0].clientX - state.startX;
    const base = state.startedOpen ? -84 : 0;
    state.lastX = Math.max(-84, Math.min(0, base + dx));
    state.row.style.transform = `translateX(${state.lastX}px)`;
    if (state.btn) state.btn.style.transform = `translateX(${84 + state.lastX}px)`;
  }, { passive: true });

  list.addEventListener('touchend', () => {
    if (!state) return;
    const open = (state.lastX ?? (state.startedOpen ? -84 : 0)) < -42;
    const finalX = open ? -84 : 0;
    state.row.style.transform = `translateX(${finalX}px)`;
    if (state.btn) state.btn.style.transform = `translateX(${84 + finalX}px)`;
    state.row.classList.toggle('open', open);
    state = null;
  });
}
function _closeSwipePren(row) {
  row.style.transform = 'translateX(0)';
  row.classList.remove('open');
  const btn = row.parentElement.querySelector('.pren-delete');
  if (btn) btn.style.transform = 'translateX(84px)';
}

function _prenFormDefaultData() {
  const mappaVisible = document.getElementById('pren-mappa-view').style.display !== 'none';
  return (mappaVisible ? S.mappaContext.data : S.prenData) || dataOraItaliana().data;
}

async function openNuovaPrenotazione(tavoloPreselezionato) {
  if (!S.sale.length) await loadSaleETavoli();
  S.prenEditId = null;
  S.prenTavoliSelezionati = typeof tavoloPreselezionato === 'number' ? [tavoloPreselezionato] : [];
  document.getElementById('pren-modal-title').textContent = 'Nuova prenotazione';
  document.getElementById('pren-nome').value = '';
  document.getElementById('pren-persone').value = '';
  document.getElementById('pren-form-data').value = _prenFormDefaultData();
  document.getElementById('pren-ora').value = '';
  document.getElementById('pren-telefono').value = '';
  document.getElementById('pren-note').value = '';
  renderPrenTavoliCheckbox();
  closeTavoloSheet();
  document.getElementById('modal-prenotazione').classList.add('open');
}
function closeNuovaPrenotazione() { document.getElementById('modal-prenotazione').classList.remove('open'); }
function bgClosePrenForm(e) { if (e.target === document.getElementById('modal-prenotazione')) closeNuovaPrenotazione(); }

async function openModificaPrenotazione(id) {
  const p = S.prenotazioni.find(x => x.id === id);
  if (!p) return;
  if (!S.sale.length) await loadSaleETavoli();
  S.prenEditId = id;
  S.prenTavoliSelezionati = (p.tavoli || []).map(t => t.id);
  document.getElementById('pren-modal-title').textContent = 'Modifica prenotazione';
  document.getElementById('pren-nome').value = p.nome;
  document.getElementById('pren-persone').value = p.persone;
  document.getElementById('pren-form-data').value = p.data;
  document.getElementById('pren-ora').value = p.ora;
  document.getElementById('pren-telefono').value = p.telefono || '';
  document.getElementById('pren-note').value = p.note || '';
  renderPrenTavoliCheckbox();
  closePrenDetail();
  document.getElementById('modal-prenotazione').classList.add('open');
}

function renderPrenTavoliCheckbox() {
  const el = document.getElementById('pren-tavoli-list');
  if (!S.tavoli.length) { el.innerHTML = `<div style="font-size:13px;color:var(--muted)">Nessun tavolo configurato</div>`; return; }
  el.innerHTML = `<div style="display:flex;flex-wrap:wrap;gap:8px">
    ${S.tavoli.map(t => `
      <label style="display:flex;align-items:center;gap:6px;padding:8px 12px;border:1.5px solid var(--border);border-radius:10px;cursor:pointer;font-size:13px">
        <input type="checkbox" value="${t.id}" ${S.prenTavoliSelezionati.includes(t.id)?'checked':''} onchange="onPrenTavoloToggle(${t.id}, this.checked)">
        Tavolo ${esc(t.numero)} (${t.posti})
      </label>
    `).join('')}
  </div>`;
}

function onPrenTavoloToggle(id, checked) {
  if (checked) { if (!S.prenTavoliSelezionati.includes(id)) S.prenTavoliSelezionati.push(id); }
  else { S.prenTavoliSelezionati = S.prenTavoliSelezionati.filter(x => x !== id); }
}

async function submitPrenotazione() {
  const nome     = document.getElementById('pren-nome').value.trim();
  const persone  = parseInt(document.getElementById('pren-persone').value, 10);
  const dataVal  = document.getElementById('pren-form-data').value;
  const ora      = document.getElementById('pren-ora').value;
  const telefono = document.getElementById('pren-telefono').value.trim();
  const note     = document.getElementById('pren-note').value.trim();

  if (!nome)    { toast('Inserisci il nome', 'error'); return; }
  if (!persone || persone <= 0) { toast('Inserisci il numero di persone', 'error'); return; }
  if (!dataVal) { toast('Seleziona la data', 'error'); return; }
  if (!ora)     { toast("Seleziona l'ora", 'error'); return; }

  const btn = document.getElementById('btn-pren-salva');
  btn.disabled = true;

  const body   = { nome, persone, data: dataVal, ora, telefono, note, tavoli: S.prenTavoliSelezionati };
  const result = S.prenEditId
    ? await api(`/api/prenotazioni/${S.prenEditId}`, { method: 'PUT', body })
    : await api('/api/prenotazioni', { method: 'POST', body });

  btn.disabled = false;

  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeNuovaPrenotazione();
  toast(S.prenEditId ? 'Prenotazione aggiornata' : 'Prenotazione registrata', 'success');
  await _refreshDopoModificaPrenotazione();
}

async function _refreshDopoModificaPrenotazione() {
  const mappaVisible = document.getElementById('pren-mappa-view').style.display !== 'none';
  const dayVisible    = document.getElementById('pren-day-view').style.display !== 'none';
  if (mappaVisible) await loadMappaServizio();
  if (dayVisible)   await loadPrenotazioni();
  loadCalendarioPrenotazioni();
}

function showPrenDetail(id) {
  const p = S.prenotazioni.find(x => x.id === id);
  if (!p) return;
  document.getElementById('pren-detail-title').textContent = p.nome;
  const tavoliTxt = p.tavoli && p.tavoli.length ? p.tavoli.map(t => `Tavolo ${t.numero}`).join(', ') : '—';
  const editBtn = CAN_GESTIRE_SALA
    ? `<button type="button" class="btn-svuota" style="width:100%;margin-top:16px" onclick="openModificaPrenotazione(${p.id})">Modifica</button>`
    : '';
  document.getElementById('pren-detail-body').innerHTML = `
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Data</span><span style="font-weight:600">${fmtDate(p.data)}</span></div>
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Ora</span><span style="font-weight:600">${esc(p.ora)}</span></div>
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Persone</span><span style="font-weight:600">${p.persone}</span></div>
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Telefono</span><span style="font-weight:600">${esc(p.telefono || '—')}</span></div>
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Tavolo</span><span style="font-weight:600">${esc(tavoliTxt)}</span></div>
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Stato</span><span class="pren-badge ${p.stato}">${PREN_STATO_LBL[p.stato] || p.stato}</span></div>
    ${p.note ? `<div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Note</span><span style="font-weight:600;text-align:right">${esc(p.note)}</span></div>` : ''}
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Operatore</span><span style="font-weight:600">${esc(p.operatore || '—')}</span></div>
    ${editBtn}
  `;
  document.getElementById('pren-detail-overlay').classList.add('open');
}
function closePrenDetail() { document.getElementById('pren-detail-overlay').classList.remove('open'); }

async function confermaEliminaPrenotazione(id) {
  if (!confirm('Eliminare questa prenotazione?')) return;
  const result = await api(`/api/prenotazioni/${id}`, { method: 'DELETE' });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  toast('Prenotazione eliminata', 'success');
  await _refreshDopoModificaPrenotazione();
}

// ── GESTIONE SALA ────────────────────────────────────────────────────────
const TAVOLO_BASE_SIZE     = 56;
const TAVOLO_SIZE_PER_POSTO = 6;

async function initSetupSalaPage() {
  S.mappaContext = {
    mode: 'setup', data: null, servizio: null, origine: 'setup',
    canvasId: 'setup-sala-map-canvas', svgId: 'setup-sala-mura-svg',
  };
  await Promise.all([loadSaleETavoli(), loadSalaMura()]);
  setGestioneSalaTab(S.setupSalaTab);
}

async function loadSaleETavoli() {
  const [saleData, tavoliData] = await Promise.all([api('/api/sale'), api('/api/tavoli')]);
  S.sale   = saleData.sale || [];
  S.tavoli = tavoliData.tavoli || [];
}

async function loadTavoliMappa() {
  const data = await api('/api/tavoli');
  S.tavoli = data.tavoli || [];
  renderMappaSala();
}

async function loadSalaMura() {
  const data = await api('/api/sala-mura');
  S.salaMuraPercorsoSalvato = data.percorso_svg || '';
}

function setGestioneSalaTab(tab) {
  S.setupSalaTab = tab;
  document.querySelectorAll('#gestione-sala-tabs .fpill').forEach(b =>
    b.classList.toggle('active', b.dataset.tab === tab)
  );
  document.getElementById('mura-toolbar').style.display = tab === 'mura' ? '' : 'none';
  document.getElementById('mura-scan-row').style.display = tab === 'mura' ? '' : 'none';
  const azioniTavoli = document.getElementById('sala-tavoli-actions');
  if (azioniTavoli) azioniTavoli.style.display = tab === 'tavoli' ? 'flex' : 'none';

  if (tab === 'mura') {
    // Si riparte da quanto già disegnato: l'utente può aggiungere altri
    // tratti o premere "Cancella" per ripartire da zero. "Salva mura"
    // sostituisce sempre l'intera planimetria con ciò che c'è a schermo.
    S.salaMuraSottopercorsi = S.salaMuraPercorsoSalvato ? [S.salaMuraPercorsoSalvato] : [];
  }
  renderSalaMuraSvg();
  renderMappaSala();
}

function renderSalaMuraSvg() {
  const svg = document.getElementById(S.mappaContext.svgId);
  if (!svg) return;
  const editing = S.mappaContext.mode === 'setup' && S.setupSalaTab === 'mura';
  const percorsi = editing ? S.salaMuraSottopercorsi : [S.salaMuraPercorsoSalvato];
  svg.innerHTML = percorsi.filter(Boolean).map(d =>
    `<path class="mura-stroke" d="${d}"></path>`
  ).join('');
  svg.classList.toggle('editable', editing);
  if (editing) _initMuraDraw(svg);
}

function setMuraTool(tool) {
  S.muraTool = tool;
  document.querySelectorAll('#mura-toolbar .fpill[data-tool]').forEach(b =>
    b.classList.toggle('active', b.dataset.tool === tool)
  );
}

function cancellaMuraDisegno() {
  S.salaMuraSottopercorsi = [];
  renderSalaMuraSvg();
}

async function salvaMura() {
  const percorso_svg = S.salaMuraSottopercorsi.join(' ');
  const result = await api('/api/sala-mura', { method: 'POST', body: { percorso_svg } });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  S.salaMuraPercorsoSalvato = percorso_svg;
  toast('Mura salvate', 'success');
  setGestioneSalaTab('tavoli');
}

// ── SCANSIONE FOTOCAMERA (mura + tavoli) ────────────────────────────────
function triggerScanSala() {
  document.getElementById('sala-scan-input').click();
}

async function onSalaScanCapture(input) {
  const file = input.files[0];
  input.value = '';
  if (!file) return;

  const btn = document.getElementById('btn-scan-sala');
  btn.disabled = true;
  btn.classList.add('scanning');
  btn.innerHTML = '<div class="btn-scan-spinner"></div> Analisi in corso...';

  let image, mediaType = 'image/jpeg';
  try {
    image = await compressImage(file, 1200, 0.8);
  } catch {
    image = await readFileAsDataURL(file);
    mediaType = file.type || 'image/jpeg';
  }

  const data = await api('/api/sala-scan', { method: 'POST', body: { image, media_type: mediaType } });

  btn.disabled = false;
  btn.classList.remove('scanning');
  btn.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/><circle cx="12" cy="13" r="4"/></svg> Scansiona sala';

  if (!data.success) {
    toast(data.error || 'Analisi non riuscita — prova con una forma predefinita', 'error');
    openFormePredefinite();
    return;
  }

  S.salaMuraSottopercorsi = data.muri || [];
  renderSalaMuraSvg();

  let prossimoNumero = S.tavoli.reduce((max, t) => {
    const n = parseInt(t.numero, 10);
    return Number.isFinite(n) && n > max ? n : max;
  }, 0);
  let creati = 0;
  for (const t of (data.tavoli || [])) {
    prossimoNumero += 1;
    const r = await api('/api/tavoli', { method: 'POST', body: {
      numero: String(prossimoNumero), forma: t.forma, posti: 2,
      posizione_x: t.posizione_x, posizione_y: t.posizione_y,
    }});
    if (r.success) creati += 1;
  }
  if (creati) await loadTavoliMappa();

  const nMuri = (data.muri || []).length;
  toast(`Rilevat${nMuri === 1 ? 'o' : 'i'} ${nMuri} mur${nMuri === 1 ? 'o' : 'a'} e ${creati} tavol${creati === 1 ? 'o' : 'i'} — rivedi e premi "Salva mura"`, 'success');
}

const FORME_PREDEFINITE = {
  rettangolo_piccolo: ['M100,100 L700,100 L700,450 L100,450 L100,100'],
  rettangolo_grande:  ['M60,60 L940,60 L940,640 L60,640 L60,60'],
  quadrato:           ['M250,80 L750,80 L750,580 L250,580 L250,80'],
  elle:               ['M80,80 L600,80 L600,350 L920,350 L920,620 L80,620 L80,80'],
};

function openFormePredefinite() { document.getElementById('modal-forme-predefinite').classList.add('open'); }
function closeFormePredefinite() { document.getElementById('modal-forme-predefinite').classList.remove('open'); }
function bgCloseFormePredefinite(e) { if (e.target === document.getElementById('modal-forme-predefinite')) closeFormePredefinite(); }

function applicaFormaPredefinita(chiave) {
  S.salaMuraSottopercorsi = [...(FORME_PREDEFINITE[chiave] || [])];
  renderSalaMuraSvg();
  closeFormePredefinite();
  toast('Forma applicata — modifica il disegno se serve, poi salva', 'success');
}

function _initMuraDraw(svg) {
  if (svg.dataset.drawInit) return;
  svg.dataset.drawInit = '1';

  let current = null, d = '';
  const getPoint = e => {
    const p = e.touches ? e.touches[0] : e;
    const r = svg.getBoundingClientRect();
    // width/height dell'svg = viewBox (1000x700, nessuno scaling): 1 unità = 1px.
    return { x: p.clientX - r.left, y: p.clientY - r.top };
  };

  const onStart = e => {
    if (S.setupSalaTab !== 'mura' || S.muraTool !== 'linea') return;
    if (e.cancelable) e.preventDefault();
    const { x, y } = getPoint(e);
    d = `M${x.toFixed(1)},${y.toFixed(1)}`;
    current = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    current.setAttribute('class', 'mura-stroke');
    current.setAttribute('d', d);
    svg.appendChild(current);
  };
  const onMove = e => {
    if (!current) return;
    if (e.cancelable) e.preventDefault();
    const { x, y } = getPoint(e);
    d += ` L${x.toFixed(1)},${y.toFixed(1)}`;
    current.setAttribute('d', d);
  };
  const onEnd = () => {
    if (!current) return;
    S.salaMuraSottopercorsi.push(d);
    current = null;
  };

  svg.addEventListener('touchstart', onStart, { passive: false });
  svg.addEventListener('touchmove',  onMove,  { passive: false });
  svg.addEventListener('touchend',   onEnd);
  svg.addEventListener('touchcancel', () => { current = null; });

  svg.addEventListener('mousedown', onStart);
  window.addEventListener('mousemove', e => { if (current) onMove(e); });
  window.addEventListener('mouseup', onEnd);
}

function canDragTavoli() {
  return S.mappaContext.mode === 'setup'
    ? (CAN_GESTIRE_SALA && S.setupSalaTab === 'tavoli')
    : CAN_GESTIRE_SALA;
}

function renderMappaSala() {
  const canvas = document.getElementById(S.mappaContext.canvasId);
  if (!canvas) return;
  const canDrag = canDragTavoli();
  const wrap = document.createElement('div');
  wrap.innerHTML = S.tavoli.map(t => {
    const size = TAVOLO_BASE_SIZE + Math.max(0, t.posti - 2) * TAVOLO_SIZE_PER_POSTO;
    const w = t.forma === 'rettangolare' ? size * 1.4 : size;
    const p = t.prenotazione;
    const tooltip = p ? ` title="${esc(p.nome)} · ${esc(p.ora)} · ${p.persone}p"` : '';
    return `<div class="tavolo-box ${t.forma} ${t.stato} ${canDrag ? '' : 'no-drag'}"
                 data-id="${t.id}" tabindex="0"${tooltip}
                 style="left:${t.posizione_x}px;top:${t.posizione_y}px;width:${w}px;height:${size}px;transform:rotate(${t.rotazione || 0}deg)">
      <span class="tavolo-numero">${esc(t.numero)}</span>
      <span class="tavolo-posti">${t.posti}p</span>
    </div>`;
  }).join('');
  // Sostituisce solo i .tavolo-box, senza toccare l'svg delle mura già presente nel canvas.
  canvas.querySelectorAll('.tavolo-box').forEach(el => el.remove());
  while (wrap.firstChild) canvas.appendChild(wrap.firstChild);

  _initTavoloDrag(canvas);
  _initTavoloKeyboardNav(canvas);
}

function _initTavoloDrag(canvas) {
  if (!canvas || canvas.dataset.dragInit) return;
  canvas.dataset.dragInit = '1';

  let drag = null;
  let lastTap = null; // { id, time } — per rilevare il doppio tap (rotazione)
  const getPoint = e => e.touches ? e.touches[0] : e;

  const onDown = e => {
    const box = e.target.closest('.tavolo-box');
    if (!box) return;
    const p = getPoint(e);
    drag = {
      el: box, id: box.dataset.id,
      startClientX: p.clientX, startClientY: p.clientY,
      startLeft: parseFloat(box.style.left) || 0,
      startTop:  parseFloat(box.style.top)  || 0,
      canDrag: canDragTavoli(),
      moved: false,
    };
    if (drag.canDrag) box.classList.add('dragging');
  };

  const onMove = e => {
    if (!drag) return;
    const p  = getPoint(e);
    const dx = p.clientX - drag.startClientX;
    const dy = p.clientY - drag.startClientY;
    if (Math.abs(dx) > 4 || Math.abs(dy) > 4) drag.moved = true;
    if (!drag.canDrag || !drag.moved) return;
    if (e.cancelable) e.preventDefault();
    drag.el.style.left = `${drag.startLeft + dx}px`;
    drag.el.style.top  = `${drag.startTop  + dy}px`;
  };

  const onUp = () => {
    if (!drag) return;
    const { el, id, moved, canDrag } = drag;
    el.classList.remove('dragging');
    if (canDrag && moved) {
      const left = parseFloat(el.style.left) || 0;
      const top  = parseFloat(el.style.top)  || 0;
      if (S.mappaContext.mode === 'setup') {
        api(`/api/tavoli/${id}`, { method: 'PATCH', body: { posizione_x: left, posizione_y: top } });
      } else {
        api('/api/sala-disposizione', { method: 'POST', body: {
          tavolo_id: Number(id), data: S.mappaContext.data, servizio: S.mappaContext.servizio,
          posizione_x: left, posizione_y: top,
        }});
      }
      lastTap = null;
    } else {
      // Tap (non un vero drag: o non c'è stato movimento, o il drag non è
      // permesso — es. mappa servizio — e allora un piccolo movimento
      // accidentale del dito non deve impedire l'apertura dello sheet).
      // La rotazione (doppio tap) resta un'operazione di Setup Sala: cambia
      // la planimetria base, non ha senso per il servizio di un solo giorno.
      const now = Date.now();
      const canRuota = canDrag && S.mappaContext.mode === 'setup';
      if (canRuota && lastTap && lastTap.id === id && now - lastTap.time < 350) {
        ruotaTavolo(Number(id));
        lastTap = null;
      } else {
        lastTap = canRuota ? { id, time: now } : null;
        openTavoloSheet(Number(id));
      }
    }
    drag = null;
  };

  canvas.addEventListener('touchstart', onDown, { passive: true });
  canvas.addEventListener('touchmove',  onMove,  { passive: false });
  canvas.addEventListener('touchend',   onUp);
  canvas.addEventListener('touchcancel', () => { drag = null; });

  canvas.addEventListener('mousedown', onDown);
  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
}

function _initTavoloKeyboardNav(canvas) {
  if (!canvas || canvas.dataset.keynavInit) return;
  canvas.dataset.keynavInit = '1';
  canvas.addEventListener('keydown', e => {
    const box = e.target.closest('.tavolo-box');
    if (!box) return;
    if (e.key === 'Enter') {
      e.preventDefault();
      openTavoloSheet(Number(box.dataset.id));
      return;
    }
    const dirs = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    const dir = dirs[e.key];
    if (!dir) return;
    e.preventDefault();
    const cx = parseFloat(box.style.left) || 0;
    const cy = parseFloat(box.style.top)  || 0;
    let best = null, bestScore = Infinity;
    canvas.querySelectorAll('.tavolo-box').forEach(other => {
      if (other === box) return;
      const ox = parseFloat(other.style.left) || 0;
      const oy = parseFloat(other.style.top)  || 0;
      const dx = ox - cx, dy = oy - cy;
      const along = dx * dir[0] + dy * dir[1];
      if (along <= 0) return;
      const across = Math.abs(dx * dir[1] - dy * dir[0]);
      const score = along + across * 2;
      if (score < bestScore) { bestScore = score; best = other; }
    });
    if (best) best.focus();
  });
}

async function ruotaTavolo(id) {
  const t = S.tavoli.find(x => x.id === id);
  if (!t) return;
  const nuovaRot = ((t.rotazione || 0) + 90) % 360;
  const el = document.querySelector(`#${S.mappaContext.canvasId} .tavolo-box[data-id="${id}"]`);
  if (el) el.style.transform = `rotate(${nuovaRot}deg)`;   // feedback immediato
  t.rotazione = nuovaRot;
  await api(`/api/tavoli/${id}`, { method: 'PATCH', body: { rotazione: nuovaRot } });
}

function openTavoloSheet(id) {
  const t = S.tavoli.find(x => x.id === id);
  if (!t) return;
  document.getElementById('tavolo-sheet-title').textContent = `Tavolo ${t.numero}`;
  document.getElementById('tavolo-sheet-body').innerHTML =
    S.mappaContext.mode === 'setup' ? _tavoloSheetSetupHtml(t) : _tavoloSheetServizioHtml(t);
  document.getElementById('tavolo-sheet-overlay').classList.add('open');
}
function closeTavoloSheet() { document.getElementById('tavolo-sheet-overlay').classList.remove('open'); }

function _tavoloSheetSetupHtml(t) {
  const statoLbl = { libero: 'Libero', occupato: 'Occupato' };
  let html = `
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Forma</span><span style="font-weight:600">${t.forma === 'rotondo' ? 'Rotondo' : 'Rettangolare'}</span></div>
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Posti</span><span style="font-weight:600">${t.posti}</span></div>
    <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Stato</span><span class="temp-badge ${t.stato === 'libero' ? 'ok' : 'danger'}">${statoLbl[t.stato]}</span></div>
  `;
  if (S.setupSalaTab === 'tavoli') {
    html += `
      <button type="button" class="btn-svuota" style="width:100%;margin-top:14px" onclick="toggleTavoloOccupato(${t.id})">${t.occupato ? 'Segna come libero' : 'Segna come occupato'}</button>
      <button type="button" class="btn-svuota" style="width:100%;margin-top:8px" onclick="openModificaTavolo(${t.id})">Modifica tavolo</button>
      <button type="button" class="btn-svuota" style="width:100%;margin-top:8px;color:var(--red);border-color:rgba(239,68,68,.3)" onclick="confermaEliminaTavolo(${t.id})">Elimina tavolo</button>
    `;
  }
  return html;
}

function _tavoloSheetServizioHtml(t) {
  const p = t.prenotazione;
  const statoLbl = { libero: 'Libero', occupato: 'Occupato', prenotato: 'Prenotato' };
  let html = `<div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Stato</span><span class="temp-badge ${t.stato === 'libero' ? 'ok' : 'danger'}">${statoLbl[t.stato]}</span></div>`;

  if (p) {
    html += `
      <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Cliente</span><span style="font-weight:600">${esc(p.nome)}</span></div>
      <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Persone</span><span style="font-weight:600">${p.persone}</span></div>
      <div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Ora</span><span style="font-weight:600">${esc(p.ora)}</span></div>
      ${p.telefono ? `<div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Telefono</span><span style="font-weight:600">${esc(p.telefono)}</span></div>` : ''}
      ${p.note ? `<div class="lot-detail-row"><span style="font-size:12px;color:var(--muted)">Note</span><span style="font-weight:600;text-align:right">${esc(p.note)}</span></div>` : ''}
    `;
  }

  if (CAN_GESTIRE_SALA) {
    if (p && PREN_STATO_AZIONE[p.stato]) {
      html += `<button type="button" class="btn-svuota" style="width:100%;margin-top:14px" onclick="avanzaStatoPrenotazione(${p.id}, '${PREN_STATO_AZIONE[p.stato].next}')">${PREN_STATO_AZIONE[p.stato].label}</button>`;
    } else if (!p) {
      html += t.occupato
        ? `<button type="button" class="btn-svuota" style="width:100%;margin-top:14px" onclick="toggleTavoloOccupatoServizio(${t.id}, false)">Libera tavolo</button>`
        : `<button type="button" class="btn-svuota" style="width:100%;margin-top:14px" onclick="toggleTavoloOccupatoServizio(${t.id}, true)">Segna occupato</button>`;
    }
    html += `<button type="button" class="btn-svuota" style="width:100%;margin-top:8px" onclick="openNuovaPrenotazione(${t.id})">Nuova prenotazione qui</button>`;
  }

  return html;
}

async function avanzaStatoPrenotazione(id, stato) {
  const result = await api(`/api/prenotazioni/${id}/stato`, { method: 'POST', body: { stato } });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeTavoloSheet();
  toast(stato === 'libero' ? 'Tavolo liberato' : 'Aggiornato', 'success');
  await loadMappaServizio();
}

async function toggleTavoloOccupatoServizio(id, occupato) {
  const result = await api(`/api/tavoli/${id}/occupato`, { method: 'POST', body: { occupato } });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeTavoloSheet();
  await loadMappaServizio();
}

async function toggleTavoloOccupato(id) {
  const t = S.tavoli.find(x => x.id === id);
  if (!t) return;
  const result = await api(`/api/tavoli/${id}`, { method: 'PATCH', body: { occupato: t.occupato ? 0 : 1 } });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeTavoloSheet();
  await loadTavoliMappa();
}

async function confermaEliminaTavolo(id) {
  if (!confirm('Eliminare questo tavolo?')) return;
  const result = await api(`/api/tavoli/${id}`, { method: 'DELETE' });
  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeTavoloSheet();
  toast('Tavolo eliminato', 'success');
  await loadTavoliMappa();
}

function openNuovoTavolo() {
  S.tavoloEditId = null;
  document.getElementById('tavolo-modal-title').textContent = 'Nuovo tavolo';
  document.getElementById('tavolo-numero').value = '';
  document.getElementById('tavolo-forma').value = 'rettangolare';
  document.getElementById('tavolo-posti').value = '2';
  document.getElementById('modal-tavolo').classList.add('open');
}

function openModificaTavolo(id) {
  const t = S.tavoli.find(x => x.id === id);
  if (!t) return;
  S.tavoloEditId = id;
  document.getElementById('tavolo-modal-title').textContent = 'Modifica tavolo';
  document.getElementById('tavolo-numero').value = t.numero;
  document.getElementById('tavolo-forma').value = t.forma;
  document.getElementById('tavolo-posti').value = t.posti;
  closeTavoloSheet();
  document.getElementById('modal-tavolo').classList.add('open');
}

function closeTavoloForm() { document.getElementById('modal-tavolo').classList.remove('open'); }
function bgCloseTavoloForm(e) { if (e.target === document.getElementById('modal-tavolo')) closeTavoloForm(); }

async function submitTavoloForm() {
  const numero = document.getElementById('tavolo-numero').value.trim();
  const forma  = document.getElementById('tavolo-forma').value;
  const posti  = parseInt(document.getElementById('tavolo-posti').value, 10);

  if (!numero) { toast('Inserisci il numero del tavolo', 'error'); return; }
  if (!posti || posti <= 0) { toast('Inserisci il numero di posti', 'error'); return; }

  const result = S.tavoloEditId
    ? await api(`/api/tavoli/${S.tavoloEditId}`, { method: 'PATCH', body: { numero, forma, posti } })
    : await api('/api/tavoli', { method: 'POST', body: { numero, forma, posti, posizione_x: 40, posizione_y: 40 } });

  if (!result.success) { toast(result.error || 'Errore', 'error'); return; }
  closeTavoloForm();
  toast(S.tavoloEditId ? 'Tavolo aggiornato' : 'Tavolo creato', 'success');
  await loadTavoliMappa();
}

// ── EXPORT PDF ────────────────────────────────────────────────────────────
function initExportPage() {
  const selAnno = document.getElementById('exp-anno');
  if (!selAnno.dataset.init) {
    const oggi = new Date();
    const annoCorrente = oggi.getFullYear();
    selAnno.innerHTML = '';
    for (let y = annoCorrente; y >= annoCorrente - 4; y--) {
      const opt = document.createElement('option');
      opt.value = y;
      opt.textContent = y;
      selAnno.appendChild(opt);
    }
    document.getElementById('exp-mese').value = String(oggi.getMonth() + 1);
    selAnno.dataset.init = '1';
  }
  onExportTipoChange();
}

function onExportTipoChange() {
  const tipo = document.getElementById('exp-tipo').value;
  document.getElementById('exp-mese-group').style.display = (tipo === 'report_mensile') ? 'none' : '';
}

async function generaExportPdf() {
  const tipo = document.getElementById('exp-tipo').value;
  const anno = document.getElementById('exp-anno').value;
  const mese = document.getElementById('exp-mese').value;

  const params = new URLSearchParams({ tipo, anno });
  if (tipo !== 'report_mensile') params.set('mese', mese);

  const btn = document.getElementById('btn-export');
  btn.classList.add('loading');
  btn.disabled = true;

  try {
    const res = await fetch(`/api/export/pdf?${params.toString()}`);
    if (!res.ok) {
      let msg = 'Errore durante la generazione del PDF';
      try { const data = await res.json(); msg = data.error || msg; } catch (e) {}
      toast(msg, 'error');
      return;
    }
    const blob     = await res.blob();
    const cd       = res.headers.get('Content-Disposition') || '';
    const match    = cd.match(/filename="?([^"]+)"?/);
    const filename = match ? match[1] : `export_${tipo}.pdf`;

    const url = URL.createObjectURL(blob);
    const a   = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 4000);

    toast('PDF generato', 'success');
  } catch (e) {
    toast('Errore di rete', 'error');
  } finally {
    btn.classList.remove('loading');
    btn.disabled = false;
  }
}

// ── CHAT (stile Discord) ────────────────────────────────────────────────
let _chatPollTimer  = null;
let _chatPhotoData  = null;   // data URL della foto selezionata, in attesa di invio
let _chatLongPressT = null;
let _chatLongPressFired = 0; // timestamp dell'ultimo tap prolungato, per non aprire anche il lightbox sulla foto
let _chatActionMsgId = null;  // messaggio bersaglio del foglio Modifica/Elimina
let _chatEditingId   = null;  // messaggio attualmente in modifica inline

function _fmtChatBubbleOra(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' });
}

function _fmtChatDateSep(iso) {
  const d = new Date(iso);
  const oggi = new Date();
  const ieri = new Date(oggi); ieri.setDate(oggi.getDate() - 1);
  const stessoGiorno = (a, b) => a.toDateString() === b.toDateString();
  if (stessoGiorno(d, oggi)) return 'Oggi';
  if (stessoGiorno(d, ieri)) return 'Ieri';
  return d.toLocaleDateString('it-IT', {
    day: 'numeric', month: 'long',
    year: d.getFullYear() !== oggi.getFullYear() ? 'numeric' : undefined,
  });
}

function _chatAvatarColor(nome) {
  let hash = 0;
  for (let i = 0; i < nome.length; i++) hash = nome.charCodeAt(i) + ((hash << 5) - hash);
  return CHAT_AVATAR_PALETTE[Math.abs(hash) % CHAT_AVATAR_PALETTE.length];
}

function initChatPage() {
  markChatCanaleLetto(S.chatCanale);
  loadChatMessages(true);
  loadChatChannelBadges();
  stopChatPolling();
  _chatPollTimer = setInterval(() => {
    if (_chatEditingId === null) { loadChatMessages(false); loadChatChannelBadges(); }
  }, 10000);
  const input = document.getElementById('chat-text-input');
  if (input && !input.dataset.enterBound) {
    input.dataset.enterBound = '1';
    input.addEventListener('keydown', e => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChatMessage(); }
    });
    input.addEventListener('input', () => _autoGrowChatInput(input));
  }
}

function stopChatPolling() {
  if (_chatPollTimer) { clearInterval(_chatPollTimer); _chatPollTimer = null; }
}

function setChatCanale(canale, btn) {
  S.chatCanale = canale;
  _chatEditingId = null;
  document.querySelectorAll('.chat-channel-item').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('chat-channel-header-name').textContent = canale;
  document.getElementById('chat-channel-header-desc').textContent = CHAT_CANALE_DESC[canale] || '';
  const input = document.getElementById('chat-text-input');
  if (input) input.placeholder = `Scrivi un messaggio in #${canale}…`;
  loadChatMessages(true);
  markChatCanaleLetto(canale);
}

async function markChatCanaleLetto(canale) {
  await api('/api/chat/letto', { method: 'POST', body: { canale } });
  const badge = document.getElementById(`chat-badge-${canale}`);
  if (badge) badge.style.display = 'none';
  loadChatUnreadBadge();
}

async function loadChatChannelBadges() {
  const data = await api('/api/chat/unread-count');
  const counts = data.counts || {};
  CHAT_MIE_CANALI.forEach(c => {
    const badge = document.getElementById(`chat-badge-${c}`);
    if (!badge) return;
    const n = c === S.chatCanale ? 0 : (counts[c] || 0);
    badge.textContent = n > 99 ? '99+' : n;
    badge.style.display = n > 0 ? 'flex' : 'none';
  });
}

async function loadChatMessages(scrollToBottom) {
  const data = await api(`/api/chat?canale=${encodeURIComponent(S.chatCanale)}`);
  S.chatMessaggi = data.messaggi || [];
  renderChatMessages(scrollToBottom);
}

function renderChatMessages(scrollToBottom) {
  const el = document.getElementById('chat-messages');
  if (!el) return;
  const eraInFondo = el.scrollHeight - el.scrollTop - el.clientHeight < 120;

  let html = '';
  let lastDateKey  = null;
  let lastAuthorId = null;
  let lastTime     = 0;

  S.chatMessaggi.forEach(m => {
    const d = new Date(m.creato_il);
    const dateKey = d.toDateString();
    if (dateKey !== lastDateKey) {
      html += `<div class="chat-date-sep"><span>${_fmtChatDateSep(m.creato_il)}</span></div>`;
      lastDateKey  = dateKey;
      lastAuthorId = null;
    }

    const isNewGroup = m.eliminato || m.utente_id !== lastAuthorId || (d.getTime() - lastTime) > 5 * 60 * 1000;
    lastAuthorId = m.eliminato ? null : m.utente_id;
    lastTime     = d.getTime();
    const ora    = _fmtChatBubbleOra(m.creato_il);

    if (m.eliminato) {
      html += `
        <div class="chat-msg-row chat-msg-first">
          <div class="chat-avatar-slot"><div class="chat-avatar" style="background:${_chatAvatarColor(m.utente_nome)}">${esc((m.utente_nome[0] || '?').toUpperCase())}</div></div>
          <div class="chat-msg-content">
            <div class="chat-msg-meta"><span class="chat-msg-author">${esc(m.utente_nome)}</span><span class="chat-msg-time">${ora}</span></div>
            <div class="chat-msg-text deleted">Messaggio eliminato</div>
          </div>
        </div>`;
      return;
    }

    const canEdit = (m.utente_id === CURRENT_USER_ID) || IS_ADMIN;
    const fotoHtml = m.foto
      ? `<img class="chat-msg-photo" src="${esc(m.foto)}" alt="" onclick="_onChatPhotoClick(event, '${esc(m.foto)}')">`
      : '';
    const testoHtml  = m.testo ? `<div class="chat-msg-text">${esc(m.testo)}</div>` : '';
    const editedHtml = m.modificato ? '<span class="chat-msg-edited">(modificato)</span>' : '';
    const badgeColor = CHAT_BADGE_COLORS[m.autore_badge] || 'var(--muted)';
    const badgeHtml  = m.autore_badge ? `<span class="chat-msg-badge" style="color:${badgeColor}">${esc(m.autore_badge)}</span>` : '';

    if (isNewGroup) {
      html += `
        <div class="chat-msg-row chat-msg-first" data-id="${m.id}" data-can-edit="${canEdit ? '1' : '0'}">
          <div class="chat-avatar-slot"><div class="chat-avatar" style="background:${_chatAvatarColor(m.utente_nome)}">${esc((m.utente_nome[0] || '?').toUpperCase())}</div></div>
          <div class="chat-msg-content">
            <div class="chat-msg-meta">
              <span class="chat-msg-author">${esc(m.utente_nome)}</span>
              ${badgeHtml}
              <span class="chat-msg-time">${ora}</span>
            </div>
            ${testoHtml}${editedHtml}
            ${fotoHtml}
          </div>
        </div>`;
    } else {
      html += `
        <div class="chat-msg-row chat-msg-cont" data-id="${m.id}" data-can-edit="${canEdit ? '1' : '0'}">
          <div class="chat-avatar-slot"><div class="chat-msg-time-gutter">${ora}</div></div>
          <div class="chat-msg-content">
            ${testoHtml}${editedHtml}
            ${fotoHtml}
          </div>
        </div>`;
    }
  });

  el.innerHTML = html || '<div class="chat-empty-hint">Nessun messaggio ancora. Scrivi il primo!</div>';

  document.querySelectorAll('.chat-msg-row[data-can-edit="1"]').forEach(_attachChatLongPress);

  if (scrollToBottom || eraInFondo) el.scrollTop = el.scrollHeight;
}

function _attachChatLongPress(el) {
  if (el.dataset.lpInit) return;
  el.dataset.lpInit = '1';
  let sx = 0, sy = 0;
  const cancel = () => clearTimeout(_chatLongPressT);
  el.addEventListener('pointerdown', e => {
    sx = e.clientX; sy = e.clientY;
    _chatLongPressT = setTimeout(() => {
      _chatLongPressFired = Date.now();
      openChatAzioni(parseInt(el.dataset.id, 10));
    }, 480);
  });
  el.addEventListener('pointermove', e => {
    if (Math.abs(e.clientX - sx) > 10 || Math.abs(e.clientY - sy) > 10) cancel();
  });
  ['pointerup', 'pointercancel', 'pointerleave'].forEach(evt => el.addEventListener(evt, cancel));
}

function _onChatPhotoClick(event, url) {
  event.stopPropagation();
  // Sopprime solo il click che segue immediatamente lo stesso gesto di tap
  // prolungato (evita di aprire anche il lightbox); un tap successivo,
  // anche a distanza di pochi istanti, deve aprirlo normalmente.
  if (Date.now() - _chatLongPressFired < 500) return;
  openChatPhotoViewer(url);
}

function openChatPhotoViewer(url) {
  document.getElementById('chat-photo-viewer-img').src = url;
  document.getElementById('modal-chat-photo-viewer').classList.add('open');
}
function closeChatPhotoViewer() {
  document.getElementById('modal-chat-photo-viewer').classList.remove('open');
}

function openChatAzioni(msgId) {
  _chatActionMsgId = msgId;
  document.getElementById('modal-chat-azioni').classList.add('open');
}
function closeChatAzioni() { document.getElementById('modal-chat-azioni').classList.remove('open'); }
function bgCloseChatAzioni(e) { if (e.target === document.getElementById('modal-chat-azioni')) closeChatAzioni(); }

function startEditChatMessage() {
  const msgId = _chatActionMsgId;
  closeChatAzioni();
  const msg = S.chatMessaggi.find(m => m.id === msgId);
  if (!msg) return;
  _chatEditingId = msgId;
  const row = document.querySelector(`.chat-msg-row[data-id="${msgId}"]`);
  if (!row) return;
  const content = row.querySelector('.chat-msg-content');
  const meta = content.querySelector('.chat-msg-meta');
  content.innerHTML = (meta ? meta.outerHTML : '') + `
    <div class="chat-edit-wrap">
      <textarea class="chat-edit-textarea" id="chat-edit-textarea">${esc(msg.testo)}</textarea>
      <div class="chat-edit-actions">
        <button type="button" class="chat-edit-cancel" onclick="cancelEditChatMessage()">Annulla</button>
        <button type="button" class="chat-edit-save" onclick="saveEditChatMessage(${msgId})">Salva</button>
      </div>
    </div>`;
  document.getElementById('chat-edit-textarea').focus();
}

function cancelEditChatMessage() {
  _chatEditingId = null;
  renderChatMessages(false);
}

async function saveEditChatMessage(msgId) {
  const testo = document.getElementById('chat-edit-textarea').value.trim();
  if (!testo) { toast('Il messaggio non può essere vuoto', 'error'); return; }
  const res = await api(`/api/chat/${msgId}`, { method: 'PUT', body: { testo } });
  if (!res.success) { toast(res.error || 'Errore', 'error'); return; }
  _chatEditingId = null;
  await loadChatMessages(false);
}

function confirmDeleteChatMessage() {
  const msgId = _chatActionMsgId;
  closeChatAzioni();
  if (!confirm('Eliminare questo messaggio?')) return;
  api(`/api/chat/${msgId}`, { method: 'DELETE' }).then(res => {
    if (!res.success) { toast(res.error || 'Errore', 'error'); return; }
    loadChatMessages(false);
  });
}

function _autoGrowChatInput(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 120) + 'px';
}

async function sendChatMessage() {
  const input = document.getElementById('chat-text-input');
  const testo = input.value.trim();
  const foto  = _chatPhotoData;
  if (!testo && !foto) return;

  input.value = '';
  _autoGrowChatInput(input);
  cancelChatPhoto();

  const res = await api('/api/chat', {
    method: 'POST',
    body: { canale: S.chatCanale, testo, foto: foto || '' },
  });
  if (!res.success) { toast(res.error || 'Errore invio messaggio', 'error'); return; }
  loadChatMessages(true);
}

async function onChatPhotoSelected(event) {
  const file = event.target.files[0];
  event.target.value = '';
  if (!file) return;
  try {
    _chatPhotoData = await compressImage(file, 1000, 0.75);
    document.getElementById('chat-photo-preview-img').src = _chatPhotoData;
    document.getElementById('chat-photo-preview').style.display = 'flex';
  } catch (e) {
    toast('Impossibile leggere la foto', 'error');
  }
}

function cancelChatPhoto() {
  _chatPhotoData = null;
  document.getElementById('chat-photo-preview').style.display = 'none';
  document.getElementById('chat-photo-preview-img').src = '';
}

async function loadChatUnreadBadge() {
  if (!CAN_CHAT) return;
  const data = await api('/api/chat/unread-count');
  const count = data.count || 0;
  const badge = document.getElementById('drw-chat-badge');
  if (badge) { badge.textContent = count > 0 ? count : ''; badge.classList.toggle('show', count > 0); }
}

// ── PUSH NOTIFICATIONS ──────────────────────────────────────────────────
function _pushUrlBase64ToUint8Array(base64String) {
  if (!base64String) throw new Error('VAPID_PUBLIC_KEY mancante o vuota lato server');
  const invalidi = [...new Set((base64String.match(/[^A-Za-z0-9_-]/g) || []).map(c => JSON.stringify(c)))];
  if (invalidi.length) {
    throw new Error(`VAPID_PUBLIC_KEY contiene caratteri non validi: ${invalidi.join(', ')} (lunghezza ${base64String.length}, attesi 87)`);
  }
  const padding = '='.repeat((4 - base64String.length % 4) % 4);
  const base64  = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
  const raw = atob(base64);
  return Uint8Array.from([...raw].map(c => c.charCodeAt(0)));
}

async function ensurePushSubscription() {
  if (!('serviceWorker' in navigator) || !('PushManager' in window) || !VAPID_PUBLIC_KEY) return false;
  try {
    const reg = await navigator.serviceWorker.ready;
    let sub = await reg.pushManager.getSubscription();
    if (!sub) {
      console.log('[Push] creo una nuova subscription nel browser...');
      sub = await reg.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: _pushUrlBase64ToUint8Array(VAPID_PUBLIC_KEY),
      });
      console.log('[Push] subscription creata nel browser:', sub.endpoint);
    }
    const res = await api('/api/push/subscribe', { method: 'POST', body: sub.toJSON() });
    if (!res || !res.success) {
      // Senza questo controllo il browser risulta "iscritto" anche quando il
      // server non ha salvato nulla nel DB — è la causa esatta per cui
      // l'admin poi vede "nessuna subscription attiva" pur avendo l'utente
      // "attivato" le notifiche.
      console.error('[Push] il server non ha confermato il salvataggio della subscription:', res);
      return false;
    }
    console.log('[Push] subscription confermata dal server');
    return true;
  } catch (e) {
    console.warn('[Push] subscribe failed', e);
    return false;
  }
}

// iOS Safari supporta le Web Push SOLO per una PWA aggiunta alla schermata
// Home e aperta da lì (standalone) — mai dentro una scheda Safari normale.
// Inoltre iOS richiede che Notification.requestPermission() parta da un
// gesto utente reale (tap): chiamarla da codice automatico (es. al primo
// login, senza un click) viene ignorata silenziosamente — il permesso
// resta 'default' per sempre, ma il nostro flag "pushAsked" veniva comunque
// segnato come "già richiesto", quindi l'utente non vedeva più alcun modo
// per riprovare. Per questo ora l'attivazione avviene SOLO tramite un tap
// esplicito (pulsante "Attiva" o Profilo), mai in automatico al login.
function _isIOS() {
  return /iPad|iPhone|iPod/.test(navigator.userAgent) && !window.MSStream;
}
function _isStandalonePwa() {
  return window.matchMedia('(display-mode: standalone)').matches || navigator.standalone === true;
}

function initPushNotifications() {
  renderNotifBanner();
}

// ── BANNER "ATTIVA NOTIFICHE" (home, entrambe le pagine) ─────────────────
function renderNotifBanner() {
  const supportato = ('Notification' in window) && ('serviceWorker' in navigator);
  // Il banner compare solo se il permesso non è mai stato chiesto
  // ('default'). Se è già concesso o è stato negato non ha senso riproporlo
  // a ogni apertura: per riattivarle c'è sempre il Profilo.
  let dismissed = false;
  try { dismissed = !!localStorage.getItem('notifBannerDismissed'); } catch (e) {}
  const show = supportato &&
               Notification.permission === 'default' &&
               !dismissed;
  const serveInstallazione = show && _isIOS() && !_isStandalonePwa();

  document.querySelectorAll('.notif-banner').forEach(el => {
    el.style.display = show ? 'flex' : 'none';
    const testoEl = el.querySelector('.notif-banner-text');
    const btnEl   = el.querySelector('.notif-banner-btn');
    if (!testoEl || !btnEl) return;
    if (serveInstallazione) {
      // Su iOS, finché l'app gira dentro Safari (non installata), il tap su
      // "Attiva" fallirebbe silenziosamente: spieghiamo il passo reale.
      testoEl.textContent = 'Per attivare le notifiche, installa Brigade sulla schermata Home (Condividi → Aggiungi a Home) e riapri l\'app da lì';
      btnEl.style.display = 'none';
    } else {
      testoEl.textContent = 'Attiva le notifiche per ricevere avvisi sui messaggi e la raccolta rifiuti';
      btnEl.style.display = '';
    }
  });
}

async function attivaNotificheBanner() {
  if (!('Notification' in window)) return;
  if (Notification.permission === 'denied') {
    toast('Notifiche bloccate dal browser — abilitale dalle impostazioni del dispositivo.', 'error');
    return;
  }
  if (_isIOS() && !_isStandalonePwa()) {
    toast('Installa prima Brigade sulla schermata Home (Condividi → Aggiungi a Home), poi riapri l\'app da lì.', 'error');
    return;
  }
  const perm = await Notification.requestPermission();
  console.log('[Push] permesso richiesto dal banner:', perm);
  if (perm === 'granted') await ensurePushSubscription();
  localStorage.setItem('pushAsked', '1');
  renderNotifBanner();
}

function dismissNotifBanner() {
  localStorage.setItem('notifBannerDismissed', '1');
  renderNotifBanner();
}

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') { closeLotDetail(); closeModificaLotto(); closeDrawer(); closeApparecchioModal(); closeRegistraTempModal(); closeNuovaPreparazione(); closePrepEtichetta(); closeScaricoModal(); closeNuovaPrenotazione(); closePrenDetail(); closeTavoloForm(); closeTavoloSheet(); closeFormePredefinite(); closeChatAzioni(); closeChatPhotoViewer(); }
});

// ── VISUAL VIEWPORT (tastiera mobile) ───────────────────────────────────
// Espone l'altezza realmente visibile (esclusa la tastiera su iOS/Android)
// come --app-vvh, usata dal layout fisso della chat per tenere la barra di
// input sempre sopra la tastiera invece che nascosta sotto di essa.
function _updateAppVvh() {
  const vv      = window.visualViewport;
  const layoutH = window.innerHeight;          // stabile: ignora la tastiera
  const visualH = vv ? vv.height : layoutH;     // si riduce anche per la tastiera
  // Sotto questa soglia la differenza è dovuta alla barra degli indirizzi del
  // browser che si nasconde/mostra durante lo scroll, non alla tastiera:
  // ignorarla evita che il layout della chat "salti" (e la barra di input
  // sembri muoversi) mentre si scorrono i messaggi. Una tastiera reale
  // riduce l'altezza molto di più.
  const shrink = layoutH - visualH;
  const h = shrink > 150 ? visualH : layoutH;
  document.documentElement.style.setProperty('--app-vvh', h + 'px');
}
_updateAppVvh();
// Deliberatamente NESSUN listener su 'scroll' del visualViewport: scorrere i
// messaggi genera comunque eventi scroll che non c'entrano con la tastiera
// e causerebbero lo stesso "salto" che questo fix vuole eliminare.
if (window.visualViewport) {
  window.visualViewport.addEventListener('resize', _updateAppVvh);
}
window.addEventListener('resize', _updateAppVvh);
window.addEventListener('load', _updateAppVvh);

// ── BOOT ─────────────────────────────────────────────────────────────────
init();

// Se l'app è stata aperta dal tap su una notifica push (?goto=pagina),
// naviga direttamente lì invece della home.
(function () {
  const goto = new URLSearchParams(location.search).get('goto');
  if (goto && document.getElementById(`page-${goto}`)) {
    history.replaceState(null, '', location.pathname);
    navTo(goto);
  }
})();

if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/static/sw.js', { scope: '/' })
      .then(() => { if (CURRENT_USER_ID) initPushNotifications(); })
      .catch(e => console.warn('[SW]', e));
  });
  navigator.serviceWorker.addEventListener('message', e => {
    if (e.data && e.data.type === 'push-click' && e.data.url) {
      const goto = new URLSearchParams(e.data.url.split('?')[1] || '').get('goto');
      if (goto && document.getElementById(`page-${goto}`)) navTo(goto);
    }
  });
}
