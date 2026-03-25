const CACHE_NAME = 'magazzino-v1';

// Asset statici da mettere in cache al momento dell'installazione
const STATIC_ASSETS = [
  '/',
  '/static/manifest.json',
  '/static/icon.svg',
  'https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&family=Outfit:wght@300;400;500;600&display=swap'
];

// ── INSTALL: pre-cacha gli asset statici ──────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      // Cacha la pagina principale e il manifest; ignora errori sui font (potrebbero essere CORS)
      return Promise.allSettled(
        STATIC_ASSETS.map(url => cache.add(url).catch(() => null))
      );
    })
  );
  self.skipWaiting();
});

// ── ACTIVATE: rimuovi cache vecchie ──────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

// ── FETCH: strategia per tipo di risorsa ─────────────────────────────────
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // API calls → Network first, fallback a risposta offline
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(networkFirstAPI(event.request));
    return;
  }

  // Navigazione (pagina HTML) → Network first, fallback a cache
  if (event.request.mode === 'navigate') {
    event.respondWith(networkFirstPage(event.request));
    return;
  }

  // Asset statici e font → Cache first, poi rete
  event.respondWith(cacheFirst(event.request));
});

// Network first con fallback cache → per le API
async function networkFirstAPI(request) {
  try {
    const response = await fetch(request);
    // Metti in cache solo le GET riuscite
    if (request.method === 'GET' && response.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    // Offline: prova dalla cache
    const cached = await caches.match(request);
    if (cached) return cached;
    // Nessuna cache: rispondi con JSON di errore offline
    return new Response(
      JSON.stringify({ success: false, error: 'Offline: nessuna connessione disponibile' }),
      { status: 503, headers: { 'Content-Type': 'application/json' } }
    );
  }
}

// Network first con fallback cache → per la pagina principale
async function networkFirstPage(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request) || await caches.match('/');
    if (cached) return cached;
    return new Response('<h1>Offline</h1><p>Connettiti per usare il Magazzino.</p>', {
      headers: { 'Content-Type': 'text/html' }
    });
  }
}

// Cache first con fallback rete → per asset statici
async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('', { status: 404 });
  }
}
