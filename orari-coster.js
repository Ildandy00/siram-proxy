/**
 * orari-coster.js — Modulo "Orari Coster" per il proxy Render.
 *
 * Fa da ponte tra la pagina del gestionale e l'agente Python sul PC:
 *
 *   browser ──(PIN)──▶ /orari/*            coda lavori, catalogo, conferme
 *   agente  ──(token)─▶ /orari/agent/*     prende i lavori, invia stato ed esito
 *
 * Il server NON parla mai con Coster e NON conosce credenziali, conn_ref o
 * registri: conserva solo gli ID degli schedule e lo stato dei lavori.
 * Tutto è in memoria: se Render si riavvia la coda si svuota e l'agente
 * reinvia l'anagrafica al primo contatto.
 *
 * Variabili d'ambiente su Render:
 *   ORARI_AGENT_TOKEN     segreto condiviso con l'agente (stringa lunga casuale)
 *   ORARI_PIN             codice richiesto dalla pagina web (meglio 8+ caratteri)
 *   ORARI_ALLOWED_ORIGIN  opzionale, es. https://siram-manutenzione.davide-cori93.workers.dev
 *                         (più origini separate da virgola; default: tutte)
 *
 * Aggancio in server.js (una riga, dopo la creazione di `app`):
 *   require('./orari-coster')(app);
 */

'use strict';

const express = require('express');
const crypto = require('crypto');

const AGENT_ONLINE_MS = 25 * 1000;          // agente considerato collegato
const QUEUE_EXPIRE_MS = 30 * 60 * 1000;     // lavoro mai preso → scaduto
const CONFIRM_EXPIRE_MS = 12 * 60 * 1000;   // l'agente rinuncia già a 10 min
const STALE_JOB_MS = 20 * 60 * 1000;        // lavoro in corso senza notizie
const KEEP_FINAL_JOBS = 100;
const KEEP_FINAL_MS = 7 * 24 * 3600 * 1000;
const MAX_ACTIVE_JOBS = 10;

const PIN_MAX_FAILURES = 8;
const PIN_WINDOW_MS = 15 * 60 * 1000;

// Righe per famiglia di programma orario: "passi" = 8, "termostato" = 15.
// Il numero vero arriva dall'anagrafica insieme allo schedule.
const MAX_ROWS = 8;
const MAX_ROWS_ANY = 15;
// Unico controllo sulla temperatura: valore scrivibile nel registro Coster.
// Nessun limite per tipo di schedule.
const HARD_T_MIN = -25;
const HARD_T_MAX = 100;

const FINAL_STATES = new Set([
  'completato', 'nessuna_modifica', 'annullato', 'errore', 'scaduto',
]);
const AGENT_STATES = new Set([
  'connessione', 'lettura', 'attesa_conferma', 'scrittura', 'verifica',
  'rollback', 'completato', 'nessuna_modifica', 'annullato', 'errore',
]);
const CANCELLABLE_STATES = new Set([
  'in_coda', 'preso', 'connessione', 'lettura', 'attesa_conferma',
]);

// ------------------------------------------------------------
// Utility
// ------------------------------------------------------------

function safeEqual(a, b) {
  const ha = crypto.createHash('sha256').update(String(a || '')).digest();
  const hb = crypto.createHash('sha256').update(String(b || '')).digest();
  return crypto.timingSafeEqual(ha, hb);
}

function clientIp(req) {
  const fwd = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return fwd || req.socket.remoteAddress || '?';
}

function newId() {
  return crypto.randomBytes(8).toString('hex');
}

function isIntIn(v, lo, hi) {
  return Number.isInteger(v) && v >= lo && v <= hi;
}

function optionalInt(v, lo, hi) {
  if (v === null || v === undefined || v === '') return { ok: true, value: null };
  const n = Number(v);
  if (!isIntIn(n, lo, hi)) return { ok: false };
  return { ok: true, value: n };
}

/** Stesse regole dell'agente (che comunque rivalida tutto). */
function validateChanges(raw, righe = MAX_ROWS, tMin = HARD_T_MIN, tMax = HARD_T_MAX) {
  const nRighe = Number.isInteger(righe) && righe > 0 && righe <= MAX_ROWS_ANY ? righe : MAX_ROWS;
  if (!Array.isArray(raw) || raw.length === 0) throw new Error('Nessuna modifica impostata.');
  if (raw.length > nRighe) throw new Error('Troppe righe.');

  const seen = new Set();
  return raw.map((item) => {
    if (!item || typeof item !== 'object') throw new Error('Formato modifiche non valido.');
    const row = Number(item.row);
    if (!isIntIn(row, 0, nRighe - 1)) throw new Error('Riga non valida.');
    if (seen.has(row)) throw new Error(`Riga ${row + 1} ripetuta.`);
    seen.add(row);

    if (item.action === 'disable') return { row, action: 'disable' };
    if (item.action !== 'modify') throw new Error(`Riga ${row + 1}: azione non valida.`);

    const days = optionalInt(item.days, 1, 127);
    const start = optionalInt(item.start, 0, 1439);
    const end = optionalInt(item.end, 0, 1439);
    if (!days.ok) throw new Error(`Riga ${row + 1}: giorni non validi.`);
    if (!start.ok) throw new Error(`Riga ${row + 1}: ora inizio non valida.`);
    if (!end.ok) throw new Error(`Riga ${row + 1}: ora fine non valida.`);

    let temp = null;
    if (item.temp !== null && item.temp !== undefined && item.temp !== '') {
      temp = Number(String(item.temp).replace(',', '.'));
      if (!Number.isFinite(temp) || temp < tMin || temp > tMax) {
        throw new Error(`Riga ${row + 1}: temperatura non scrivibile nel registro Coster (${tMin}…${tMax} °C).`);
      }
      temp = Math.round(temp * 10) / 10;
    }

    if (start.value !== null && end.value !== null && end.value <= start.value) {
      throw new Error(`Riga ${row + 1}: l'ora finale deve essere successiva a quella iniziale.`);
    }
    if (days.value === null && start.value === null && end.value === null && temp === null) {
      throw new Error(`Riga ${row + 1}: 'Modifica' senza alcun valore.`);
    }

    return { row, action: 'modify', days: days.value, start: start.value, end: end.value, temp };
  });
}

// ------------------------------------------------------------
// Modulo
// ------------------------------------------------------------

module.exports = function mountOrariCoster(app) {
  const AGENT_TOKEN = process.env.ORARI_AGENT_TOKEN || '';
  const PIN = process.env.ORARI_PIN || '';
  const ALLOWED = (process.env.ORARI_ALLOWED_ORIGIN || '*')
    .split(',').map((s) => s.trim()).filter(Boolean);

  const enabled = Boolean(AGENT_TOKEN && PIN);
  if (!enabled) {
    console.warn('[orari] ORARI_AGENT_TOKEN o ORARI_PIN non impostati: modulo disattivato.');
  }

  const state = {
    agent: { lastSeen: 0, version: null, host: null },
    catalog: null,
    catalogHash: null,
    catalogTime: null,
    jobs: new Map(),        // id → job
    pinFailures: new Map(), // ip → { count, first }
    // Straordinari: la verità sta sul PC (SQLite locale dell'agente).
    // Qui: ultima fotografia inviata dall'agente + richieste non ancora confermate.
    straordinari: [],
    straordinariHash: null,
    straordinariTime: null,
    straOps: new Map(),     // op_id → { op_id, tipo:'crea'|'annulla', ... , creato }
    // Righe viste davvero su Coster nell'ultima lettura (schedule_id → n).
    // Alcune centraline espongono più righe di quelle previste dalla famiglia.
    righeViste: new Map(),
  };

  // ----------------------------------------------------------
  // Helpers sui lavori
  // ----------------------------------------------------------

  function setState(job, stato, messaggio) {
    const now = Date.now();
    if (job.stato !== stato) {
      job.storia.push({ t: now, stato, messaggio: messaggio || '' });
      if (job.storia.length > 60) job.storia.splice(1, job.storia.length - 60);
    }
    job.stato = stato;
    if (messaggio !== undefined) job.messaggio = messaggio;
    job.aggiornato = now;
    if (FINAL_STATES.has(stato) && !job.concluso) job.concluso = now;
  }

  function jobPublic(job, full = true) {
    const out = {
      id: job.id,
      tipo: job.tipo,
      schedule_id: job.schedule_id,
      etichetta: job.etichetta,
      stato: job.stato,
      messaggio: job.messaggio,
      creato: job.creato,
      aggiornato: job.aggiornato,
      concluso: job.concluso || null,
      annulla_richiesto: job.annulla_richiesto,
      finale: FINAL_STATES.has(job.stato),
    };
    if (full) {
      out.modifiche = job.modifiche;
      out.dati = job.dati;
      out.storia = job.storia;
    }
    return out;
  }

  /** L'agente riporta quante righe ha davvero letto: si tiene da conto. */
  function ricordaRighe(job, dati) {
    const n = Number(dati && dati.righe);
    if (!job || !Number.isInteger(n) || n < 1 || n > MAX_ROWS_ANY) return;
    state.righeViste.set(String(job.schedule_id), n);
  }

  function findSchedule(scheduleId) {
    const cat = state.catalog;
    if (!cat) return null;
    return cat.schedules.find((s) => String(s.id) === String(scheduleId)) || null;
  }

  function righeDi(scheduleId) {
    const sch = findSchedule(scheduleId);
    const n = Number(sch && sch.righe);
    const base = Number.isInteger(n) && n > 0 && n <= MAX_ROWS_ANY ? n : MAX_ROWS;
    const viste = Number(state.righeViste.get(String(scheduleId)));
    return Number.isInteger(viste) && viste > base ? Math.min(viste, MAX_ROWS_ANY) : base;
  }

  function labelFor(scheduleId) {
    const cat = state.catalog;
    if (!cat) return null;
    const sch = findSchedule(scheduleId);
    if (!sch) return null;
    const imp = cat.impianti.find((i) => i.ref === sch.impianto_ref) || {};
    const com = cat.commesse.find((c) => c.ref === imp.commessa_ref) || {};
    return {
      commessa: com.nome || '',
      impianto: [imp.codice_k, imp.nome].filter(Boolean).join(' - '),
      schedule: `${sch.nome} [${sch.scheduler_id}]`,
    };
  }

  function sweep() {
    const now = Date.now();
    for (const job of state.jobs.values()) {
      if (FINAL_STATES.has(job.stato)) continue;

      if (job.stato === 'in_coda' && now - job.creato > QUEUE_EXPIRE_MS) {
        setState(job, 'scaduto', 'Agente non disponibile entro 30 minuti: richiesta non eseguita.');
      } else if (job.stato === 'attesa_conferma' && now - job.aggiornato > CONFIRM_EXPIRE_MS) {
        setState(job, 'annullato', 'Conferma non arrivata in tempo: nessuna modifica eseguita.');
      } else if (job.stato !== 'in_coda' && job.stato !== 'attesa_conferma'
                 && job.stato !== 'confermato' && now - job.aggiornato > STALE_JOB_MS) {
        setState(job, 'errore', 'Nessuna notizia dall\'agente da 20 minuti. Verificare su Coster.');
      }
    }

    const finals = [...state.jobs.values()]
      .filter((j) => FINAL_STATES.has(j.stato))
      .sort((a, b) => b.aggiornato - a.aggiornato);
    finals.forEach((job, idx) => {
      if (idx >= KEEP_FINAL_JOBS || now - job.aggiornato > KEEP_FINAL_MS) state.jobs.delete(job.id);
    });

    for (const [ip, f] of state.pinFailures) {
      if (now - f.first > PIN_WINDOW_MS) state.pinFailures.delete(ip);
    }
  }

  const timer = setInterval(sweep, 30 * 1000);
  if (timer.unref) timer.unref();

  // ----------------------------------------------------------
  // Router
  // ----------------------------------------------------------

  const router = express.Router();
  router.use(express.json({ limit: '5mb' }));

  // CORS (compatibile anche con un cors() globale già presente)
  router.use((req, res, next) => {
    const origin = req.headers.origin;
    if (ALLOWED.includes('*')) {
      res.setHeader('Access-Control-Allow-Origin', '*');
    } else if (origin && ALLOWED.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Orari-Pin, Authorization');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Cache-Control', 'no-store');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    if (!enabled) return res.status(503).json({ error: 'Modulo orari non configurato sul server.' });
    next();
  });

  // ---------------- AGENTE ----------------

  const agent = express.Router();

  agent.use((req, res, next) => {
    const auth = String(req.headers.authorization || '');
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    if (!safeEqual(token, AGENT_TOKEN)) return res.status(401).json({ error: 'Token agente non valido.' });
    state.agent.lastSeen = Date.now();
    next();
  });

  agent.post('/poll', (req, res) => {
    const body = req.body || {};
    state.agent.version = String(body.version || '').slice(0, 20);
    state.agent.host = String(body.host || '').slice(0, 60);

    // L'agente lavora un lavoro alla volta: se sta chiedendo lavoro,
    // quelli che risultano "in corso" sono rimasti orfani (agente riavviato).
    for (const job of state.jobs.values()) {
      if (!FINAL_STATES.has(job.stato) && !['in_coda', 'confermato', 'attesa_conferma'].includes(job.stato)) {
        const warn = ['scrittura', 'verifica', 'rollback'].includes(job.stato)
          ? ' La scrittura era in corso: CONTROLLARE IL PROGRAMMA SU COSTER.'
          : '';
        setState(job, 'errore', `Agente riavviato durante il lavoro.${warn}`);
      }
      if (['confermato', 'attesa_conferma'].includes(job.stato)) {
        setState(job, 'annullato', 'Agente riavviato prima della scrittura: nessuna modifica eseguita.');
      }
    }

    const needCatalog = !state.catalogHash || body.catalog_hash !== state.catalogHash;

    // Straordinari: conferme, fotografia, richieste pendenti
    for (const opId of Array.isArray(body.ack_ops) ? body.ack_ops : []) {
      state.straOps.delete(String(opId));
    }
    if (Array.isArray(body.straordinari)) {
      state.straordinari = body.straordinari.slice(0, 500);
      state.straordinariHash = String(body.straordinari_hash || '');
      state.straordinariTime = Date.now();
    }
    const needStra = !state.straordinariHash || body.straordinari_hash !== state.straordinariHash;
    const straOps = [...state.straOps.values()].sort((a, b) => a.creato - b.creato).slice(0, 20);

    const next = [...state.jobs.values()]
      .filter((j) => j.stato === 'in_coda')
      .sort((a, b) => a.creato - b.creato)[0];

    let job = null;
    if (next && !needCatalog && !straOps.length) {   // prima si registrano gli straordinari
      setState(next, 'preso', 'Presa in carico dall\'agente.');
      job = {
        id: next.id,
        tipo: next.tipo,
        schedule_id: next.schedule_id,
        modifiche: next.modifiche,
      };
    }

    res.json({
      need_catalog: needCatalog,
      job,
      need_straordinari: needStra,
      straordinari_ops: straOps,
    });
  });

  agent.post('/catalog', (req, res) => {
    const { catalog, hash } = req.body || {};
    if (!catalog || !Array.isArray(catalog.commesse) || !Array.isArray(catalog.impianti)
        || !Array.isArray(catalog.schedules)) {
      return res.status(400).json({ error: 'Catalogo non valido.' });
    }
    state.catalog = catalog;
    state.catalogHash = String(hash || '');
    state.catalogTime = Date.now();
    res.json({ ok: true });
  });

  agent.get('/jobs/:id', (req, res) => {
    const job = state.jobs.get(req.params.id);
    if (!job) return res.json({ stato: 'annullato', annulla_richiesto: true, mancante: true });
    res.json({ stato: job.stato, annulla_richiesto: job.annulla_richiesto });
  });

  agent.post('/jobs/:id', (req, res) => {
    const job = state.jobs.get(req.params.id);
    const { stato, messaggio, dati } = req.body || {};

    if (!AGENT_STATES.has(stato)) return res.status(400).json({ error: 'Stato non valido.' });

    if (!job) {
      // Server riavviato: se l'agente comunica un esito finale non c'è nulla da fare;
      // se è a metà (prima della scrittura) deve fermarsi.
      if (stato === 'scrittura') return res.status(409).json({ error: 'Lavoro sconosciuto.' });
      return res.json({ stato: 'annullato', annulla_richiesto: true, mancante: true });
    }

    const msg = String(messaggio || '').slice(0, 4000);
    ricordaRighe(job, dati);

    if (FINAL_STATES.has(stato)) {
      setState(job, stato, msg);
      if (dati !== undefined) job.dati = dati;
      return res.json({ stato: job.stato, annulla_richiesto: job.annulla_richiesto });
    }

    if (FINAL_STATES.has(job.stato)) {
      // Lavoro già chiuso lato server (scaduto/annullato): l'agente deve fermarsi
      return res.json({ stato: job.stato, annulla_richiesto: true });
    }

    if (stato === 'scrittura' && !['confermato', 'scrittura'].includes(job.stato)) {
      return res.status(409).json({ error: 'Scrittura non confermata dal sito.' });
    }
    if (['verifica', 'rollback'].includes(stato)
        && !['scrittura', 'verifica', 'rollback'].includes(job.stato)) {
      return res.status(409).json({ error: 'Transizione non valida.' });
    }

    setState(job, stato, msg);
    if (dati !== undefined) job.dati = dati;
    res.json({ stato: job.stato, annulla_richiesto: job.annulla_richiesto });
  });

  router.use('/agent', agent);

  // ---------------- PAGINA WEB ----------------

  const web = express.Router();

  web.use((req, res, next) => {
    const ip = clientIp(req);
    const now = Date.now();
    const f = state.pinFailures.get(ip);
    if (f && f.count >= PIN_MAX_FAILURES && now - f.first < PIN_WINDOW_MS) {
      return res.status(429).json({ error: 'Troppi tentativi con PIN errato. Riprova tra 15 minuti.' });
    }
    if (!safeEqual(req.headers['x-orari-pin'], PIN)) {
      if (!f || now - f.first > PIN_WINDOW_MS) state.pinFailures.set(ip, { count: 1, first: now });
      else f.count += 1;
      return res.status(401).json({ error: 'PIN non valido.' });
    }
    state.pinFailures.delete(ip);
    req.orariIp = ip;
    next();
  });

  web.get('/stato', (req, res) => {
    const now = Date.now();
    const active = [...state.jobs.values()].filter((j) => !FINAL_STATES.has(j.stato));
    res.json({
      agente_online: now - state.agent.lastSeen < AGENT_ONLINE_MS,
      ultimo_contatto: state.agent.lastSeen || null,
      versione_agente: state.agent.version,
      host_agente: state.agent.host,
      catalogo_pronto: Boolean(state.catalog),
      catalogo_aggiornato: state.catalogTime,
      lavori_attivi: active.length,
      ora_server: now,
    });
  });

  web.get('/catalogo', (req, res) => {
    if (!state.catalog) {
      return res.status(503).json({ error: 'Anagrafica non ancora ricevuta dall\'agente.' });
    }
    res.json({ catalogo: state.catalog, aggiornato: state.catalogTime });
  });

  web.get('/jobs', (req, res) => {
    const list = [...state.jobs.values()]
      .sort((a, b) => b.creato - a.creato)
      .slice(0, 30)
      .map((j) => jobPublic(j, false));
    res.json({ lavori: list });
  });

  web.get('/jobs/:id', (req, res) => {
    const job = state.jobs.get(req.params.id);
    if (!job) return res.status(404).json({ error: 'Lavoro non trovato (il server potrebbe essersi riavviato).' });
    res.json(jobPublic(job));
  });

  web.post('/jobs', (req, res) => {
    const { tipo, schedule_id: scheduleId, modifiche } = req.body || {};

    if (!['lettura', 'modifica'].includes(tipo)) return res.status(400).json({ error: 'Tipo non valido.' });
    if (!state.catalog) return res.status(503).json({ error: 'Anagrafica non ancora disponibile.' });

    const etichetta = labelFor(scheduleId);
    if (!etichetta) return res.status(400).json({ error: 'Schedule non presente in anagrafica.' });

    const active = [...state.jobs.values()].filter((j) => !FINAL_STATES.has(j.stato));
    if (active.length >= MAX_ACTIVE_JOBS) {
      return res.status(429).json({ error: 'Troppi lavori in coda. Attendi che finiscano.' });
    }
    if (tipo === 'modifica' && active.some((j) => String(j.schedule_id) === String(scheduleId) && j.tipo === 'modifica')) {
      return res.status(409).json({ error: 'C\'è già una modifica in corso su questo schedule.' });
    }

    let changes = null;
    if (tipo === 'modifica') {
      try {
        changes = validateChanges(modifiche, righeDi(scheduleId));
      } catch (err) {
        return res.status(400).json({ error: err.message });
      }
    }

    const now = Date.now();
    const job = {
      id: newId(),
      tipo,
      schedule_id: scheduleId,
      etichetta,
      modifiche: changes,
      stato: 'in_coda',
      messaggio: now - state.agent.lastSeen < AGENT_ONLINE_MS
        ? 'In coda: l\'agente la prende a breve.'
        : 'In coda: l\'agente sul PC al momento non è collegato.',
      dati: null,
      creato: now,
      aggiornato: now,
      concluso: null,
      annulla_richiesto: false,
      richiesto_da: req.orariIp,
      storia: [{ t: now, stato: 'in_coda', messaggio: '' }],
    };
    state.jobs.set(job.id, job);
    console.log(`[orari] nuovo lavoro ${job.id} ${tipo} ${etichetta.impianto} / ${etichetta.schedule}`);
    res.status(201).json(jobPublic(job));
  });

  web.post('/jobs/:id/conferma', (req, res) => {
    const job = state.jobs.get(req.params.id);
    if (!job) return res.status(404).json({ error: 'Lavoro non trovato.' });
    if (job.stato !== 'attesa_conferma' || job.annulla_richiesto) {
      return res.status(409).json({ error: `Il lavoro non è in attesa di conferma (stato: ${job.stato}).` });
    }
    job.confermato_da = req.orariIp;
    setState(job, 'confermato', 'Confermato: l\'agente procede con la scrittura.');
    console.log(`[orari] lavoro ${job.id} confermato da ${req.orariIp}`);
    res.json(jobPublic(job));
  });

  web.post('/jobs/:id/annulla', (req, res) => {
    const job = state.jobs.get(req.params.id);
    if (!job) return res.status(404).json({ error: 'Lavoro non trovato.' });
    if (!CANCELLABLE_STATES.has(job.stato)) {
      return res.status(409).json({ error: `Non annullabile nello stato "${job.stato}".` });
    }
    if (job.stato === 'in_coda' || job.stato === 'attesa_conferma') {
      setState(job, 'annullato', 'Annullato dal sito: nessuna modifica eseguita.');
    } else {
      job.annulla_richiesto = true;
      job.messaggio = 'Annullamento richiesto…';
      job.aggiornato = Date.now();
    }
    res.json(jobPublic(job));
  });


  // ---------------- STRAORDINARI ----------------
  // Il PC è l'unico che li scrive e ripristina: qui si inoltrano le richieste
  // (finché l'agente non le conferma) e si mostra la sua fotografia.

  const STRA_MAX_FASCE = 4;
  const STRA_MAX_RANGE_DAYS = 7;
  const STRA_MAX_DAYS_AHEAD = 90;
  const STRA_ACTIVE = new Set(['programmato', 'applicato']);

  const hhmm = (m) => `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;
  const fasceText = (f) => f.map((x) => `${hhmm(x.start)}-${hhmm(x.end)} ${Number(x.temp).toFixed(1)}°C`).join(', ');

  function parseIsoDate(v) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(v || ''))) return null;
    const d = new Date(`${v}T12:00:00Z`);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  const isoOf = (d) => d.toISOString().slice(0, 10);

  function validateFasce(raw) {
    if (!Array.isArray(raw) || !raw.length) throw new Error('Inserisci almeno una fascia oraria.');
    if (raw.length > STRA_MAX_FASCE) throw new Error(`Al massimo ${STRA_MAX_FASCE} fasce.`);
    const out = raw.map((f, n) => {
      const start = optionalInt(f && f.start, 0, 1439);
      const end = optionalInt(f && f.end, 0, 1439);
      if (!start.ok || !end.ok || start.value === null || end.value === null) throw new Error(`Fascia ${n + 1}: orari non validi.`);
      if (end.value <= start.value) throw new Error(`Fascia ${n + 1}: l'ora finale deve essere successiva a quella iniziale.`);
      const temp = Number(String(f.temp).replace(',', '.'));
      if (!Number.isFinite(temp) || temp < HARD_T_MIN || temp > HARD_T_MAX) {
        throw new Error(`Fascia ${n + 1}: temperatura non scrivibile (${HARD_T_MIN}…${HARD_T_MAX} °C).`);
      }
      return { start: start.value, end: end.value, temp: Math.round(temp * 10) / 10 };
    }).sort((a, b) => a.start - b.start);
    for (let i = 1; i < out.length; i++) {
      if (out[i].start < out[i - 1].end) throw new Error('Le fasce si sovrappongono.');
    }
    return out;
  }

  web.get('/straordinari', (req, res) => {
    const ops = [...state.straOps.values()];
    const cancelling = new Set(ops.filter((o) => o.tipo === 'annulla').map((o) => o.id));
    const pending = ops.filter((o) => o.tipo === 'crea').map((o) => ({
      id: o.id,
      schedule_id: o.schedule_id,
      data: o.data,
      giorno: o.giorno,
      fasce: o.fasce,
      fasce_testo: fasceText(o.fasce),
      note: o.note,
      etichetta: o.etichetta,
      stato: 'in_attesa_agente',
      messaggio: 'In attesa che il PC lo registri.',
      creato: o.creato,
    }));
    const lista = state.straordinari.map((x) => (cancelling.has(x.id) ? { ...x, annulla_in_invio: true } : x));
    res.json({
      lista: [...pending, ...lista],
      aggiornato: state.straordinariTime,
      agente_online: Date.now() - state.agent.lastSeen < AGENT_ONLINE_MS,
    });
  });

  web.post('/straordinari', (req, res) => {
    const { schedule_id: scheduleId, data_dal: dal, data_al: al, fasce, note } = req.body || {};
    if (!state.catalog) return res.status(503).json({ error: 'Anagrafica non ancora disponibile.' });
    const etichetta = labelFor(scheduleId);
    if (!etichetta) return res.status(400).json({ error: 'Schedule non presente in anagrafica.' });

    const d1 = parseIsoDate(dal);
    const d2 = al ? parseIsoDate(al) : d1;
    if (!d1 || !d2) return res.status(400).json({ error: 'Date non valide.' });
    if (d2 < d1) return res.status(400).json({ error: 'La data finale è prima di quella iniziale.' });
    const giorni = Math.round((d2 - d1) / 86400000) + 1;
    if (giorni > STRA_MAX_RANGE_DAYS) {
      return res.status(400).json({ error: `Al massimo ${STRA_MAX_RANGE_DAYS} giorni per richiesta.` });
    }
    let oggiIso;
    try { oggiIso = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Rome' }); } catch (e) { oggiIso = ''; }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(oggiIso)) oggiIso = new Date().toISOString().slice(0, 10);
    const oggi = parseIsoDate(oggiIso);
    if (d1 < oggi) return res.status(400).json({ error: 'La data è già passata.' });
    if (d2 > new Date(oggi.getTime() + STRA_MAX_DAYS_AHEAD * 86400000)) {
      return res.status(400).json({ error: `Date oltre ${STRA_MAX_DAYS_AHEAD} giorni.` });
    }

    let clean;
    try { clean = validateFasce(fasce); } catch (err) { return res.status(400).json({ error: err.message }); }

    const dates = [];
    for (let i = 0; i < giorni; i++) dates.push(isoOf(new Date(d1.getTime() + i * 86400000)));

    const busy = new Set([
      ...state.straordinari.filter((x) => String(x.schedule_id) === String(scheduleId) && STRA_ACTIVE.has(x.stato)).map((x) => x.data),
      ...[...state.straOps.values()].filter((o) => o.tipo === 'crea' && String(o.schedule_id) === String(scheduleId)).map((o) => o.data),
    ]);
    const clash = dates.filter((d) => busy.has(d));
    if (clash.length) {
      return res.status(409).json({ error: `Esiste già uno straordinario su questo schedule il ${clash.join(', ')}.` });
    }

    const GG = ['domenica', 'lunedì', 'martedì', 'mercoledì', 'giovedì', 'venerdì', 'sabato'];
    const now = Date.now();
    const created = dates.map((d, i) => {
      const op = {
        op_id: `op${newId()}`,
        tipo: 'crea',
        id: `st${newId()}`,
        schedule_id: Number.isFinite(Number(scheduleId)) ? Number(scheduleId) : scheduleId,
        data: d,
        giorno: GG[parseIsoDate(d).getUTCDay()],
        fasce: clean,
        note: String(note || '').slice(0, 300),
        etichetta,
        creato: now + i,
        richiesto_da: req.orariIp,
      };
      state.straOps.set(op.op_id, op);
      return op.id;
    });
    console.log(`[orari] straordinari richiesti ${dates.join(',')} ${etichetta.impianto} / ${etichetta.schedule}: ${fasceText(clean)}`);
    res.status(201).json({ ok: true, ids: created });
  });

  web.post('/straordinari/:id/annulla', (req, res) => {
    const id = String(req.params.id);
    for (const [opId, op] of state.straOps) {
      if (op.tipo === 'crea' && op.id === id) {
        state.straOps.delete(opId);
        return res.json({ ok: true, messaggio: 'Annullato prima che il PC lo registrasse.' });
      }
    }
    const item = state.straordinari.find((x) => x.id === id);
    if (!item) return res.status(404).json({ error: 'Straordinario non trovato.' });
    if (!STRA_ACTIVE.has(item.stato) || item.annulla) {
      return res.status(409).json({ error: `Non annullabile (stato: ${item.stato}).` });
    }
    const opId = `op${newId()}`;
    state.straOps.set(opId, { op_id: opId, tipo: 'annulla', id, creato: Date.now() });
    res.json({
      ok: true,
      messaggio: item.stato === 'applicato'
        ? 'Annullamento inviato: il PC ripristinerà il programma ordinario.'
        : 'Annullamento inviato al PC.',
    });
  });

  router.use('/', web);

  app.use('/orari', router);
  console.log(`[orari] modulo Orari Coster ${enabled ? 'attivo' : 'DISATTIVATO'} su /orari`);
};
