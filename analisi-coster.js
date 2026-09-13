/**
 * analisi-coster.js — Modulo "Analisi impianti" per il proxy Render.
 *
 * Porta nel gestionale la dashboard del motore di analisi che oggi gira in
 * locale (dashboard.py su analytics.db):
 *
 *   browser ──(PIN)──▶ /analisi/*         fotografia dei dati, registrazioni
 *   agente  ──(token)─▶ /analisi/agent/*  invia la fotografia, prende le scritture
 *
 * Il server non calcola niente e non conosce il database: tiene in memoria
 * l'ultima fotografia compressa che l'agente gli ha mandato (poche centinaia
 * di kB) e la restituisce alla pagina così com'è. Se Render si riavvia, la
 * fotografia si perde e l'agente la rimanda al primo contatto.
 *
 * Le registrazioni fatte dal sito (diario interventi e letture dei contatori)
 * non vengono scritte qui: si mettono in coda, il PC le scrive nei CSV del
 * progetto e rimanda la fotografia aggiornata.
 *
 * Variabili d'ambiente su Render:
 *   ORARI_AGENT_TOKEN     lo stesso segreto del modulo orari (l'agente è lo stesso)
 *   ORARI_PIN             PIN della pagina; ANALISI_PIN se se ne vuole uno diverso
 *   ORARI_ALLOWED_ORIGIN  opzionale, origini ammesse
 *
 * Aggancio in server.js, accanto all'altro modulo: n
 *   require('./analisi-coster')(app);
 */

'use strict';

const express = require('express');
const crypto = require('crypto');

const AGENT_ONLINE_MS = 40 * 1000;
const SNAPSHOT_MAX_BYTES = 25 * 1024 * 1024;   // fotografia compressa
const OP_EXPIRE_MS = 15 * 60 * 1000;           // scrittura mai presa dal PC
const OP_KEEP_MS = 30 * 60 * 1000;             // quanto restano gli esiti
const MAX_OPS = 30;

const PIN_MAX_FAILURES = 8;
const PIN_WINDOW_MS = 15 * 60 * 1000;

const TIPI_SCRITTURA = new Set(['diario', 'contatori']);

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

function testo(v, max) {
  return String(v === null || v === undefined ? '' : v).trim().slice(0, max);
}

/** Data in formato gg/mm/aaaa. Ritorna la stringa pulita oppure ''. */
function dataIt(v) {
  const s = testo(v, 10);
  if (!s) return '';
  const m = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(s);
  if (!m) return '';
  const [, gg, mm, aaaa] = m;
  const d = new Date(Number(aaaa), Number(mm) - 1, Number(gg));
  if (d.getFullYear() !== Number(aaaa) || d.getMonth() !== Number(mm) - 1 || d.getDate() !== Number(gg)) return '';
  return s;
}

/** Controlli minimi lato server: il PC rivalida comunque tutto prima di scrivere. */
function validaScrittura(tipo, dati) {
  const d = dati && typeof dati === 'object' ? dati : {};
  const codice_k = testo(d.codice_k, 40);
  if (!codice_k) throw new Error('Impianto mancante.');

  if (tipo === 'diario') {
    const data = dataIt(d.data);
    if (!data) throw new Error('Data non valida (gg/mm/aaaa).');
    const intervento = testo(d.intervento, 300);
    if (!intervento) throw new Error('Descrivi che cosa è stato fatto.');
    return {
      codice_k,
      data,
      ambito: testo(d.ambito, 80) || 'impianto',
      finding_code: testo(d.finding_code, 60),
      intervento,
      eseguito_da: testo(d.eseguito_da, 60),
      note: testo(d.note, 300),
    };
  }

  const data = dataIt(d.data);
  if (!data) throw new Error('Data della lettura non valida (gg/mm/aaaa).');
  const accensioni = Number(d.accensioni);
  const ore = Number(d.ore_funzionamento);
  if (!Number.isFinite(accensioni) || accensioni < 0 || accensioni > 10000000) {
    throw new Error('Accensioni non valide.');
  }
  if (!Number.isFinite(ore) || ore < 0 || ore > 1000000) {
    throw new Error('Ore di funzionamento non valide.');
  }
  return {
    codice_k,
    data,
    accensioni: Math.round(accensioni),
    ore_funzionamento: Math.round(ore * 10) / 10,
    riferimento: dataIt(d.riferimento),
    note: testo(d.note, 300),
  };
}

module.exports = function mountAnalisiCoster(app) {
  const AGENT_TOKEN = process.env.ORARI_AGENT_TOKEN || '';
  const PIN = process.env.ANALISI_PIN || process.env.ORARI_PIN || '';
  const ALLOWED = (process.env.ORARI_ALLOWED_ORIGIN || '*')
    .split(',').map((s) => s.trim()).filter(Boolean);

  const enabled = Boolean(AGENT_TOKEN && PIN);
  if (!enabled) {
    console.warn('[analisi] ORARI_AGENT_TOKEN o PIN non impostati: modulo disattivato.');
  }

  const state = {
    agent: { lastSeen: 0, version: null, host: null },
    snapshot: null,         // Buffer gzip
    impronta: null,
    generatoIl: null,
    ricevutoAlle: null,
    ops: new Map(),         // op_id → { op_id, tipo, dati, stato, messaggio, creato, aggiornato }
    pinFailures: new Map(),
  };

  function opPublic(op) {
    return {
      op_id: op.op_id, tipo: op.tipo, stato: op.stato,
      messaggio: op.messaggio || '', creato: op.creato, aggiornato: op.aggiornato,
    };
  }

  function sweep() {
    const now = Date.now();
    for (const op of state.ops.values()) {
      if (op.stato === 'in_coda' && now - op.creato > OP_EXPIRE_MS) {
        op.stato = 'errore';
        op.messaggio = 'Il PC non ha preso in carico la registrazione entro 15 minuti.';
        op.aggiornato = now;
      }
      if (['fatta', 'errore'].includes(op.stato) && now - op.aggiornato > OP_KEEP_MS) {
        state.ops.delete(op.op_id);
      }
    }
    for (const [ip, f] of state.pinFailures) {
      if (now - f.first > PIN_WINDOW_MS) state.pinFailures.delete(ip);
    }
  }

  const timer = setInterval(sweep, 60 * 1000);
  if (timer.unref) timer.unref();

  const router = express.Router();

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
    if (!enabled) return res.status(503).json({ error: 'Modulo analisi non configurato sul server.' });
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

  /** Legge il corpo della richiesta così com'è, senza interpretarlo. */
  function corpoGrezzo(req, res, next) {
    if (Buffer.isBuffer(req.body)) return next();
    const pezzi = [];
    let totale = 0;
    let chiuso = false;
    const fine = (codice, messaggio) => {
      if (chiuso) return;
      chiuso = true;
      res.status(codice).json({ error: messaggio });
      req.destroy();
    };
    req.on('data', (c) => {
      totale += c.length;
      if (totale > SNAPSHOT_MAX_BYTES) return fine(413, 'Fotografia troppo grande.');
      pezzi.push(c);
    });
    req.on('end', () => {
      if (chiuso) return;
      req.body = Buffer.concat(pezzi);
      next();
    });
    req.on('error', () => fine(400, 'Trasferimento interrotto.'));
  }

  // La fotografia arriva già compressa: niente JSON, si salva il blocco così com'è.
  agent.post('/fotografia', corpoGrezzo,
    (req, res) => {
      const corpo = req.body;
      if (!Buffer.isBuffer(corpo) || corpo.length === 0) {
        return res.status(400).json({
          error: 'Fotografia vuota. Se in server.js c\'è un express.json() globale, '
               + 'il modulo analisi va agganciato prima di quello.',
        });
      }
      state.snapshot = corpo;
      state.impronta = testo(req.headers['x-impronta'], 64) || crypto.createHash('sha1').update(corpo).digest('hex');
      state.generatoIl = testo(req.headers['x-generato-il'], 32);
      state.ricevutoAlle = Date.now();
      console.log(`[analisi] fotografia ricevuta (${Math.round(corpo.length / 1024)} kB, impronta ${state.impronta.slice(0, 8)})`);
      res.json({ ok: true });
    });

  agent.use(express.json({ limit: '1mb' }));

  agent.post('/poll', (req, res) => {
    const body = req.body || {};
    state.agent.version = testo(body.version, 20) || state.agent.version;
    state.agent.host = testo(body.host, 60) || state.agent.host;

    // Esiti delle scritture applicate dal PC
    for (const ack of Array.isArray(body.ack) ? body.ack.slice(0, MAX_OPS) : []) {
      const op = state.ops.get(testo(ack && ack.op_id, 40));
      if (!op) continue;
      op.stato = ack.esito === 'ok' ? 'fatta' : 'errore';
      op.messaggio = testo(ack.messaggio, 300);
      op.aggiornato = Date.now();
    }

    const impronta = testo(body.impronta, 64);
    const serve = !state.snapshot || !impronta || impronta !== state.impronta;

    const ops = [];
    for (const op of state.ops.values()) {
      if (op.stato !== 'in_coda') continue;
      op.stato = 'presa';
      op.aggiornato = Date.now();
      ops.push({ op_id: op.op_id, tipo: op.tipo, dati: op.dati });
      if (ops.length >= 10) break;
    }

    res.json({ serve_fotografia: serve, ops });
  });

  router.use('/agent', agent);

  // ---------------- PAGINA WEB ----------------

  const web = express.Router();
  web.use(express.json({ limit: '1mb' }));

  web.use((req, res, next) => {
    const ip = clientIp(req);
    const f = state.pinFailures.get(ip);
    if (f && f.count >= PIN_MAX_FAILURES && Date.now() - f.first < PIN_WINDOW_MS) {
      return res.status(429).json({ error: 'Troppi tentativi con PIN sbagliato. Riprova tra qualche minuto.' });
    }
    if (!safeEqual(String(req.headers['x-orari-pin'] || ''), PIN)) {
      const prev = f && Date.now() - f.first < PIN_WINDOW_MS ? f : { count: 0, first: Date.now() };
      prev.count += 1;
      state.pinFailures.set(ip, prev);
      return res.status(401).json({ error: 'PIN non valido.' });
    }
    state.pinFailures.delete(ip);
    next();
  });

  web.get('/stato', (req, res) => {
    res.json({
      pronto: Boolean(state.snapshot),
      impronta: state.impronta,
      generato_il: state.generatoIl,
      ricevuto_alle: state.ricevutoAlle,
      dimensione_kb: state.snapshot ? Math.round(state.snapshot.length / 1024) : 0,
      agente_online: Date.now() - state.agent.lastSeen < AGENT_ONLINE_MS,
      ultimo_contatto: state.agent.lastSeen || null,
      versione_agente: state.agent.version,
      host_agente: state.agent.host,
      scritture: [...state.ops.values()].map(opPublic),
    });
  });

  web.get('/dati', (req, res) => {
    if (!state.snapshot) {
      return res.status(503).json({
        error: 'Fotografia non ancora disponibile: il PC la manda appena è collegato.',
      });
    }
    // Il blocco è già compresso: lo si dichiara e il browser lo apre da solo.
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Content-Encoding', 'gzip');
    res.setHeader('ETag', `"${state.impronta}"`);
    if (String(req.headers['if-none-match'] || '').includes(state.impronta)) {
      return res.status(304).end();
    }
    res.end(state.snapshot);
  });

  web.post('/scritture', (req, res) => {
    const tipo = testo(req.body && req.body.tipo, 20);
    if (!TIPI_SCRITTURA.has(tipo)) return res.status(400).json({ error: 'Tipo di registrazione non valido.' });

    const attive = [...state.ops.values()].filter((o) => ['in_coda', 'presa'].includes(o.stato));
    if (attive.length >= MAX_OPS) {
      return res.status(429).json({ error: 'Troppe registrazioni in attesa del PC.' });
    }

    let dati;
    try {
      dati = validaScrittura(tipo, req.body && req.body.dati);
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    const now = Date.now();
    const op = {
      op_id: `an${newId()}`, tipo, dati,
      stato: 'in_coda',
      messaggio: Date.now() - state.agent.lastSeen < AGENT_ONLINE_MS
        ? 'In coda: il PC la scrive tra pochi secondi.'
        : 'In coda: verrà scritta appena il PC sarà collegato.',
      creato: now, aggiornato: now,
    };
    state.ops.set(op.op_id, op);
    console.log(`[analisi] registrazione ${tipo} in coda per ${dati.codice_k}`);
    res.status(201).json(opPublic(op));
  });

  web.get('/scritture/:id', (req, res) => {
    const op = state.ops.get(req.params.id);
    if (!op) return res.status(404).json({ error: 'Registrazione non trovata (il server potrebbe essersi riavviato).' });
    res.json(opPublic(op));
  });

  router.use('/', web);

  app.use('/analisi', router);
  console.log(`[analisi] modulo Analisi impianti ${enabled ? 'attivo' : 'DISATTIVATO'} su /analisi`);
};
