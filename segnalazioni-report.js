/**
 * segnalazioni-report.js — report dell'agente segnalazioni dentro il gestionale (12/11/2026).
 *
 * Il PC dell'ufficio (scripts\segnalazioni_freddo.py) analizza le segnalazioni di freddo, orari corti e blocco
 * caldaia e manda qui, per ogni intervento, un report ridotto con la foto del sinottico di Coster:
 *
 *   PC      ──(token)─▶ POST /segnalazioni/agent/report     salva il report di un intervento
 *   PC      ──(token)─▶ GET  /segnalazioni/agent/indice     cosa c'e' gia' (per rimandare dopo un riavvio)
 *   pagina  ──────────▶ GET  /segnalazioni/indice           {id intervento: verdetto breve} per il cartellino
 *   pagina  ──────────▶ GET  /segnalazioni/report/:id       il report (senza immagine)
 *   pagina  ──────────▶ GET  /segnalazioni/immagine/:id/:n  la foto (PNG)
 *
 * Il server non calcola niente e non scrive nel foglio: tiene i report in memoria. Se Render si riavvia si
 * perdono e il PC li rimanda al giro dopo (ogni 10 minuti). Le letture sono libere come /dati-responsabile.
 *
 * Variabili d'ambiente: ORARI_AGENT_TOKEN (lo stesso dell'agente orari), ORARI_ALLOWED_ORIGIN (facoltativa).
 * Aggancio in server.js accanto agli altri moduli:  require('./segnalazioni-report')(app);
 */

'use strict';

const express = require('express');
const crypto = require('crypto');

const MAX_REPORT = 300;                    // oltre, si buttano i piu' vecchi
const MAX_IMMAGINI = 2;
const MAX_IMMAGINE_BYTES = 1400 * 1024;    // per immagine, in base64
const MAX_GIORNI = 21;

function safeEqual(a, b) {
  const ha = crypto.createHash('sha256').update(String(a || '')).digest();
  const hb = crypto.createHash('sha256').update(String(b || '')).digest();
  return crypto.timingSafeEqual(ha, hb);
}

function testo(v, max) {
  return String(v === null || v === undefined ? '' : v).trim().slice(0, max);
}

const VERDETTI = new Set(['fondata', 'non_fondata', 'dati_insufficienti']);

function tabella(t) {
  // {testa: [..], righe: [[{t, rosso}]]}: solo testo, lo disegna la pagina
  if (!t || !Array.isArray(t.righe)) return null;
  return {
    titolo: testo(t.titolo, 40),
    testa: (Array.isArray(t.testa) ? t.testa : []).slice(0, 6).map((x) => testo(x, 30)),
    righe: t.righe.slice(0, 10).map((r) => (Array.isArray(r) ? r : []).slice(0, 6).map((c) => ({
      t: testo(c && typeof c === 'object' ? c.t : c, 140),
      rosso: Boolean(c && typeof c === 'object' && c.rosso),
    }))),
  };
}

module.exports = function segnalazioniReport(app) {
  const AGENT_TOKEN = process.env.ORARI_AGENT_TOKEN || '';
  if (!AGENT_TOKEN) {
    console.warn('[segnalazioni] ORARI_AGENT_TOKEN non impostato: modulo disattivato.');
    return;
  }
  const origini = String(process.env.ORARI_ALLOWED_ORIGIN || '').split(',').map((x) => x.trim()).filter(Boolean);
  const report = new Map();                // id intervento -> report

  const router = express.Router();

  router.use((req, res, next) => {
    const origin = req.headers.origin;
    if (origin && (!origini.length || origini.includes(origin))) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
  });

  function soloAgente(req, res, next) {
    const auth = String(req.headers.authorization || '');
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    if (!safeEqual(token, AGENT_TOKEN)) return res.status(401).json({ error: 'Token agente non valido.' });
    next();
  }

  function pulisci() {
    const limite = Date.now() - MAX_GIORNI * 86400 * 1000;
    for (const [id, r] of report) if (r.ricevuto < limite) report.delete(id);
    if (report.size > MAX_REPORT) {
      const vecchi = [...report.entries()].sort((a, b) => a[1].ricevuto - b[1].ricevuto).slice(0, report.size - MAX_REPORT);
      for (const [id] of vecchi) report.delete(id);
    }
  }

  const corpo = express.json({ limit: '6mb' });

  router.post('/agent/report', soloAgente, corpo, (req, res) => {
    const b = req.body || {};
    const id = testo(b.id, 40);
    if (!/^[A-Za-z0-9_-]{4,40}$/.test(id)) return res.status(400).json({ error: 'id intervento non valido' });
    // 13/11/2026: il PC non manda piu' il verdetto (solo orari e foto); se c'e' deve essere uno di quelli noti
    if (b.verdetto && !VERDETTI.has(b.verdetto)) return res.status(400).json({ error: 'verdetto non valido' });
    const immagini = [];
    for (const im of (Array.isArray(b.immagini) ? b.immagini : []).slice(0, MAX_IMMAGINI)) {
      const dati = String((im && im.base64) || '');
      if (!dati || dati.length > MAX_IMMAGINE_BYTES || !/^[A-Za-z0-9+/=]+$/.test(dati)) continue;
      const buf = Buffer.from(dati, 'base64');
      if (buf.length < 8 || buf.readUInt32BE(0) !== 0x89504e47) continue;      // solo PNG
      immagini.push({ titolo: testo(im.titolo, 60), buf });
    }
    report.set(id, {
      id,
      impronta: testo(b.impronta, 40),
      ricevuto: Date.now(),
      dati_del: testo(b.dati_del, 20),
      prova: Boolean(b.prova),
      riferimento: testo(b.riferimento, 60),
      impianto: testo(b.impianto, 120),
      giorno: testo(b.giorno, 30),
      verdetto: b.verdetto || '',
      certezza: testo(b.certezza, 10),
      causa: testo(b.causa, 80),
      sintesi: testo(b.sintesi, 300),
      primo_controllo: testo(b.primo_controllo, 300),
      tabelle: (Array.isArray(b.tabelle) ? b.tabelle : []).slice(0, 4).map(tabella).filter(Boolean),
      immagini,
    });
    pulisci();
    res.json({ ok: true, immagini: immagini.length, totale: report.size });
  });

  router.get('/agent/indice', soloAgente, (req, res) => {
    pulisci();
    const out = {};
    for (const [id, r] of report) out[id] = r.impronta;
    res.json({ report: out });
  });

  router.get('/indice', (req, res) => {
    pulisci();
    const out = {};
    for (const [id, r] of report) out[id] = { v: r.verdetto, c: r.causa, d: r.dati_del };
    res.setHeader('Cache-Control', 'no-store');
    res.json({ report: out });
  });

  router.get('/report/:id', (req, res) => {
    const r = report.get(String(req.params.id || ''));
    if (!r) return res.status(404).json({ error: 'nessun report per questo intervento' });
    const { immagini, ricevuto, impronta, ...resto } = r;
    res.setHeader('Cache-Control', 'no-store');
    res.json({ ...resto, immagini: immagini.map((im, n) => ({ titolo: im.titolo, n: n + 1 })) });
  });

  router.get('/immagine/:id/:n', (req, res) => {
    const r = report.get(String(req.params.id || ''));
    const im = r && r.immagini[Number(req.params.n) - 1];
    if (!im) return res.sendStatus(404);
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.send(im.buf);
  });

  app.use('/segnalazioni', router);
  console.log('[segnalazioni] modulo attivo.');
};
