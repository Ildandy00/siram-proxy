/**
 * PRATICHE SIRAM — pannello laterale in Gmail
 * ------------------------------------------------------------
 * Apri una mail → clic sull'icona "Pratiche Siram" → "Analizza".
 * Il pannello riconosce il documento (chiamata, richiesta offerte, offerta,
 * buono d'ordine, materiale disponibile), l'impianto e la pratica, salva i PDF
 * nella sottocartella del lavoro e aggiorna la pratica nel gestionale.
 * Nulla viene salvato senza il clic su "Conferma".
 *
 * Proprietà dello script (Impostazioni progetto → Proprietà script):
 *   PROXY_URL          https://siram-proxy.onrender.com            (default)
 *   ADDON_TOKEN        stesso valore della variabile ADDON_TOKEN su Render
 *   ANTHROPIC_API_KEY  chiave API Claude (serve per leggere le offerte)
 *   CLAUDE_MODEL       claude-haiku-5-5                            (default)
 *   CARTELLA_RADICE_ID 1WLRoEu8ZsjhVzwtZ1rItYy6L0Woi4Vve              (default: LAVORI in P2 — Gestione)
 *   CARTELLA_EXTRA_ID  1vRvjlIMGNsL75596iUenKjCsAeRTF034              (default: Lavori IN CORSO — Extra)
 *   EMAIL_SEGRETERIA   marta.fratini@veolia.com                    (default)
 *   GESTIONALE_URL     https://siram-manutenzione.davide-cori93.workers.dev/responsabile.html
 */

// ============================================================
//  CONFIG
// ============================================================
function cfg_(k) {
  const def = {
    PROXY_URL: 'https://siram-proxy.onrender.com',
    CLAUDE_MODEL: 'claude-haiku-5-5',
    CARTELLA_RADICE_ID: '1WLRoEu8ZsjhVzwtZ1rItYy6L0Woi4Vve',
    CARTELLA_EXTRA_ID: '1vRvjlIMGNsL75596iUenKjCsAeRTF034',
    EMAIL_SEGRETERIA: 'marta.fratini@veolia.com',
    GESTIONALE_URL: 'https://siram-manutenzione.davide-cori93.workers.dev/responsabile.html',
  };
  const v = PropertiesService.getScriptProperties().getProperty(k);
  return v || def[k] || '';
}

const TIPI = {
  chiamata:   '📞 Chiamata / nuovo lavoro',
  richiesta:  '✉️ Ho chiesto le offerte',
  offerta:    '🏷️ Offerta fornitore',
  bdo:        "📋 Buono d'ordine",
  materiale:  '📦 Materiale disponibile',
  archivia:   '📁 Solo archivia allegati',
};
const NUOVA = '__nuova__';
const NESSUNO = '-';

// Commesse con lavori Extra: ognuna ha la sua cartella "lavori in corso".
// La sigla va nel nome delle cartelle nuove: aaaammgg + SIGLA + _Kxxx_Impianto_Titolo
const COMMESSE_EXTRA = [
  { key: 'pesaro',    nome: 'Pesaro',           re: /pesaro/i,    cartella: '1vRvjlIMGNsL75596iUenKjCsAeRTF034', sigla: 'PS' },
  { key: 'verucchio', nome: 'Verucchio',        re: /verucchio/i, cartella: '1m5rc1lNc_bWeITSEJLf0ULJU8I5QZ3w3', sigla: 'PS' },
  { key: 'misano',    nome: 'Misano Adriatico', re: /misano/i,    cartella: '1WcSCyz9iau3kt9GgsNg3kVKQaXzB8Bbe', sigla: 'PS' },
  { key: 'gabicce',   nome: 'Gabicce Mare',     re: /gabicce/i,   cartella: '1iKV_sclqtTXVplussbVDIvc_P_3M8VpX', sigla: 'PS' },
];
function commessa_(key) { return COMMESSE_EXTRA.find(c => c.key === key) || COMMESSE_EXTRA[0]; }
// la cartella si può cambiare dalle Proprietà script (CARTELLA_EXTRA_ID per Pesaro, CARTELLA_EXTRA_VERUCCHIO, ...)
function cartellaCommessa_(key) {
  const c = commessa_(key);
  return (c.key === 'pesaro' ? cfg_('CARTELLA_EXTRA_ID') : cfg_('CARTELLA_EXTRA_' + c.key.toUpperCase())) || c.cartella;
}
function commessaDaTesto_(testo) {
  const t = String(testo || '');
  const trovata = COMMESSE_EXTRA.filter(c => c.key !== 'pesaro').find(c => c.re.test(t));
  return trovata ? trovata.key : 'pesaro';
}
function commessaDaComune_(comune) {
  const c = COMMESSE_EXTRA.find(x => x.re.test(String(comune || '')));
  return c ? c.key : 'pesaro';
}
// Impianto scelto nel pannello: id della cartella impianto (Pesaro) oppure "IMP:codice" (impianto del gestionale)
function infoImpianto_(sel, impianti) {
  if (!sel || sel === NESSUNO) return null;
  if (sel.indexOf('IMP:') === 0) {
    const cod = sel.slice(4);
    const i = (impianti || []).find(x => x.codice === cod) || { codice: cod, descrizione: '', comune: '' };
    return { codice: i.codice, k: normK_(i.codice) || i.codice, nome: i.descrizione, comune: i.comune, cartellaId: '' };
  }
  let nome = '';
  try { nome = (cartelleImpianti_().find(x => x.id === sel) || {}).nome || DriveApp.getFolderById(sel).getName(); } catch (e) { /* */ }
  const k = normK_(nome);
  const i = (impianti || []).find(x => k && normK_(x.codice) === k);
  return { codice: i ? i.codice : k, k, nome: nome.replace(/^K\s?\d+[_\s-]*/i, ''), nomeCartella: nome, comune: i ? i.comune : 'Pesaro', cartellaId: sel };
}
const CREA_CARTELLA = '__crea__';

// ============================================================
//  UTILITÀ
// ============================================================
function oggi_() { return Utilities.formatDate(new Date(), 'Europe/Rome', 'yyyy-MM-dd'); }
function normK_(v) {
  const m = String(v || '').match(/(?:^|[^A-Z0-9])K\s?0*(\d{1,4})(?![0-9])/i);
  return m ? 'K' + m[1].padStart(3, '0') : '';
}
function tuttiK_(testo) {
  const out = [];
  const re = /(?:^|[^A-Z0-9])K\s?0*(\d{1,4})(?![0-9])/gi;
  let m;
  while ((m = re.exec(String(testo || '')))) { const k = 'K' + m[1].padStart(3, '0'); if (!out.includes(k)) out.push(k); }
  return out;
}
function emailDi_(from) { const m = String(from || '').match(/<([^>]+)>/); return (m ? m[1] : String(from || '')).trim().toLowerCase(); }
function nomeDi_(from) { const m = String(from || '').match(/^\s*"?([^"<]+?)"?\s*</); return m ? m[1].trim() : emailDi_(from); }
function esterno_(from) { return !/@veolia\.com$/i.test(emailDi_(from)); }
function pulisciNomeFile_(s) { return String(s || '').replace(/[\\/:*?"<>|#%]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 120); }
function tronca_(s, n) { s = String(s || ''); return s.length > n ? s.slice(0, n) + '…' : s; }
function dataITaISO_(s) {
  const m = String(s || '').match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (!m) return String(s || '').match(/^\d{4}-\d{2}-\d{2}$/) ? s : '';
  const y = m[3].length === 2 ? '20' + m[3] : m[3];
  return `${y}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}`;
}
function isoAIT_(s) { const m = String(s || '').match(/^(\d{4})-(\d{2})-(\d{2})/); return m ? `${m[3]}/${m[2]}/${m[1]}` : (s || ''); }
// "Sostituzione servomotore" da "Rich. Off servomotore" ecc.
function titoloDaOggetto_(subj) {
  return String(subj || '')
    .replace(/^\s*((R|RE|FW|FWD|I|R:|Inoltra)\s*:\s*)+/gi, '')
    .replace(/\[EXTERNAL\]/gi, '')
    .replace(/rich(iesta)?\.?\s*off(erta)?\.?/gi, '')
    .replace(/invio buono ordine.*?-\s*/i, '')
    .replace(/\bK\s?\d{1,4}\b\.?/gi, '')
    .replace(/\s+/g, ' ').trim().replace(/^[-–:\s]+|[-–:.\s]+$/g, '')
    .replace(/^./, c => c.toUpperCase());
}

// ============================================================
//  SERVER (gestionale su Render)
// ============================================================
function server_(metodo, path, corpo) {
  const opt = { method: metodo, muteHttpExceptions: true, headers: { 'x-addon-token': cfg_('ADDON_TOKEN') } };
  if (corpo) { opt.contentType = 'application/json'; opt.payload = JSON.stringify(corpo); }
  let r;
  try { r = UrlFetchApp.fetch(cfg_('PROXY_URL') + path, opt); }
  catch (e) { throw new Error('Server non raggiungibile (forse si sta avviando, riprova tra 30 secondi): ' + e.message); }
  const code = r.getResponseCode();
  let j = null;
  try { j = JSON.parse(r.getContentText()); } catch (e) { /* risposta non JSON */ }
  if (code === 401) throw new Error('Token non valido: controlla ADDON_TOKEN');
  if (j && j.ok === false && j.errore) throw new Error('Server: ' + j.errore);
  if (code >= 500 || !j) throw new Error('Il server non risponde correttamente (' + code + '). Se è appena ripartito, riprova tra 30 secondi.');
  if (j.ok === false) throw new Error(j.errore || 'Errore dal server');
  return j;
}
function azione_(azione, dati) { return server_('post', '/addon/azione', { azione, dati }); }

// ============================================================
//  DRIVE — cartelle impianto e sottocartelle lavoro
// ============================================================
function cartelleImpianti_(forza) {
  const cache = CacheService.getUserCache();
  if (!forza) { const c = cache.get('cartelleImpianti'); if (c) return JSON.parse(c); }
  const out = [];
  const it = DriveApp.getFolderById(cfg_('CARTELLA_RADICE_ID')).getFolders();
  while (it.hasNext()) { const f = it.next(); out.push({ id: f.getId(), nome: f.getName(), k: normK_(f.getName()) }); }
  out.sort((a, b) => a.nome.localeCompare(b.nome));
  cache.put('cartelleImpianti', JSON.stringify(out), 21600);
  return out;
}
function trovaOCreaSottocartella_(cartellaImpiantoId, titolo) {
  const padre = DriveApp.getFolderById(cartellaImpiantoId);
  const nome = pulisciNomeFile_(padre.getName() + '_' + titolo);
  const it = padre.getFoldersByName(nome);
  if (it.hasNext()) return it.next();
  return padre.createFolder(nome);
}
// Nuova cartella lavoro: Gestione → dentro la cartella impianto (solo Pesaro, "LAVORI in P2");
// Extra → cartella della commessa, nome aaaammgg + SIGLA + _Kxxx_Impianto_Titolo
function creaCartellaLavoro_(tipoLavoro, info, titolo, commessaKey) {
  if (tipoLavoro !== 'Extra') {
    if (!info || !info.cartellaId) throw new Error('Per i lavori in gestione serve un impianto di Pesaro (cartella in LAVORI in P2)');
    return trovaOCreaSottocartella_(info.cartellaId, titolo);
  }
  const c = commessa_(commessaKey);
  const nome = pulisciNomeFile_(Utilities.formatDate(new Date(), 'Europe/Rome', 'yyyyMMdd') + c.sigla + '_' + ((info && info.k) || 'K000') +
    (info && info.nome ? '_' + info.nome : '') + '_' + titolo);
  const radice = DriveApp.getFolderById(cartellaCommessa_(c.key));
  const it = radice.getFoldersByName(nome);
  return it.hasNext() ? it.next() : radice.createFolder(nome);
}
// Cartelle lavoro già esistenti per un impianto (per collegarle invece di crearne una nuova)
function cartelleLavoroEsistenti_(tipoLavoro, info, collegate, commessaKey) {
  const out = [];
  try {
    if (tipoLavoro === 'Extra') {
      const it = DriveApp.getFolderById(cartellaCommessa_(commessaKey)).getFolders();
      while (it.hasNext()) {
        const f = it.next(); const n = f.getName();
        if (/^_/.test(n)) continue;
        const kCart = leggiNomeCartella_(n, '').k;
        if (!info || !info.k || kCart === normK_(info.k) || n.toLowerCase().indexOf(String(info.codice || '').toLowerCase()) >= 0) out.push({ id: f.getId(), nome: n });
      }
    } else if (info && info.cartellaId) {
      const it = DriveApp.getFolderById(info.cartellaId).getFolders();
      while (it.hasNext()) { const f = it.next(); out.push({ id: f.getId(), nome: f.getName() }); }
    }
  } catch (e) { console.warn(e.message); }
  return out.filter(c => (collegate || []).indexOf(c.id) < 0).sort((a, b) => b.nome.localeCompare(a.nome));
}
// "Lavori extra canone" nel task del BdO o nel testo → Extra; "P2" / "in gestione" → Gestione
function tipoLavoroDaTesto_(testo) {
  const t = String(testo || '');
  if (/extra[\s-]*canone|lavori\s+extra|\bextra\b/i.test(t)) return 'Extra';
  return 'Gestione';
}

function salvaAllegato_(cartella, blob, nomeFile) {
  const nome = pulisciNomeFile_(nomeFile);
  const it = cartella.getFilesByName(nome);
  if (it.hasNext()) return it.next(); // già archiviato: non duplico
  return cartella.createFile(blob.copyBlob().setName(nome));
}

// Testo di un PDF: conversione temporanea in Google Doc (con OCR), poi cestino
function testoPdf_(blob) {
  let id = null;
  try {
    const f = Drive.Files.create({ name: 'tmp_pratiche_' + Date.now(), mimeType: 'application/vnd.google-apps.document' }, blob.copyBlob(), { ocrLanguage: 'it', fields: 'id' });
    id = f.id;
    return DocumentApp.openById(id).getBody().getText();
  } catch (e) {
    console.warn('Conversione PDF non riuscita: ' + e.message);
    return '';
  } finally {
    if (id) { try { DriveApp.getFileById(id).setTrashed(true); } catch (e) { /* pazienza */ } }
  }
}

// ============================================================
//  LETTURA DEL BUONO D'ORDINE (formato fisso Veolia)
// ============================================================
function parseBdo_(testo, oggetto, corpo) {
  const t = String(testo || '').replace(/\r/g, '');
  const r = {};
  let m;
  // l'ordine del testo estratto dal PDF cambia (a volte il valore viene prima dell'etichetta): cerco nel blocco "Riferimenti Ordine"
  const iRif = t.search(/Riferimenti Ordine/i);
  const blocco = iRif >= 0 ? t.slice(iRif, iRif + 250) : '';
  if ((m = t.match(/Ordine di acquisto[\s:]*([0-9]{0,2}[A-Z]{2,3}[0-9]{8,})/i)) || (m = blocco.match(/\b(\d?[A-Z]{2,3}0\d{8,})\b/))) r.numeroBdo = m[1];
  if ((m = t.match(/Data Ordine[\s:]*(\d{2}\/\d{2}\/\d{4})/i)) || (m = t.match(/(\d{2}\/\d{2}\/\d{4})\s*\n\s*Data Ordine/i)) || (m = blocco.match(/(\d{2}\/\d{2}\/\d{4})/))) r.dataBdo = m[1];
  // il fornitore: accanto all'etichetta (se il testo è impaginato), altrimenti in testa ai Commenti ("ARBO SPA - Fornitura...")
  if ((m = t.match(/All'attenzione di[ \t:]*([^\n]+?)(?:\s{2,}|\n|$)/i)) && !/:\s*$/.test(m[1]) && !/^(Codice|Numero|Indirizzo|Termini)/i.test(m[1])) r.fornitoreBdo = m[1].trim();
  if ((m = t.match(/Offerta\s*n\.?\s*([A-Z0-9\/\-]+)\s*del\s*(\d{2}\/\d{2}\/\d{4})/i))) { r.numeroOfferta = m[1]; r.dataOfferta = m[2]; }
  if ((m = t.match(/entro il[\s:]*(\d{2}\/\d{2}\/\d{4})/i))) r.consegnaEntro = m[1];
  if ((m = t.match(/TOTALE IVA ESCLUSA[\s:]*([\d.]+,\d{2})/i))) r.importoBdo = m[1];
  if ((m = t.match(/Commenti[\s:]*([^\n]+)/i))) r.commenti = m[1].trim();
  if ((m = t.match(/Richiedente[\s:]*([^\n]+)/i))) r.richiedente = m[1].trim();
  if ((m = t.match(/Task[\s:]*([^\n]+)/i))) r.task = m[1].trim();
  if (!r.fornitoreBdo && r.commenti && /\s-\s/.test(r.commenti)) r.fornitoreBdo = r.commenti.split(/\s-\s/)[0].trim();
  // ripieghi dall'oggetto e dal testo della mail
  const s = String(oggetto || '');
  if ((m = s.match(/buono ordine n\.?\s*([A-Z0-9]+)\s+del\s+(\d{2}\/\d{2}\/\d{4})(?:.*?\bad\s+(.+?)\s+-\s+(.*))?$/i))) {
    if (!r.numeroBdo) r.numeroBdo = m[1];
    if (!r.dataBdo) r.dataBdo = m[2];
    if (!r.fornitoreBdo && m[3]) r.fornitoreBdo = m[3].trim();
    if (!r.commenti && m[4]) r.commenti = m[4].trim();
  }
  if (!r.numeroOfferta && (m = String(corpo || '').match(/offerta\s*n\.?\s*([A-Z0-9\/\-]+)\s*del\s*(\d{2}\/\d{2}\/\d{4})/i))) { r.numeroOfferta = m[1]; r.dataOfferta = m[2]; }
  r.k = normK_(r.commenti) || normK_(s) || normK_(t);
  r.discordanzaNumero = !!(s && r.numeroBdo && /buono ordine n\.?\s*([A-Z0-9]+)/i.test(s) && s.match(/buono ordine n\.?\s*([A-Z0-9]+)/i)[1] !== r.numeroBdo);
  return r;
}

// ============================================================
//  CLAUDE — offerte e chiamate in formato libero
// ============================================================
function claude_(istruzioni, testo, pdfBlob, schema) {
  const key = cfg_('ANTHROPIC_API_KEY');
  if (!key) return null;
  const content = [];
  if (pdfBlob) content.push({ type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: Utilities.base64Encode(pdfBlob.getBytes()) } });
  content.push({ type: 'text', text: istruzioni + '\n\n' + tronca_(testo, 30000) });
  const body = {
    model: cfg_('CLAUDE_MODEL'), max_tokens: 1024,
    tools: [{ name: 'registra', description: 'Registra i dati estratti', input_schema: schema }],
    tool_choice: { type: 'tool', name: 'registra' },
    messages: [{ role: 'user', content }],
  };
  const r = UrlFetchApp.fetch('https://api.anthropic.com/v1/messages', {
    method: 'post', contentType: 'application/json', muteHttpExceptions: true,
    headers: { 'x-api-key': key, 'anthropic-version': '2023-06-01' }, payload: JSON.stringify(body),
  });
  if (r.getResponseCode() !== 200) { console.warn('Claude ' + r.getResponseCode() + ': ' + r.getContentText().slice(0, 300)); return null; }
  const j = JSON.parse(r.getContentText());
  const tu = (j.content || []).find(c => c.type === 'tool_use');
  return tu ? tu.input : null;
}

const SCHEMA_ESTRAZIONE = {
  type: 'object',
  properties: {
    tipo: { type: 'string', enum: ['chiamata', 'richiesta', 'offerta', 'bdo', 'materiale', 'archivia'], description: "Cosa rappresenta l'ultima mail rilevante della conversazione" },
    codiceImpianto: { type: 'string', description: "Codice K dell'impianto (es. K012) se citato o deducibile dall'elenco cartelle, altrimenti vuoto" },
    titoloLavoro: { type: 'string', description: 'Titolo breve del lavoro, 2-6 parole, es. "Sostituzione servomotore"' },
    ubicazione: { type: 'string', description: "Dove si trova il componente nell'edificio (es. Pala 3, sala stampa), se citato" },
    problema: { type: 'string', description: 'Descrizione sintetica del problema o della richiesta, una o due frasi' },
    fornitore: { type: 'string', description: "Ragione sociale del fornitore che emette l'offerta" },
    numeroOfferta: { type: 'string' },
    dataOfferta: { type: 'string', description: 'gg/mm/aaaa' },
    importo: { type: 'string', description: 'Totale imponibile IVA esclusa in formato 1234,56; vuoto se assente' },
    descrizioneOfferta: { type: 'string', description: 'Oggetto della fornitura in poche parole' },
    noteMateriale: { type: 'string', description: 'Se il fornitore comunica disponibilità/consegna della merce: cosa dice (es. disponibile in filiale a Pesaro)' },
  },
  required: ['tipo'],
};

// ============================================================
//  ANALISI DELLA CONVERSAZIONE
// ============================================================
function leggiConversazione_(messageId) {
  const msg = GmailApp.getMessageById(messageId);
  const thread = msg.getThread();
  const messaggi = thread.getMessages();
  const io = Session.getActiveUser().getEmail().toLowerCase();
  const allegati = [];
  messaggi.forEach((m, mi) => {
    m.getAttachments({ includeInlineImages: false }).forEach((a, ai) => {
      const nome = a.getName();
      const ct = a.getContentType();
      if (/\.(png|jpe?g|gif)$/i.test(nome) && a.getSize() < 60000) return; // loghi delle firme
      allegati.push({ mi, ai, nome, pdf: /pdf/i.test(ct) || /\.pdf$/i.test(nome), size: a.getSize(), da: m.getFrom(), data: Utilities.formatDate(m.getDate(), 'Europe/Rome', 'yyyy-MM-dd'), corrente: m.getId() === messageId });
    });
  });
  const testi = messaggi.map(m => `--- Da: ${m.getFrom()} | A: ${m.getTo()} | Cc: ${m.getCc()} | ${Utilities.formatDate(m.getDate(), 'Europe/Rome', 'dd/MM/yyyy HH:mm')}\n${tronca_(m.getPlainBody().split(/\n\s*(Da|From|Il giorno .* ha scritto|On .* wrote)\s*:/)[0], 4000)}`);
  return { msg, thread, messaggi, io, allegati, testo: `Oggetto: ${thread.getFirstMessageSubject()}\n\n` + testi.join('\n\n') };
}

function blobAllegato_(conv, mi, ai) {
  return conv.messaggi[mi].getAttachments({ includeInlineImages: false })[ai];
}

// Prima ipotesi veloce, senza server né AI
function ipotesiRapida_(conv) {
  const subj = conv.thread.getFirstMessageSubject() || '';
  const ultimo = conv.messaggi[conv.messaggi.length - 1];
  const pdfs = conv.allegati.filter(a => a.pdf);
  const segreteria = cfg_('EMAIL_SEGRETERIA').toLowerCase();
  if (/buono\s*(d['’]?\s*)?ordine/i.test(subj) || pdfs.some(a => /buono[\s_]*ordine/i.test(a.nome))) {
    // nella discussione del BdO, una risposta del fornitore senza PDF = notizia sul materiale
    const ultEst = esterno_(ultimo.getFrom()) && !ultimo.getAttachments().length;
    if (ultEst && /disponibil|pront|ritir|consegn|spedit|arrivat|evas/i.test(ultimo.getPlainBody().slice(0, 1500))) return 'materiale';
    return 'bdo';
  }
  if (pdfs.some(a => esterno_(a.da))) return 'offerta';
  if (emailDi_(ultimo.getFrom()) === conv.io && /rich(iesta)?\.?\s*off|preventiv|quotazion/i.test(subj)) return 'richiesta';
  if (emailDi_(ultimo.getFrom()) === segreteria && /ordine/i.test(subj)) return 'bdo';
  return 'chiamata';
}

function analizza_(messageId) {
  const conv = leggiConversazione_(messageId);
  const subj = conv.thread.getFirstMessageSubject() || '';
  const a = {
    messageId, threadId: conv.thread.getId(), oggetto: subj,
    tipo: ipotesiRapida_(conv),
    allegati: conv.allegati,
    campi: {}, avvisi: [],
  };
  // allegati selezionati di default: i PDF del messaggio aperto, altrimenti dell'ultimo messaggio che ne ha
  const pdfs = conv.allegati.filter(x => x.pdf);
  let preSel = pdfs.filter(x => x.corrente);
  if (!preSel.length && pdfs.length) { const ultMi = Math.max.apply(null, pdfs.map(x => x.mi)); preSel = pdfs.filter(x => x.mi === ultMi); }
  a.selezionati = preSel.map(x => x.mi + ':' + x.ai);

  // contesto dal gestionale (pratica già legata a questa conversazione, pratiche aperte)
  let ctx = { aperte: [], praticaThread: null, impianti: [] };
  try { ctx = server_('get', '/addon/contesto?threadId=' + encodeURIComponent(a.threadId)); }
  catch (e) { a.avvisi.push('⚠️ ' + e.message); a.serverKo = true; }
  a.aperte = ctx.aperte || [];
  a.impiantiGestionale = ctx.impianti || [];
  a.collegate = (ctx.cartelleCollegate || []).map(c => c.cartella);
  a.tipoLavoro = tipoLavoroDaTesto_(conv.testo);
  a.commessa = commessaDaTesto_(conv.testo);
  a.cartellaLavoro = CREA_CARTELLA;

  // codici K citati nella conversazione
  const kCitati = tuttiK_(conv.testo);
  let testoPdf = '';

  if (a.tipo === 'bdo') {
    const bdoAll = pdfs.find(x => /buono[\s_]*ordine/i.test(x.nome)) || pdfs[0];
    if (bdoAll) {
      testoPdf = testoPdf_(blobAllegato_(conv, bdoAll.mi, bdoAll.ai));
      a.selezionati = [bdoAll.mi + ':' + bdoAll.ai];
    }
    const b = parseBdo_(testoPdf, subj, conv.testo);
    Object.assign(a.campi, {
      numeroBdo: b.numeroBdo || '', dataBdo: b.dataBdo || '', fornitoreBdo: b.fornitoreBdo || '',
      importoBdo: b.importoBdo || '', numeroOfferta: b.numeroOfferta || '', consegnaEntro: b.consegnaEntro || '',
    });
    if (b.k) kCitati.unshift(b.k);
    if (b.task) a.tipoLavoro = /extra/i.test(b.task) ? 'Extra' : 'Gestione';
    if (b.discordanzaNumero) a.avvisi.push("ℹ️ Il numero nell'oggetto della mail è diverso da quello nel PDF: uso quello del PDF.");
    if (!b.importoBdo) a.avvisi.push("⚠️ Importo non trovato nel PDF: controllalo a mano.");
    a.titoloSuggerito = titoloDaOggetto_(b.commenti || subj);
  }

  // offerte, chiamate, materiale: lettura con Claude
  if (a.tipo !== 'bdo') {
    let pdfPerClaude = null;
    const offAll = a.tipo === 'offerta' ? pdfs.find(x => esterno_(x.da) && a.selezionati.includes(x.mi + ':' + x.ai)) || pdfs.find(x => esterno_(x.da)) : null;
    if (offAll) {
      const blob = blobAllegato_(conv, offAll.mi, offAll.ai);
      testoPdf = testoPdf_(blob);
      if (testoPdf.replace(/\s/g, '').length < 150 && offAll.size < 4500000) pdfPerClaude = blob; // scansione: lo legge Claude
    }
    const cartelle = (function () { try { return cartelleImpianti_(); } catch (e) { return []; } })();
    const istr = 'Sei l\'assistente di un tecnico di commessa che gestisce impianti termici del Comune di Pesaro. ' +
      'Analizza la conversazione email' + (testoPdf ? " e il testo dell'allegato PDF" : '') + ' ed estrai i dati richiesti. ' +
      'Non inventare: lascia vuoto ciò che non c\'è. Il codice impianto è nella forma K seguito da tre cifre. ' +
      'Elenco cartelle impianto: ' + cartelle.map(c => c.nome).join(' | ');
    const ex = claude_(istr, conv.testo + (testoPdf ? '\n\n=== TESTO PDF ===\n' + testoPdf : ''), pdfPerClaude, SCHEMA_ESTRAZIONE);
    if (ex) {
      // le regole certe (BdO, risposta sul materiale, PDF del fornitore) prevalgono; Claude decide solo i casi incerti
      const certo = ['bdo', 'materiale', 'offerta'].includes(a.tipo);
      if (ex.tipo && TIPI[ex.tipo] && (!certo || ex.tipo === 'materiale')) a.tipo = ex.tipo;
      if (ex.codiceImpianto) kCitati.push(normK_(ex.codiceImpianto));
      a.titoloSuggerito = ex.titoloLavoro || '';
      a.campi.ubicazione = ex.ubicazione || '';
      a.campi.problema = ex.problema || '';
      Object.assign(a.campi, {
        fornitore: ex.fornitore || '', numeroOfferta: ex.numeroOfferta || '', dataOfferta: ex.dataOfferta || '',
        importo: ex.importo || '', descrizioneOfferta: ex.descrizioneOfferta || '', noteMateriale: ex.noteMateriale || '',
      });
    } else if (!cfg_('ANTHROPIC_API_KEY')) {
      a.avvisi.push('ℹ️ Chiave Claude non impostata: compila i campi a mano.');
    }
    if (!a.campi.fornitore && offAll) a.campi.fornitore = nomeDi_(offAll.da);
    if (!a.titoloSuggerito) a.titoloSuggerito = titoloDaOggetto_(subj);
  }

  // ── abbinamento pratica ──
  const kOrdinati = kCitati.filter(Boolean).filter((k, i, arr) => arr.indexOf(k) === i);
  a.kTrovati = kOrdinati;
  let pratica = ctx.praticaThread ? a.aperte.find(p => p.id === ctx.praticaThread.id) || ctx.praticaThread : null;
  if (pratica) a.motivoAbbinamento = 'conversazione già collegata';
  if (!pratica && a.tipo === 'bdo' && a.campi.numeroOfferta) {
    const n = a.campi.numeroOfferta.replace(/\D/g, '');
    pratica = a.aperte.find(p => (p.offerte || []).some(o => o.numeroOfferta && o.numeroOfferta.replace(/\D/g, '') === n));
    if (pratica) a.motivoAbbinamento = 'offerta n. ' + a.campi.numeroOfferta;
  }
  if (!pratica && kOrdinati.length) {
    const cand = a.aperte.filter(p => normK_(p.codiceImpianto) === kOrdinati[0] || normK_(p.nomeImpianto) === kOrdinati[0]);
    if (cand.length === 1 && a.tipo !== 'chiamata') { pratica = cand[0]; a.motivoAbbinamento = 'unica pratica aperta su ' + kOrdinati[0]; }
    else if (cand.length > 1) a.avvisi.push(`ℹ️ Ci sono ${cand.length} pratiche aperte su ${kOrdinati[0]}: scegli quella giusta.`);
  }
  a.praticaId = pratica ? pratica.id : NUOVA;
  if (pratica && pratica.tipoLavoro) a.tipoLavoro = pratica.tipoLavoro;
  // impianto: per Pesaro la cartella in LAVORI in P2, per le altre commesse l'impianto del gestionale
  a.cartellaImpiantoId = '';
  const kRif = pratica ? (normK_(pratica.codiceImpianto) || normK_(pratica.nomeImpianto)) : kOrdinati[0];
  if (pratica) { const ip = a.impiantiGestionale.find(i => i.codice === pratica.codiceImpianto); if (ip) a.commessa = commessaDaComune_(ip.comune); }
  if (a.commessa !== 'pesaro') {
    const re = commessa_(a.commessa).re;
    const ip = a.impiantiGestionale.find(i => re.test(i.comune || '') && kRif && normK_(i.codice) === kRif);
    if (ip) a.cartellaImpiantoId = 'IMP:' + ip.codice;
  } else {
    try {
      const c = kRif ? cartelleImpianti_().find(x => x.k === kRif) : null;
      a.cartellaImpiantoId = c ? c.id : '';
    } catch (e) { a.avvisi.push('⚠️ Cartella LAVORI non accessibile: ' + e.message); }
  }
  if (!a.cartellaImpiantoId && !pratica) a.avvisi.push(kOrdinati.length ? `⚠️ Impianto ${kOrdinati[0]} non trovato: sceglilo dall'elenco.` : '⚠️ Impianto non riconosciuto: sceglilo dall\'elenco.');
  // per richieste e notizie sul materiale salvo solo ciò che è allegato alla mail aperta
  if (a.tipo === 'materiale' || a.tipo === 'richiesta') a.selezionati = conv.allegati.filter(x => x.corrente).map(x => x.mi + ':' + x.ai);
  a.campi.titolo = a.titoloSuggerito || '';
  a.campi.richiestoDa = '';
  if (a.tipo === 'chiamata') {
    const primo = conv.messaggi[0];
    a.campi.richiestoDa = emailDi_(primo.getFrom()) === conv.io ? 'Davide' : nomeDi_(primo.getFrom());
  }
  return a;
}

// stato dell'analisi in cache (le azioni dei pulsanti si passano solo stringhe)
function salvaStato_(a) { CacheService.getUserCache().put('an_' + a.messageId, JSON.stringify(a), 1800); }
function leggiStato_(mid) { const c = CacheService.getUserCache().get('an_' + mid); return c ? JSON.parse(c) : null; }
function input_(e, nome) {
  const fi = (e.commonEventObject && e.commonEventObject.formInputs) || {};
  const v = fi[nome] && fi[nome].stringInputs ? fi[nome].stringInputs.value : null;
  return v;
}
function unisciInput_(e, a) {
  ['titolo', 'ubicazione', 'problema', 'richiestoDa', 'fornitore', 'numeroOfferta', 'dataOfferta', 'importo', 'descrizioneOfferta',
    'numeroBdo', 'dataBdo', 'fornitoreBdo', 'importoBdo', 'consegnaEntro', 'noteMateriale', 'statoMateriale'].forEach(k => {
    const v = input_(e, k); if (v) a.campi[k] = v[0];
  });
  const t = input_(e, 'tipo'); if (t) a.tipo = t[0];
  const p = input_(e, 'pratica'); if (p) a.praticaId = p[0];
  const cm = input_(e, 'commessa');
  if (cm && cm[0] !== a.commessa) { a.commessa = cm[0]; a.cartellaImpiantoId = ''; a.cartellaLavoro = CREA_CARTELLA; }
  else { const c = input_(e, 'cartellaImpianto'); if (c) a.cartellaImpiantoId = c[0] === NESSUNO ? '' : c[0]; }
  const tl = input_(e, 'tipoLavoro'); if (tl) a.tipoLavoro = tl[0];
  if (a.tipoLavoro !== 'Extra' && a.commessa !== 'pesaro') { a.commessa = 'pesaro'; a.cartellaImpiantoId = ''; }
  const cl = input_(e, 'cartellaLavoro'); if (cl) a.cartellaLavoro = cl[0];
  // le caselle non spuntate non vengono inviate: se il widget c'era, nessun valore = nessun allegato
  if (a.allegati && a.allegati.length) a.selezionati = input_(e, 'allegati') || [];
  return a;
}

// ============================================================
//  CARD
// ============================================================
function onHomepage() {
  const card = CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader().setTitle('Pratiche Siram').setSubtitle('Lavori in gestione ed Extra'))
    .addSection(CardService.newCardSection()
      .addWidget(CardService.newTextParagraph().setText('Apri una mail (chiamata, offerta, buono d\'ordine, risposta del fornitore) e premi <b>Analizza</b>: il pannello propone pratica e cartella, tu confermi.'))
      .addWidget(CardService.newTextParagraph().setText('Per una <b>cartella già esistente</b>: selezionala in Google Drive e usa il pannello Pratiche Siram a destra.'))
      .addWidget(bottoneImporta_('📁 Apri Lavori IN CORSO (Extra)', cfg_('CARTELLA_EXTRA_ID')))
      .addWidget(bottoneImporta_('📁 Apri LAVORI in P2 (Gestione)', cfg_('CARTELLA_RADICE_ID')))
      .addWidget(CardService.newTextButton().setText('🔄 Aggiorna elenco impianti').setOnClickAction(CardService.newAction().setFunctionName('onAggiornaCartelle')))
      .addWidget(CardService.newTextButton().setText('🔌 Prova connessione').setOnClickAction(CardService.newAction().setFunctionName('onProvaConnessione')))
      .addWidget(CardService.newTextButton().setText('Apri il gestionale').setOpenLink(CardService.newOpenLink().setUrl(cfg_('GESTIONALE_URL')))));
  return card.build();
}

// Le cartelle esistenti si importano dal pannello in Drive: il pulsante apre la cartella (o "Lavori IN CORSO")
function bottoneImporta_(testo, idCartella) {
  const id = idCartella || cfg_('CARTELLA_EXTRA_ID');
  return CardService.newTextButton().setText(testo).setOpenLink(CardService.newOpenLink().setUrl('https://drive.google.com/drive/folders/' + id));
}

function onGmailMessage(e) {
  if (e.gmail && e.gmail.accessToken) GmailApp.setCurrentMessageAccessToken(e.gmail.accessToken);
  const mid = e.gmail.messageId;
  const msg = GmailApp.getMessageById(mid);
  const conv = { thread: msg.getThread(), messaggi: msg.getThread().getMessages(), io: Session.getActiveUser().getEmail().toLowerCase(), allegati: [] };
  conv.messaggi.forEach((m, mi) => m.getAttachments({ includeInlineImages: false }).forEach((x, ai) => conv.allegati.push({ mi, ai, nome: x.getName(), pdf: /pdf/i.test(x.getContentType()) || /\.pdf$/i.test(x.getName()), da: m.getFrom() })));
  const ipotesi = ipotesiRapida_(conv);
  const k = tuttiK_(conv.thread.getFirstMessageSubject())[0] || '';
  const nPdf = conv.allegati.filter(x => x.pdf).length;
  const sez = CardService.newCardSection()
    .addWidget(CardService.newDecoratedText().setTopLabel('Sembra').setText(TIPI[ipotesi]).setWrapText(true))
    .addWidget(CardService.newDecoratedText().setTopLabel('Impianto citato').setText(k || '—'))
    .addWidget(CardService.newDecoratedText().setTopLabel('PDF nella conversazione').setText(String(nPdf)))
    .addWidget(CardService.newTextButton().setText('⚡ Analizza').setTextButtonStyle(CardService.TextButtonStyle.FILLED)
      .setOnClickAction(CardService.newAction().setFunctionName('onAnalizza').setParameters({ mid })));
  return CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader().setTitle('Pratiche Siram').setSubtitle(tronca_(conv.thread.getFirstMessageSubject(), 60)))
    .addSection(sez).build();
}

function onAnalizza(e) {
  if (e.gmail && e.gmail.accessToken) GmailApp.setCurrentMessageAccessToken(e.gmail.accessToken);
  try {
    const a = analizza_(e.parameters.mid);
    salvaStato_(a);
    return aggiornaCard_(cardModulo_(a));
  } catch (err) {
    return aggiornaCard_(cardErrore_(err, e.parameters.mid), 'Errore: ' + err.message);
  }
}

function onCambio(e) {
  const a = leggiStato_(e.parameters.mid);
  if (!a) return aggiornaCard_(cardErrore_(new Error('Sessione scaduta, premi di nuovo Analizza'), e.parameters.mid));
  unisciInput_(e, a);
  // scegliendo una pratica esistente, la cartella impianto segue la pratica
  if (a.praticaId !== NUOVA) {
    const p = a.aperte.find(x => x.id === a.praticaId);
    if (p) {
      const ip = (a.impiantiGestionale || []).find(i => i.codice === p.codiceImpianto);
      a.commessa = ip ? commessaDaComune_(ip.comune) : 'pesaro';
      if (a.commessa !== 'pesaro') a.cartellaImpiantoId = 'IMP:' + p.codiceImpianto;
      else { try { const c = cartelleImpianti_().find(x => x.k === (normK_(p.codiceImpianto) || normK_(p.nomeImpianto))); if (c) a.cartellaImpiantoId = c.id; } catch (er) { /* */ } }
    }
  }
  salvaStato_(a);
  return aggiornaCard_(cardModulo_(a));
}

function aggiornaCard_(card, notifica) {
  const b = CardService.newActionResponseBuilder().setNavigation(CardService.newNavigation().updateCard(card));
  if (notifica) b.setNotification(CardService.newNotification().setText(notifica));
  return b.build();
}

function cardErrore_(err, mid) {
  return CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader().setTitle('Qualcosa non va'))
    .addSection(CardService.newCardSection()
      .addWidget(CardService.newTextParagraph().setText(String(err.message || err)))
      .addWidget(CardService.newTextButton().setText('🔁 Riprova').setOnClickAction(CardService.newAction().setFunctionName('onAnalizza').setParameters({ mid: mid || '' }))))
    .build();
}

function campoTesto_(nome, titolo, valore, multi, suggerimento) {
  const w = CardService.newTextInput().setFieldName(nome).setTitle(titolo).setValue(valore || '');
  if (multi) w.setMultiline(true);
  if (suggerimento) w.setHint(suggerimento);
  return w;
}

function cardModulo_(a) {
  const mid = a.messageId;
  const cambio = CardService.newAction().setFunctionName('onCambio').setParameters({ mid });
  const card = CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader().setTitle(TIPI[a.tipo]).setSubtitle(tronca_(a.oggetto, 60)));

  if (a.avvisi.length) {
    card.addSection(CardService.newCardSection().addWidget(CardService.newTextParagraph().setText(a.avvisi.join('<br>'))));
  }

  // 1. Che cos'è
  const selTipo = CardService.newSelectionInput().setType(CardService.SelectionInputType.DROPDOWN).setFieldName('tipo').setTitle('Che cos\'è').setOnChangeAction(cambio);
  Object.keys(TIPI).forEach(k => selTipo.addItem(TIPI[k], k, k === a.tipo));

  // 2. Pratica
  const selPra = CardService.newSelectionInput().setType(CardService.SelectionInputType.DROPDOWN).setFieldName('pratica').setTitle('Pratica').setOnChangeAction(cambio);
  selPra.addItem('➕ Nuovo lavoro (crea pratica e sottocartella)', NUOVA, a.praticaId === NUOVA);
  const kPrimo = (a.kTrovati || [])[0];
  const aperte = (a.aperte || []).slice().sort((x, y) => {
    const xk = normK_(x.codiceImpianto) === kPrimo ? 0 : 1, yk = normK_(y.codiceImpianto) === kPrimo ? 0 : 1;
    return xk - yk || String(x.codiceImpianto).localeCompare(String(y.codiceImpianto));
  });
  aperte.forEach(p => selPra.addItem(`${p.tipoLavoro === 'Extra' ? '[Extra] ' : ''}${normK_(p.codiceImpianto) || p.codiceImpianto} · ${tronca_(p.titolo || '(senza titolo)', 40)} — ${p.stato}`, p.id, p.id === a.praticaId));
  const sez1 = CardService.newCardSection().addWidget(selTipo).addWidget(selPra);
  if (a.motivoAbbinamento && a.praticaId !== NUOVA) sez1.addWidget(CardService.newTextParagraph().setText('<font color="#8fbe7a">✓ Riconosciuta: ' + a.motivoAbbinamento + '</font>'));
  card.addSection(sez1);

  // 3. Nuovo lavoro: impianto + dati chiamata
  if (a.praticaId === NUOVA) {
    const selTL = CardService.newSelectionInput().setType(CardService.SelectionInputType.RADIO_BUTTON).setFieldName('tipoLavoro').setTitle('Tipo lavoro').setOnChangeAction(cambio);
    selTL.addItem('In gestione (P2)', 'Gestione', a.tipoLavoro !== 'Extra').addItem('Extra canone', 'Extra', a.tipoLavoro === 'Extra');
    // commessa (solo per gli Extra: ognuna ha la sua cartella lavori)
    const extra = a.tipoLavoro === 'Extra';
    const comm = extra ? (a.commessa || 'pesaro') : 'pesaro';
    let selComm = null;
    if (extra) {
      selComm = CardService.newSelectionInput().setType(CardService.SelectionInputType.DROPDOWN).setFieldName('commessa').setTitle('Commessa').setOnChangeAction(cambio);
      COMMESSE_EXTRA.forEach(c => selComm.addItem(c.nome, c.key, c.key === comm));
    }
    const selImp = CardService.newSelectionInput().setType(CardService.SelectionInputType.DROPDOWN).setFieldName('cartellaImpianto').setTitle('Impianto').setOnChangeAction(cambio);
    selImp.addItem('— scegli l\'impianto —', NESSUNO, !a.cartellaImpiantoId);
    if (comm === 'pesaro') {
      let cartelle = [];
      try { cartelle = cartelleImpianti_(); } catch (er) { /* avviso già mostrato */ }
      cartelle.forEach(c => selImp.addItem(tronca_(c.nome, 60), c.id, c.id === a.cartellaImpiantoId));
    } else {
      const re = commessa_(comm).re;
      (a.impiantiGestionale || []).filter(i => re.test(i.comune || '')).sort((x, y) => String(x.codice).localeCompare(String(y.codice)))
        .forEach(i => selImp.addItem(tronca_(i.codice + ' — ' + i.descrizione, 60), 'IMP:' + i.codice, 'IMP:' + i.codice === a.cartellaImpiantoId));
    }
    const infoImp = infoImpianto_(a.cartellaImpiantoId, a.impiantiGestionale);
    // cartella del lavoro: nuova oppure una già esistente non ancora collegata
    const selCart = CardService.newSelectionInput().setType(CardService.SelectionInputType.DROPDOWN).setFieldName('cartellaLavoro').setTitle('Cartella del lavoro').setOnChangeAction(cambio);
    selCart.addItem('➕ Crea una cartella nuova', CREA_CARTELLA, a.cartellaLavoro === CREA_CARTELLA);
    const esistenti = (a.cartellaImpiantoId || extra) ? cartelleLavoroEsistenti_(a.tipoLavoro, infoImp, a.collegate, comm) : [];
    esistenti.slice(0, 60).forEach(c => selCart.addItem('📁 ' + tronca_(c.nome, 70), c.id, c.id === a.cartellaLavoro));
    const usaEsistente = a.cartellaLavoro && a.cartellaLavoro !== CREA_CARTELLA;
    const sezNuovo = CardService.newCardSection().setHeader('Nuovo lavoro').addWidget(selTL);
    if (selComm) sezNuovo.addWidget(selComm);
    card.addSection(sezNuovo
      .addWidget(selImp)
      .addWidget(selCart)
      .addWidget(campoTesto_('titolo', 'Titolo lavoro *', a.campi.titolo, false, usaEsistente ? 'Se vuoto, uso il nome della cartella' : 'Diventa il nome della cartella'))
      .addWidget(campoTesto_('ubicazione', 'Ubicazione', a.campi.ubicazione))
      .addWidget(campoTesto_('problema', 'Problema / richiesta', a.campi.problema, true))
      .addWidget(campoTesto_('richiestoDa', 'Richiesto da', a.campi.richiestoDa)));
  }

  // 4. Dati del documento
  const sez = CardService.newCardSection();
  if (a.tipo === 'offerta') {
    sez.setHeader('Offerta')
      .addWidget(campoTesto_('fornitore', 'Fornitore *', a.campi.fornitore))
      .addWidget(campoTesto_('numeroOfferta', 'N. offerta', a.campi.numeroOfferta))
      .addWidget(campoTesto_('dataOfferta', 'Data (gg/mm/aaaa)', a.campi.dataOfferta))
      .addWidget(campoTesto_('importo', 'Importo € IVA escl.', a.campi.importo))
      .addWidget(campoTesto_('descrizioneOfferta', 'Oggetto', a.campi.descrizioneOfferta));
  } else if (a.tipo === 'bdo') {
    sez.setHeader("Buono d'ordine")
      .addWidget(campoTesto_('numeroBdo', 'N. ordine *', a.campi.numeroBdo))
      .addWidget(campoTesto_('dataBdo', 'Data ordine', a.campi.dataBdo))
      .addWidget(campoTesto_('fornitoreBdo', 'Fornitore', a.campi.fornitoreBdo))
      .addWidget(campoTesto_('importoBdo', 'Totale € IVA escl.', a.campi.importoBdo))
      .addWidget(campoTesto_('numeroOfferta', 'Offerta citata n.', a.campi.numeroOfferta))
      .addWidget(campoTesto_('consegnaEntro', 'Consegna entro', a.campi.consegnaEntro));
  } else if (a.tipo === 'materiale') {
    const selMat = CardService.newSelectionInput().setType(CardService.SelectionInputType.RADIO_BUTTON).setFieldName('statoMateriale').setTitle('Stato materiale');
    ['Disponibile', 'Consegnato', 'In attesa'].forEach(v => selMat.addItem(v, v, (a.campi.statoMateriale || 'Disponibile') === v));
    sez.setHeader('Materiale').addWidget(selMat).addWidget(campoTesto_('noteMateriale', 'Note', a.campi.noteMateriale, false, 'Es. disponibile in filiale a Pesaro'));
  } else if (a.tipo === 'richiesta') {
    sez.addWidget(CardService.newTextParagraph().setText('Segno sulla pratica che le offerte sono state richieste oggi e collego questa conversazione: le risposte dei fornitori verranno riconosciute da sole.'));
  }
  if (a.tipo !== 'chiamata' && a.tipo !== 'archivia' && a.tipo !== 'richiesta') card.addSection(sez);
  else if (a.tipo === 'richiesta') card.addSection(sez);

  // 5. Allegati
  if (a.allegati.length) {
    const selAll = CardService.newSelectionInput().setType(CardService.SelectionInputType.CHECK_BOX).setFieldName('allegati').setTitle('Allegati da salvare nella cartella del lavoro');
    a.allegati.forEach(x => {
      const v = x.mi + ':' + x.ai;
      selAll.addItem(`${x.pdf ? '📄' : '📎'} ${tronca_(x.nome, 45)} · ${tronca_(nomeDi_(x.da), 20)}`, v, a.selezionati.includes(v));
    });
    card.addSection(CardService.newCardSection().addWidget(selAll));
  }

  // 6. Conferma
  card.setFixedFooter(CardService.newFixedFooter()
    .setPrimaryButton(CardService.newTextButton().setText('✅ Conferma').setOnClickAction(CardService.newAction().setFunctionName('onConferma').setParameters({ mid }))));
  return card.build();
}

// ============================================================
//  CONFERMA
// ============================================================
function onConferma(e) {
  if (e.gmail && e.gmail.accessToken) GmailApp.setCurrentMessageAccessToken(e.gmail.accessToken);
  const a = leggiStato_(e.parameters.mid);
  if (!a) return aggiornaCard_(cardErrore_(new Error('Sessione scaduta, premi di nuovo Analizza'), e.parameters.mid));
  unisciInput_(e, a);
  salvaStato_(a);
  const fatto = [];
  try {
    if (a.serverKo) { // riprovo il contatto col server prima di scrivere
      const ctx = server_('get', '/addon/contesto?threadId=' + encodeURIComponent(a.threadId));
      a.aperte = ctx.aperte || []; a.impiantiGestionale = ctx.impianti || []; a.serverKo = false;
    }
    const c = a.campi;
    let pratica = a.praticaId !== NUOVA ? a.aperte.find(p => p.id === a.praticaId) : null;
    let cartellaLavoro = null;

    // 1. pratica nuova: sottocartella + pratica
    if (!pratica) {
      const usaEsistente = a.cartellaLavoro && a.cartellaLavoro !== CREA_CARTELLA;
      let titolo = c.titolo;
      const comm = a.tipoLavoro === 'Extra' ? (a.commessa || 'pesaro') : 'pesaro';
      const info = infoImpianto_(a.cartellaImpiantoId, a.impiantiGestionale);
      let codice = info ? info.codice : '', nomeImp = info ? (info.nomeCartella || info.nome) : '';
      if (usaEsistente) {
        // cartella già creata a mano: impianto e titolo li ricavo dal nome se non li hai scritti
        cartellaLavoro = DriveApp.getFolderById(a.cartellaLavoro);
        const n = leggiNomeCartella_(cartellaLavoro.getName(), a.tipoLavoro === 'Extra' ? '' : (info && info.nomeCartella) || '');
        if (!codice) { const ip = (a.impiantiGestionale || []).find(i => n.k && normK_(i.codice) === n.k); codice = ip ? ip.codice : n.k; }
        if (!nomeImp) nomeImp = n.impianto;
        if (!titolo) titolo = n.titolo;
      } else {
        if (!info) throw new Error("Scegli l'impianto");
        if (!titolo) throw new Error('Scrivi il titolo del lavoro');
        cartellaLavoro = creaCartellaLavoro_(a.tipoLavoro, info, titolo, comm);
      }
      if (!codice) throw new Error("Impianto non riconosciuto: sceglilo dall'elenco");
      const r = azione_('crea', {
        codiceImpianto: codice, nomeImpianto: nomeImp, titolo, ubicazione: c.ubicazione, tipoLavoro: a.tipoLavoro,
        problema: c.problema, richiestoDa: c.richiestoDa, cartellaDriveId: cartellaLavoro.getId(), linkCartella: cartellaLavoro.getUrl(),
        threadId: a.threadId, origine: 'Gmail',
        dataRichiestaOfferte: a.tipo === 'richiesta' ? oggi_() : '',
      });
      pratica = r.pratica;
      a.praticaCreataDaCartella = usaEsistente;
      fatto.push(`Pratica ${a.tipoLavoro === 'Extra' ? 'Extra ' + commessa_(comm).nome : 'in gestione'} creata: ${titolo}`);
      fatto.push((usaEsistente ? 'Collegata la cartella: ' : 'Cartella: ') + cartellaLavoro.getName());
    } else {
      // pratica esistente senza cartella → la creo ora
      if (pratica.cartellaDriveId) cartellaLavoro = DriveApp.getFolderById(pratica.cartellaDriveId);
      else {
        const ip = (a.impiantiGestionale || []).find(i => i.codice === pratica.codiceImpianto);
        const comm = ip ? commessaDaComune_(ip.comune) : 'pesaro';
        let sel = a.cartellaImpiantoId;
        if (!sel) sel = comm !== 'pesaro' ? 'IMP:' + pratica.codiceImpianto : (cartelleImpianti_().find(x => x.k === (normK_(pratica.codiceImpianto) || normK_(pratica.nomeImpianto))) || {}).id;
        const info = infoImpianto_(sel, a.impiantiGestionale);
        if (!info) throw new Error("Impianto della pratica non trovato: collega la cartella dal gestionale");
        cartellaLavoro = creaCartellaLavoro_(pratica.tipoLavoro, info, pratica.titolo || c.titolo || 'Lavoro', comm);
        azione_('aggiorna', { id: pratica.id, campi: { cartellaDriveId: cartellaLavoro.getId(), linkCartella: cartellaLavoro.getUrl() } });
        fatto.push('Cartella creata: ' + cartellaLavoro.getName());
      }
    }

    // 2. allegati
    const conv = leggiConversazione_(a.messageId);
    const salvati = [];
    const etichetta = { offerta: 'OFFERTA', bdo: 'BDO', materiale: 'DDT', chiamata: 'CHIAMATA', richiesta: 'RICHIESTA', archivia: 'DOC' }[a.tipo];
    (a.selezionati || []).forEach(v => {
      const [mi, ai] = v.split(':').map(Number);
      const info = a.allegati.find(x => x.mi === mi && x.ai === ai);
      if (!info) return;
      const chi = a.tipo === 'offerta' ? (c.fornitore || nomeDi_(info.da)) : a.tipo === 'bdo' ? (c.numeroBdo || '') : nomeDi_(info.da);
      const nome = `${info.data}_${etichetta}_${chi}_${info.nome}`.replace(/__+/g, '_');
      const f = salvaAllegato_(cartellaLavoro, blobAllegato_(conv, mi, ai), nome);
      salvati.push(f);
    });
    if (salvati.length) fatto.push(salvati.length + ' allegat' + (salvati.length === 1 ? 'o salvato' : 'i salvati'));
    const primoPdf = salvati.find(f => /\.pdf$/i.test(f.getName())) || salvati[0];

    // 3. aggiornamento pratica
    if (a.tipo === 'offerta') {
      if (!c.fornitore) throw new Error('Manca il fornitore');
      azione_('offerta', { idPratica: pratica.id, fornitore: c.fornitore, numeroOfferta: c.numeroOfferta, data: dataITaISO_(c.dataOfferta), importo: c.importo, descrizione: c.descrizioneOfferta, linkDrive: primoPdf ? primoPdf.getUrl() : '', fileId: primoPdf ? primoPdf.getId() : '', threadId: a.threadId });
      fatto.push(`Offerta ${c.fornitore}${c.importo ? ' € ' + c.importo : ''} registrata`);
    } else if (a.tipo === 'bdo') {
      if (!c.numeroBdo) throw new Error("Manca il numero del buono d'ordine");
      azione_('bdo', { idPratica: pratica.id, numeroBdo: c.numeroBdo, dataBdo: dataITaISO_(c.dataBdo), fornitoreBdo: c.fornitoreBdo, importoBdo: c.importoBdo, numeroOfferta: c.numeroOfferta, consegnaEntro: dataITaISO_(c.consegnaEntro), linkBdo: primoPdf ? primoPdf.getUrl() : '', threadId: a.threadId });
      fatto.push(`BdO ${c.numeroBdo} registrato — attesa merce`);
    } else if (a.tipo === 'materiale') {
      azione_('materiale', { idPratica: pratica.id, statoMateriale: c.statoMateriale || 'Disponibile', note: c.noteMateriale, threadId: a.threadId });
      fatto.push('Materiale: ' + (c.statoMateriale || 'Disponibile'));
    } else if (a.tipo === 'richiesta' && a.praticaId !== NUOVA) {
      azione_('aggiorna', { id: pratica.id, campi: { dataRichiestaOfferte: oggi_() }, threadId: a.threadId, diario: 'Offerte richieste: ' + tronca_(a.oggetto, 60) });
      fatto.push('Segnata richiesta offerte');
    } else if (a.praticaId !== NUOVA) {
      azione_('collegaThread', { idPratica: pratica.id, threadId: a.threadId, diario: 'Collegata mail: ' + tronca_(a.oggetto, 60) });
      fatto.push('Conversazione collegata alla pratica');
    }

    CacheService.getUserCache().remove('an_' + a.messageId);
    return aggiornaCard_(cardFatto_(fatto, cartellaLavoro, pratica, a.praticaCreataDaCartella), '✅ Fatto');
  } catch (err) {
    console.error(err);
    if (fatto.length) err = new Error(err.message + ' — già fatto: ' + fatto.join('; '));
    return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText('⚠️ ' + err.message)).build();
  }
}

function cardFatto_(fatto, cartella, pratica, daCartella) {
  const sez = CardService.newCardSection();
  fatto.forEach(t => sez.addWidget(CardService.newDecoratedText().setText('✓ ' + t).setWrapText(true)));
  // cartella già piena di documenti: propongo di leggerli (offerte, BdO, DDT) e completare la pratica
  if (daCartella && cartella && pratica) {
    sez.addWidget(CardService.newTextParagraph().setText('La cartella ha già dei documenti: aprila in Drive, selezionala e premi <b>Completa dai documenti</b> nel pannello a destra.'));
    sez.addWidget(bottoneImporta_('📁 Apri la cartella in Drive', cartella.getParents().hasNext() ? cartella.getParents().next().getId() : cartella.getId()));
  }
  if (cartella) sez.addWidget(CardService.newTextButton().setText('📁 Apri cartella').setOpenLink(CardService.newOpenLink().setUrl(cartella.getUrl())));
  sez.addWidget(CardService.newTextButton().setText('Apri il gestionale').setOpenLink(CardService.newOpenLink().setUrl(cfg_('GESTIONALE_URL'))));
  return CardService.newCardBuilder().setHeader(CardService.newCardHeader().setTitle('Fatto').setSubtitle(pratica ? (pratica.titolo || '') : '')).addSection(sez).build();
}

function onAggiornaCartelle() {
  const n = cartelleImpianti_(true).length;
  return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText(n + ' cartelle impianto lette')).build();
}
function onProvaConnessione() {
  let msg;
  try { const c = server_('get', '/addon/contesto'); msg = '✅ Server ok · ' + (c.aperte || []).length + ' pratiche aperte'; }
  catch (e) { msg = '❌ ' + e.message; }
  try { msg += ' · ' + cartelleImpianti_().length + ' cartelle'; } catch (e) { msg += ' · ❌ Drive: ' + e.message; }
  msg += cfg_('ANTHROPIC_API_KEY') ? ' · Claude ok' : ' · Claude non impostato';
  return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText(msg)).build();
}

// Da eseguire una volta dall'editor per autorizzare e provare tutto
function provaInstallazione() {
  console.log('Cartelle impianto: ' + cartelleImpianti_(true).length);
  console.log('Server: ' + JSON.stringify(server_('get', '/addon/contesto')).slice(0, 200));
  console.log('Claude: ' + (cfg_('ANTHROPIC_API_KEY') ? 'chiave presente' : 'chiave MANCANTE'));
  COMMESSE_EXTRA.forEach(c => { try { console.log('Extra ' + c.nome + ': ' + DriveApp.getFolderById(cartellaCommessa_(c.key)).getName()); } catch (e) { console.log('Extra ' + c.nome + ': ❌ ' + e.message); } });
}
