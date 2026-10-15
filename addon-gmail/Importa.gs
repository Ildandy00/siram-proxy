/**
 * PRATICHE SIRAM — importazione da una cartella Drive già esistente
 * ------------------------------------------------------------
 * Pagina web (stesso progetto del pannello Gmail): elenca le cartelle lavoro
 * di "LAVORI in P2" (Gestione) e "Lavori IN CORSO" (Extra) non ancora collegate
 * a una pratica, legge i documenti (buono d'ordine, DDT, offerte, foto) e crea
 * o completa la pratica nel gestionale.
 */

// ============================================================
//  PAGINA WEB
// ============================================================
function doGet(e) {
  const t = HtmlService.createTemplateFromFile('Importa');
  t.cartellaIniziale = (e && e.parameter && e.parameter.cartella) || '';
  t.praticaIniziale = (e && e.parameter && e.parameter.pratica) || '';
  t.gestionaleUrl = cfg_('GESTIONALE_URL');
  return t.evaluate()
    .setTitle('Importa cartella — Pratiche Siram')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function urlPaginaImporta_() {
  return cfg_('WEBAPP_URL') || (function () { try { return ScriptApp.getService().getUrl() || ''; } catch (e) { return ''; } })();
}

// ============================================================
//  NOMI DELLE CARTELLE
// ============================================================
const RE_STATO_CARTELLA = /^(DA FINIRE|DA FARE|SOSPES[OA]\b.*|ANCORA DA REALIZZARE|DA REALIZZARE|GIA'? FATTURAT.*|GIA'? CONTABILIZZAT.*|Fare il LAVORO.*|Determina.*|FATTO|CONCLUS[OA]|TERMINAT[OA]|IN CORSO|URGENTE)$/i;

/**
 * "20251118PS_K052_Comando POLIZIA MUNICIPALE_Sostituzione fancoil_DA FINIRE"
 * "K088_Scuola Media LEOPARDI_Vaso d'espansione aperto da sostituire"  (padre: "K088_Scuola Media LEOPARDI")
 */
function leggiNomeCartella_(nome, nomePadre) {
  const r = { data: '', k: '', impianto: '', titolo: '', stato: '' };
  let resto = String(nome || '').trim();
  let m = resto.match(/^(\d{4})(\d{2})(\d{2})\s*PS[_\s-]*/i);
  if (m) { r.data = `${m[1]}-${m[2]}-${m[3]}`; resto = resto.slice(m[0].length); }
  // gestione: tolgo il nome della cartella impianto
  if (nomePadre && resto.toLowerCase().indexOf(String(nomePadre).toLowerCase() + '_') === 0) {
    r.k = normK_(nomePadre);
    r.impianto = String(nomePadre).replace(/^K\s?\d+[_\s-]*/i, '').trim();
    resto = resto.slice(String(nomePadre).length + 1);
  } else {
    m = resto.match(/^K\s?0*(\d{1,4})(?![0-9])[_\s-]*/i);
    if (m) { r.k = 'K' + m[1].padStart(3, '0'); resto = resto.slice(m[0].length); }
    const parti = resto.split('_').map(s => s.trim()).filter(Boolean);
    if (parti.length > 1) { r.impianto = parti.shift(); resto = parti.join('_'); }
  }
  const parti = resto.split('_').map(s => s.trim()).filter(Boolean);
  const stati = [];
  while (parti.length > 1 && RE_STATO_CARTELLA.test(parti[parti.length - 1])) stati.unshift(parti.pop());
  r.titolo = parti.join(' — ');
  r.stato = stati.join(' · ');
  return r;
}

function idDaLink_(s) {
  s = String(s || '').trim();
  const m = s.match(/folders\/([A-Za-z0-9_-]{10,})/) || s.match(/[?&]id=([A-Za-z0-9_-]{10,})/);
  if (m) return m[1];
  return /^[A-Za-z0-9_-]{10,}$/.test(s) ? s : '';
}

// ============================================================
//  ELENCO CARTELLE (con cache, la scansione di 64 impianti è lenta)
// ============================================================
function cacheGrande_(chiave, valore) {
  const c = CacheService.getUserCache();
  if (valore === undefined) {
    const n = Number(c.get(chiave + '_n') || 0);
    if (!n) return null;
    const pezzi = c.getAll(Array.from({ length: n }, (_, i) => chiave + '_' + i));
    try { return JSON.parse(Array.from({ length: n }, (_, i) => pezzi[chiave + '_' + i] || '').join('')); } catch (e) { return null; }
  }
  const s = JSON.stringify(valore), dim = 90000, n = Math.ceil(s.length / dim), obj = {};
  for (let i = 0; i < n; i++) obj[chiave + '_' + i] = s.slice(i * dim, (i + 1) * dim);
  obj[chiave + '_n'] = String(n);
  c.putAll(obj, 21600);
}

function scansionaCartelle_() {
  const out = [];
  // Extra: cartelle dirette di "Lavori IN CORSO" (salto quelle che iniziano con "_", es. l'archivio contabilizzati)
  const itE = DriveApp.getFolderById(cfg_('CARTELLA_EXTRA_ID')).getFolders();
  while (itE.hasNext()) {
    const f = itE.next(); const nome = f.getName();
    if (/^_/.test(nome)) continue;
    const n = leggiNomeCartella_(nome, '');
    out.push({ id: f.getId(), nome, tipo: 'Extra', k: n.k, impianto: n.impianto, titolo: n.titolo, stato: n.stato, data: n.data, agg: f.getLastUpdated().getTime() });
  }
  // Gestione: sottocartelle delle cartelle impianto di "LAVORI in P2"
  cartelleImpianti_(true).forEach(ci => {
    const it = DriveApp.getFolderById(ci.id).getFolders();
    while (it.hasNext()) {
      const f = it.next(); const nome = f.getName();
      const n = leggiNomeCartella_(nome, ci.nome);
      out.push({ id: f.getId(), nome, tipo: 'Gestione', k: n.k || ci.k, impianto: n.impianto, titolo: n.titolo, stato: n.stato, data: n.data, agg: f.getLastUpdated().getTime() });
    }
  });
  out.sort((a, b) => b.agg - a.agg);
  return out;
}

// Chiamata dalla pagina
function elencoCartelleImport(forza) {
  let cartelle = forza ? null : cacheGrande_('cartelleLavoro');
  if (!cartelle) { cartelle = scansionaCartelle_(); cacheGrande_('cartelleLavoro', cartelle); }
  let collegate = [], erroreServer = '';
  try { collegate = server_('get', '/addon/contesto').cartelleCollegate || []; }
  catch (e) { erroreServer = e.message; }
  const mappa = {};
  collegate.forEach(c => { mappa[c.cartella] = c; });
  cartelle.forEach(c => { c.collegata = mappa[c.id] || null; });
  return { cartelle, erroreServer, aggiornato: Utilities.formatDate(new Date(), 'Europe/Rome', 'dd/MM HH:mm') };
}

// ============================================================
//  LETTURA DEI DOCUMENTI
// ============================================================
const RE_DOC = {
  bdo: /\bB\.?\s?O\.?\b|buono\s*(d['’]?\s*)?ordine|ordine\s*(di\s*)?acquisto|\b\d?[A-Z]{2,3}0\d{8,}\b/i,
  ddt: /\bD\.?D\.?T\.?\b|\bbolla\b|documento\s*di\s*trasporto/i,
  offerta: /offert|preventiv|quotazion|\bOFF\.?\b|computo/i,
  consuntivo: /consuntiv|fattur|rapportin|verbale/i,
};

function tuttiIFile_(cartella, percorso, out, prof) {
  const itF = cartella.getFiles();
  while (itF.hasNext()) { const f = itF.next(); out.push({ f, nome: f.getName(), mime: f.getMimeType(), percorso }); }
  if (prof >= 2) return out;
  const itC = cartella.getFolders();
  while (itC.hasNext()) { const c = itC.next(); tuttiIFile_(c, (percorso ? percorso + '/' : '') + c.getName(), out, prof + 1); }
  return out;
}

function classificaPerNome_(nome) {
  if (RE_DOC.bdo.test(nome)) return 'bdo';
  if (RE_DOC.ddt.test(nome)) return 'ddt';
  if (RE_DOC.offerta.test(nome)) return 'offerta';
  if (RE_DOC.consuntivo.test(nome)) return 'altro';
  return '';
}
function classificaPerTesto_(t) {
  if (/ordine di acquisto/i.test(t) && /veolia/i.test(t)) return 'bdo';
  if (/documento di trasporto|d\.?d\.?t\.?\s*n|causale del trasporto/i.test(t)) return 'ddt';
  if (/offerta|preventivo|quotazione/i.test(t)) return 'offerta';
  return 'altro';
}

const SCHEMA_OFFERTA = {
  type: 'object',
  properties: {
    fornitore: { type: 'string', description: "Ragione sociale di chi emette l'offerta (non Veolia/Siram)" },
    numeroOfferta: { type: 'string' },
    dataOfferta: { type: 'string', description: 'gg/mm/aaaa' },
    importo: { type: 'string', description: 'Totale imponibile IVA esclusa, formato 1234,56; vuoto se assente' },
    descrizione: { type: 'string', description: 'Oggetto della fornitura in poche parole' },
  },
  required: ['fornitore'],
};
const SCHEMA_DDT = {
  type: 'object',
  properties: {
    fornitore: { type: 'string', description: 'Chi ha emesso il DDT (mittente)' },
    numeroDdt: { type: 'string' },
    dataDdt: { type: 'string', description: 'gg/mm/aaaa' },
    descrizione: { type: 'string', description: 'Merce consegnata in poche parole' },
  },
};

function datiDocumentoClaude_(tipo, testo, file) {
  if (!cfg_('ANTHROPIC_API_KEY')) return null;
  const schema = tipo === 'ddt' ? SCHEMA_DDT : SCHEMA_OFFERTA;
  const istr = tipo === 'ddt'
    ? "Questo è un documento di trasporto (DDT) ricevuto da Veolia/Siram. Estrai i dati. Non inventare: lascia vuoto ciò che non c'è."
    : "Questa è un'offerta/preventivo di un fornitore a Veolia/Siram. Estrai i dati. Il fornitore NON è Veolia né Siram. Non inventare.";
  const scansione = String(testo || '').replace(/\s/g, '').length < 150;
  let blob = null;
  if (scansione && file && file.getSize() < 4500000) blob = file.getBlob();
  return claude_(istr + '\nNome del file: ' + (file ? file.getName() : ''), scansione ? '' : testo, blob, schema);
}

function numeroDaNomeDdt_(nome) {
  const m = String(nome).match(/\bn\.?\s*°?\s*([A-Z0-9][A-Z0-9\/\-.]*\d)/i);
  return m ? m[1] : '';
}
function fornitoreDaNome_(nome, tipo) {
  let s = String(nome).replace(/\.[a-z0-9]{2,4}$/i, '');
  if (tipo === 'ddt') { const m = s.match(/D\.?D\.?T\.?\s+(.+?)\s+n\.?\s*°?\s*\d/i); if (m) return m[1].trim(); }
  if (tipo === 'bdo') { const m = s.match(/\b\d?[A-Z]{2,3}0\d{8,}\s+([^_]+)/); if (m) return m[1].trim(); }
  return '';
}

// Chiamata dalla pagina: legge la cartella e restituisce l'anteprima (non scrive nulla)
function analizzaCartellaImport(linkOId) {
  const id = idDaLink_(linkOId);
  if (!id) throw new Error('Link della cartella non valido');
  const cart = DriveApp.getFolderById(id);
  const nome = cart.getName();
  const avvisi = [];

  // tipo e cartella impianto
  const extraId = cfg_('CARTELLA_EXTRA_ID'), gestId = cfg_('CARTELLA_RADICE_ID');
  let tipoLavoro = '', nomePadre = '';
  const padri = cart.getParents();
  while (padri.hasNext()) {
    const p = padri.next();
    if (p.getId() === extraId) { tipoLavoro = 'Extra'; break; }
    const nonni = p.getParents();
    while (nonni.hasNext()) { if (nonni.next().getId() === gestId) { tipoLavoro = 'Gestione'; nomePadre = p.getName(); } }
    if (tipoLavoro) break;
  }
  if (!tipoLavoro) {
    tipoLavoro = /^\d{8}\s*PS/i.test(nome) ? 'Extra' : 'Gestione';
    avvisi.push('ℹ️ La cartella non è dentro "LAVORI in P2" né "Lavori IN CORSO": controlla il tipo di lavoro.');
  }
  const n = leggiNomeCartella_(nome, nomePadre);

  // impianto nel gestionale
  let ctx = { impianti: [], cartelleCollegate: [] };
  try { ctx = server_('get', '/addon/contesto'); } catch (e) { avvisi.push('⚠️ ' + e.message); }
  const imp = (ctx.impianti || []).find(i => n.k && normK_(i.codice) === n.k);
  if (!n.k) avvisi.push("⚠️ Codice impianto (Kxxx) non trovato nel nome della cartella: scrivilo a mano.");
  else if (!imp) avvisi.push(`ℹ️ ${n.k} non è nell'anagrafica impianti del gestionale: la pratica viene creata lo stesso.`);
  const collegata = (ctx.cartelleCollegate || []).find(c => c.cartella === id) || null;

  // documenti
  const files = tuttiIFile_(cart, '', [], 0);
  const foto = files.filter(x => /^image\//.test(x.mime) || /\.(jpe?g|png|heic|webp)$/i.test(x.nome));
  const docs = files.filter(x => foto.indexOf(x) < 0);
  const res = { offerte: [], bdo: null, ddt: null, altri: [] };
  const dettagli = [];
  let letti = 0;
  docs.forEach(x => {
    let tipo = classificaPerNome_(x.nome);
    const pdf = /pdf/i.test(x.mime) || /\.pdf$/i.test(x.nome);
    let testo = '';
    if (pdf && (tipo === 'bdo' || tipo === 'ddt' || tipo === 'offerta' || (!tipo && letti < 6))) {
      testo = testoPdf_(x.f.getBlob()); letti++;
      if (!tipo) tipo = classificaPerTesto_(testo);
    }
    if (!tipo) tipo = 'altro';
    const link = x.f.getUrl(), fileId = x.f.getId();
    if (tipo === 'bdo' && !res.bdo) {
      const b = parseBdo_(testo, x.nome, '');
      const numNome = (x.nome.match(/\b(\d?[A-Z]{2,3}0\d{8,})\b/) || [])[1] || '';
      res.bdo = {
        numeroBdo: b.numeroBdo || numNome, dataBdo: dataITaISO_(b.dataBdo), fornitoreBdo: b.fornitoreBdo || fornitoreDaNome_(x.nome, 'bdo'),
        importoBdo: b.importoBdo || '', numeroOfferta: b.numeroOfferta || '', consegnaEntro: dataITaISO_(b.consegnaEntro), linkBdo: link, file: x.nome,
        extra: /extra/i.test(b.task || '') || undefined,
      };
      if (b.task && /extra/i.test(b.task) && tipoLavoro !== 'Extra') avvisi.push('ℹ️ Il buono d\'ordine è sul task "' + b.task + '": forse è un lavoro Extra.');
      if (!res.bdo.importoBdo) avvisi.push("⚠️ Importo del buono d'ordine non letto: controllalo.");
    } else if (tipo === 'ddt' && !res.ddt) {
      const c = datiDocumentoClaude_('ddt', testo, x.f) || {};
      res.ddt = {
        numeroDdt: c.numeroDdt || numeroDaNomeDdt_(x.nome), data: dataITaISO_(c.dataDdt) || Utilities.formatDate(x.f.getDateCreated(), 'Europe/Rome', 'yyyy-MM-dd'),
        fornitore: c.fornitore || fornitoreDaNome_(x.nome, 'ddt'), note: [c.fornitore || fornitoreDaNome_(x.nome, 'ddt'), c.descrizione].filter(Boolean).join(' — '),
        linkDdt: link, file: x.nome,
      };
    } else if (tipo === 'offerta') {
      const c = datiDocumentoClaude_('offerta', testo, x.f) || {};
      res.offerte.push({
        fornitore: c.fornitore || '', numeroOfferta: c.numeroOfferta || '', data: dataITaISO_(c.dataOfferta),
        importo: c.importo || '', descrizione: c.descrizione || x.nome.replace(/\.[a-z]{2,4}$/i, ''), linkDrive: link, fileId, file: x.nome,
      });
    } else {
      res.altri.push(x.nome);
    }
    dettagli.push({ nome: x.nome, tipo, percorso: x.percorso, link });
  });
  // se l'offerta non dice chi è il fornitore, uso quello del BdO
  res.offerte.forEach(o => { if (!o.fornitore && res.bdo && res.bdo.fornitoreBdo && res.offerte.length === 1) o.fornitore = res.bdo.fornitoreBdo; });
  if (res.offerte.some(o => !o.fornitore)) avvisi.push("⚠️ Un'offerta non ha il fornitore: scrivilo, altrimenti non viene registrata.");
  if (res.bdo && !res.offerte.length) avvisi.push("ℹ️ Nessuna offerta nella cartella: l'offerta scelta verrà creata dai dati del buono d'ordine.");

  return {
    cartella: { id, nome, url: cart.getUrl() },
    tipoLavoro, codiceImpianto: imp ? imp.codice : n.k, nomeImpianto: (imp && imp.descrizione) || n.impianto || nomePadre,
    titolo: n.titolo, statoCartella: n.stato, dataChiamata: n.data || Utilities.formatDate(cart.getDateCreated(), 'Europe/Rome', 'yyyy-MM-dd'),
    numFoto: foto.length, offerte: res.offerte, bdo: res.bdo, ddt: res.ddt, altri: res.altri, documenti: dettagli,
    collegata, avvisi,
  };
}

// Chiamata dalla pagina con l'anteprima (eventualmente corretta a mano)
function salvaImportCartella(prev, idPratica) {
  if (!prev || !prev.cartella) throw new Error('Anteprima mancante');
  if (!prev.titolo) throw new Error('Scrivi il titolo del lavoro');
  if (!prev.codiceImpianto) throw new Error("Scrivi il codice impianto (Kxxx)");
  const dati = {
    idPratica: idPratica || '',
    pratica: {
      codiceImpianto: prev.codiceImpianto, nomeImpianto: prev.nomeImpianto, titolo: prev.titolo, tipoLavoro: prev.tipoLavoro,
      dataChiamata: prev.dataChiamata, cartellaDriveId: prev.cartella.id, linkCartella: prev.cartella.url, numFoto: prev.numFoto,
      problema: prev.problema || '', ubicazione: prev.ubicazione || '', origine: 'cartella Drive',
      notaImport: prev.statoCartella ? 'stato nel nome della cartella: ' + prev.statoCartella : '',
    },
    offerte: (prev.offerte || []).filter(o => o.fornitore && !o.escludi),
    bdo: prev.bdo && prev.bdo.numeroBdo && !prev.bdo.escludi ? prev.bdo : null,
    ddt: prev.ddt && !prev.ddt.escludi ? prev.ddt : null,
  };
  const r = azione_('importa', dati);
  return { ok: true, id: r.id, stato: r.pratica ? r.pratica.stato : '', titolo: r.pratica ? r.pratica.titolo : prev.titolo };
}

function include_(nome) { return HtmlService.createHtmlOutputFromFile(nome).getContent(); }
