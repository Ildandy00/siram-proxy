/**
 * PRATICHE SIRAM — lettura di una cartella lavoro già esistente
 * ------------------------------------------------------------
 * Usato dal pannello in Drive (Drive.gs): legge nome della cartella, buono d'ordine,
 * DDT, offerte e foto, e crea o completa la pratica nel gestionale.
 */

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
const SCHEMA_DOC = {
  type: 'object',
  properties: {
    tipo: { type: 'string', enum: ['offerta', 'ddt', 'bdo', 'altro'], description: "offerta/preventivo di un fornitore; ddt = documento di trasporto; bdo = buono/ordine d'acquisto Veolia; altro" },
    fornitore: { type: 'string', description: "Ragione sociale del fornitore/mittente (mai Veolia né Siram)" },
    numero: { type: 'string', description: "Numero dell'offerta, del DDT o dell'ordine" },
    data: { type: 'string', description: 'Data del documento gg/mm/aaaa' },
    importo: { type: 'string', description: 'Totale imponibile IVA esclusa, formato 1234,56; vuoto se assente' },
    descrizione: { type: 'string', description: 'Oggetto/merce in poche parole' },
  },
  required: ['tipo'],
};
// Una richiesta a Claude per ogni PDF, spedite tutte insieme con fetchAll
function leggiPdfInParallelo_(lista) {
  const key = cfg_('ANTHROPIC_API_KEY');
  const out = {};
  if (!key || !lista.length) return out;
  const reqs = lista.map(x => ({
    url: 'https://api.anthropic.com/v1/messages', method: 'post', contentType: 'application/json', muteHttpExceptions: true,
    headers: { 'x-api-key': key, 'anthropic-version': '2023-06-01' },
    payload: JSON.stringify({
      model: cfg_('CLAUDE_MODEL'), max_tokens: 800,
      tools: [{ name: 'registra', description: 'Registra i dati del documento', input_schema: SCHEMA_DOC }],
      tool_choice: { type: 'tool', name: 'registra' },
      messages: [{ role: 'user', content: [
        { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: Utilities.base64Encode(x.f.getBlob().getBytes()) } },
        { type: 'text', text: 'Documento di un lavoro di manutenzione impianti per Veolia/Siram. Nome del file: "' + x.nome + '". Classificalo ed estrai i dati. Non inventare: lascia vuoto ciò che non c\'è.' },
      ] }],
    }),
  }));
  let risp = [];
  try { risp = UrlFetchApp.fetchAll(reqs); } catch (e) { console.warn('Claude: ' + e.message); return out; }
  risp.forEach((r, i) => {
    if (r.getResponseCode() !== 200) { console.warn('Claude ' + r.getResponseCode() + ' su ' + lista[i].nome + ': ' + r.getContentText().slice(0, 200)); return; }
    try { const tu = (JSON.parse(r.getContentText()).content || []).find(c => c.type === 'tool_use'); if (tu) out[lista[i].f.getId()] = tu.input; } catch (e) { /* */ }
  });
  return out;
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

// Legge la cartella e restituisce l'anteprima (non scrive nulla)
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
  const isPdf = x => /pdf/i.test(x.mime) || /\.pdf$/i.test(x.nome);
  docs.forEach(x => { x.tipo = classificaPerNome_(x.nome); });

  // 1. buono d'ordine: formato fisso Veolia, lo leggo con le regole (più affidabile dell'AI)
  const xBdo = docs.find(x => x.tipo === 'bdo' && isPdf(x));
  if (xBdo) {
    const b = parseBdo_(testoPdf_(xBdo.f.getBlob()), xBdo.nome, '');
    const numNome = (xBdo.nome.match(/\b(\d?[A-Z]{2,3}0\d{8,})\b/) || [])[1] || '';
    res.bdo = {
      numeroBdo: b.numeroBdo || numNome, dataBdo: dataITaISO_(b.dataBdo), fornitoreBdo: b.fornitoreBdo || fornitoreDaNome_(xBdo.nome, 'bdo'),
      importoBdo: b.importoBdo || '', numeroOfferta: b.numeroOfferta || '', consegnaEntro: dataITaISO_(b.consegnaEntro), linkBdo: xBdo.f.getUrl(), file: xBdo.nome,
    };
    if (b.task && /extra/i.test(b.task) && tipoLavoro !== 'Extra') avvisi.push('ℹ️ Il buono d\'ordine è sul task "' + b.task + '": forse è un lavoro Extra.');
    if (!res.bdo.importoBdo) avvisi.push("⚠️ Importo del buono d'ordine non letto: controllalo.");
  }

  // 2. gli altri PDF li legge Claude, tutti insieme in parallelo (il pannello ha 30 secondi)
  const daLeggere = docs.filter(x => x !== xBdo && isPdf(x) && x.f.getSize() < 4500000 && (x.tipo !== 'bdo')).slice(0, 8);
  const letture = leggiPdfInParallelo_(daLeggere);
  docs.forEach(x => {
    if (x === xBdo) { dettagli.push({ nome: x.nome, tipo: 'bdo' }); return; }
    const c = letture[x.f.getId()] || {};
    let tipo = x.tipo || (c.tipo && c.tipo !== 'altro' ? c.tipo : '') || 'altro';
    if (!x.tipo && c.tipo === 'bdo' && res.bdo) tipo = 'altro';
    const link = x.f.getUrl(), fileId = x.f.getId();
    if (tipo === 'ddt' && !res.ddt) {
      const forn = c.fornitore || fornitoreDaNome_(x.nome, 'ddt');
      res.ddt = {
        numeroDdt: c.numero || numeroDaNomeDdt_(x.nome), data: dataITaISO_(c.data) || Utilities.formatDate(x.f.getDateCreated(), 'Europe/Rome', 'yyyy-MM-dd'),
        fornitore: forn, note: [forn, c.descrizione].filter(Boolean).join(' — '), linkDdt: link, file: x.nome,
      };
    } else if (tipo === 'offerta') {
      res.offerte.push({
        fornitore: c.fornitore || '', numeroOfferta: c.numero || '', data: dataITaISO_(c.data),
        importo: c.importo || '', descrizione: c.descrizione || x.nome.replace(/\.[a-z]{2,4}$/i, ''), linkDrive: link, fileId, file: x.nome,
      });
    } else if (tipo === 'bdo' && !res.bdo) {
      res.bdo = { numeroBdo: c.numero || '', dataBdo: dataITaISO_(c.data), fornitoreBdo: c.fornitore || '', importoBdo: c.importo || '', numeroOfferta: '', consegnaEntro: '', linkBdo: link, file: x.nome };
    } else {
      tipo = 'altro';
      res.altri.push(x.nome);
    }
    dettagli.push({ nome: x.nome, tipo });
  });
  if (!cfg_('ANTHROPIC_API_KEY') && daLeggere.length) avvisi.push('ℹ️ Chiave Claude non impostata: offerte e DDT letti solo dal nome del file.');
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

// Salva l'anteprima (eventualmente corretta a mano)
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

