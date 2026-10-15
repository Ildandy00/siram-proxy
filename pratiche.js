// ============================================================
//  PRATICHE v2 — lavori in gestione (P2 a nostro carico)
//
//  Iter: Chiamata → Richiesta offerte → Offerte ricevute → Offerta scelta
//        → Ordinato (BdO, attesa merce) → Materiale disponibile
//        → Intervento assegnato → Chiusa
//
//  Lo STATO non si fa avanzare a mano: si calcola dai dati presenti
//  (offerte, BdO, materiale, interventi con [PRA:ID] nelle note, chiusura).
//  La colonna D viene comunque riscritta ad ogni modifica, per chi legge il foglio.
//
//  Foglio "Pratiche" (A→AH):
//  A=ID | B=IDIntervento | C=CodiceImpianto | D=Stato |
//  E=DataChiamata | F=Problema | G=LinkCartella |
//  H=DataOffertaScelta | I=ImportoOffertaScelta | J=(libero, ex LinkPreventivo) |
//  K=DataBdo | L=NumeroBdo | M=LinkBdo |
//  N=DataMateriale | O=(libero, ex NumeroDdt) | P=LinkDdt |
//  Q=DataChiusura | R=NoteChiusura | S=CreatoIl | T=InGestione |
//  U=Titolo | V=Ubicazione | W=CartellaDriveId | X=ThreadIds |
//  Y=ImportoBdo | Z=FornitoreBdo | AA=ConsegnaEntro | AB=NumeroOffertaBdo |
//  AC=StatoMateriale | AD=NoteMateriale | AE=RichiestoDa | AF=Diario |
//  AG=DataRichiestaOfferte | AH=NomeImpianto | AI=TipoLavoro (Gestione/Extra) | AJ=NumFoto
//  (O = NumeroDdt)
//
//  Foglio "Offerte" (A→L):
//  A=ID | B=IDPratica | C=Fornitore | D=Descrizione | E=Importo | F=Data |
//  G=LinkDrive | H=Selezionata | I=Note | J=NumeroOfferta | K=ThreadId | L=FileId
//
//  Le pratiche create prima della v2 restano leggibili: i vecchi stati
//  vengono interpretati e le colonne nuove sono semplicemente vuote.
// ============================================================

const STATI = [
  'Chiamata', 'Richiesta offerte', 'Offerte ricevute', 'Offerta scelta',
  'Ordinato', 'Materiale disponibile', 'Intervento assegnato', 'Chiusa',
];

const COL_PRATICHE = [
  'id', 'idIntervento', 'codiceImpianto', 'stato',
  'dataChiamata', 'problema', 'linkCartella',
  'dataOffertaScelta', 'importoOffertaScelta', 'libJ',
  'dataBdo', 'numeroBdo', 'linkBdo',
  'dataMateriale', 'numeroDdt', 'linkDdt',
  'dataChiusura', 'noteChiusura', 'creatoIl', 'inGestione',
  'titolo', 'ubicazione', 'cartellaDriveId', 'threadIds',
  'importoBdo', 'fornitoreBdo', 'consegnaEntro', 'numeroOffertaBdo',
  'statoMateriale', 'noteMateriale', 'richiestoDa', 'diario',
  'dataRichiestaOfferte', 'nomeImpianto', 'tipoLavoro', 'numFoto',
];
const HEAD_PRATICHE = [
  'ID', 'IDIntervento', 'CodiceImpianto', 'Stato',
  'DataChiamata', 'Problema', 'LinkCartella',
  'DataOffertaScelta', 'ImportoOffertaScelta', '-',
  'DataBdo', 'NumeroBdo', 'LinkBdo',
  'DataMateriale', 'NumeroDdt', 'LinkDdt',
  'DataChiusura', 'NoteChiusura', 'CreatoIl', 'InGestione',
  'Titolo', 'Ubicazione', 'CartellaDriveId', 'ThreadIds',
  'ImportoBdo', 'FornitoreBdo', 'ConsegnaEntro', 'NumeroOffertaBdo',
  'StatoMateriale', 'NoteMateriale', 'RichiestoDa', 'Diario',
  'DataRichiestaOfferte', 'NomeImpianto', 'TipoLavoro', 'NumFoto',
];
const COL_OFFERTE = ['id', 'idPratica', 'fornitore', 'descrizione', 'importo', 'data', 'linkDrive', 'selezionata', 'note', 'numeroOfferta', 'threadId', 'fileId'];
const HEAD_OFFERTE = ['ID', 'IDPratica', 'Fornitore', 'Descrizione', 'Importo', 'Data', 'LinkDrive', 'Selezionata', 'Note', 'NumeroOfferta', 'ThreadId', 'FileId'];

// Campi che il gestionale / il componente Gmail possono scrivere liberamente
const CAMPI_MODIFICABILI = [
  'codiceImpianto', 'nomeImpianto', 'titolo', 'ubicazione', 'problema', 'richiestoDa',
  'dataChiamata', 'linkCartella', 'cartellaDriveId', 'dataRichiestaOfferte',
  'dataBdo', 'numeroBdo', 'linkBdo', 'importoBdo', 'fornitoreBdo', 'consegnaEntro', 'numeroOffertaBdo',
  'statoMateriale', 'dataMateriale', 'noteMateriale', 'linkDdt',
  'dataChiusura', 'noteChiusura', 'idIntervento', 'tipoLavoro', 'numeroDdt', 'numFoto',
];

const STATI_LEGACY = {
  'Richiesta': 'Chiamata', 'Offerta': 'Offerte ricevute', 'Preventivo': 'Offerta scelta',
  'BdO': 'Ordinato', 'DDT': 'Materiale disponibile', 'Chiusa': 'Chiusa',
};

function colLetter(n) { // 0 → A
  let s = ''; n += 1;
  while (n > 0) { const m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); }
  return s;
}
const ULTIMA_COL_PRA = colLetter(COL_PRATICHE.length - 1); // AJ
const ULTIMA_COL_OFF = colLetter(COL_OFFERTE.length - 1);  // L

function oggiISO() {
  // data locale Italia, non UTC
  return new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Rome' });
}
function oraIT() {
  return new Date().toLocaleString('it-IT', { timeZone: 'Europe/Rome', day: '2-digit', month: '2-digit', year: '2-digit', hour: '2-digit', minute: '2-digit' });
}
function normData(v) {
  if (!v) return '';
  const s = String(v).trim();
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (m) { const y = m[3].length === 2 ? '20' + m[3] : m[3]; return `${y}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}`; }
  return s;
}
// "2.016,43" / "2016.43" / "€ 1.893,88" → "2016.43"
function normImporto(v) {
  if (v === undefined || v === null || v === '') return '';
  let s = String(v).replace(/[€\s]|EUR/gi, '');
  if (s.includes(',')) s = s.replace(/\./g, '').replace(',', '.');
  const n = parseFloat(s);
  return isNaN(n) ? '' : n.toFixed(2);
}
// "K12", "k 012", "K012_Municipio" → "K012"
function normK(v) {
  const m = String(v || '').match(/K\s*0*(\d{1,4})/i);
  return m ? 'K' + m[1].padStart(3, '0') : '';
}

function rigaAPratica(r) {
  const p = {};
  COL_PRATICHE.forEach((c, i) => { p[c] = r[i] !== undefined ? String(r[i]) : ''; });
  ['dataChiamata', 'dataOffertaScelta', 'dataBdo', 'dataMateriale', 'dataChiusura', 'creatoIl', 'consegnaEntro', 'dataRichiestaOfferte'].forEach(k => { p[k] = normData(p[k]); });
  p.inGestione = p.inGestione !== 'NO'; // v2: tutte in gestione salvo vecchie marcate NO
  p.threadIds = p.threadIds ? p.threadIds.split(',').map(s => s.trim()).filter(Boolean) : [];
  p.statoSalvato = p.stato;
  p.tipoLavoro = p.tipoLavoro === 'Extra' ? 'Extra' : 'Gestione';
  delete p.libJ;
  return p;
}
function praticaARiga(p) {
  return COL_PRATICHE.map(c => {
    if (c === 'threadIds') return (p.threadIds || []).join(',');
    if (c === 'inGestione') return p.inGestione === false ? 'NO' : 'SI';
    if (c === 'libJ') return p[c] || '';
    const v = p[c];
    return v === undefined || v === null ? '' : String(v);
  });
}
function rigaAOfferta(r) {
  const o = {};
  COL_OFFERTE.forEach((c, i) => { o[c] = r[i] !== undefined ? String(r[i]) : ''; });
  o.data = normData(o.data);
  o.selezionata = o.selezionata === 'SI';
  return o;
}
function offertaARiga(o) {
  return COL_OFFERTE.map(c => c === 'selezionata' ? (o.selezionata ? 'SI' : 'NO') : (o[c] === undefined || o[c] === null ? '' : String(o[c])));
}

// Stato calcolato dai dati
function calcolaStato(p, offerteP, interventi) {
  if (p.dataChiusura) return 'Chiusa';
  const tag = '[PRA:' + p.id + ']';
  // solo gli interventi di realizzazione (nota con [PRA:ID]); idIntervento è la chiamata d'origine
  if ((interventi || []).some(r => (r.note || '').includes(tag))) return 'Intervento assegnato';
  if (p.statoMateriale === 'Disponibile' || p.statoMateriale === 'Consegnato' || p.dataMateriale) return 'Materiale disponibile';
  if (p.numeroBdo) return 'Ordinato';
  if (offerteP.some(o => o.selezionata)) return 'Offerta scelta';
  if (offerteP.length) return 'Offerte ricevute';
  if (p.dataRichiestaOfferte) return 'Richiesta offerte';
  // vecchie pratiche senza dati nuovi: uso lo stato salvato tradotto
  const leg = STATI_LEGACY[p.statoSalvato];
  if (leg && STATI.indexOf(leg) > 0) return leg;
  return 'Chiamata';
}

module.exports = function montaPratiche(app, deps) {
  const { getSheets, leggi, SHEET_ID, SH, getSheetId } = deps;
  const ADDON_TOKEN = process.env.ADDON_TOKEN || '';
  let intestazioniOk = false;

  // Allarga il foglio se ha meno colonne del necessario (altrimenti Google rifiuta la scrittura)
  async function assicuraColonne(sheets, nomeFoglio, nCol) {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID, fields: 'sheets.properties' });
    const sh = meta.data.sheets.find(x => x.properties.title === nomeFoglio);
    if (!sh) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: [{ addSheet: { properties: { title: nomeFoglio, gridProperties: { columnCount: nCol } } } }] } });
      return;
    }
    const attuali = (sh.properties.gridProperties && sh.properties.gridProperties.columnCount) || 0;
    if (attuali < nCol) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: [{ appendDimension: { sheetId: sh.properties.sheetId, dimension: 'COLUMNS', length: nCol - attuali } }] } });
    }
  }

  async function assicuraIntestazioni(sheets) {
    if (intestazioniOk) return;
    await assicuraColonne(sheets, SH.PRATICHE, HEAD_PRATICHE.length);
    await assicuraColonne(sheets, SH.OFFERTE, HEAD_OFFERTE.length);
    const [rp, ro] = await Promise.all([
      sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${SH.PRATICHE}!A1:${ULTIMA_COL_PRA}1` }).catch(() => ({ data: {} })),
      sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${SH.OFFERTE}!A1:${ULTIMA_COL_OFF}1` }).catch(() => ({ data: {} })),
    ]);
    const hp = (rp.data.values && rp.data.values[0]) || [];
    const ho = (ro.data.values && ro.data.values[0]) || [];
    const upd = [];
    if (hp.length < HEAD_PRATICHE.length || hp[34] !== 'TipoLavoro') upd.push({ range: `${SH.PRATICHE}!A1:${ULTIMA_COL_PRA}1`, values: [HEAD_PRATICHE] });
    if (ho.length < HEAD_OFFERTE.length || ho[9] !== 'NumeroOfferta') upd.push({ range: `${SH.OFFERTE}!A1:${ULTIMA_COL_OFF}1`, values: [HEAD_OFFERTE] });
    if (upd.length) {
      await sheets.spreadsheets.values.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { valueInputOption: 'RAW', data: upd } });
    }
    intestazioniOk = true;
  }

  async function caricaTutto(sheets) {
    await assicuraIntestazioni(sheets);
    const [rPra, rOff, rInt] = await Promise.all([
      leggi(sheets, SH.PRATICHE).catch(() => [[]]),
      leggi(sheets, SH.OFFERTE).catch(() => [[]]),
      leggi(sheets, SH.INTERVENTI).catch(() => [[]]),
    ]);
    const interventi = rInt.slice(1).filter(r => r[0]).map(r => ({ id: r[0] || '', note: r[6] || '', stato: r[5] || '' }));
    const offerte = rOff.slice(1).map((r, i) => ({ r, riga: i + 2 })).filter(x => x.r[0]).map(x => Object.assign(rigaAOfferta(x.r), { _riga: x.riga }));
    const pratiche = rPra.slice(1).map((r, i) => ({ r, riga: i + 2 })).filter(x => x.r[0]).map(x => {
      const p = rigaAPratica(x.r);
      p._riga = x.riga;
      p.stato = calcolaStato(p, offerte.filter(o => o.idPratica === p.id), interventi);
      return p;
    });
    return { pratiche, offerte, interventi };
  }

  function pulisci(p) { const c = Object.assign({}, p); delete c._riga; delete c.statoSalvato; return c; }
  function pulisciOff(o) { const c = Object.assign({}, o); delete c._riga; return c; }

  async function scriviPratica(sheets, p) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${SH.PRATICHE}!A${p._riga}:${ULTIMA_COL_PRA}${p._riga}`,
      valueInputOption: 'RAW', requestBody: { values: [praticaARiga(p)] },
    });
  }
  async function scriviOfferta(sheets, o) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${SH.OFFERTE}!A${o._riga}:${ULTIMA_COL_OFF}${o._riga}`,
      valueInputOption: 'RAW', requestBody: { values: [offertaARiga(o)] },
    });
  }

  function aggiungiDiario(p, testo) {
    if (!testo) return;
    const riga = `${oraIT()} · ${testo}`;
    p.diario = p.diario ? p.diario + '\n' + riga : riga;
  }
  function aggiungiThread(p, threadId) {
    if (threadId && !p.threadIds.includes(threadId)) p.threadIds.push(threadId);
  }

  // Ricalcola stato, salva la pratica e restituisce lo stato aggiornato
  async function salvaERisposta(sheets, dati, p) {
    const offP = dati.offerte.filter(o => o.idPratica === p.id);
    p.stato = calcolaStato(p, offP, dati.interventi);
    await scriviPratica(sheets, p);
    return { ok: true, pratica: pulisci(p), offerte: offP.map(pulisciOff) };
  }

  // ── AZIONI (usate sia dal gestionale sia dal componente Gmail) ──

  async function azCrea(sheets, b) {
    const dati = await caricaTutto(sheets);
    const codice = b.codiceImpianto || normK(b.nomeImpianto) || '';
    if (!codice) throw new Error('Impianto mancante');
    if (b.cartellaDriveId) {
      const gia = dati.pratiche.find(x => x.cartellaDriveId === b.cartellaDriveId);
      if (gia) throw new Error(`Cartella già collegata alla pratica "${gia.titolo || gia.id}"`);
    }
    const id = 'PRA-' + Math.random().toString(36).substring(2, 10).toUpperCase();
    const p = {
      id, idIntervento: b.idIntervento || '', codiceImpianto: codice, stato: 'Chiamata',
      dataChiamata: normData(b.dataChiamata) || oggiISO(), problema: b.problema || '',
      linkCartella: b.linkCartella || '', creatoIl: oggiISO(), inGestione: true,
      titolo: b.titolo || '', ubicazione: b.ubicazione || '', cartellaDriveId: b.cartellaDriveId || '',
      threadIds: [], richiestoDa: b.richiestoDa || '', nomeImpianto: b.nomeImpianto || '',
      dataRichiestaOfferte: normData(b.dataRichiestaOfferte) || '', diario: '',
      tipoLavoro: b.tipoLavoro === 'Extra' ? 'Extra' : 'Gestione', numFoto: b.numFoto ? String(b.numFoto) : '',
    };
    aggiungiThread(p, b.threadId);
    aggiungiDiario(p, b.diario || ('Pratica aperta' + (b.origine ? ' da ' + b.origine : '')));
    p.stato = calcolaStato(p, [], dati.interventi);
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: `${SH.PRATICHE}!A:${ULTIMA_COL_PRA}`,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [praticaARiga(p)] },
    });
    return { ok: true, id, pratica: p, offerte: [] };
  }

  async function trovaPratica(sheets, id) {
    const dati = await caricaTutto(sheets);
    const p = dati.pratiche.find(x => x.id === id);
    if (!p) throw new Error('Pratica non trovata: ' + id);
    return { dati, p };
  }

  async function azAggiorna(sheets, b) {
    const { dati, p } = await trovaPratica(sheets, b.id);
    const campi = b.campi || {};
    Object.keys(campi).forEach(k => {
      if (!CAMPI_MODIFICABILI.includes(k)) return;
      let v = campi[k];
      if (/^data|consegnaEntro/.test(k)) v = normData(v);
      if (/^importo/.test(k)) v = normImporto(v);
      p[k] = v === undefined || v === null ? '' : String(v);
    });
    aggiungiThread(p, b.threadId);
    aggiungiDiario(p, b.diario);
    return salvaERisposta(sheets, dati, p);
  }

  async function azOfferta(sheets, b) {
    const { dati, p } = await trovaPratica(sheets, b.idPratica);
    if (!b.fornitore) throw new Error('Fornitore mancante');
    // stesso numero offerta dallo stesso fornitore → aggiorno invece di duplicare
    let o = b.numeroOfferta ? dati.offerte.find(x => x.idPratica === p.id && x.numeroOfferta && x.numeroOfferta === String(b.numeroOfferta)) : null;
    if (!o && b.fileId) o = dati.offerte.find(x => x.idPratica === p.id && x.fileId === b.fileId) || null;
    const nuova = !o;
    if (nuova) o = { id: 'OFF-' + Math.random().toString(36).substring(2, 10).toUpperCase(), idPratica: p.id, selezionata: false };
    Object.assign(o, {
      fornitore: b.fornitore, descrizione: b.descrizione || o.descrizione || '',
      importo: normImporto(b.importo) || o.importo || '', data: normData(b.data) || o.data || oggiISO(),
      linkDrive: b.linkDrive || o.linkDrive || '', note: b.note || o.note || '',
      numeroOfferta: b.numeroOfferta ? String(b.numeroOfferta) : (o.numeroOfferta || ''),
      threadId: b.threadId || o.threadId || '', fileId: b.fileId || o.fileId || '',
    });
    if (nuova) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID, range: `${SH.OFFERTE}!A:${ULTIMA_COL_OFF}`,
        valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [offertaARiga(o)] },
      });
      dati.offerte.push(o);
    } else {
      await scriviOfferta(sheets, o);
    }
    aggiungiThread(p, b.threadId);
    aggiungiDiario(p, `${nuova ? 'Offerta' : 'Offerta aggiornata'} ${o.fornitore}${o.numeroOfferta ? ' n. ' + o.numeroOfferta : ''}${o.importo ? ' — € ' + o.importo : ''}`);
    const r = await salvaERisposta(sheets, dati, p);
    r.idOfferta = o.id;
    return r;
  }

  async function marcaSelezionata(sheets, dati, p, idOfferta) {
    for (const o of dati.offerte.filter(x => x.idPratica === p.id)) {
      const sel = !!idOfferta && o.id === idOfferta;
      if (o.selezionata !== sel) { o.selezionata = sel; await scriviOfferta(sheets, o); }
    }
    const scelta = dati.offerte.find(x => x.idPratica === p.id && x.selezionata);
    p.dataOffertaScelta = scelta ? oggiISO() : '';
    p.importoOffertaScelta = scelta ? scelta.importo : '';
    return scelta;
  }

  async function azSeleziona(sheets, b) {
    const { dati, p } = await trovaPratica(sheets, b.idPratica);
    const scelta = await marcaSelezionata(sheets, dati, p, b.idOfferta || null);
    aggiungiDiario(p, scelta ? `Scelta offerta ${scelta.fornitore}${scelta.importo ? ' — € ' + scelta.importo : ''}` : 'Scelta offerta annullata');
    return salvaERisposta(sheets, dati, p);
  }

  async function azEliminaOfferta(sheets, b) {
    const dati = await caricaTutto(sheets);
    const o = dati.offerte.find(x => x.id === b.id);
    if (!o) return { ok: true };
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.OFFERTE), dimension: 'ROWS', startIndex: o._riga - 1, endIndex: o._riga } } }] },
    });
    const p = dati.pratiche.find(x => x.id === o.idPratica);
    if (!p) return { ok: true };
    const dati2 = await caricaTutto(sheets);
    const p2 = dati2.pratiche.find(x => x.id === p.id);
    aggiungiDiario(p2, `Eliminata offerta ${o.fornitore}`);
    return salvaERisposta(sheets, dati2, p2);
  }

  async function azBdo(sheets, b) {
    const { dati, p } = await trovaPratica(sheets, b.idPratica);
    p.numeroBdo = b.numeroBdo || p.numeroBdo;
    p.dataBdo = normData(b.dataBdo) || p.dataBdo || oggiISO();
    p.importoBdo = normImporto(b.importoBdo) || p.importoBdo;
    p.fornitoreBdo = b.fornitoreBdo || p.fornitoreBdo;
    p.consegnaEntro = normData(b.consegnaEntro) || p.consegnaEntro;
    p.numeroOffertaBdo = b.numeroOfferta || p.numeroOffertaBdo;
    p.linkBdo = b.linkBdo || p.linkBdo;
    if (!p.statoMateriale) p.statoMateriale = 'In attesa';
    aggiungiThread(p, b.threadId);
    // aggancio automatico all'offerta citata nel BdO (per numero, poi per fornitore)
    const offP = dati.offerte.filter(o => o.idPratica === p.id);
    let match = p.numeroOffertaBdo ? offP.find(o => o.numeroOfferta && o.numeroOfferta.replace(/\D/g, '') === String(p.numeroOffertaBdo).replace(/\D/g, '')) : null;
    if (!match && p.fornitoreBdo) {
      const f = p.fornitoreBdo.toLowerCase().replace(/s\.?p\.?a\.?|s\.?r\.?l\.?|[^a-z0-9]/g, '');
      match = offP.find(o => { const g = (o.fornitore || '').toLowerCase().replace(/s\.?p\.?a\.?|s\.?r\.?l\.?|[^a-z0-9]/g, ''); return f && g && (f.includes(g) || g.includes(f)); });
    }
    if (match && !match.selezionata) await marcaSelezionata(sheets, dati, p, match.id);
    // se il BdO arriva senza offerte registrate, creo l'offerta dai dati del BdO
    if (!offP.length && p.fornitoreBdo) {
      const o = { id: 'OFF-' + Math.random().toString(36).substring(2, 10).toUpperCase(), idPratica: p.id, fornitore: p.fornitoreBdo, descrizione: 'Da buono d\'ordine', importo: p.importoBdo, data: p.dataBdo, linkDrive: '', selezionata: true, note: '', numeroOfferta: p.numeroOffertaBdo || '', threadId: '', fileId: '' };
      await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: `${SH.OFFERTE}!A:${ULTIMA_COL_OFF}`, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [offertaARiga(o)] } });
      dati.offerte.push(o);
      p.dataOffertaScelta = p.dataBdo; p.importoOffertaScelta = p.importoBdo;
    }
    aggiungiDiario(p, `Buono d'ordine ${p.numeroBdo}${p.fornitoreBdo ? ' a ' + p.fornitoreBdo : ''}${p.importoBdo ? ' — € ' + p.importoBdo : ''}${p.consegnaEntro ? ' (consegna entro ' + p.consegnaEntro + ')' : ''}`);
    return salvaERisposta(sheets, dati, p);
  }

  async function azMateriale(sheets, b) {
    const { dati, p } = await trovaPratica(sheets, b.idPratica);
    p.statoMateriale = b.statoMateriale || 'Disponibile';
    p.dataMateriale = p.statoMateriale === 'In attesa' ? '' : (normData(b.data) || oggiISO());
    if (b.note !== undefined) p.noteMateriale = b.note;
    if (b.numeroDdt) p.numeroDdt = String(b.numeroDdt);
    if (b.linkDdt) p.linkDdt = b.linkDdt;
    aggiungiThread(p, b.threadId);
    aggiungiDiario(p, `Materiale: ${p.statoMateriale}${p.numeroDdt ? ' (DDT ' + p.numeroDdt + ')' : ''}${b.note ? ' — ' + b.note : ''}`);
    return salvaERisposta(sheets, dati, p);
  }

  async function azCollegaThread(sheets, b) {
    const { dati, p } = await trovaPratica(sheets, b.idPratica);
    aggiungiThread(p, b.threadId);
    aggiungiDiario(p, b.diario || 'Collegata conversazione email');
    return salvaERisposta(sheets, dati, p);
  }

  async function azChiudi(sheets, b) {
    const { dati, p } = await trovaPratica(sheets, b.id);
    if (b.riapri) { p.dataChiusura = ''; aggiungiDiario(p, 'Pratica riaperta'); }
    else {
      p.dataChiusura = normData(b.data) || oggiISO();
      if (b.note !== undefined) p.noteChiusura = b.note;
      aggiungiDiario(p, 'Pratica chiusa' + (b.note ? ' — ' + b.note : ''));
    }
    return salvaERisposta(sheets, dati, p);
  }

  // Importazione da una cartella Drive già esistente (o completamento di una pratica)
  // b = { idPratica?, pratica:{...campi crea}, offerte:[...], bdo:{...}|null, ddt:{...}|null }
  async function azImporta(sheets, b) {
    let id = b.idPratica;
    const pr = b.pratica || {};
    if (!id) {
      const r = await azCrea(sheets, Object.assign({}, pr, { diario: 'Pratica importata dalla cartella Drive' + (pr.notaImport ? ' — ' + pr.notaImport : '') }));
      id = r.id;
    } else {
      const { dati, p } = await trovaPratica(sheets, id);
      // completo solo i campi vuoti, non sovrascrivo quello che hai già scritto
      ['titolo', 'ubicazione', 'problema', 'nomeImpianto', 'cartellaDriveId', 'linkCartella', 'tipoLavoro', 'numFoto', 'dataChiamata'].forEach(k => {
        if (pr[k] && (!p[k] || (k === 'numFoto'))) p[k] = k === 'dataChiamata' ? normData(pr[k]) : String(pr[k]);
      });
      aggiungiDiario(p, 'Completata dai documenti della cartella Drive');
      await salvaERisposta(sheets, dati, p);
    }
    // al completamento salto quello che la pratica ha già (stesso file, stesso BdO, stesso DDT)
    const giaOff = (await caricaTutto(sheets)).offerte.filter(o => o.idPratica === id);
    for (const o of (b.offerte || [])) {
      if (!o.fornitore) continue;
      if (giaOff.some(x => (o.fileId && x.fileId === o.fileId) || (o.numeroOfferta && x.numeroOfferta === String(o.numeroOfferta)))) continue;
      await azOfferta(sheets, Object.assign({}, o, { idPratica: id }));
    }
    const { p: attuale } = await trovaPratica(sheets, id);
    if (b.bdo && b.bdo.numeroBdo && !attuale.numeroBdo) {
      await azBdo(sheets, Object.assign({}, b.bdo, { idPratica: id }));
    }
    if (b.ddt && !(attuale.numeroDdt && attuale.numeroDdt === String(b.ddt.numeroDdt || ''))) {
      await azMateriale(sheets, { idPratica: id, statoMateriale: 'Consegnato', data: b.ddt.data, numeroDdt: b.ddt.numeroDdt, linkDdt: b.ddt.linkDdt, note: b.ddt.note });
    }
    const { dati, p } = await trovaPratica(sheets, id);
    const r = await salvaERisposta(sheets, dati, p);
    r.id = id;
    return r;
  }

  const AZIONI = {
    crea: azCrea, aggiorna: azAggiorna, offerta: azOfferta, seleziona: azSeleziona,
    eliminaOfferta: azEliminaOfferta, bdo: azBdo, materiale: azMateriale,
    collegaThread: azCollegaThread, chiudi: azChiudi, importa: azImporta,
  };

  function rotta(path, az) {
    app.post(path, async (req, res) => {
      try { const sheets = await getSheets(); res.json(await AZIONI[az](sheets, req.body || {})); }
      catch (err) { res.status(200).json({ ok: false, errore: err.message }); }
    });
  }

  // ── Gestionale ──
  app.get('/pratiche', async (req, res) => {
    try {
      const sheets = await getSheets();
      const d = await caricaTutto(sheets);
      res.json({ ok: true, stati: STATI, pratiche: d.pratiche.map(pulisci), offerte: d.offerte.map(pulisciOff) });
    } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
  });
  rotta('/pratica/crea', 'crea');
  rotta('/pratica/aggiorna', 'aggiorna');
  rotta('/pratica/offerta', 'offerta');
  rotta('/pratica/seleziona-offerta', 'seleziona');
  rotta('/pratica/elimina-offerta', 'eliminaOfferta');
  rotta('/pratica/bdo', 'bdo');
  rotta('/pratica/materiale', 'materiale');
  rotta('/pratica/chiudi', 'chiudi');

  app.post('/elimina-pratica', async (req, res) => {
    try {
      const sheets = await getSheets();
      const d = await caricaTutto(sheets);
      const p = d.pratiche.find(x => x.id === req.body.id);
      const righeOff = d.offerte.filter(o => o.idPratica === req.body.id).map(o => o._riga);
      const sidOff = await getSheetId(sheets, SH.OFFERTE);
      const requests = [];
      // dal basso verso l'alto, così gli indici restano validi
      righeOff.sort((a, b) => b - a).forEach(r => requests.push({ deleteDimension: { range: { sheetId: sidOff, dimension: 'ROWS', startIndex: r - 1, endIndex: r } } }));
      if (p) requests.push({ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.PRATICHE), dimension: 'ROWS', startIndex: p._riga - 1, endIndex: p._riga } } });
      if (requests.length) await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests } });
      res.json({ ok: true });
    } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
  });

  // ── Componente Gmail ──
  function controllaToken(req, res) {
    if (!ADDON_TOKEN) return true;
    if ((req.headers['x-addon-token'] || '') === ADDON_TOKEN) return true;
    res.status(401).json({ ok: false, errore: 'Token non valido' });
    return false;
  }

  // Contesto per il pannello: pratiche aperte, pratica legata alla conversazione, impianti
  app.get('/addon/contesto', async (req, res) => {
    if (!controllaToken(req, res)) return;
    try {
      const sheets = await getSheets();
      const [d, rImp] = await Promise.all([caricaTutto(sheets), leggi(sheets, SH.IMPIANTI)]);
      const threadId = String(req.query.threadId || '');
      const daThread = threadId ? d.pratiche.find(p => p.threadIds.includes(threadId)) : null;
      // altre conversazioni: offerte con quel thread
      const daOfferta = !daThread && threadId ? d.offerte.find(o => o.threadId === threadId) : null;
      const praticaThread = daThread || (daOfferta ? d.pratiche.find(p => p.id === daOfferta.idPratica) : null);
      const aperte = d.pratiche.filter(p => p.stato !== 'Chiusa').map(p => ({
        id: p.id, codiceImpianto: p.codiceImpianto, nomeImpianto: p.nomeImpianto, titolo: p.titolo,
        stato: p.stato, numeroBdo: p.numeroBdo, fornitoreBdo: p.fornitoreBdo, cartellaDriveId: p.cartellaDriveId, tipoLavoro: p.tipoLavoro,
        offerte: d.offerte.filter(o => o.idPratica === p.id).map(o => ({ id: o.id, fornitore: o.fornitore, numeroOfferta: o.numeroOfferta, importo: o.importo, selezionata: o.selezionata })),
      }));
      const impianti = rImp.slice(1).filter(r => r[0]).map(r => ({ codice: r[0] || '', descrizione: r[1] || '', comune: r[2] || '' }));
      const cartelleCollegate = d.pratiche.filter(p => p.cartellaDriveId).map(p => ({ cartella: p.cartellaDriveId, id: p.id, titolo: p.titolo, stato: p.stato }));
      res.json({ ok: true, stati: STATI, aperte, praticaThread: praticaThread ? pulisci(praticaThread) : null, impianti, cartelleCollegate });
    } catch (err) { console.error('addon/contesto:', err); res.status(500).json({ ok: false, errore: err.message }); }
  });

  // Unico punto d'ingresso per le azioni dal pannello Gmail
  app.post('/addon/azione', async (req, res) => {
    if (!controllaToken(req, res)) return;
    try {
      const { azione, dati } = req.body || {};
      if (!AZIONI[azione]) return res.json({ ok: false, errore: 'Azione sconosciuta: ' + azione });
      const sheets = await getSheets();
      res.json(await AZIONI[azione](sheets, dati || {}));
    } catch (err) { res.status(200).json({ ok: false, errore: err.message }); }
  });

  // Chiamato da /aggiorna-intervento quando un intervento si chiude
  async function suInterventoChiuso(sheets, noteIntervento, notaChiusura) {
    const m = String(noteIntervento || '').match(/\[PRA:([A-Z0-9-]+)\]/);
    if (!m) return;
    const d = await caricaTutto(sheets);
    const p = d.pratiche.find(x => x.id === m[1]);
    if (!p || p.dataChiusura) return;
    p.dataChiusura = oggiISO();
    if (notaChiusura) p.noteChiusura = notaChiusura;
    aggiungiDiario(p, 'Chiusa dall\'operaio' + (notaChiusura ? ' — ' + notaChiusura : ''));
    await salvaERisposta(sheets, d, p);
  }
  async function suInterventoRiaperto(sheets, noteIntervento) {
    const m = String(noteIntervento || '').match(/\[PRA:([A-Z0-9-]+)\]/);
    if (!m) return;
    const d = await caricaTutto(sheets);
    const p = d.pratiche.find(x => x.id === m[1]);
    if (!p || !p.dataChiusura) return;
    p.dataChiusura = '';
    aggiungiDiario(p, 'Riaperta: intervento riaperto');
    await salvaERisposta(sheets, d, p);
  }

  return { caricaTutto, pulisci, pulisciOff, suInterventoChiuso, suInterventoRiaperto, STATI };
};

module.exports.normK = normK;
module.exports.normImporto = normImporto;
module.exports.normData = normData;
module.exports.calcolaStato = calcolaStato;
