const express    = require('express');
const cors       = require('cors');
const { google } = require('googleapis');

const app  = express();
const PORT = process.env.PORT || 3000;

const SHEET_ID = process.env.SHEET_ID || '1JsQz8FiUMFGjFQ5tuodgjexxe1hE8UE87ORFDi_geWE';

const SH = {
  IMPIANTI:    'Impianti',
  CATALOGO:    'CatalogoAttivita',
  INTERVENTI:  'Interventi',
  CHECKLIST:   'ChecklistEsecuzione',
  ASSENZE:     'Assenze',
  PUSHTOKENS:  'PushTokens',
  PRATICHE:    'Pratiche',
  OFFERTE:     'Offerte',
  RDACAT:      'RdaCat',
  REPERIBILITA:'Reperibilita',
};

const webpush = require('web-push');
if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(
    process.env.VAPID_EMAIL || 'mailto:admin@siram.it',
    process.env.VAPID_PUBLIC_KEY,
    process.env.VAPID_PRIVATE_KEY
  );
}
const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || '';

async function pushNotifica(sheets, operai, titolo, corpo) {
  if (!process.env.VAPID_PUBLIC_KEY) return;
  try {
    const rows = await leggi(sheets, SH.PUSHTOKENS).catch(() => []);
    const targets = rows.slice(1).filter(r => r[0] && operai.includes(r[0]));
    for (const row of targets) {
      try {
        const sub = JSON.parse(row[1]);
        await webpush.sendNotification(sub, JSON.stringify({ title: titolo, body: corpo, icon: '/icon.svg' }));
      } catch(e) { console.warn('Push fallita per', row[0], e.statusCode); }
    }
  } catch(e) { console.warn('pushNotifica error:', e.message); }
}

function getAuth() {
  const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  return new google.auth.GoogleAuth({ credentials: creds, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
}
async function getSheets() { const auth = getAuth(); return google.sheets({ version: 'v4', auth }); }

app.use(cors());
app.use(express.json());
app.get('/', (req, res) => res.json({ ok: true, service: 'Siram Proxy' }));

async function leggi(sheets, foglio) {
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: foglio });
  return r.data.values || [];
}

function fmtData(val) {
  if (!val) return '';
  try { const d = new Date(val); if (isNaN(d.getTime())) return ''; return d.toISOString().slice(0,10); } catch(e) { return ''; }
}
function fmtDateTime(val) {
  if (!val) return '';
  try { const d = new Date(val); if (isNaN(d.getTime())) return ''; return d.toLocaleString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' }); } catch(e) { return ''; }
}

app.get('/vapid-public', (req, res) => res.json({ key: VAPID_PUBLIC }));

app.post('/registra-push', async (req, res) => {
  try {
    const { operaio, subscription } = req.body;
    if (!operaio || !subscription) return res.json({ ok: false });
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PUSHTOKENS).catch(() => []);
    const idx = rows.findIndex((r,i) => i > 0 && r[0] === operaio);
    if (idx > 0) {
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.PUSHTOKENS}!A${idx+1}:B${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[operaio, JSON.stringify(subscription)]] } });
    } else {
      await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.PUSHTOKENS, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[operaio, JSON.stringify(subscription)]] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.get('/dati', async (req, res) => {
  try {
    const sheets = await getSheets();
    const [rImp, rCat, rInt, rChk] = await Promise.all([
      leggi(sheets, SH.IMPIANTI), leggi(sheets, SH.CATALOGO),
      leggi(sheets, SH.INTERVENTI), leggi(sheets, SH.CHECKLIST),
    ]);
    const impianti   = rImp.slice(1).filter(r=>r[0]).map(r=>({ codice:r[0]||'', descrizione:r[1]||'', comune:r[2]||'', indirizzo:r[3]||'', operaioDefault:r[4]||'' }));
    const catalogo   = rCat.slice(1).filter(r=>r[0]).map(r=>({ codiceImpianto:r[0]||'', tipoVisita:r[1]||'', attivita:r[2]||'', ordine:Number(r[3])||0, obbligatoria:r[4]||'SI' }));
    const interventi = rInt.slice(1).filter(r=>r[0]).map(r=>({ id:r[0]||'', codiceImpianto:r[1]||'', dataPrevista:fmtData(r[2]), operaio:r[3]||'', tipoVisita:r[4]||'', stato:r[5]||'', note:r[6]||'', dataChiusura:fmtData(r[7]), creatoIl:fmtData(r[8]), secondoOperaio:r[9]||'', interventoCollegato:r[10]||'', linkDrive:r[11]||'', dataFine:fmtData(r[12]), operaioSecondario2:r[13]||'' }));
    const checklist  = rChk.slice(1).filter(r=>r[0]).map(r=>({ id:r[0]||'', idIntervento:r[1]||'', attivita:r[2]||'', eseguita:r[3]||'NO', oraCompletamento:fmtDateTime(r[4]), note:r[5]||'', extra:r[6]||'NO' }));
    res.json({ impianti, catalogo, interventi, checklist });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/aggiorna-voce', async (req, res) => {
  try {
    const { id, eseguita, note } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.CHECKLIST);
    const idx = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx === -1) return res.json({ ok: false, errore: 'Voce non trovata' });
    const rowNum = idx + 1;
    if (eseguita !== undefined) {
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.CHECKLIST}!D${rowNum}`, valueInputOption: 'RAW', requestBody: { values: [[eseguita]] } });
      const ora = eseguita === 'SI' ? new Date().toLocaleString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' }) : '';
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.CHECKLIST}!E${rowNum}`, valueInputOption: 'RAW', requestBody: { values: [[ora]] } });
    }
    if (note !== undefined) {
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.CHECKLIST}!F${rowNum}`, valueInputOption: 'RAW', requestBody: { values: [[note]] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/aggiorna-intervento', async (req, res) => {
  try {
    const { id, stato, operaio } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const ora    = stato === 'Chiuso' ? new Date().toLocaleString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' }) : '';

    async function aggiornaRiga(rigaId) {
      const i = rows.findIndex((r,idx) => idx > 0 && r[0] === rigaId);
      if (i < 1) return;
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!F${i+1}`, valueInputOption: 'RAW', requestBody: { values: [[stato]] } });
      if (stato === 'Chiuso') {
        await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!H${i+1}`, valueInputOption: 'RAW', requestBody: { values: [[ora]] } });
      }
      if (stato === 'Aperto') {
        const notaAttuale = rows[i][6] || '';
        const dataRiapertura = new Date().toLocaleString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' });
        const notaAggiornata = notaAttuale ? notaAttuale + ` | 🔄 Riaperto il ${dataRiapertura}` : `🔄 Riaperto il ${dataRiapertura}`;
        await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!G${i+1}:H${i+1}`, valueInputOption: 'RAW', requestBody: { values: [[notaAggiornata, '']] } });
      }
    }

    const notaChiusura = req.body.notaChiusura;
    if (stato === 'Chiuso' && notaChiusura) {
      const rowNota = rows.findIndex((r,idx) => idx > 0 && r[0] === id);
      if (rowNota > 0) {
        const notaEsistente = rows[rowNota][6] || '';
        const nuovaNota = notaEsistente ? notaEsistente + ' | Chiusura: ' + notaChiusura : 'Chiusura: ' + notaChiusura;
        await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!G${rowNota+1}`, valueInputOption: 'RAW', requestBody: { values: [[nuovaNota]] } });
      }
    }
    if (operaio) {
      const rowOp = rows.findIndex((r,idx) => idx > 0 && r[0] === id);
      if (rowOp > 0) {
        await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!D${rowOp+1}`, valueInputOption: 'RAW', requestBody: { values: [[operaio]] } });
      }
    }
    await aggiornaRiga(id);
    const mainRow = rows.find((r,idx) => idx > 0 && r[0] === id);
    const collegato = mainRow && mainRow[10] ? mainRow[10] : null;
    if (collegato) await aggiornaRiga(collegato);
    const inverso = rows.find((r,idx) => idx > 0 && r[10] === id);
    if (inverso && inverso[0] !== id) await aggiornaRiga(inverso[0]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/aggiungi-extra', async (req, res) => {
  try {
    const { idIntervento, attivita } = req.body;
    const sheets = await getSheets();
    const id = 'CHK-' + Math.random().toString(36).substring(2,10).toUpperCase();
    await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.CHECKLIST, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[id, idIntervento, attivita, 'NO', '', '', 'SI']] } });
    res.json({ ok: true, id });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.get('/dati-responsabile', async (req, res) => {
  try {
    const sheets = await getSheets();
    const [rImp, rCat, rInt, rChk, rAss, rPrat, rOff] = await Promise.all([
      leggi(sheets, SH.IMPIANTI), leggi(sheets, SH.CATALOGO),
      leggi(sheets, SH.INTERVENTI), leggi(sheets, SH.CHECKLIST),
      leggi(sheets, SH.ASSENZE).catch(() => [[]]),
      leggi(sheets, SH.PRATICHE).catch(() => [[]]),
      leggi(sheets, SH.OFFERTE).catch(() => [[]]),
    ]);
    const impianti   = rImp.slice(1).filter(r=>r[0]).map(r=>({ codice:r[0]||'', descrizione:r[1]||'', comune:r[2]||'', indirizzo:r[3]||'', operaioDefault:r[4]||'' }));
    const catalogo   = rCat.slice(1).filter(r=>r[0]).map(r=>({ codiceImpianto:r[0]||'', tipoVisita:r[1]||'', attivita:r[2]||'', ordine:Number(r[3])||0, obbligatoria:r[4]||'SI' }));
    const interventi = rInt.slice(1).filter(r=>r[0]).map(r=>({ id:r[0]||'', codiceImpianto:r[1]||'', dataPrevista:fmtData(r[2]), operaio:r[3]||'', tipoVisita:r[4]||'', stato:r[5]||'', note:r[6]||'', dataChiusura:fmtData(r[7]), creatoIl:fmtData(r[8]), secondoOperaio:r[9]||'', interventoCollegato:r[10]||'', linkDrive:r[11]||'', dataFine:fmtData(r[12]), operaioSecondario2:r[13]||'' }));
    const checklist  = rChk.slice(1).filter(r=>r[0]).map(r=>({ id:r[0]||'', idIntervento:r[1]||'', attivita:r[2]||'', eseguita:r[3]||'NO', oraCompletamento:fmtDateTime(r[4]), note:r[5]||'', extra:r[6]||'NO' }));
    const assenze    = rAss.slice(1).filter(r=>r[0]).map(r=>({ id:r[0]||'', operaio:r[1]||'', dataInizio:fmtData(r[2]), dataFine:fmtData(r[3]), tipo:r[4]||'', note:r[5]||'' }));
    // Pratiche — 19 colonne A→S
    const pratiche = rPrat.slice(1).filter(r=>r[0]).map(r=>({
      id:               r[0]||'',
      idIntervento:     r[1]||'',
      codiceImpianto:   r[2]||'',
      stato:            r[3]||'Richiesta',
      dataRichiesta:    fmtData(r[4]),
      noteRichiesta:    r[5]||'',
      linkRichiesta:    r[6]||'',
      dataPreventivo:   fmtData(r[7]),
      importoPreventivo:r[8]||'',
      linkPreventivo:   r[9]||'',
      dataBdo:          fmtData(r[10]),
      numeroBdo:        r[11]||'',
      linkBdo:          r[12]||'',
      dataDdt:          fmtData(r[13]),
      numeroDdt:        r[14]||'',
      linkDdt:          r[15]||'',
      dataChiusura:     fmtData(r[16]),
      noteChiusura:     r[17]||'',
      creatoIl:         fmtData(r[18]),
      inGestione:       r[19]==='SI',
    }));
    // Offerte — foglio separato
    // A=ID | B=IDPratica | C=Fornitore | D=Descrizione | E=Importo | F=Data | G=LinkDrive | H=Selezionata | I=Note
    const offerte = rOff.slice(1).filter(r=>r[0]).map(r=>({
      id:          r[0]||'',
      idPratica:   r[1]||'',
      fornitore:   r[2]||'',
      descrizione: r[3]||'',
      importo:     r[4]||'',
      data:        fmtData(r[5]),
      linkDrive:   r[6]||'',
      selezionata: r[7]==='SI',
      note:        r[8]||'',
    }));
    res.json({ impianti, catalogo, interventi, checklist, assenze, pratiche, offerte });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/crea-intervento', async (req, res) => {
  try {
    const { codiceImpianto, dataPrevista, operaio, tipoVisita, note, attivitaExtra } = req.body;
    const statoIniziale      = req.body.statoOverride || 'Aperto';
    const dataFine           = req.body.dataFine || '';
    const operaioSecondario2 = req.body.operaioSecondario2 || '';
    const sheets = await getSheets();
    const id   = 'INT-' + Math.random().toString(36).substring(2,10).toUpperCase();
    const oggi = new Date().toLocaleDateString('it-IT');
    await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.INTERVENTI, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[id, codiceImpianto, dataPrevista, operaio, tipoVisita, statoIniziale, note||'', '', oggi, '', req.body.interventoCollegato||'', '', dataFine, operaioSecondario2]] } });
    const rCat = await leggi(sheets, SH.CATALOGO);
    const voci = rCat.slice(1).filter(r=>r[0]===codiceImpianto&&r[1]===tipoVisita).sort((a,b)=>(Number(a[3])||0)-(Number(b[3])||0));
    const chkRows = voci.map(r => { const chkId='CHK-'+Math.random().toString(36).substring(2,10).toUpperCase(); return [chkId, id, r[2]||'', 'NO', '', '', 'NO']; });
    if (attivitaExtra && attivitaExtra.length > 0) {
      attivitaExtra.forEach(att => { const chkId='CHK-'+Math.random().toString(36).substring(2,10).toUpperCase(); chkRows.push([chkId, id, att, 'NO', '', '', 'SI']); });
    }
    if (chkRows.length > 0) {
      await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.CHECKLIST, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: chkRows } });
    }
    if (statoIniziale !== 'DaAssegnare') {
      const rImp   = await leggi(sheets, SH.IMPIANTI);
      const impRow = rImp.slice(1).find(r=>r[0]===codiceImpianto);
      const nomeImp = impRow ? impRow[1] : codiceImpianto;
      const dataFmt = new Date(dataPrevista+'T00:00:00').toLocaleDateString('it-IT',{weekday:'short',day:'numeric',month:'short'});
      await pushNotifica(sheets, [operaio], '📋 Nuovo intervento assegnato', `${nomeImp} — ${tipoVisita} · ${dataFmt}`);
    }
    res.json({ ok: true, id });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/elimina-intervento', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rChk = await leggi(sheets, SH.CHECKLIST);
    const chkIdxs = rChk.map((r,i)=>i).filter(i=>i>0&&rChk[i][1]===id).reverse();
    for (const idx of chkIdxs) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.CHECKLIST), dimension:'ROWS', startIndex:idx, endIndex:idx+1 } } }] } });
    }
    const rInt = await leggi(sheets, SH.INTERVENTI);
    const intIdx = rInt.findIndex((r,i)=>i>0&&r[0]===id);
    if (intIdx > 0) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.INTERVENTI), dimension:'ROWS', startIndex:intIdx, endIndex:intIdx+1 } } }] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/crea-assenza', async (req, res) => {
  try {
    const { operaio, dataInizio, dataFine, tipo, note } = req.body;
    const sheets = await getSheets();
    const id = 'ASS-' + Math.random().toString(36).substring(2,10).toUpperCase();
    await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.ASSENZE, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[id, operaio, dataInizio, dataFine, tipo, note||'']] } });
    res.json({ ok: true, id });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/elimina-assenza', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rAss = await leggi(sheets, SH.ASSENZE);
    const idx = rAss.findIndex((r,i)=>i>0&&r[0]===id);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.ASSENZE), dimension:'ROWS', startIndex:idx, endIndex:idx+1 } } }] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/notifica-fmp', async (req, res) => {
  try {
    const { operaio, codiceImpianto, note, id } = req.body;
    const sheets = await getSheets();
    const rImp   = await leggi(sheets, SH.IMPIANTI);
    const impRow = rImp.slice(1).find(r=>r[0]===codiceImpianto);
    const nome   = impRow ? impRow[1] : codiceImpianto;
    await pushNotifica(sheets, [operaio], '🚨 Nuova segnalazione FMP', `${nome} — ${note.slice(0,80)}`);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/imposta-collegamento', async (req, res) => {
  try {
    const { id, interventoCollegato } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const idx    = rows.findIndex((r,i)=>i>0&&r[0]===id);
    if (idx < 1) return res.json({ ok: false });
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!K${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[interventoCollegato]] } });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/segnala-secondo', async (req, res) => {
  try {
    const { id, secondoOperaio } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const idx    = rows.findIndex((r,i)=>i>0&&r[0]===id);
    if (idx < 1) return res.json({ ok: false, errore: 'Intervento non trovato' });
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!J${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[secondoOperaio]] } });
    if (secondoOperaio) {
      const row = rows[idx];
      const rImp = await leggi(sheets, SH.IMPIANTI);
      const impRow = rImp.slice(1).find(r=>r[0]===row[1]);
      const nomeImp = impRow ? impRow[1] : row[1];
      const dataFmt = row[2] ? new Date(row[2]+'T00:00:00').toLocaleDateString('it-IT',{weekday:'short',day:'numeric',month:'short'}) : '';
      await pushNotifica(sheets, [secondoOperaio], '👥 Richiesto il tuo supporto', `${nomeImp} · ${dataFmt} — insieme a ${row[3]}`);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/posticipa-intervento', async (req, res) => {
  try {
    const { id, nuovaData } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const idx    = rows.findIndex((r,i)=>i>0&&r[0]===id);
    if (idx < 1) return res.json({ ok: false, errore: 'Intervento non trovato' });
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!C${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[nuovaData]] } });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/salva-catalogo', async (req, res) => {
  try {
    const { codiceImpianto, tipoVisita, attivita, ordine, obbligatoria } = req.body;
    const sheets = await getSheets();
    await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.CATALOGO, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[codiceImpianto, tipoVisita, attivita, ordine||1, obbligatoria||'SI']] } });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/elimina-catalogo', async (req, res) => {
  try {
    const { codice, tipoVisita, attivita } = req.body;
    const sheets = await getSheets();
    const rows = await leggi(sheets, SH.CATALOGO);
    const idx = rows.findIndex((r,i)=>i>0&&r[0]===codice&&r[1]===tipoVisita&&r[2]===attivita);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.CATALOGO), dimension:'ROWS', startIndex:idx, endIndex:idx+1 } } }] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// ============================================================
//  PRATICHE — CRUD COMPLETO
//  Colonne foglio "Pratiche" (20 colonne, A→T):
//  A=ID | B=IDIntervento | C=CodiceImpianto | D=Stato |
//  E=DataRichiesta | F=NoteRichiesta | G=LinkRichiesta |
//  H=DataPreventivo | I=ImportoPreventivo | J=LinkPreventivo |
//  K=DataBdo | L=NumeroBdo | M=LinkBdo |
//  N=DataDdt | O=NumeroDdt | P=LinkDdt |
//  Q=DataChiusura | R=NoteChiusura | S=CreatoIl | T=InGestione
//
//  Stato iter: Richiesta → Offerta → Preventivo → BdO → DDT → Chiusa
//  InGestione=SI bypassa il preventivo
//  Gli interventi di realizzazione sono nel foglio Interventi con
//  note contenente [PRA:ID] come riferimento alla pratica
//  Le offerte sono gestite nel foglio separato "Offerte"
// ============================================================

// GET /pratiche
app.get('/pratiche', async (req, res) => {
  try {
    const sheets   = await getSheets();
    const rows     = await leggi(sheets, SH.PRATICHE).catch(() => []);
    const pratiche = rows.slice(1).filter(r=>r[0]).map(r=>({
      id:               r[0]||'',
      idIntervento:     r[1]||'',
      codiceImpianto:   r[2]||'',
      stato:            r[3]||'Richiesta',
      dataRichiesta:    fmtData(r[4]),
      noteRichiesta:    r[5]||'',
      linkRichiesta:    r[6]||'',
      dataPreventivo:   fmtData(r[7]),
      importoPreventivo:r[8]||'',
      linkPreventivo:   r[9]||'',
      dataBdo:          fmtData(r[10]),
      numeroBdo:        r[11]||'',
      linkBdo:          r[12]||'',
      dataDdt:          fmtData(r[13]),
      numeroDdt:        r[14]||'',
      linkDdt:          r[15]||'',
      dataChiusura:     fmtData(r[16]),
      noteChiusura:     r[17]||'',
      creatoIl:         fmtData(r[18]),
      inGestione:       r[19]==='SI',
    }));
    res.json({ pratiche });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /crea-pratica
app.post('/crea-pratica', async (req, res) => {
  try {
    const { idIntervento, codiceImpianto, noteRichiesta, linkRichiesta } = req.body;
    if (!codiceImpianto) return res.json({ ok: false, errore: 'codiceImpianto richiesto' });
    const sheets  = await getSheets();
    const id      = 'PRA-' + Math.random().toString(36).substring(2,10).toUpperCase();
    const oggi    = new Date().toLocaleDateString('it-IT');
    const dataOggi = new Date().toISOString().slice(0,10);
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: SH.PRATICHE,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[
        id, idIntervento||'', codiceImpianto, 'Richiesta',
        dataOggi, noteRichiesta||'', linkRichiesta||'',
        '', '', '',   // preventivo
        '', '', '',   // bdo
        '', '', '',   // ddt
        '', '',       // chiusura
        oggi,         // creatoIl
        'NO',         // inGestione
      ]] },
    });
    res.json({ ok: true, id });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /aggiorna-pratica
app.post('/aggiorna-pratica', async (req, res) => {
  try {
    const { id, step, dati } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PRATICHE);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Pratica non trovata' });

    const STATI = ['Richiesta','Offerta','Preventivo','BdO','DDT','Chiusa'];

    const stepMap = {
      richiesta:  { range: `${SH.PRATICHE}!E${idx+1}:G${idx+1}`, fields: ['dataRichiesta','noteRichiesta','linkRichiesta'],      statoNew: 'Richiesta' },
      preventivo: { range: `${SH.PRATICHE}!H${idx+1}:J${idx+1}`, fields: ['dataPreventivo','importoPreventivo','linkPreventivo'], statoNew: 'Preventivo' },
      bdo:        { range: `${SH.PRATICHE}!K${idx+1}:M${idx+1}`, fields: ['dataBdo','numeroBdo','linkBdo'],                      statoNew: 'BdO' },
      ddt:        { range: `${SH.PRATICHE}!N${idx+1}:P${idx+1}`, fields: ['dataDdt','numeroDdt','linkDdt'],                      statoNew: 'DDT' },
      chiuso:     { range: `${SH.PRATICHE}!Q${idx+1}:R${idx+1}`, fields: ['dataChiusura','noteChiusura'],                        statoNew: 'Chiusa' },
    };

    const s = stepMap[step];
    if (!s) return res.json({ ok: false, errore: 'Step non valido' });

    const values = s.fields.map((f,fi) => dati[f] !== undefined ? dati[f] : (rows[idx][7+fi] || ''));
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: s.range,
      valueInputOption: 'RAW', requestBody: { values: [values] },
    });

    // Avanza stato solo in avanti
    const statoAttuale = rows[idx][3] || 'Richiesta';
    const idxAtt = STATI.indexOf(statoAttuale);
    const idxNuo = STATI.indexOf(s.statoNew);
    if (idxNuo > idxAtt) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID, range: `${SH.PRATICHE}!D${idx+1}`,
        valueInputOption: 'RAW', requestBody: { values: [[s.statoNew]] },
      });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /avanza-stato-offerta — porta pratica in stato "Offerta" quando si aggiunge la prima offerta
app.post('/avanza-stato-offerta', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PRATICHE);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Pratica non trovata' });
    const STATI = ['Richiesta','Offerta','Preventivo','BdO','DDT','Chiusa'];
    const statoAtt = rows[idx][3] || 'Richiesta';
    if (STATI.indexOf(statoAtt) < STATI.indexOf('Offerta')) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID, range: `${SH.PRATICHE}!D${idx+1}`,
        valueInputOption: 'RAW', requestBody: { values: [['Offerta']] },
      });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /imposta-gestione — segna pratica come "in gestione" e avanza a BdO
app.post('/imposta-gestione', async (req, res) => {
  try {
    const { id, valore } = req.body; // valore: true/false
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PRATICHE);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Pratica non trovata' });
    // Salva flag in colonna T (indice 19)
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${SH.PRATICHE}!T${idx+1}`,
      valueInputOption: 'RAW', requestBody: { values: [[valore ? 'SI' : 'NO']] },
    });
    // Se attivato, avanza stato a BdO (salta Preventivo)
    if (valore) {
      const STATI = ['Richiesta','Offerta','Preventivo','BdO','DDT','Chiusa'];
      const statoAtt = rows[idx][3] || 'Richiesta';
      if (STATI.indexOf(statoAtt) < STATI.indexOf('BdO')) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID, range: `${SH.PRATICHE}!D${idx+1}`,
          valueInputOption: 'RAW', requestBody: { values: [['BdO']] },
        });
      }
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /elimina-pratica
app.post('/elimina-pratica', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PRATICHE);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.PRATICHE), dimension:'ROWS', startIndex:idx, endIndex:idx+1 } } }] },
      });
    }
    // Elimina anche le offerte collegate
    const rOff = await leggi(sheets, SH.OFFERTE).catch(() => []);
    const idxOff = rOff.map((r,i)=>i).filter(i=>i>0&&rOff[i][1]===id).reverse();
    for (const io of idxOff) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.OFFERTE), dimension:'ROWS', startIndex:io, endIndex:io+1 } } }] },
      });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// ============================================================
//  OFFERTE — foglio separato
//  Colonne: A=ID | B=IDPratica | C=Fornitore | D=Descrizione |
//           E=Importo | F=Data | G=LinkDrive | H=Selezionata | I=Note
// ============================================================

// GET /offerte?idPratica=PRA-XXX
app.get('/offerte', async (req, res) => {
  try {
    const { idPratica } = req.query;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.OFFERTE).catch(() => []);
    const offerte = rows.slice(1).filter(r=>r[0]&&(!idPratica||r[1]===idPratica)).map(r=>({
      id:          r[0]||'',
      idPratica:   r[1]||'',
      fornitore:   r[2]||'',
      descrizione: r[3]||'',
      importo:     r[4]||'',
      data:        fmtData(r[5]),
      linkDrive:   r[6]||'',
      selezionata: r[7]==='SI',
      note:        r[8]||'',
    }));
    res.json({ offerte });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /crea-offerta
app.post('/crea-offerta', async (req, res) => {
  try {
    const { idPratica, fornitore, descrizione, importo, data, linkDrive, note } = req.body;
    if (!idPratica || !fornitore) return res.json({ ok: false, errore: 'idPratica e fornitore richiesti' });
    const sheets = await getSheets();
    const id     = 'OFF-' + Math.random().toString(36).substring(2,10).toUpperCase();
    const oggi   = data || new Date().toISOString().slice(0,10);
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: SH.OFFERTE,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[id, idPratica, fornitore, descrizione||'', importo||'', oggi, linkDrive||'', 'NO', note||'']] },
    });
    // Porta pratica in stato Offerta se era ancora in Richiesta
    const rows = await leggi(sheets, SH.PRATICHE);
    const idx  = rows.findIndex((r,i) => i > 0 && r[0] === idPratica);
    if (idx > 0) {
      const STATI = ['Richiesta','Offerta','Preventivo','BdO','DDT','Chiusa'];
      const statoAtt = rows[idx][3] || 'Richiesta';
      if (STATI.indexOf(statoAtt) < STATI.indexOf('Offerta')) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID, range: `${SH.PRATICHE}!D${idx+1}`,
          valueInputOption: 'RAW', requestBody: { values: [['Offerta']] },
        });
      }
    }
    res.json({ ok: true, id });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /seleziona-offerta — seleziona o deseleziona un'offerta
// id: ID offerta da selezionare, oppure null per deselezionare tutte
app.post('/seleziona-offerta', async (req, res) => {
  try {
    const { id, idPratica } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.OFFERTE);
    for (let i = 1; i < rows.length; i++) {
      if (rows[i][1] === idPratica) {
        const sel = (id && rows[i][0] === id) ? 'SI' : 'NO';
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID, range: `${SH.OFFERTE}!H${i+1}`,
          valueInputOption: 'RAW', requestBody: { values: [[sel]] },
        });
      }
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /elimina-offerta
app.post('/elimina-offerta', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.OFFERTE);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.OFFERTE), dimension:'ROWS', startIndex:idx, endIndex:idx+1 } } }] },
      });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// ============================================================
//  GET /rdacat / POST /crea-rdacat / POST /aggiorna-rdacat / POST /elimina-rdacat
// ============================================================
app.get('/rdacat', async (req, res) => {
  try {
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.RDACAT).catch(() => []);
    const richieste = rows.slice(1).filter(r=>r[0]).map(r=>({ id:r[0]||'', idIntervento:r[1]||'', codiceImpianto:r[2]||'', tipologia:r[3]||'', nota:r[4]||'', operaio:r[5]||'', stato:r[6]||'Inviata', creatoIl:r[7]||'', aggiornatoIl:r[8]||'' }));
    res.json({ richieste });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/crea-rdacat', async (req, res) => {
  try {
    const { idIntervento, codiceImpianto, tipologia, nota, operaio } = req.body;
    const sheets = await getSheets();
    const id     = 'RDA-' + Math.random().toString(36).substring(2,10).toUpperCase();
    const oggi   = new Date().toLocaleDateString('it-IT');
    await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.RDACAT, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[id, idIntervento, codiceImpianto, tipologia, nota, operaio, 'Inviata', oggi, '']] } });
    res.json({ ok: true, id });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/aggiorna-rdacat', async (req, res) => {
  try {
    const { id, stato } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.RDACAT);
    const idx    = rows.findIndex((r,i)=>i>0&&r[0]===id);
    if (idx < 1) return res.json({ ok: false, errore: 'RDA non trovata' });
    const oggi = new Date().toLocaleDateString('it-IT');
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.RDACAT}!G${idx+1}:I${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[stato, rows[idx][7], oggi]] } });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/elimina-rdacat', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.RDACAT);
    const idx    = rows.findIndex((r,i)=>i>0&&r[0]===id);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: [{ deleteDimension: { range: { sheetId: await getSheetId(sheets, SH.RDACAT), dimension:'ROWS', startIndex:idx, endIndex:idx+1 } } }] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// ============================================================
//  REPERIBILITA
// ============================================================
app.get('/reperibile', async (req, res) => {
  try {
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.REPERIBILITA).catch(() => []);
    const oggi   = new Date();
    const dow    = oggi.getDay() === 0 ? 6 : oggi.getDay() - 1;
    const lun    = new Date(oggi); lun.setDate(oggi.getDate() - dow); lun.setHours(0,0,0,0);
    const lunStr = lun.toISOString().slice(0,10);
    const riga   = rows.slice(1).find(r => { if(!r[0]) return false; try { const d=new Date(r[0]); return d.toISOString().slice(0,10)===lunStr; } catch(e){return false;} });
    const settimane = [];
    for (let i=-2; i<=6; i++) {
      const s = new Date(lun); s.setDate(lun.getDate()+i*7);
      const sStr = s.toISOString().slice(0,10);
      const rigaS = rows.slice(1).find(r=>{ try{return new Date(r[0]).toISOString().slice(0,10)===sStr;}catch(e){return false;} });
      settimane.push({ data:sStr, operaio:rigaS?rigaS[1]:'' });
    }
    res.json({ corrente:{ data:lunStr, operaio:riga?riga[1]:null }, settimane });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/salva-reperibile', async (req, res) => {
  try {
    const { data, operaio } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.REPERIBILITA).catch(() => []);
    const idx    = rows.findIndex((r,i)=>{ if(i===0||!r[0]) return false; try{return new Date(r[0]).toISOString().slice(0,10)===data;}catch(e){return false;} });
    if (idx > 0) {
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.REPERIBILITA}!A${idx+1}:B${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[data, operaio]] } });
    } else {
      await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.REPERIBILITA, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[data, operaio]] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/salva-link-drive', async (req, res) => {
  try {
    const { id, tipo, linkDrive } = req.body;
    const foglio = SH.INTERVENTI;
    const rows   = await (await getSheets()).spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: foglio }).then(r=>r.data.values||[]);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Record non trovato' });
    const sheets = await getSheets();
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${foglio}!L${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[linkDrive]] } });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.post('/aggiorna-multigiorno', async (req, res) => {
  try {
    const { id, dataFine, operaioSecondario2 } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const idx    = rows.findIndex((r,i)=>i>0&&r[0]===id);
    if (idx < 1) return res.json({ ok: false, errore: 'Intervento non trovato' });
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!M${idx+1}:N${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[dataFine||'', operaioSecondario2||'']] } });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

async function getSheetId(sheets, name) {
  const meta  = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = meta.data.sheets.find(s => s.properties.title === name);
  if (!sheet) throw new Error('Foglio non trovato: ' + name);
  return sheet.properties.sheetId;
}

app.listen(PORT, () => console.log(`Siram Proxy attivo sulla porta ${PORT}`));
