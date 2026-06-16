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
  PRESENZE:    'Presenze',
  ASSEGNAZIONE:'Assegnazione',
  };
// Operai a cui notificare le nuove richieste del contenitore
const OPERAI_TUTTI = ['Matteo', 'Stefano', 'Michele', 'Ezio'];

const webpush = require('web-push');
if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(
    process.env.VAPID_EMAIL || 'mailto:admin@siram.it',
    process.env.VAPID_PUBLIC_KEY,
    process.env.VAPID_PRIVATE_KEY
  );
}
const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || '';

// ── OneSignal per notifiche push native ──
const ONESIGNAL_APP_ID  = process.env.ONESIGNAL_APP_ID  || '';
const ONESIGNAL_API_KEY = process.env.ONESIGNAL_API_KEY || '';
const oneSignalPronto = !!(ONESIGNAL_APP_ID && ONESIGNAL_API_KEY);
if (oneSignalPronto) {
  console.log('OneSignal configurato (push attivo)');
} else {
  console.warn('OneSignal non configurato — variabili ONESIGNAL_APP_ID/ONESIGNAL_API_KEY mancanti');
}

// ── Invio notifica push via OneSignal usando external_id (nome operaio) ──
// operai: array di nomi operaio (es. ['Matteo'])
async function pushNotifica(sheets, operai, titolo, corpo) {
  if (!oneSignalPronto) { console.warn('pushNotifica: OneSignal non configurato'); return; }
  try {
    const resp = await fetch('https://onesignal.com/api/v1/notifications', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Basic ' + ONESIGNAL_API_KEY
      },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        include_aliases: { external_id: operai },
        target_channel: 'push',
        headings: { en: titolo, it: titolo },
        contents: { en: corpo, it: corpo }
      })
    });
    const data = await resp.json().catch(() => ({}));
    if (data.errors) {
      console.warn('OneSignal errori:', JSON.stringify(data.errors));
    } else {
      console.log('OneSignal inviata a', operai.join(','), '— id:', data.id || '?');
    }
  } catch (e) {
    console.warn('pushNotifica (OneSignal) error:', e.message);
  }
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
    const { operaio, subscription, fcmToken } = req.body;
    if (!operaio || (!subscription && !fcmToken)) return res.json({ ok: false });

    const dato = fcmToken ? fcmToken : JSON.stringify(subscription);
    const tipo = fcmToken ? 'fcm' : 'web';

    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PUSHTOKENS).catch(() => []);
    const idx = rows.findIndex((r,i) => i > 0 && r[0] === operaio);
    if (idx > 0) {
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.PUSHTOKENS}!A${idx+1}:C${idx+1}`, valueInputOption: 'RAW', requestBody: { values: [[operaio, dato, tipo]] } });
    } else {
      await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.PUSHTOKENS, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[operaio, dato, tipo]] } });
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

// GET /impianti-operaio?operaio=Matteo
// Restituisce i codici impianto assegnati all'operaio dal foglio Assegnazione
app.get('/impianti-operaio', async (req, res) => {
  try {
    const { operaio } = req.query;
    if (!operaio) return res.json({ codici: [] });
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.ASSEGNAZIONE || 'Assegnazione');
    // Foglio Assegnazione: A=Codice, B=Descrizione, C=Comune, D=Operaio
    const codici = rows.slice(1)
      .filter(r => r[0] && r[3] && r[3].toString().trim() === operaio)
      .map(r => r[0].toString().trim().toUpperCase());
    res.json({ codici });
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
        // Nota di chiusura dell'operaio nella colonna dedicata O
        await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!O${rowNota+1}`, valueInputOption: 'RAW', requestBody: { values: [[notaChiusura]] } });
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
      const operaioTrim = (operaio || '').toString().trim();
      if (!operaioTrim) {
        // Richiesta senza operaio → entra nel contenitore: avvisa TUTTI gli operai
        await pushNotifica(sheets, OPERAI_TUTTI, '📦 Nuova richiesta nel contenitore', `${nomeImp} — ${tipoVisita} · ${dataFmt}`);
      } else {
        // Intervento assegnato a un operaio specifico
        await pushNotifica(sheets, [operaioTrim], '📋 Nuovo intervento assegnato', `${nomeImp} — ${tipoVisita} · ${dataFmt}`);
      }
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
    const corpo  = `${nome} — ${(note || '').slice(0,80)}`;
    const operaioTrim = (operaio || '').toString().trim();
    if (operaioTrim) {
      await pushNotifica(sheets, [operaioTrim], '🚨 Nuova segnalazione FMP', corpo);
    } else {
      // Operaio vuoto → la richiesta è nel contenitore: avvisa tutti
      await pushNotifica(sheets, OPERAI_TUTTI, '📦 Nuova richiesta nel contenitore', corpo);
    }
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

// GET /preventivi — stub per compatibilità con client vecchi
app.get('/preventivi', (req, res) => res.json({ preventivi: [] }));
app.post('/richiedi-preventivo', (req, res) => res.json({ ok: true, id: 'PREV-' + Math.random().toString(36).substring(2,10).toUpperCase() }));

// ============================================================
//  CONFIG PRESENZE — orari e parametri letti dal client nativo
//  Modifica qui gli orari senza dover ricompilare l'APK.
//  Il worker nativo Android chiama questo endpoint per sapere
//  a che ora rilevare la presenza.
// ============================================================
app.get('/config-presenze', (req, res) => {
  res.json({
    orari: ['07:15', '16:00'],   // orari di rilevamento (HH:MM, 24h)
    tolleranzaMin: 5,            // finestra ± minuti attorno a ogni orario
    giorni: 'sempre',
    attivo: true
  });
});

// ============================================================
//  PRESENZE GPS
//  Foglio Presenze: A=ID | B=Operaio | C=Data | D=Ora | E=Tipo
//                  F=Lat | G=Lon | H=ImpiantoPiuVicino | I=DistanzaKm | J=FuoriRaggio | K=LinkGMaps
// ============================================================

function distKm(lat1, lon1, lat2, lon2) {
  if (!lat1||!lon1||!lat2||!lon2) return 9999;
  const R = 6371;
  const dLat = (lat2-lat1)*Math.PI/180;
  const dLon = (lon2-lon1)*Math.PI/180;
  const a = Math.sin(dLat/2)**2 +
            Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

app.post('/registra-presenza', async (req, res) => {
  try {
    const { operaio, lat, lon, tipo } = req.body;
    // tipo: 'Arrivo' | 'Pausa' | 'Rientro' | 'Uscita'
    if (!operaio || lat == null || lon == null || !tipo)
      return res.json({ ok: false, errore: 'Parametri mancanti' });

    const sheets  = await getSheets();
    const oggi    = new Date();
    const dataStr = oggi.toLocaleDateString('en-CA', { timeZone: 'Europe/Rome' }); // yyyy-MM-dd in ora italiana
    const oraStr  = oggi.toLocaleTimeString('it-IT', { hour:'2-digit', minute:'2-digit', timeZone: 'Europe/Rome' });
    const id      = 'PRE-' + Math.random().toString(36).substring(2,10).toUpperCase();

    // Trova impianto più vicino tra quelli dell'operaio oggi
    const rInt = await leggi(sheets, SH.INTERVENTI);
    const rImp = await leggi(sheets, SH.IMPIANTI);

    // Impianti del giorno per questo operaio
    const impiantiOggi = rInt.slice(1).filter(r =>
      r[3] === operaio && r[2] && r[2].toString().slice(0,10) === dataStr
    ).map(r => r[1]); // codici impianto

    // Cerca coordinate impianti nel foglio Impianti (col F=Lat G=Lon se presenti)
    let impiantoPiuVicino = '', distanzaMin = 9999;
    rImp.slice(1).forEach(r => {
      const codice = r[0] ? r[0].toString().trim() : '';
      if (!impiantiOggi.includes(codice)) return;
      const iLat = parseFloat(r[5]);
      const iLon = parseFloat(r[6]);
      if (isNaN(iLat) || isNaN(iLon)) return;
      const d = distKm(parseFloat(lat), parseFloat(lon), iLat, iLon);
      if (d < distanzaMin) { distanzaMin = d; impiantoPiuVicino = codice; }
    });

    const fuoriRaggio = distanzaMin > 2 && impiantoPiuVicino !== '';
    const distStr     = distanzaMin < 9999 ? distanzaMin.toFixed(2) : '';

    // Salva nel foglio Presenze
    const gmapsLink = `https://maps.google.com/?q=${lat},${lon}`;
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: SH.PRESENZE,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[
        id, operaio, dataStr, oraStr, tipo,
        lat, lon, impiantoPiuVicino, distStr,
        fuoriRaggio ? 'SI' : 'NO', gmapsLink
      ]] },
    });
    if (fuoriRaggio && (tipo === 'Arrivo' || tipo === 'Rientro')) {
      const nomeImp = rImp.slice(1).find(r => r[0] === impiantoPiuVicino);
      const descImp = nomeImp ? nomeImp[1] : impiantoPiuVicino;
      await pushNotifica(sheets, ['1234'], // PIN responsabile come identificativo
        `⚠️ ${operaio} fuori raggio`,
        `${tipo} · ${oraStr} · ${distanzaMin.toFixed(1)}km da ${descImp}`
      ).catch(e => console.warn('Push responsabile fallita:', e.message));
    }

    res.json({ ok: true, id, distanzaKm: distStr, fuoriRaggio, impiantoPiuVicino });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// GET /presenze?operaio=Matteo&data=2026-05-01
app.get('/presenze', async (req, res) => {
  try {
    const { operaio, data } = req.query;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PRESENZE).catch(() => []);
    const presenze = rows.slice(1).filter(r =>
      r[0] &&
      (!operaio || r[1] === operaio) &&
      (!data    || r[2] === data)
    ).map(r => ({
      id: r[0]||'', operaio: r[1]||'', data: r[2]||'', ora: r[3]||'',
      tipo: r[4]||'', lat: r[5]||'', lon: r[6]||'',
      impiantoPiuVicino: r[7]||'', distanzaKm: r[8]||'',
      fuoriRaggio: r[9]==='SI',
    }));
    res.json({ presenze });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// ============================================================
//  OWNTRACKS RECEIVER
//  OwnTracks manda POST con payload JSON tipo:
//  { "_type":"location", "tid":"Matteo", "lat":43.9, "lon":12.9,
//    "tst":1234567890, "acc":10, "batt":80 }
//  tid = Device ID impostato nell'app = nome operaio
// ============================================================

const ORARI_PRESENZA_OT = [
  { ora: '07:30', tipo: 'Arrivo' },
  { ora: '12:30', tipo: 'Pausa' },
  { ora: '13:30', tipo: 'Rientro' },
  { ora: '16:30', tipo: 'Uscita' },
  { ora: '18:00', tipo: 'Uscita' },
];
const TOLLERANZA_MIN_OT = 5; // ±5 minuti dall'orario target

// ── Endpoint test OwnTracks — verifica connessione e ultimo payload ricevuto
// Chiamata: GET /owntracks-test
// Rimuovere dopo il collaudo
let _ultimoPayloadOT = null;
let _payloadPerUtente = {}; // { 'Davide': {...}, 'Marta': {...} }

app.get('/owntracks-test', (req, res) => {
  res.json({
    ok: true,
    messaggio: 'Endpoint OwnTracks attivo',
    orarioServer: new Date().toLocaleString('it-IT', { timeZone: 'Europe/Rome' }),
    orariRilevamento: ORARI_PRESENZA_OT.map(o => o.ora + ' → ' + o.tipo),
    tolleranzaMinuti: TOLLERANZA_MIN_OT,
    ultimoPayload: _ultimoPayloadOT,
    perUtente: Object.fromEntries(
      Object.entries(_payloadPerUtente).map(([nome, p]) => [nome, {
        ora: p._ricevutoAlle,
        lat: p.lat,
        lon: p.lon,
        batteria: p.batt + '%',
        gmaps: `https://maps.google.com/?q=${p.lat},${p.lon}`,
      }])
    ),
  });
});

function orarioItalia(date) {
  return date.toLocaleTimeString('it-IT', {
    hour: '2-digit', minute: '2-digit',
    timeZone: 'Europe/Rome'
  });
}

function dataItalia(date) {
  return date.toLocaleDateString('it-IT', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    timeZone: 'Europe/Rome'
  }).split('/').reverse().join('-'); // → yyyy-MM-dd
}

function minDiff(ora1, ora2) {
  // Differenza in minuti tra due orari HH:MM
  const [h1, m1] = ora1.split(':').map(Number);
  const [h2, m2] = ora2.split(':').map(Number);
  return Math.abs((h1*60+m1) - (h2*60+m2));
}

app.post('/owntracks', async (req, res) => {
  try {
    const payload = req.body;
    _ultimoPayloadOT = { ...payload, _ricevutoAlle: new Date().toLocaleString('it-IT', { timeZone: 'Europe/Rome' }) };

    // Salva anche per utente specifico (ricava nome dal topic)
    const nomeTemp = payload.topic ? payload.topic.split('/')[1] : payload.tid;
    if (nomeTemp) _payloadPerUtente[nomeTemp] = _ultimoPayloadOT;

    // OwnTracks manda vari tipi — interessa solo "location"
    if (!payload || payload._type !== 'location') {
      return res.json({ ok: true }); // risponde ok per non far retry
    }

    // Ricava nome operaio dal topic (owntracks/NomeOperaio/deviceid)
    // oppure dal campo tid come fallback
    let operaio = '';
    if (payload.topic) {
      const parti = payload.topic.split('/');
      // topic formato: owntracks/Username/DeviceID → parti[1] = Username
      if (parti.length >= 2) operaio = parti[1];
    }
    if (!operaio && payload.tid) operaio = payload.tid;
    const lat = payload.lat;
    const lon = payload.lon;

    if (!operaio || lat == null || lon == null) {
      return res.json({ ok: false, errore: 'Dati mancanti' });
    }

    // Calcola orario attuale in Italia
    const now    = new Date();
    const oraOra = orarioItalia(now); // es. "07:31"

    console.log(`[OwnTracks] ricevuto da topic:${payload.topic} → operaio:${operaio} @ ${oraOra}`);
    const match = ORARI_PRESENZA_OT.find(o => minDiff(oraOra, o.ora) <= TOLLERANZA_MIN_OT);
    if (!match) {
      console.log(`[OwnTracks] ${operaio} @ ${oraOra} — fuori finestra orari`);
      return res.json([]); // OwnTracks si aspetta array vuoto
    }

    const tipo    = match.tipo;
    const dataStr = dataItalia(now);
    const oraStr  = oraOra;

    // Controlla se questo tipo è già stato registrato oggi per questo operaio
    const sheets = await getSheets();
    const rPres  = await leggi(sheets, SH.PRESENZE).catch(() => []);
    const giaReg = rPres.slice(1).some(r =>
      r[1] === operaio && r[2] === dataStr && r[4] === tipo
    );

    if (giaReg) {
      console.log(`[OwnTracks] ${operaio} ${tipo} già registrato oggi`);
      return res.json([]); // OwnTracks si aspetta array vuoto
    }

    // Trova impianto più vicino tra quelli in programma oggi per questo operaio
    const rInt = await leggi(sheets, SH.INTERVENTI);
    const rImp = await leggi(sheets, SH.IMPIANTI);

    const impiantiOggi = rInt.slice(1)
      .filter(r => r[3] === operaio && r[2] && r[2].toString().slice(0,10) === dataStr)
      .map(r => r[1]);

    let impiantoPiuVicino = '', distanzaMin = 9999;
    rImp.slice(1).forEach(r => {
      const codice = r[0] ? r[0].toString().trim() : '';
      if (!impiantiOggi.includes(codice)) return;
      const iLat = parseFloat(r[5]);
      const iLon = parseFloat(r[6]);
      if (isNaN(iLat) || isNaN(iLon)) return;
      const d = distKm(parseFloat(lat), parseFloat(lon), iLat, iLon);
      if (d < distanzaMin) { distanzaMin = d; impiantoPiuVicino = codice; }
    });

    const fuoriRaggio = distanzaMin > 2 && impiantoPiuVicino !== '';
    const distStr     = distanzaMin < 9999 ? distanzaMin.toFixed(2) : '';
    const id          = 'PRE-' + Math.random().toString(36).substring(2,10).toUpperCase();

    // Salva nel foglio Presenze
    const gmapsLinkOT = `https://maps.google.com/?q=${lat},${lon}`;
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: SH.PRESENZE,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[
        id, operaio, dataStr, oraStr, tipo,
        lat, lon, impiantoPiuVicino, distStr,
        fuoriRaggio ? 'SI' : 'NO', gmapsLinkOT
      ]] },
    });

    console.log(`[OwnTracks] ✓ ${operaio} ${tipo} @ ${oraStr} — fuoriRaggio:${fuoriRaggio}`);

    // Notifica responsabile se fuori raggio
    if (fuoriRaggio && (tipo === 'Arrivo' || tipo === 'Rientro')) {
      const impRow  = rImp.slice(1).find(r => r[0] === impiantoPiuVicino);
      const descImp = impRow ? impRow[1] : impiantoPiuVicino;
      await pushNotifica(sheets, ['Responsabile'],
        `⚠️ ${operaio} fuori raggio`,
        `${tipo} · ${oraStr} · ${parseFloat(distStr).toFixed(1)}km da ${descImp}`
      ).catch(e => console.warn('Push responsabile:', e.message));
    }

    // OwnTracks si aspetta risposta vuota o array vuoto
    res.json([]);

  } catch (err) {
    console.error('[OwnTracks] Errore:', err.message);
    res.json([]); // risponde comunque per non far retry infiniti
  }
});

// ── Endpoint test presenza — forza registrazione ignorando orario
// POST /test-presenza  body: { operaio, lat, lon, tipo }
// RIMUOVERE DOPO IL COLLAUDO
// GET /test-presenza-tutti — registra posizione attuale di tutti gli operai tracciati
// RIMUOVERE DOPO IL COLLAUDO
app.get('/test-presenza-tutti', async (req, res) => {
  try {
    if (!Object.keys(_payloadPerUtente).length)
      return res.json({ ok: false, errore: 'Nessuna posizione in memoria — fai inviare la posizione da OwnTracks prima' });

    const sheets  = await getSheets();
    const now     = new Date();
    const dataStr = dataItalia(now);
    const oraStr  = orarioItalia(now);
    const tipo    = req.query.tipo || 'Test';
    const rInt    = await leggi(sheets, SH.INTERVENTI);
    const rImp    = await leggi(sheets, SH.IMPIANTI);
    const risultati = [];

    for (const [operaio, pos] of Object.entries(_payloadPerUtente)) {
      const lat = pos.lat;
      const lon = pos.lon;
      const id  = 'PRE-' + Math.random().toString(36).substring(2,10).toUpperCase();

      // Trova impianto più vicino
      const impiantiOggi = rInt.slice(1)
        .filter(r => r[3] === operaio && r[2] && r[2].toString().slice(0,10) === dataStr)
        .map(r => r[1]);

      let impiantoPiuVicino = '', distanzaMin = 9999;
      rImp.slice(1).forEach(r => {
        const codice = r[0] ? r[0].toString().trim() : '';
        if (!impiantiOggi.includes(codice)) return;
        const iLat = parseFloat(r[5]), iLon = parseFloat(r[6]);
        if (isNaN(iLat) || isNaN(iLon)) return;
        const d = distKm(parseFloat(lat), parseFloat(lon), iLat, iLon);
        if (d < distanzaMin) { distanzaMin = d; impiantoPiuVicino = codice; }
      });

      const fuoriRaggio   = distanzaMin > 2 && impiantoPiuVicino !== '';
      const distStr       = distanzaMin < 9999 ? distanzaMin.toFixed(2) : '';
      const gmapsLink     = `https://maps.google.com/?q=${lat},${lon}`;

      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID, range: SH.PRESENZE,
        valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [[
          id, operaio, dataStr, oraStr, tipo + '(TEST)',
          lat, lon, impiantoPiuVicino, distStr,
          fuoriRaggio ? 'SI' : 'NO', gmapsLink
        ]] },
      });

      risultati.push({ operaio, lat, lon, tipo, impiantoPiuVicino, distanzaKm: distStr, fuoriRaggio, gmapsLink, ora: pos.ora });
    }

    res.json({ ok: true, registrati: risultati.length, risultati });
  } catch(err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// ============================================================
//  GPSLOGGER RECEIVER
//  GPSLogger invia GET con parametri nell'URL:
//  /gpslogger?lat=43.9&lon=12.8&operaio=Matteo&batt=80&acc=10
// ============================================================

let _payloadGPSLogger = {}; // { 'Matteo': { lat, lon, batt, acc, ora } }

app.get('/gpslogger', async (req, res) => {
  try {
    const { lat, lon, operaio, batt, acc } = req.query;

    if (!operaio || lat == null || lon == null)
      return res.send('ok'); // GPSLogger si aspetta risposta testuale semplice

    const now    = new Date();
    const oraOra = orarioItalia(now);

    // Salva ultima posizione per utente
    _payloadGPSLogger[operaio] = {
      lat: parseFloat(lat), lon: parseFloat(lon),
      batt: batt || '?', acc: acc || '?',
      ora: now.toLocaleString('it-IT', { timeZone: 'Europe/Rome' }),
    };
    // Aggiorna anche _payloadPerUtente per compatibilità con test-presenza-tutti
    _payloadPerUtente[operaio] = _payloadGPSLogger[operaio];

    console.log(`[GPSLogger] ${operaio} @ ${oraOra} lat:${lat} lon:${lon} batt:${batt}%`);

    // Controlla se è un orario di rilevamento
    const match = ORARI_PRESENZA_OT.find(o => minDiff(oraOra, o.ora) <= TOLLERANZA_MIN_OT);
    if (!match) return res.send('ok'); // fuori finestra — solo aggiorna posizione

    const tipo    = match.tipo;
    const dataStr = dataItalia(now);
    const oraStr  = oraOra;

    // Anti-duplicato
    const sheets = await getSheets();
    const rPres  = await leggi(sheets, SH.PRESENZE).catch(() => []);
    const giaReg = rPres.slice(1).some(r =>
      r[1] === operaio && r[2] === dataStr && r[4] === tipo
    );
    if (giaReg) { console.log(`[GPSLogger] ${operaio} ${tipo} già registrato`); return res.send('ok'); }

    // Trova impianto più vicino
    const rInt = await leggi(sheets, SH.INTERVENTI);
    const rImp = await leggi(sheets, SH.IMPIANTI);
    const impiantiOggi = rInt.slice(1)
      .filter(r => r[3] === operaio && r[2] && r[2].toString().slice(0,10) === dataStr)
      .map(r => r[1]);

    let impiantoPiuVicino = '', distanzaMin = 9999;
    rImp.slice(1).forEach(r => {
      const codice = r[0] ? r[0].toString().trim() : '';
      if (!impiantiOggi.includes(codice)) return;
      const iLat = parseFloat(r[5]), iLon = parseFloat(r[6]);
      if (isNaN(iLat) || isNaN(iLon)) return;
      const d = distKm(parseFloat(lat), parseFloat(lon), iLat, iLon);
      if (d < distanzaMin) { distanzaMin = d; impiantoPiuVicino = codice; }
    });

    const fuoriRaggio = distanzaMin > 2 && impiantoPiuVicino !== '';
    const distStr     = distanzaMin < 9999 ? distanzaMin.toFixed(2) : '';
    const gmapsLink   = `https://maps.google.com/?q=${lat},${lon}`;
    const id          = 'PRE-' + Math.random().toString(36).substring(2,10).toUpperCase();

    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: SH.PRESENZE,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[
        id, operaio, dataStr, oraStr, tipo,
        lat, lon, impiantoPiuVicino, distStr,
        fuoriRaggio ? 'SI' : 'NO', gmapsLink
      ]] },
    });

    console.log(`[GPSLogger] ✓ ${operaio} ${tipo} @ ${oraStr} — fuoriRaggio:${fuoriRaggio}`);

    if (fuoriRaggio && (tipo === 'Arrivo' || tipo === 'Rientro')) {
      const impRow  = rImp.slice(1).find(r => r[0] === impiantoPiuVicino);
      const descImp = impRow ? impRow[1] : impiantoPiuVicino;
      await pushNotifica(sheets, ['Responsabile'],
        `⚠️ ${operaio} fuori raggio`,
        `${tipo} · ${oraStr} · ${parseFloat(distStr).toFixed(1)}km da ${descImp}`
      ).catch(() => {});
    }

    res.send('ok');
  } catch(err) {
    console.error('[GPSLogger] Errore:', err.message);
    res.send('ok'); // risponde sempre ok per non bloccare GPSLogger
  }
});

// GET /gpslogger-test — stato attuale di tutti gli operai tracciati via GPSLogger
app.get('/gpslogger-test', (req, res) => {
  res.json({
    ok: true,
    orarioServer: new Date().toLocaleString('it-IT', { timeZone: 'Europe/Rome' }),
    operaiTracciati: Object.keys(_payloadGPSLogger).length,
    perUtente: Object.fromEntries(
      Object.entries(_payloadGPSLogger).map(([nome, p]) => [nome, {
        ora: p.ora, lat: p.lat, lon: p.lon,
        batteria: p.batt + '%',
        accuratezza: p.acc + 'm',
        gmaps: `https://maps.google.com/?q=${p.lat},${p.lon}`,
      }])
    ),
  });
});

app.post('/test-presenza', async (req, res) => {
  try {
    const { operaio, lat, lon, tipo } = req.body;
    if (!operaio || lat == null || lon == null || !tipo)
      return res.json({ ok: false, errore: 'Parametri: operaio, lat, lon, tipo' });

    const sheets  = await getSheets();
    const now     = new Date();
    const dataStr = dataItalia(now);
    const oraStr  = orarioItalia(now);
    const id      = 'PRE-' + Math.random().toString(36).substring(2,10).toUpperCase();

    // Trova impianto più vicino
    const rInt = await leggi(sheets, SH.INTERVENTI);
    const rImp = await leggi(sheets, SH.IMPIANTI);
    const impiantiOggi = rInt.slice(1)
      .filter(r => r[3] === operaio && r[2] && r[2].toString().slice(0,10) === dataStr)
      .map(r => r[1]);

    let impiantoPiuVicino = '', distanzaMin = 9999;
    rImp.slice(1).forEach(r => {
      const codice = r[0] ? r[0].toString().trim() : '';
      if (!impiantiOggi.includes(codice)) return;
      const iLat = parseFloat(r[5]), iLon = parseFloat(r[6]);
      if (isNaN(iLat) || isNaN(iLon)) return;
      const d = distKm(parseFloat(lat), parseFloat(lon), iLat, iLon);
      if (d < distanzaMin) { distanzaMin = d; impiantoPiuVicino = codice; }
    });

    const fuoriRaggio = distanzaMin > 2 && impiantoPiuVicino !== '';
    const distStr     = distanzaMin < 9999 ? distanzaMin.toFixed(2) : '';

    const gmapsLinkTest = `https://maps.google.com/?q=${lat},${lon}`;
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: SH.PRESENZE,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[id, operaio, dataStr, oraStr, tipo + '(TEST)',
        lat, lon, impiantoPiuVicino, distStr, fuoriRaggio ? 'SI' : 'NO', gmapsLinkTest]] },
    });

    res.json({ ok: true, id, dataStr, oraStr, tipo, impiantoPiuVicino, distanzaKm: distStr, fuoriRaggio });
  } catch(err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.listen(PORT, () => console.log(`Siram Proxy attivo sulla porta ${PORT}`));
