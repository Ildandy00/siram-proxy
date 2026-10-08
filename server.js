const express    = require('express');
const cors       = require('cors');
const { google } = require('googleapis');

const app  = express();
const PORT = process.env.PORT || 3000;
require('./orari-coster')(app);
  
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
  CONTATORI:   'Contatori',
  LETTURE:     'Letture',
  CONFIG:      'Config',
  NOTEGIORNO:  'NoteGiorno',
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

  // Scarta destinatari vuoti/nulli (es. interventi nel Contenitore senza operaio):
  // inviare a external_id vuoto fa rifiutare la richiesta da OneSignal
  // ("alias_id's must be an array of non empty strings").
  const destinatari = (Array.isArray(operai) ? operai : [operai])
    .filter(o => o && o.toString().trim() !== '' && o.toString().trim() !== 'DaAssegnare');
  if (destinatari.length === 0) {
    console.log('pushNotifica: nessun destinatario valido, invio saltato');
    return;
  }

  try {
    const resp = await fetch('https://onesignal.com/api/v1/notifications', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Basic ' + ONESIGNAL_API_KEY
      },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        include_aliases: { external_id: destinatari },
        target_channel: 'push',
        headings: { en: titolo, it: titolo },
        contents: { en: corpo, it: corpo }
      })
    });
    const data = await resp.json().catch(() => ({}));
    if (data.errors) {
      console.warn('OneSignal errori:', JSON.stringify(data.errors));
    } else {
      console.log('OneSignal inviata a', destinatari.join(','), '— id:', data.id || '?');
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
    const interventi = rInt.slice(1).filter(r=>r[0]).map(r=>({ id:r[0]||'', codiceImpianto:r[1]||'', dataPrevista:fmtData(r[2]), operaio:r[3]||'', tipoVisita:r[4]||'', stato:r[5]||'', note:r[6]||'', dataChiusura:fmtData(r[7]), creatoIl:fmtData(r[8]), secondoOperaio:r[9]||'', interventoCollegato:r[10]||'', linkDrive:r[11]||'', dataFine:fmtData(r[12]), operaioSecondario2:r[13]||'', notaChiusura:r[14]||'', noteResponsabile:r[15]||'' }));
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
    const noteResponsabile = req.body.noteResponsabile;
    if (noteResponsabile !== undefined) {
      const rowNR = rows.findIndex((r,idx) => idx > 0 && r[0] === id);
      if (rowNR > 0) {
        // Nota di risoluzione del responsabile nella colonna dedicata P
        await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.INTERVENTI}!P${rowNR+1}`, valueInputOption: 'RAW', requestBody: { values: [[noteResponsabile]] } });
      }
    }
    if (operaio !== undefined) {
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
    const interventi = rInt.slice(1).filter(r=>r[0]).map(r=>({ id:r[0]||'', codiceImpianto:r[1]||'', dataPrevista:fmtData(r[2]), operaio:r[3]||'', tipoVisita:r[4]||'', stato:r[5]||'', note:r[6]||'', dataChiusura:fmtData(r[7]), creatoIl:fmtData(r[8]), secondoOperaio:r[9]||'', interventoCollegato:r[10]||'', linkDrive:r[11]||'', dataFine:fmtData(r[12]), operaioSecondario2:r[13]||'', notaChiusura:r[14]||'', noteResponsabile:r[15]||'' }));
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
// ── Notifica push ──
    // Se l'intervento è nel Contenitore (nessun operaio) → avvisa tutti e 4
    // gli operai, così qualcuno lo prende in carico. Altrimenti avvisa il singolo.
    if (statoIniziale !== 'DaAssegnare') {
      const rImp    = await leggi(sheets, SH.IMPIANTI);
      const impRow  = rImp.slice(1).find(r => r[0] === codiceImpianto);
      const nomeImp = impRow ? impRow[1] : codiceImpianto;
      const dataFmt = new Date(dataPrevista + 'T00:00:00').toLocaleDateString('it-IT', { weekday:'short', day:'numeric', month:'short' });

      const inContenitore = !operaio || operaio.toString().trim() === '';
      if (inContenitore) {
        await pushNotifica(sheets, ['Matteo', 'Stefano', 'Michele', 'Ezio', 'Aziz'],
          '📦 Nuova richiesta nel contenitore',
          `${nomeImp} — ${tipoVisita} · ${dataFmt} · da prendere in carico`);
      } else {
        await pushNotifica(sheets, [operaio],
          '📋 Nuovo intervento assegnato',
          `${nomeImp} — ${tipoVisita} · ${dataFmt}`);
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
    // Se la segnalazione non ha un operaio assegnato (es. impianto senza
    // operaio di default), avvisa tutti e 4 così qualcuno la prende in carico.
    const inContenitore = !operaio || operaio.toString().trim() === '' || operaio.toString().trim() === 'DaAssegnare';
    const destinatari = inContenitore ? ['Matteo', 'Stefano', 'Michele', 'Ezio', 'Aziz'] : [operaio];
    await pushNotifica(sheets, destinatari, '🚨 Nuova segnalazione FMP', `${nome} — ${note.slice(0,80)}`);
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
// ============================================================
//  REPERIBILITA'
//  Foglio Reperibilita: A = Data inizio (lunedi') | B = Operaio | C = Data fine
//  (C vuota = domenica della stessa settimana). Le colonne A e B restano
//  quelle lette da FmpMailProcessor.gs, quindi lo script mail non cambia.
// ============================================================
function isoGiorno(d) {
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}
function piuGiorni(iso, n) {
  const d = new Date(iso + 'T12:00:00'); d.setDate(d.getDate() + n); return isoGiorno(d);
}
function turniReperibilita(rows) {
  return rows.slice(1).map((r, i) => {
    const inizio = normData(r[0]);
    if (!inizio) return null;
    return { riga: i + 2, dataInizio: inizio, dataFine: normData(r[2]) || piuGiorni(inizio, 6),
             operaio: String(r[1] || '').trim() };
  }).filter(Boolean);
}

app.get('/reperibile', async (req, res) => {
  try {
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.REPERIBILITA).catch(() => []);
    const turni  = turniReperibilita(rows);
    const oggiIso = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Rome' });
    const oggi    = new Date(oggiIso + 'T12:00:00');
    const dow     = oggi.getDay() === 0 ? 6 : oggi.getDay() - 1;
    const lunIso  = piuGiorni(oggiIso, -dow);

    const settimane = [];
    for (let i = -2; i <= 6; i++) {
      const ini = piuGiorni(lunIso, i * 7), fin = piuGiorni(ini, 6);
      const t = turni.find(x => x.dataInizio === ini);
      settimane.push({ dataInizio: ini, dataFine: t ? t.dataFine : fin, data: ini, operaio: t ? t.operaio : '' });
    }
    const att = turni.filter(x => x.dataInizio <= oggiIso && x.dataFine >= oggiIso).pop();
    res.json({
      corrente: { dataInizio: lunIso, dataFine: piuGiorni(lunIso, 6), data: lunIso, operaio: att ? att.operaio : null },
      settimane
    });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

async function scriviReperibile(dataInizio, dataFine, operaio) {
  const ini = normData(dataInizio);
  if (!ini) throw new Error('Data di inizio non valida');
  const fin = normData(dataFine) || piuGiorni(ini, 6);
  const sheets = await getSheets();
  const rows   = await leggi(sheets, SH.REPERIBILITA).catch(() => []);
  if (!rows.length) {
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: SH.REPERIBILITA + '!A1:C1',
      valueInputOption: 'RAW', requestBody: { values: [['Data Lunedì', 'Operaio', 'Data fine']] } });
  }
  const t = turniReperibilita(rows).find(x => x.dataInizio === ini);
  if (t) {
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SH.REPERIBILITA}!A${t.riga}:C${t.riga}`,
      valueInputOption: 'RAW', requestBody: { values: [[ini, operaio, fin]] } });
  } else {
    await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.REPERIBILITA + '!A:C',
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: [[ini, operaio, fin]] } });
  }
}

// usato da responsabile.html
app.post('/imposta-reperibile', async (req, res) => {
  try {
    const { dataInizio, dataFine, operaio } = req.body;
    if (!operaio) return res.json({ ok: false, errore: 'Operaio mancante' });
    await scriviReperibile(dataInizio, dataFine, operaio);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// vecchio formato { data, operaio }
app.post('/salva-reperibile', async (req, res) => {
  try {
    await scriviReperibile(req.body.data, '', req.body.operaio);
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

// ============================================================
//  NOTE DEL GIORNO — appunti del responsabile su una data
//  Foglio NoteGiorno: ID | Data | Testo | Destinatari | Creato | Letto da
//  Destinatari vuoto = nota privata del responsabile.
//  L'app operaio vede solo le note in cui compare tra i destinatari.
// ============================================================
const NOTE_COLONNE = ['ID', 'Data', 'Testo', 'Destinatari', 'Creato', 'Letto da'];

async function noteFoglio(sheets) {
  try { return await leggi(sheets, SH.NOTEGIORNO); }
  catch (e) {
    await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: SH.NOTEGIORNO } } }] } });
    await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: SH.NOTEGIORNO + '!A1',
      valueInputOption: 'RAW', requestBody: { values: [NOTE_COLONNE] } });
    return [NOTE_COLONNE];
  }
}
const listaNomi = v => String(v || '').split(',').map(x => x.trim()).filter(Boolean);
function notaDaRiga(r) {
  return { id: r[0] || '', data: fmtData(r[1]) || String(r[1] || ''), testo: r[2] || '',
           destinatari: listaNomi(r[3]), creato: r[4] || '', lettoDa: listaNomi(r[5]) };
}

// GET /note-giorno                -> tutte (responsabile)
// GET /note-giorno?operaio=Matteo -> solo le sue, da oggi in avanti
app.get('/note-giorno', async (req, res) => {
  try {
    const sheets = await getSheets();
    const rows = await noteFoglio(sheets);
    let note = rows.slice(1).filter(r => r[0]).map(notaDaRiga);
    const op = String(req.query.operaio || '').trim();
    if (op) {
      const oggi = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Rome' });
      note = note.filter(n => n.destinatari.indexOf(op) >= 0 && n.data >= oggi);
    }
    note.sort((a, b) => a.data.localeCompare(b.data));
    res.json({ ok: true, note });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /salva-nota-giorno { id?, data, testo, destinatari: [] }
app.post('/salva-nota-giorno', async (req, res) => {
  try {
    const { data, testo } = req.body;
    const dest = (req.body.destinatari || []).map(String).filter(Boolean);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(data || '') || !String(testo || '').trim())
      return res.json({ ok: false, errore: 'Data e testo obbligatori' });
    const sheets = await getSheets();
    const rows = await noteFoglio(sheets);
    const ora = new Date().toLocaleString('it-IT', { timeZone: 'Europe/Rome' });
    let id = req.body.id, nuovi = dest;
    const idx = id ? rows.findIndex((r, i) => i > 0 && r[0] === id) : -1;
    if (idx > 0) {
      const prima = listaNomi(rows[idx][3]);
      nuovi = dest.filter(d => prima.indexOf(d) < 0);
      // testo cambiato: la nota torna "da leggere" per tutti
      const letti = String(rows[idx][2]) === String(testo) ? (rows[idx][5] || '') : '';
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID,
        range: `${SH.NOTEGIORNO}!B${idx + 1}:F${idx + 1}`, valueInputOption: 'RAW',
        requestBody: { values: [[data, testo, dest.join(','), rows[idx][4] || ora, letti]] } });
      if (!letti) nuovi = dest;
    } else {
      id = 'NOTA-' + Math.random().toString(36).substring(2, 10).toUpperCase();
      await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: SH.NOTEGIORNO,
        valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [[id, data, testo, dest.join(','), ora, '']] } });
    }
    if (nuovi.length) {
      const g = data.split('-').reverse().join('/');
      pushNotifica(sheets, nuovi, '📝 Nota per il ' + g, String(testo).slice(0, 120)).catch(() => {});
    }
    res.json({ ok: true, id });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /elimina-nota-giorno { id }
app.post('/elimina-nota-giorno', async (req, res) => {
  try {
    const sheets = await getSheets();
    const rows = await noteFoglio(sheets);
    const idx = rows.findIndex((r, i) => i > 0 && r[0] === req.body.id);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: [{ deleteDimension: {
        range: { sheetId: await getSheetId(sheets, SH.NOTEGIORNO), dimension: 'ROWS', startIndex: idx, endIndex: idx + 1 } } }] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /nota-letta { id, operaio } — l'operaio chiude il banner
app.post('/nota-letta', async (req, res) => {
  try {
    const { id, operaio } = req.body;
    const sheets = await getSheets();
    const rows = await noteFoglio(sheets);
    const idx = rows.findIndex((r, i) => i > 0 && r[0] === id);
    if (idx > 0 && operaio) {
      const letti = listaNomi(rows[idx][5]);
      if (letti.indexOf(operaio) < 0) letti.push(operaio);
      await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID,
        range: `${SH.NOTEGIORNO}!F${idx + 1}`, valueInputOption: 'RAW', requestBody: { values: [[letti.join(',')]] } });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// GET /preventivi — stub per compatibilità con client vecchi
app.get('/preventivi', (req, res) => res.json({ preventivi: [] }));
app.post('/richiedi-preventivo', (req, res) => res.json({ ok: true, id: 'PREV-' + Math.random().toString(36).substring(2,10).toUpperCase() }));

// ============================================================
//  SCADENZE RCEE — GET /scadenze-rcee
//  Legge il foglio "ScadenzeRCEE" (colonne individuate dal nome in
//  riga 1), raggruppa per Codice Impianto (un RCEE = un impianto),
//  e restituisce le scadenze ordinate per giorni mancanti crescenti.
// ============================================================
app.get('/scadenze-rcee', async (req, res) => {
  try {
    const sheets = await getSheets();
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'ScadenzeRCEE',
      valueRenderOption: 'FORMATTED_VALUE',
    });
    const rows = resp.data.values || [];
    if (rows.length < 2) return res.json({ ok: true, scadenze: [] });

    const norm = (s) => (s || '').toString().toLowerCase()
      .replace(/[àáâ]/g,'a').replace(/[èé]/g,'e').replace(/[ìí]/g,'i')
      .replace(/[òó]/g,'o').replace(/[ùú]/g,'u').replace(/[^a-z0-9]/g,'');
    const H = rows[0].map(norm);
    const find = (pred) => { for (let i=0;i<H.length;i++) if (pred(H[i])) return i; return -1; };
    const C = {
      targa:  find(h => h.indexOf('targat') >= 0),
      imp:    find(h => h === 'codiceimpianto' || (h.indexOf('impianto')>=0 && h.indexOf('codice')>=0 && h.indexOf('potenza')<0)),
      anag:   find(h => h.indexOf('anagrafica') >= 0),
      desc:   find(h => h.indexOf('descrizione') >= 0),
      comm:   find(h => h.indexOf('commessa') >= 0),
      alim:   find(h => h.indexOf('alimentazione') >= 0),
      pottot: find(h => h.indexOf('potenza')>=0 && h.indexOf('impianto')>=0),
      ult:    find(h => h.indexOf('ultimo') >= 0),
      per:    find(h => h.indexOf('periodic') >= 0),
      prox:   find(h => h.indexOf('prossima')>=0 || h.indexOf('scadenza')>=0),
      giorni: find(h => h.indexOf('giorni') >= 0),
      stato:  find(h => h.indexOf('stato') >= 0),
    };
    const g = (r, i) => (i >= 0 && r[i] !== undefined) ? r[i] : '';

    const perImpianto = {};
    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      const cod  = (g(r, C.imp)  || '').toString().trim();
      const anag = (g(r, C.anag) || '').toString().trim();
      if (!cod && !anag) continue;
      const stato = (g(r, C.stato) || '').toString().trim().toUpperCase();
      if (!(stato === 'OK' || stato === 'IN SCADENZA' || stato === 'SCADUTO')) continue;

      const key = cod || ('ANAG:' + anag);
      if (!perImpianto[key]) {
        perImpianto[key] = {
          codiceImpianto: cod,
          codiceAnagrafica: anag,
          targa: (g(r, C.targa) || '').toString().trim(),
          descrizione: (g(r, C.desc) || '').toString().trim(),
          commessa: (g(r, C.comm) || '').toString().trim(),
          alimentazione: (g(r, C.alim) || '').toString().trim(),
          potenzaImpianto: (g(r, C.pottot) || '').toString().trim(),
          dataUltimo: (g(r, C.ult) || '').toString().trim(),
          periodicita: (g(r, C.per) || '').toString().trim(),
          prossimaScadenza: (g(r, C.prox) || '').toString().trim(),
          giorni: parseInt((g(r, C.giorni) || '').toString().replace(/[^\-0-9]/g, ''), 10),
          stato: stato,
          nGeneratori: 0,
        };
      }
      perImpianto[key].nGeneratori++;
    }

    const scadenze = Object.values(perImpianto).sort((a, b) => {
      const ga = isNaN(a.giorni) ? 1e9 : a.giorni;
      const gb = isNaN(b.giorni) ? 1e9 : b.giorni;
      return ga - gb;
    });

    res.json({ ok: true, scadenze });
  } catch (err) {
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  LETTURE CONTATORI
//
//  Foglio "Contatori" (generato da CostruisciContatori.gs):
//   A=IdContatore | B=CodiceImpianto | C=CodiceIT | D=CodElem |
//   E=Fascia | F=Vettore | G=Tipo | H=DescrizioneElemento |
//   I=Unita | J=DescrizioneImport | K=UltimaLettura |
//   L=DataUltimaLettura | M=RigaImport | N=Attivo | O=Ordine | P=StatoMerge
//
//  Foglio "Letture" (storico, append/aggiorna):
//   A=ID | B=DataOra | C=IdContatore | D=CodiceImpianto | E=CodiceIT |
//   F=CodElem | G=Fascia | H=Operaio | I=Valore | J=Unita |
//   K=Evento | L=Commenti | M=MeseCompetenza | N=Lat | O=Lon |
//   P=LetturaPrecedente | Q=Consumo | R=StringaImport
//
//  La stringa di importazione nasce insieme alla riga:
//   CODICE_IT ; COD_ELEM ; FASCIA ; ; AAAAMMGG ; ; ; LETTURA ;
//   es. IT045643Y;5;;;20260724;;;6989;   (decimali con la virgola)
//
//  Data AAAAMMGG nella stringa (regola dal 23/09/2026):
//   1. DATA_CAMPAGNA_<mese> nel foglio Config, se presente (data unica
//      per tutte le righe del mese, quando il committente la impone);
//   2. altrimenti la data effettiva della lettura (colonna B DataOra).
//  Mai piu' "ultimo giorno della finestra": dava date nel futuro
//  (es. 20260924 per una lettura fatta il 23/09).
//  Contatore non letto = nessuna riga = nessuna stringa: l'assenza viene
//  segnalata come anomalia dal gestionale, invece di passare inosservata
//  come farebbe una lettura vecchia ridatata.
//
//  Foglio "Config": A=Chiave | B=Valore
//   GIORNI_LETTURA           default, es. "19,20,21,22,23,24"
//   GIORNI_LETTURA_2026-08   override del singolo mese (vince sul default)
//   DATA_CAMPAGNA_2026-08    data unica forzata nella stringa (facoltativa)
//   LETTURE_SEMPRE_APERTE    "SI" per disattivare il blocco sui giorni
//
//  I giorni cambiano di mese in mese: si aggiunge la riga del mese quando
//  vengono comunicati. Se la riga del mese manca, vale GIORNI_LETTURA.
//  Le stesse chiavi sono lette da GeneraImportazione.gs: server e script
//  devono vedere la stessa finestra.
// ============================================================

const GIORNI_LETTURA_DEFAULT = [19, 20, 21, 22, 23, 24];
const EVENTI_LETTURA = ['LETTURA NORMALE', 'GUASTO'];

// Data odierna in fuso italiano, formato yyyy-MM-dd
function oggiItalia() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Europe/Rome' });
}

// Numeri con virgola decimale: "1063,15" -> 1063.15 ; "1.234,5" -> 1234.5
function numLettura(v) {
  if (v === null || v === undefined || v === '') return null;
  if (typeof v === 'number') return v;
  let s = v.toString().trim();
  if (!s) return null;
  if (s.indexOf(',') >= 0) s = s.replace(/\./g, '').replace(',', '.');
  const n = parseFloat(s);
  return isNaN(n) ? null : n;
}

function normIntestazione(s) {
  return (s || '').toString().toLowerCase()
    .replace(/[àáâ]/g,'a').replace(/[èé]/g,'e').replace(/[ìí]/g,'i')
    .replace(/[òó]/g,'o').replace(/[ùú]/g,'u').replace(/[^a-z0-9]/g,'');
}

// Il valore dentro la stringa vuole la virgola: 215.9 -> "215,9"
function valoreStringa(v) {
  if (v === null || v === undefined || v === '') return '';
  const n = Math.round(Number(v) * 1000) / 1000;   // toglie il rumore dei float
  return String(n).replace('.', ',');
}

// Normalizza una data in 'yyyy-MM-dd'. Accetta 'yyyy-MM-dd',
// 'dd/MM/yyyy', 'dd/MM/yyyy, HH:mm' e 'yyyymmdd'. Stringa vuota se non valida.
function normData(v) {
  if (v === null || v === undefined) return '';
  const s = v.toString().trim();
  if (!s) return '';
  let m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (m) return m[1] + '-' + m[2].padStart(2, '0') + '-' + m[3].padStart(2, '0');
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return m[3] + '-' + m[2].padStart(2, '0') + '-' + m[1].padStart(2, '0');
  m = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (m) return m[1] + '-' + m[2] + '-' + m[3];
  return '';
}

/**
 * Stringa di importazione, stesso tracciato della formula nel file importazioni:
 *   =CONCATENA(N;O;P;Q;R;S;V;U;T;W;X;Y)
 *   CODICE_IT ; COD_ELEM ; FASCIA ; ; AAAAMMGG ; ; ; LETTURA ;
 * dataStringa: DATA_CAMPAGNA del mese se configurata, altrimenti la data
 * effettiva della lettura. Vuota = oggi (momento del salvataggio).
 */
function costruisciStringa(codiceIT, codElem, fascia, dataStringa, valore) {
  const aaaammgg = (normData(dataStringa) || oggiItalia()).replace(/-/g, '');
  return [
    codiceIT || '',
    codElem || '',
    fascia || '',
    '',
    aaaammgg,
    '',
    '',
    valoreStringa(valore),
    '',
  ].join(';');
}

// Individua le colonne per nome, con posizione di riserva
function mappaColonne(intestazioni, definizioni) {
  const H = (intestazioni || []).map(normIntestazione);
  const out = {};
  Object.keys(definizioni).forEach(campo => {
    const def = definizioni[campo];
    let idx = H.indexOf(normIntestazione(def.nome));
    if (idx < 0) idx = def.pos;
    out[campo] = idx;
  });
  return out;
}

const COL_CONTATORI = {
  id:        { nome: 'IdContatore',         pos: 0 },
  impianto:  { nome: 'CodiceImpianto',      pos: 1 },
  codiceIT:  { nome: 'CodiceIT',            pos: 2 },
  codElem:   { nome: 'CodElem',             pos: 3 },
  fascia:    { nome: 'Fascia',              pos: 4 },
  vettore:   { nome: 'Vettore',             pos: 5 },
  tipo:      { nome: 'Tipo',                pos: 6 },
  descr:     { nome: 'DescrizioneElemento', pos: 7 },
  unita:     { nome: 'Unita',               pos: 8 },
  ultima:    { nome: 'UltimaLettura',       pos: 10 },
  dataUltima:{ nome: 'DataUltimaLettura',   pos: 11 },
  attivo:    { nome: 'Attivo',              pos: 13 },
  ordine:    { nome: 'Ordine',              pos: 14 },
  statoMerge:{ nome: 'StatoMerge',          pos: 15 },
};

// "19,20,21 22-23" -> [19,20,21,22,23]
function parseGiorni(v) {
  const parsed = (v || '').toString().split(/[^0-9]+/)
    .map(x => parseInt(x, 10))
    .filter(n => !isNaN(n) && n >= 1 && n <= 31);
  return parsed.length ? Array.from(new Set(parsed)).sort((a, b) => a - b) : null;
}

// Mese successivo a 'yyyy-MM'
function meseDopo(mese) {
  let a = parseInt(mese.slice(0, 4), 10);
  let m = parseInt(mese.slice(5, 7), 10) + 1;
  if (m > 12) { m = 1; a++; }
  return a + '-' + String(m).padStart(2, '0');
}

// Legge la configurazione letture dal foglio Config (assente = default).
// I giorni possono essere definiti per singolo mese: la chiave del mese
// vince sul default generico.
async function configLetture(sheets) {
  const oggi       = oggiItalia();                    // yyyy-MM-dd
  const meseOggi   = oggi.slice(0, 7);
  const giornoOggi = parseInt(oggi.slice(8, 10), 10);

  let generici     = null;
  let sempreAperte = false;
  const perMese    = {};   // '2026-08' -> [giorni]
  const campagne   = {};   // '2026-08' -> '2026-08-24'

  try {
    const rows = await leggi(sheets, SH.CONFIG);
    rows.slice(1).forEach(r => {
      const k = (r[0] || '').toString().trim().toUpperCase();
      const v = (r[1] || '').toString().trim();
      if (!k || !v) return;

      if (k === 'LETTURE_SEMPRE_APERTE') {
        sempreAperte = v.toUpperCase() === 'SI';
      } else if (k === 'GIORNI_LETTURA') {
        generici = parseGiorni(v);
      } else if (k.indexOf('GIORNI_LETTURA_') === 0) {
        const m = k.slice('GIORNI_LETTURA_'.length);
        if (/^\d{4}-\d{2}$/.test(m)) {
          const g = parseGiorni(v);
          if (g) perMese[m] = g;
        }
      } else if (k.indexOf('DATA_CAMPAGNA_') === 0) {
        const m = k.slice('DATA_CAMPAGNA_'.length);
        const d = normData(v);
        if (/^\d{4}-\d{2}$/.test(m) && d) campagne[m] = d;
      }
    });
  } catch (e) {
    console.warn('Foglio Config assente o illeggibile — uso i giorni di default');
  }

  // Giorni validi per un dato mese, con la loro provenienza
  function giorniDi(mese) {
    if (perMese[mese]) return { giorni: perMese[mese], fonte: 'GIORNI_LETTURA_' + mese };
    if (generici)      return { giorni: generici,      fonte: 'GIORNI_LETTURA' };
    return { giorni: GIORNI_LETTURA_DEFAULT.slice(), fonte: 'default nel codice' };
  }

  const corrente   = giorniDi(meseOggi);
  const giorni     = corrente.giorni;
  const apertoOggi = sempreAperte || giorni.indexOf(giornoOggi) >= 0;

  // Prossimo giorno utile: in questo mese se ce n'è ancora uno, altrimenti
  // il primo del mese dopo — che può avere una finestra diversa.
  let prossimaFinestra = '';
  const prossimo = giorni.find(g => g >= giornoOggi);
  if (prossimo !== undefined) {
    prossimaFinestra = String(prossimo).padStart(2, '0') + '/' + meseOggi.slice(5, 7);
  } else {
    const mp = meseDopo(meseOggi);
    const gp = giorniDi(mp).giorni;
    if (gp.length) prossimaFinestra = String(gp[0]).padStart(2, '0') + '/' + mp.slice(5, 7);
  }

  // Data della stringa: DATA_CAMPAGNA del mese se configurata, altrimenti
  // resta vuota e ogni lettura prende la propria data effettiva.
  const dataCampagna = campagne[meseOggi] || '';
  const fonteData    = dataCampagna ? ('DATA_CAMPAGNA_' + meseOggi) : 'data effettiva della lettura';

  return {
    giorni,
    fonteGiorni: corrente.fonte,
    mesiConfigurati: Object.keys(perMese).sort(),
    dataCampagna,
    fonteData,
    campagne,
    sempreAperte,
    apertoOggi,
    oggi,
    giornoOggi,
    prossimaFinestra,
  };
}

// GET /config-letture
app.get('/config-letture', async (req, res) => {
  try {
    const sheets = await getSheets();
    const cfg    = await configLetture(sheets);
    res.json({ ok: true, ...cfg, eventi: EVENTI_LETTURA });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// GET /letture-dati
// Restituisce config + anagrafica contatori (con ultima lettura disponibile)
// + le letture del mese di competenza corrente.
app.get('/letture-dati', async (req, res) => {
  try {
    const sheets = await getSheets();
    const [rCon, rLet, cfg] = await Promise.all([
      leggi(sheets, SH.CONTATORI).catch(() => []),
      leggi(sheets, SH.LETTURE).catch(() => []),
      configLetture(sheets),
    ]);

    if (rCon.length < 2) {
      return res.json({ ok: true, ...cfg, contatori: [], letture: [],
        avviso: 'Foglio Contatori vuoto — lancia costruisciContatori() in Apps Script' });
    }

    const C = mappaColonne(rCon[0], COL_CONTATORI);
    const g = (r, i) => (i >= 0 && r[i] !== undefined && r[i] !== null) ? r[i].toString().trim() : '';

    // Storico letture indicizzato per contatore.
    // L'ultima lettura di riferimento e' l'ultima riga di un mese DIVERSO da
    // quello corrente: la lettura appena inserita non deve diventare la
    // "precedente" di se stessa. Stessa regola usata da /salva-lettura.
    const ultimaDaLetture = {};
    const lettureMese     = [];
    const meseCorrente    = cfg.oggi.slice(0, 7);

    rLet.slice(1).forEach(r => {
      const idc = (r[2] || '').toString().trim();
      if (!idc) return;
      const meseRiga = (r[12] || '').toString().trim();

      if (meseRiga !== meseCorrente) {
        ultimaDaLetture[idc] = {
          valore: numLettura(r[8]),
          data:   (r[1] || '').toString().trim(),
        };
      } else {
        lettureMese.push({
          id:          (r[0] || '').toString(),
          dataOra:     (r[1] || '').toString(),
          idContatore: idc,
          operaio:     (r[7] || '').toString(),
          valore:      numLettura(r[8]),
          evento:      (r[10] || '').toString(),
          commenti:    (r[11] || '').toString(),
          consumo:     numLettura(r[16]),
          stringa:     (r[17] || '').toString(),
        });
      }
    });

    const contatori = rCon.slice(1).filter(r => g(r, C.id)).map(r => {
      const id  = g(r, C.id);
      const ult = ultimaDaLetture[id];
      return {
        id,
        codiceImpianto: g(r, C.impianto),
        codiceIT:       g(r, C.codiceIT),
        codElem:        g(r, C.codElem),
        fascia:         g(r, C.fascia),
        vettore:        g(r, C.vettore),
        tipo:           g(r, C.tipo),
        descrizione:    g(r, C.descr),
        unita:          g(r, C.unita),
        ordine:         parseInt(g(r, C.ordine), 10) || 99,
        attivo:         (g(r, C.attivo) || 'SI').toUpperCase() !== 'NO',
        statoMerge:     g(r, C.statoMerge),
        ultimaLettura:      ult ? ult.valore : numLettura(g(r, C.ultima)),
        dataUltimaLettura:  ult ? ult.data   : g(r, C.dataUltima),
        origineUltima:      ult ? 'app' : 'import',
      };
    }).filter(c => c.attivo && c.codiceImpianto && c.statoMerge !== 'IMPIANTO NON TROVATO');

    res.json({ ok: true, ...cfg, contatori, letture: lettureMese, meseCorrente });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// POST /salva-lettura
// body: { idContatore, valore, evento, commenti, operaio, lat, lon }
// Una sola lettura per contatore per mese di competenza: se esiste già,
// la riga viene aggiornata invece di crearne una seconda.
app.post('/salva-lettura', async (req, res) => {
  try {
    const { idContatore, valore, evento, commenti, operaio, lat, lon } = req.body;
    if (!idContatore || !operaio) return res.json({ ok: false, errore: 'idContatore e operaio richiesti' });

    const val = numLettura(valore);
    if (val === null) return res.json({ ok: false, errore: 'Valore non numerico' });

    const sheets = await getSheets();
    const cfg    = await configLetture(sheets);
    if (!cfg.apertoOggi) {
      return res.json({ ok: false, chiuso: true,
        errore: 'Le letture sono aperte solo nei giorni ' + cfg.giorni.join(', ') + ' del mese' });
    }

    const rCon = await leggi(sheets, SH.CONTATORI).catch(() => []);
    if (rCon.length < 2) return res.json({ ok: false, errore: 'Foglio Contatori vuoto' });
    const C = mappaColonne(rCon[0], COL_CONTATORI);
    const g = (r, i) => (i >= 0 && r[i] !== undefined && r[i] !== null) ? r[i].toString().trim() : '';

    const riga = rCon.slice(1).find(r => g(r, C.id) === idContatore);
    if (!riga) return res.json({ ok: false, errore: 'Contatore non trovato: ' + idContatore });

    const rLet = await leggi(sheets, SH.LETTURE).catch(() => []);
    const mese = cfg.oggi.slice(0, 7);

    // Lettura precedente = ultima riga in Letture di un mese diverso,
    // altrimenti il valore di bootstrap dal file importazioni.
    let precedente = null;
    for (let i = rLet.length - 1; i >= 1; i--) {
      const r = rLet[i];
      if ((r[2] || '').toString().trim() !== idContatore) continue;
      if ((r[12] || '').toString().trim() === mese) continue;
      precedente = numLettura(r[8]);
      break;
    }
    if (precedente === null) precedente = numLettura(g(riga, C.ultima));

    // Lettura inferiore alla precedente: non si scrive, mai.
    if (precedente !== null && val < precedente) {
      return res.json({ ok: false, calo: true, precedente,
        errore: 'Lettura ' + valoreStringa(val) + ' inferiore alla precedente (' +
                valoreStringa(precedente) + '): non salvata. Ricontrolla il contatore.' });
    }

    const consumo = (precedente !== null && val >= precedente) ? +(val - precedente).toFixed(3) : '';
    const ora = new Date().toLocaleString('it-IT', {
      day:'2-digit', month:'2-digit', year:'numeric',
      hour:'2-digit', minute:'2-digit', timeZone:'Europe/Rome'
    });

    const eventoFinale = EVENTI_LETTURA.indexOf((evento || '').toUpperCase()) >= 0
      ? evento.toUpperCase() : EVENTI_LETTURA[0];

    // La stringa nasce insieme alla riga, con la data di campagna del mese
    const stringa = costruisciStringa(
      g(riga, C.codiceIT), g(riga, C.codElem), g(riga, C.fascia),
      cfg.dataCampagna, val
    );

    // Riga già presente per questo contatore nel mese corrente?
    const idxEsistente = rLet.findIndex((r, i) =>
      i > 0 &&
      (r[2] || '').toString().trim() === idContatore &&
      (r[12] || '').toString().trim() === mese
    );

    const valori = [
      idxEsistente > 0 ? (rLet[idxEsistente][0] || '') : ('LET-' + Math.random().toString(36).substring(2,10).toUpperCase()),
      ora,
      idContatore,
      g(riga, C.impianto),
      g(riga, C.codiceIT),
      g(riga, C.codElem),
      g(riga, C.fascia),
      operaio,
      val,
      g(riga, C.unita),
      eventoFinale,
      commenti || '',
      mese,
      lat != null ? lat : '',
      lon != null ? lon : '',
      precedente !== null ? precedente : '',
      consumo,
      stringa,
    ];

    if (idxEsistente > 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SH.LETTURE}!A${idxEsistente+1}:R${idxEsistente+1}`,
        valueInputOption: 'RAW', requestBody: { values: [valori] },
      });
    } else {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID, range: SH.LETTURE,
        valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [valori] },
      });
    }

    res.json({
      ok: true,
      id: valori[0],
      aggiornata: idxEsistente > 0,
      precedente,
      consumo,
      stringa,
      calo: (precedente !== null && val < precedente),
    });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

/**
 * GET /rigenera-stringhe?mese=2026-09   (vuoto = mese corrente, "tutti" = tutto lo storico)
 * Ricalcola la colonna R delle letture con la regola attuale:
 *   DATA_CAMPAGNA_<mese> se configurata, altrimenti la data effettiva della
 *   lettura presa dalla colonna B. Scrive solo le celle che cambiano.
 */
app.get('/rigenera-stringhe', async (req, res) => {
  try {
    const sheets = await getSheets();
    const cfg    = await configLetture(sheets);
    const q      = (req.query.mese || '').toString().trim().toLowerCase();
    const tutti  = q === 'tutti';
    const mese   = tutti ? '' : (q || cfg.oggi.slice(0, 7));
    if (!tutti && !/^\d{4}-\d{2}$/.test(mese)) {
      return res.json({ ok: false, errore: 'mese non valido: usa AAAA-MM oppure "tutti"' });
    }

    const rLet = await leggi(sheets, SH.LETTURE).catch(() => []);
    const dati = [];
    let esaminate = 0, senzaData = 0;

    rLet.forEach((r, i) => {
      if (i === 0) return;
      const meseRiga = (r[12] || '').toString().trim();
      if (!tutti && meseRiga !== mese) return;
      if (!(r[4] || '').toString().trim()) return;   // riga senza codice IT
      esaminate++;

      let data = cfg.campagne[meseRiga] || normData(r[1]);
      if (!data) { senzaData++; data = cfg.oggi; }

      const nuova = costruisciStringa(
        (r[4] || '').toString().trim(),
        (r[5] || '').toString().trim(),
        (r[6] || '').toString().trim(),
        data,
        numLettura(r[8])
      );
      const vecchia = (r[17] || '').toString().trim();
      if (nuova !== vecchia) dati.push({ riga: i + 1, vecchia, nuova });
    });

    if (dati.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: {
          valueInputOption: 'RAW',
          data: dati.map(d => ({ range: `${SH.LETTURE}!R${d.riga}`, values: [[d.nuova]] })),
        },
      });
    }

    res.json({
      ok: true,
      mese: tutti ? 'tutti' : mese,
      regola: (!tutti && cfg.campagne[mese]) ? ('DATA_CAMPAGNA_' + mese + ' = ' + cfg.campagne[mese])
                                             : 'data effettiva della lettura',
      esaminate,
      rigenerate: dati.length,
      senzaData,
      esempi: dati.slice(0, 5).map(d => d.vecchia + '  ->  ' + d.nuova),
    });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.listen(PORT, () => console.log(`Siram Proxy attivo sulla porta ${PORT}`));
