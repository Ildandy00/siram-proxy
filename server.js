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
  CONTATORI:   'Contatori',
  LETTURE:     'Letture',
  CONFIG:      'Config',
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
        await pushNotifica(sheets, ['Matteo', 'Stefano', 'Michele', 'Ezio'],
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
//  Contatore non letto = nessuna riga = nessuna stringa: l'assenza viene
//  segnalata come anomalia dal gestionale, invece di passare inosservata
//  come farebbe una lettura vecchia ridatata.
//
//  Foglio "Config": A=Chiave | B=Valore
//   GIORNI_LETTURA           default, es. "19,20,21,22,23,24"
//   GIORNI_LETTURA_2026-08   override del singolo mese (vince sul default)
//   DATA_CAMPAGNA_2026-08    data che finisce nella stringa di importazione
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

/**
 * Stringa di importazione, stesso tracciato della formula nel file importazioni:
 *   =CONCATENA(N;O;P;Q;R;S;V;U;T;W;X;Y)
 *   CODICE_IT ; COD_ELEM ; FASCIA ; ; AAAAMMGG ; ; ; LETTURA ;
 * dataCampagna arriva come 'yyyy-MM-dd' ed e' UGUALE per tutte le righe del
 * mese: non e' la data in cui l'operaio ha letto.
 */
function costruisciStringa(codiceIT, codElem, fascia, dataCampagna, valore) {
  const aaaammgg = (dataCampagna || oggiItalia()).replace(/-/g, '');
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
        if (/^\d{4}-\d{2}$/.test(m)) campagne[m] = v;
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

  // Data di campagna del mese: se non configurata, l'ultimo giorno della finestra
  let dataCampagna = campagne[meseOggi] || '';
  let fonteData    = dataCampagna ? ('DATA_CAMPAGNA_' + meseOggi) : '';
  if (!dataCampagna && giorni.length) {
    dataCampagna = meseOggi + '-' + String(giorni[giorni.length - 1]).padStart(2, '0');
    fonteData    = 'ultimo giorno della finestra';
  }

  return {
    giorni,
    fonteGiorni: corrente.fonte,
    mesiConfigurati: Object.keys(perMese).sort(),
    dataCampagna,
    fonteData,
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
 * GET /rigenera-stringhe?mese=2026-07
 * Ricalcola la colonna R per tutte le letture di un mese usando la data di
 * campagna configurata adesso. Serve solo se DATA_CAMPAGNA viene cambiata
 * dopo che le letture sono già state raccolte: senza questo, le stringhe
 * resterebbero con la data vecchia.
 */
app.get('/rigenera-stringhe', async (req, res) => {
  try {
    const sheets = await getSheets();
    const cfg    = await configLetture(sheets);
    const mese   = (req.query.mese || cfg.oggi.slice(0, 7)).toString().trim();
    if (!/^\d{4}-\d{2}$/.test(mese)) return res.json({ ok: false, errore: 'mese non valido (AAAA-MM)' });

    // La data di campagna del mese richiesto può non essere quella corrente
    let dataCampagna = cfg.dataCampagna;
    if (mese !== cfg.oggi.slice(0, 7)) {
      const rows = await leggi(sheets, SH.CONFIG).catch(() => []);
      const riga = rows.slice(1).find(r =>
        (r[0] || '').toString().trim().toUpperCase() === 'DATA_CAMPAGNA_' + mese);
      if (riga && riga[1]) dataCampagna = riga[1].toString().trim();
      else return res.json({ ok: false, errore: 'DATA_CAMPAGNA_' + mese + ' non presente nel foglio Config' });
    }

    const rLet = await leggi(sheets, SH.LETTURE).catch(() => []);
    const dati = [];
    rLet.forEach((r, i) => {
      if (i === 0) return;
      if ((r[12] || '').toString().trim() !== mese) return;
      dati.push({
        riga: i + 1,
        stringa: costruisciStringa(
          (r[4] || '').toString().trim(),
          (r[5] || '').toString().trim(),
          (r[6] || '').toString().trim(),
          dataCampagna,
          numLettura(r[8])
        ),
      });
    });

    if (!dati.length) return res.json({ ok: true, mese, dataCampagna, rigenerate: 0 });

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        valueInputOption: 'RAW',
        data: dati.map(d => ({ range: `${SH.LETTURE}!R${d.riga}`, values: [[d.stringa]] })),
      },
    });

    res.json({
      ok: true, mese, dataCampagna,
      rigenerate: dati.length,
      esempi: dati.slice(0, 5).map(d => d.stringa),
    });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

app.listen(PORT, () => console.log(`Siram Proxy attivo sulla porta ${PORT}`));
