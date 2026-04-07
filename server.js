const express    = require('express');
const cors       = require('cors');
const { google } = require('googleapis');

const app  = express();
const PORT = process.env.PORT || 3000;

// ============================================================
//  CONFIGURAZIONE
//  Su Render imposta queste variabili d'ambiente:
//  SHEET_ID            → ID del foglio Google Sheets
//  GOOGLE_CREDENTIALS  → contenuto del file JSON del service account (tutto su una riga)
// ============================================================
const SHEET_ID = process.env.SHEET_ID || '1JsQz8FiUMFGjFQ5tuodgjexxe1hE8UE87ORFDi_geWE';

const SH = {
  IMPIANTI:    'Impianti',
  CATALOGO:    'CatalogoAttivita',
  INTERVENTI:  'Interventi',
  CHECKLIST:   'ChecklistEsecuzione',
  ASSENZE:     'Assenze',
  PUSHTOKENS:  'PushTokens',
  PREVENTIVI:  'Preventivi',
  RDACAT:        'RdaCat',
  REPERIBILITA:  'Reperibilita',
};

// ============================================================
//  WEB PUSH
//  Su Render imposta anche:
//  VAPID_PUBLIC_KEY  → chiave pubblica VAPID
//  VAPID_PRIVATE_KEY → chiave privata VAPID
//  VAPID_EMAIL       → mailto:tua@email.com
//  Per generare le chiavi: node -e "require('web-push').generateVAPIDKeys()"
// ============================================================
const webpush = require('web-push');

if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(
    process.env.VAPID_EMAIL || 'mailto:admin@siram.it',
    process.env.VAPID_PUBLIC_KEY,
    process.env.VAPID_PRIVATE_KEY
  );
}

const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || '';

// Manda notifica push a una lista di operai
async function pushNotifica(sheets, operai, titolo, corpo) {
  if (!process.env.VAPID_PUBLIC_KEY) return; // skip se non configurato
  try {
    const rows = await leggi(sheets, SH.PUSHTOKENS).catch(() => []);
    const targets = rows.slice(1).filter(r => r[0] && operai.includes(r[0]));
    for (const row of targets) {
      try {
        const sub = JSON.parse(row[1]);
        await webpush.sendNotification(sub, JSON.stringify({
          title: titolo,
          body:  corpo,
          icon:  '/icon.svg',
        }));
      } catch(e) {
        // Token scaduto — rimuovi (silent)
        console.warn('Push fallita per', row[0], e.statusCode);
      }
    }
  } catch(e) {
    console.warn('pushNotifica error:', e.message);
  }
}


function getAuth() {
  const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
}

async function getSheets() {
  const auth = getAuth();
  return google.sheets({ version: 'v4', auth });
}

// ============================================================
//  MIDDLEWARE
// ============================================================
app.use(cors());
app.use(express.json());

// Health check
app.get('/', (req, res) => res.json({ ok: true, service: 'Siram Proxy' }));

// ============================================================
//  HELPER — leggi un foglio
// ============================================================
async function leggi(sheets, foglio) {
  const r = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: foglio,
  });
  return r.data.values || [];
}

// ============================================================
//  HELPER — formatta data da oggetto Date seriale Sheets
// ============================================================
function fmtData(val) {
  if (!val) return '';
  // Sheets restituisce le date come stringhe ISO o seriali
  try {
    const d = new Date(val);
    if (isNaN(d.getTime())) return '';
    return d.toISOString().slice(0, 10);
  } catch(e) { return ''; }
}

function fmtDateTime(val) {
  if (!val) return '';
  try {
    const d = new Date(val);
    if (isNaN(d.getTime())) return '';
    return d.toLocaleString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' });
  } catch(e) { return ''; }
}

// ============================================================
//  GET /vapid-public — restituisce la chiave pubblica VAPID
// ============================================================
app.get('/vapid-public', (req, res) => {
  res.json({ key: VAPID_PUBLIC });
});

// ============================================================
//  POST /registra-push
//  Body: { operaio, subscription }
// ============================================================
app.post('/registra-push', async (req, res) => {
  try {
    const { operaio, subscription } = req.body;
    if (!operaio || !subscription) return res.json({ ok: false });
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PUSHTOKENS).catch(() => []);

    // Cerca se esiste già una riga per questo operaio e aggiornala
    const idx = rows.findIndex((r,i) => i > 0 && r[0] === operaio);
    if (idx > 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SH.PUSHTOKENS}!A${idx+1}:B${idx+1}`,
        valueInputOption: 'RAW',
        requestBody: { values: [[operaio, JSON.stringify(subscription)]] },
      });
    } else {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: SH.PUSHTOKENS,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [[operaio, JSON.stringify(subscription)]] },
      });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /registra-push error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  GET /dati — tutti i dati per l'operaio
// ============================================================
app.get('/dati', async (req, res) => {
  try {
    const sheets = await getSheets();

    const [rImp, rCat, rInt, rChk] = await Promise.all([
      leggi(sheets, SH.IMPIANTI),
      leggi(sheets, SH.CATALOGO),
      leggi(sheets, SH.INTERVENTI),
      leggi(sheets, SH.CHECKLIST),
    ]);

    const impianti = rImp.slice(1).filter(r => r[0]).map(r => ({
      codice:         r[0] || '',
      descrizione:    r[1] || '',
      comune:         r[2] || '',
      indirizzo:      r[3] || '',
      operaioDefault: r[4] || '',
    }));

    const catalogo = rCat.slice(1).filter(r => r[0]).map(r => ({
      codiceImpianto: r[0] || '',
      tipoVisita:     r[1] || '',
      attivita:       r[2] || '',
      ordine:         Number(r[3]) || 0,
      obbligatoria:   r[4] || 'SI',
    }));

    const interventi = rInt.slice(1).filter(r => r[0]).map(r => ({
      id:                  r[0] || '',
      codiceImpianto:      r[1] || '',
      dataPrevista:        fmtData(r[2]),
      operaio:             r[3] || '',
      tipoVisita:          r[4] || '',
      stato:               r[5] || '',
      note:                r[6] || '',
      dataChiusura:        fmtData(r[7]),
      creatoIl:            fmtData(r[8]),
      secondoOperaio:      r[9] || '',
      interventoCollegato: r[10] || '',
    }));

    const checklist = rChk.slice(1).filter(r => r[0]).map(r => ({
      id:               r[0] || '',
      idIntervento:     r[1] || '',
      attivita:         r[2] || '',
      eseguita:         r[3] || 'NO',
      oraCompletamento: fmtDateTime(r[4]),
      note:             r[5] || '',
      extra:            r[6] || 'NO',
    }));

    res.json({ impianti, catalogo, interventi, checklist });
  } catch (err) {
    console.error('GET /dati error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /aggiorna-voce — spunta/nota singola voce checklist
//  Body: { id, eseguita?, note? }
// ============================================================
app.post('/aggiorna-voce', async (req, res) => {
  try {
    const { id, eseguita, note } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.CHECKLIST);

    const idx = rows.findIndex((r, i) => i > 0 && r[0] === id);
    if (idx === -1) return res.json({ ok: false, errore: 'Voce non trovata' });

    const rowNum = idx + 1; // 1-indexed per Sheets API

    if (eseguita !== undefined) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SH.CHECKLIST}!D${rowNum}`,
        valueInputOption: 'RAW',
        requestBody: { values: [[eseguita]] },
      });
      // Ora completamento
      const ora = eseguita === 'SI'
        ? new Date().toLocaleString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' })
        : '';
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SH.CHECKLIST}!E${rowNum}`,
        valueInputOption: 'RAW',
        requestBody: { values: [[ora]] },
      });
    }

    if (note !== undefined) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SH.CHECKLIST}!F${rowNum}`,
        valueInputOption: 'RAW',
        requestBody: { values: [[note]] },
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('POST /aggiorna-voce error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /aggiorna-intervento — cambia stato aperto/chiuso
//  Body: { id, stato }
// ============================================================
app.post('/aggiorna-intervento', async (req, res) => {
  try {
    const { id, stato, operaio } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const ora    = stato === 'Chiuso'
      ? new Date().toLocaleString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' })
      : '';

    // Helper: aggiorna stato (e data chiusura) di una singola riga
    async function aggiornaRiga(rigaId) {
      const i = rows.findIndex((r, idx) => idx > 0 && r[0] === rigaId);
      if (i < 1) return;
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SH.INTERVENTI}!F${i+1}`,
        valueInputOption: 'RAW',
        requestBody: { values: [[stato]] },
      });
      if (stato === 'Chiuso') {
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID,
          range: `${SH.INTERVENTI}!H${i+1}`,
          valueInputOption: 'RAW',
          requestBody: { values: [[ora]] },
        });
      }
    }

    // Salva nota di chiusura se presente (colonna G = Note)
    const notaChiusura = req.body.notaChiusura;
    if (stato === 'Chiuso' && notaChiusura) {
      const rowNota = rows.findIndex((r, idx) => idx > 0 && r[0] === id);
      if (rowNota > 0) {
        const notaEsistente = rows[rowNota][6] || '';
        const nuovaNota = notaEsistente
          ? notaEsistente + ' | Chiusura: ' + notaChiusura
          : 'Chiusura: ' + notaChiusura;
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID,
          range: `${SH.INTERVENTI}!G${rowNota+1}`,
          valueInputOption: 'RAW',
          requestBody: { values: [[nuovaNota]] },
        });
      }
    }

    // Aggiorna operaio se cambiato (colonna D = indice 3)
    if (operaio) {
      const rowOp = rows.findIndex((r, idx) => idx > 0 && r[0] === id);
      if (rowOp > 0) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID,
          range: `${SH.INTERVENTI}!D${rowOp+1}`,
          valueInputOption: 'RAW',
          requestBody: { values: [[operaio]] },
        });
      }
    }

    // Aggiorna l'intervento principale
    await aggiornaRiga(id);

    // Aggiorna l'eventuale intervento collegato (colonna K)
    const mainRow = rows.find((r, idx) => idx > 0 && r[0] === id);
    const collegato = mainRow && mainRow[10] ? mainRow[10] : null;
    if (collegato) await aggiornaRiga(collegato);

    // Se non è nella colonna K, cerca anche in direzione inversa
    // (intervento che ha questo id come collegato)
    const inverso = rows.find((r, idx) => idx > 0 && r[10] === id);
    if (inverso && inverso[0] !== id) await aggiornaRiga(inverso[0]);

    res.json({ ok: true });
  } catch (err) {
    console.error('POST /aggiorna-intervento error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /aggiungi-extra — aggiunge voce extra alla checklist
//  Body: { idIntervento, attivita }
// ============================================================
app.post('/aggiungi-extra', async (req, res) => {
  try {
    const { idIntervento, attivita } = req.body;
    const sheets = await getSheets();

    const id = 'CHK-' + Math.random().toString(36).substring(2, 10).toUpperCase();

    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: SH.CHECKLIST,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[id, idIntervento, attivita, 'NO', '', '', 'SI']] },
    });

    res.json({ ok: true, id });
  } catch (err) {
    console.error('POST /aggiungi-extra error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});


// ============================================================
//  GET /dati-responsabile — dati completi per la vista desktop
//  Include impianti, interventi, checklist, assenze, catalogo
// ============================================================
app.get('/dati-responsabile', async (req, res) => {
  try {
    const sheets = await getSheets();
    const [rImp, rCat, rInt, rChk, rAss] = await Promise.all([
      leggi(sheets, SH.IMPIANTI),
      leggi(sheets, SH.CATALOGO),
      leggi(sheets, SH.INTERVENTI),
      leggi(sheets, SH.CHECKLIST),
      leggi(sheets, SH.ASSENZE).catch(() => [[]]),
    ]);

    const impianti = rImp.slice(1).filter(r => r[0]).map(r => ({
      codice: r[0]||'', descrizione: r[1]||'', comune: r[2]||'',
      indirizzo: r[3]||'', operaioDefault: r[4]||'',
    }));
    const catalogo = rCat.slice(1).filter(r => r[0]).map(r => ({
      codiceImpianto: r[0]||'', tipoVisita: r[1]||'',
      attivita: r[2]||'', ordine: Number(r[3])||0, obbligatoria: r[4]||'SI',
    }));
    const interventi = rInt.slice(1).filter(r => r[0]).map(r => ({
      id: r[0]||'', codiceImpianto: r[1]||'', dataPrevista: fmtData(r[2]),
      operaio: r[3]||'', tipoVisita: r[4]||'', stato: r[5]||'',
      note: r[6]||'', dataChiusura: fmtData(r[7]), creatoIl: fmtData(r[8]),
      secondoOperaio: r[9]||'', interventoCollegato: r[10]||'',
    }));
    const checklist = rChk.slice(1).filter(r => r[0]).map(r => ({
      id: r[0]||'', idIntervento: r[1]||'', attivita: r[2]||'',
      eseguita: r[3]||'NO', oraCompletamento: fmtDateTime(r[4]),
      note: r[5]||'', extra: r[6]||'NO',
    }));
    const assenze = rAss.slice(1).filter(r => r[0]).map(r => ({
      id: r[0]||'', operaio: r[1]||'', dataInizio: fmtData(r[2]),
      dataFine: fmtData(r[3]), tipo: r[4]||'', note: r[5]||'',
    }));

    res.json({ impianti, catalogo, interventi, checklist, assenze });
  } catch (err) {
    console.error('GET /dati-responsabile error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /crea-intervento
//  Body: { codiceImpianto, dataPrevista, operaio, tipoVisita, note, attivitaExtra[] }
// ============================================================
app.post('/crea-intervento', async (req, res) => {
  try {
    const { codiceImpianto, dataPrevista, operaio, tipoVisita, note, attivitaExtra } = req.body;
    const statoIniziale       = req.body.statoOverride || 'Aperto';
    const dataFine            = req.body.dataFine || '';
    const operaioSecondario2  = req.body.operaioSecondario2 || '';
    const sheets = await getSheets();

    const id    = 'INT-' + Math.random().toString(36).substring(2, 10).toUpperCase();
    const oggi  = new Date().toLocaleDateString('it-IT');

    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: SH.INTERVENTI,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[
        id, codiceImpianto, dataPrevista, operaio, tipoVisita,
        statoIniziale, note||'', '', oggi, '',
        req.body.interventoCollegato || '', '', dataFine, operaioSecondario2
      ]] },
    });

    // Genera checklist dal catalogo
    const rCat = await leggi(sheets, SH.CATALOGO);
    const voci = rCat.slice(1)
      .filter(r => r[0] === codiceImpianto && r[1] === tipoVisita)
      .sort((a,b) => (Number(a[3])||0) - (Number(b[3])||0));

    const chkRows = voci.map(r => {
      const chkId = 'CHK-' + Math.random().toString(36).substring(2, 10).toUpperCase();
      return [chkId, id, r[2]||'', 'NO', '', '', 'NO'];
    });

    if (attivitaExtra && attivitaExtra.length > 0) {
      attivitaExtra.forEach(att => {
        const chkId = 'CHK-' + Math.random().toString(36).substring(2, 10).toUpperCase();
        chkRows.push([chkId, id, att, 'NO', '', '', 'SI']);
      });
    }

    if (chkRows.length > 0) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: SH.CHECKLIST,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values: chkRows },
      });
    }

    // Notifica push all'operaio assegnato (solo se non è un backlog)
    if (statoIniziale !== 'DaAssegnare') {
    const rImp = await leggi(sheets, SH.IMPIANTI);
    const impRow = rImp.slice(1).find(r => r[0] === codiceImpianto);
    const nomeImp = impRow ? impRow[1] : codiceImpianto;
    const dataFmt = new Date(dataPrevista+'T00:00:00').toLocaleDateString('it-IT',{weekday:'short',day:'numeric',month:'short'});
    await pushNotifica(sheets, [operaio],
      '📋 Nuovo intervento assegnato',
      `${nomeImp} — ${tipoVisita} · ${dataFmt}`
    );
    }

    res.json({ ok: true, id });
  } catch (err) {
    console.error('POST /crea-intervento error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /elimina-intervento
//  Body: { id }
// ============================================================
app.post('/elimina-intervento', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();

    // Elimina checklist collegata
    const rChk = await leggi(sheets, SH.CHECKLIST);
    const chkIdxs = rChk.map((r,i) => i).filter(i => i > 0 && rChk[i][1] === id).reverse();
    for (const idx of chkIdxs) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: {
          range: { sheetId: await getSheetId(sheets, SH.CHECKLIST), dimension: 'ROWS', startIndex: idx, endIndex: idx+1 }
        }}]},
      });
    }

    // Elimina intervento
    const rInt = await leggi(sheets, SH.INTERVENTI);
    const intIdx = rInt.findIndex((r,i) => i > 0 && r[0] === id);
    if (intIdx > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: {
          range: { sheetId: await getSheetId(sheets, SH.INTERVENTI), dimension: 'ROWS', startIndex: intIdx, endIndex: intIdx+1 }
        }}]},
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('POST /elimina-intervento error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /crea-assenza
//  Body: { operaio, dataInizio, dataFine, tipo, note }
// ============================================================
app.post('/crea-assenza', async (req, res) => {
  try {
    const { operaio, dataInizio, dataFine, tipo, note } = req.body;
    const sheets = await getSheets();
    const id = 'ASS-' + Math.random().toString(36).substring(2, 10).toUpperCase();

    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: SH.ASSENZE,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[id, operaio, dataInizio, dataFine, tipo, note||'']] },
    });

    res.json({ ok: true, id });
  } catch (err) {
    console.error('POST /crea-assenza error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /elimina-assenza
//  Body: { id }
// ============================================================
app.post('/elimina-assenza', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rAss = await leggi(sheets, SH.ASSENZE);
    const idx = rAss.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: {
          range: { sheetId: await getSheetId(sheets, SH.ASSENZE), dimension: 'ROWS', startIndex: idx, endIndex: idx+1 }
        }}]},
      });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /elimina-assenza error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /notifica-fmp — push notification da GAS per mail FMP
//  Body: { operaio, codiceImpianto, note, id }
// ============================================================
app.post('/notifica-fmp', async (req, res) => {
  try {
    const { operaio, codiceImpianto, note, id } = req.body;
    const sheets = await getSheets();
    const rImp   = await leggi(sheets, SH.IMPIANTI);
    const impRow = rImp.slice(1).find(r => r[0] === codiceImpianto);
    const nome   = impRow ? impRow[1] : codiceImpianto;
    await pushNotifica(sheets, [operaio],
      '🚨 Nuova segnalazione FMP',
      `${nome} — ${note.slice(0,80)}`
    );
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /notifica-fmp error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /imposta-collegamento
//  Body: { id, interventoCollegato }
// ============================================================
app.post('/imposta-collegamento', async (req, res) => {
  try {
    const { id, interventoCollegato } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false });
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SH.INTERVENTI}!K${idx+1}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[interventoCollegato]] },
    });
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /imposta-collegamento error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /segnala-secondo
//  Body: { id, secondoOperaio }  — secondoOperaio='' per rimuovere
// ============================================================
app.post('/segnala-secondo', async (req, res) => {
  try {
    const { id, secondoOperaio } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Intervento non trovato' });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SH.INTERVENTI}!J${idx+1}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[secondoOperaio]] },
    });

    // Notifica push al secondo operaio (se stiamo assegnando, non rimuovendo)
    if (secondoOperaio) {
      const row     = rows[idx];
      const codice  = row[1] || '';
      const data    = row[2] || '';
      const primario = row[3] || '';
      const rImp    = await leggi(sheets, SH.IMPIANTI);
      const impRow  = rImp.slice(1).find(r => r[0] === codice);
      const nomeImp = impRow ? impRow[1] : codice;
      const dataFmt = data ? new Date(data+'T00:00:00').toLocaleDateString('it-IT',{weekday:'short',day:'numeric',month:'short'}) : '';
      await pushNotifica(sheets, [secondoOperaio],
        '👥 Richiesto il tuo supporto',
        `${nomeImp} · ${dataFmt} — insieme a ${primario}`
      );
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('POST /segnala-secondo error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /posticipa-intervento
//  Body: { id, nuovaData }
// ============================================================
app.post('/posticipa-intervento', async (req, res) => {
  try {
    const { id, nuovaData } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Intervento non trovato' });
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SH.INTERVENTI}!C${idx+1}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[nuovaData]] },
    });
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /posticipa-intervento error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /salva-catalogo
// ============================================================
app.post('/salva-catalogo', async (req, res) => {
  try {
    const { codiceImpianto, tipoVisita, attivita, ordine, obbligatoria } = req.body;
    const sheets = await getSheets();
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: SH.CATALOGO,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[codiceImpianto, tipoVisita, attivita, ordine||1, obbligatoria||'SI']] },
    });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// ============================================================
//  POST /elimina-catalogo
// ============================================================
app.post('/elimina-catalogo', async (req, res) => {
  try {
    const { codice, tipoVisita, attivita } = req.body;
    const sheets = await getSheets();
    const rows = await leggi(sheets, SH.CATALOGO);
    const idx = rows.findIndex((r,i) => i > 0 && r[0]===codice && r[1]===tipoVisita && r[2]===attivita);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: {
          range: { sheetId: await getSheetId(sheets, SH.CATALOGO), dimension:'ROWS', startIndex:idx, endIndex:idx+1 }
        }}]},
      });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, errore: err.message }); }
});

// ============================================================
//  GET /preventivi — lista tutte le richieste preventivo
// ============================================================
app.get('/preventivi', async (req, res) => {
  try {
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PREVENTIVI).catch(() => []);
    const preventivi = rows.slice(1).filter(r => r[0]).map(r => ({
      id:             r[0] || '',
      idIntervento:   r[1] || '',
      idChecklist:    r[2] || '',
      codiceImpianto: r[3] || '',
      attivita:       r[4] || '',
      notaOperaio:    r[5] || '',
      operaio:        r[6] || '',
      stato:          r[7] || 'Richiesto',
      creatoIl:       r[8] || '',
      aggiornatoIl:   r[9] || '',
      linkDrive:      r[10] || '',
    }));
    res.json({ preventivi });
  } catch (err) {
    console.error('GET /preventivi error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /richiedi-preventivo
//  Body: { idIntervento, idChecklist, codiceImpianto, attivita, notaOperaio, operaio }
// ============================================================
app.post('/richiedi-preventivo', async (req, res) => {
  try {
    const { idIntervento, idChecklist, codiceImpianto, attivita, notaOperaio, operaio } = req.body;
    const sheets = await getSheets();
    const id     = 'PRE-' + Math.random().toString(36).substring(2, 10).toUpperCase();
    const oggi   = new Date().toLocaleDateString('it-IT');

    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: SH.PREVENTIVI,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[
        id, idIntervento, idChecklist, codiceImpianto,
        attivita, notaOperaio, operaio, 'Richiesto', oggi, ''
      ]] },
    });

    // Notifica push al responsabile — non abbiamo il token responsabile
    // ma possiamo loggare per ora
    console.log(`Preventivo richiesto da ${operaio} per ${codiceImpianto}: ${attivita}`);

    res.json({ ok: true, id });
  } catch (err) {
    console.error('POST /richiedi-preventivo error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /aggiorna-preventivo
//  Body: { id, stato }  stato: Richiesto|Inviato|Accettato|Rifiutato
// ============================================================
app.post('/aggiorna-preventivo', async (req, res) => {
  try {
    const { id, stato } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PREVENTIVI);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Preventivo non trovato' });

    const oggi = new Date().toLocaleDateString('it-IT');
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SH.PREVENTIVI}!H${idx+1}:J${idx+1}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[stato, rows[idx][8], oggi]] },
    });

    // Se accettato o rifiutato, sblocca la voce checklist collegata
    if (stato === 'Accettato' || stato === 'Rifiutato') {
      const idChecklist = rows[idx][2];
      if (idChecklist) {
        const rChk = await leggi(sheets, SH.CHECKLIST);
        const chkIdx = rChk.findIndex((r,i) => i > 0 && r[0] === idChecklist);
        if (chkIdx > 0) {
          // Rimuovi flag preventivo dalla nota (colonna F)
          const notaAttuale = rChk[chkIdx][5] || '';
          const nuovaNota = notaAttuale.replace('[PREVENTIVO]', '').trim();
          await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `${SH.CHECKLIST}!F${chkIdx+1}`,
            valueInputOption: 'RAW',
            requestBody: { values: [[nuovaNota]] },
          });
        }
      }
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('POST /aggiorna-preventivo error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /elimina-preventivo
//  Body: { id }
// ============================================================
app.post('/elimina-preventivo', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.PREVENTIVI);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: {
          range: { sheetId: await getSheetId(sheets, SH.PREVENTIVI), dimension:'ROWS', startIndex:idx, endIndex:idx+1 }
        }}]},
      });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /elimina-preventivo error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  GET /rdacat — lista richieste RDA/CAT
// ============================================================
app.get('/rdacat', async (req, res) => {
  try {
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.RDACAT).catch(() => []);
    const richieste = rows.slice(1).filter(r => r[0]).map(r => ({
      id:             r[0] || '',
      idIntervento:   r[1] || '',
      codiceImpianto: r[2] || '',
      tipologia:      r[3] || '',
      nota:           r[4] || '',
      operaio:        r[5] || '',
      stato:          r[6] || 'Inviata',
      creatoIl:       r[7] || '',
      aggiornatoIl:   r[8] || '',
    }));
    res.json({ richieste });
  } catch (err) {
    console.error('GET /rdacat error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /crea-rdacat
//  Body: { idIntervento, codiceImpianto, tipologia, nota, operaio }
// ============================================================
app.post('/crea-rdacat', async (req, res) => {
  try {
    const { idIntervento, codiceImpianto, tipologia, nota, operaio } = req.body;
    const sheets = await getSheets();
    const id     = 'RDA-' + Math.random().toString(36).substring(2, 10).toUpperCase();
    const oggi   = new Date().toLocaleDateString('it-IT');

    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: SH.RDACAT,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[
        id, idIntervento, codiceImpianto, tipologia, nota, operaio, 'Inviata', oggi, ''
      ]] },
    });

    // Notifica push al responsabile (non abbiamo token responsabile, logghiamo)
    console.log(`RDA/CAT da ${operaio} — ${tipologia} — ${codiceImpianto}`);

    res.json({ ok: true, id });
  } catch (err) {
    console.error('POST /crea-rdacat error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /aggiorna-rdacat
//  Body: { id, stato }  stato: Inviata|In lavorazione|Completata|Annullata
// ============================================================
app.post('/aggiorna-rdacat', async (req, res) => {
  try {
    const { id, stato } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.RDACAT);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'RDA non trovata' });
    const oggi = new Date().toLocaleDateString('it-IT');
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SH.RDACAT}!G${idx+1}:I${idx+1}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[stato, rows[idx][7], oggi]] },
    });
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /aggiorna-rdacat error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /elimina-rdacat
//  Body: { id }
// ============================================================
app.post('/elimina-rdacat', async (req, res) => {
  try {
    const { id } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.RDACAT);
    const idx    = rows.findIndex((r,i) => i > 0 && r[0] === id);
    if (idx > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ deleteDimension: {
          range: { sheetId: await getSheetId(sheets, SH.RDACAT), dimension:'ROWS', startIndex:idx, endIndex:idx+1 }
        }}]},
      });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /elimina-rdacat error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  GET /reperibile — operaio reperibile della settimana corrente
// ============================================================
app.get('/reperibile', async (req, res) => {
  try {
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.REPERIBILITA).catch(() => []);
    const oggi   = new Date();
    // Lunedì della settimana corrente
    const dow    = oggi.getDay() === 0 ? 6 : oggi.getDay() - 1;
    const lun    = new Date(oggi);
    lun.setDate(oggi.getDate() - dow);
    lun.setHours(0,0,0,0);
    const lunStr = lun.toISOString().slice(0,10);

    // Cerca riga con data lunedì della settimana corrente
    const riga = rows.slice(1).find(r => {
      if (!r[0]) return false;
      try {
        const d = new Date(r[0]);
        return d.toISOString().slice(0,10) === lunStr;
      } catch(e) { return false; }
    });

    // Lista settimane (ultime 4 + prossime 4)
    const settimane = [];
    for (let i = -2; i <= 6; i++) {
      const s = new Date(lun);
      s.setDate(lun.getDate() + i * 7);
      const sStr = s.toISOString().slice(0,10);
      const rigaS = rows.slice(1).find(r => {
        try { return new Date(r[0]).toISOString().slice(0,10) === sStr; } catch(e) { return false; }
      });
      settimane.push({ data: sStr, operaio: rigaS ? rigaS[1] : '' });
    }

    res.json({
      corrente:   { data: lunStr, operaio: riga ? riga[1] : null },
      settimane,
    });
  } catch (err) {
    console.error('GET /reperibile error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /salva-reperibile
//  Body: { data (lunedì settimana), operaio }
// ============================================================
app.post('/salva-reperibile', async (req, res) => {
  try {
    const { data, operaio } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.REPERIBILITA).catch(() => []);

    const idx = rows.findIndex((r,i) => {
      if (i === 0 || !r[0]) return false;
      try { return new Date(r[0]).toISOString().slice(0,10) === data; } catch(e) { return false; }
    });

    if (idx > 0) {
      // Aggiorna riga esistente
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SH.REPERIBILITA}!A${idx+1}:B${idx+1}`,
        valueInputOption: 'RAW',
        requestBody: { values: [[data, operaio]] },
      });
    } else {
      // Aggiungi nuova riga
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: SH.REPERIBILITA,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [[data, operaio]] },
      });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /salva-reperibile error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /salva-link-drive
//  Body: { id, tipo ('intervento'|'preventivo'), linkDrive }
// ============================================================
app.post('/salva-link-drive', async (req, res) => {
  try {
    const { id, tipo, linkDrive } = req.body;
    const sheets = await getSheets();
    const foglio = tipo === 'preventivo' ? SH.PREVENTIVI : SH.INTERVENTI;
    const colIdx = tipo === 'preventivo' ? 11 : 12; // K=11, L=12 (1-based)
    const rows   = await leggi(sheets, foglio);
    const idx    = rows.findIndex((r, i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Record non trovato' });
    const col = String.fromCharCode(64 + colIdx);
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${foglio}!${col}${idx+1}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[linkDrive]] },
    });
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /salva-link-drive error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// ============================================================
//  POST /aggiorna-multigiorno
//  Body: { id, dataFine, operaioSecondario2 }
// ============================================================
app.post('/aggiorna-multigiorno', async (req, res) => {
  try {
    const { id, dataFine, operaioSecondario2 } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);
    const idx    = rows.findIndex((r, i) => i > 0 && r[0] === id);
    if (idx < 1) return res.json({ ok: false, errore: 'Intervento non trovato' });
    // M=col13, N=col14 (1-based)
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SH.INTERVENTI}!M${idx+1}:N${idx+1}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[dataFine || '', operaioSecondario2 || '']] },
    });
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /aggiorna-multigiorno error:', err.message);
    res.status(500).json({ ok: false, errore: err.message });
  }
});

// Helper — ottieni sheetId numerico dal nome foglio
async function getSheetId(sheets, name) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = meta.data.sheets.find(s => s.properties.title === name);
  if (!sheet) throw new Error('Foglio non trovato: ' + name);
  return sheet.properties.sheetId;
}

// ============================================================
//  START
// ============================================================
app.listen(PORT, () => {
  console.log(`Siram Proxy attivo sulla porta ${PORT}`);
});
