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
  IMPIANTI:   'Impianti',
  CATALOGO:   'CatalogoAttivita',
  INTERVENTI: 'Interventi',
  CHECKLIST:  'ChecklistEsecuzione',
};

// ============================================================
//  AUTH Google Sheets
// ============================================================
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
      id:             r[0] || '',
      codiceImpianto: r[1] || '',
      dataPrevista:   fmtData(r[2]),
      operaio:        r[3] || '',
      tipoVisita:     r[4] || '',
      stato:          r[5] || '',
      note:           r[6] || '',
      dataChiusura:   fmtData(r[7]),
      creatoIl:       fmtData(r[8]),
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
    const { id, stato } = req.body;
    const sheets = await getSheets();
    const rows   = await leggi(sheets, SH.INTERVENTI);

    const idx = rows.findIndex((r, i) => i > 0 && r[0] === id);
    if (idx === -1) return res.json({ ok: false, errore: 'Intervento non trovato' });

    const rowNum = idx + 1;

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SH.INTERVENTI}!F${rowNum}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[stato]] },
    });

    if (stato === 'Chiuso') {
      const ora = new Date().toLocaleString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SH.INTERVENTI}!H${rowNum}`,
        valueInputOption: 'RAW',
        requestBody: { values: [[ora]] },
      });
    }

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
//  START
// ============================================================
app.listen(PORT, () => {
  console.log(`Siram Proxy attivo sulla porta ${PORT}`);
});
