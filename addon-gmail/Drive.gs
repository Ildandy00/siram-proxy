/**
 * PRATICHE SIRAM — pannello laterale in Google Drive
 * ------------------------------------------------------------
 * Selezioni la cartella di un lavoro (in "LAVORI in P2" o "Lavori IN CORSO")
 * → il pannello a destra legge buono d'ordine, DDT, offerte e foto
 * → controlli i dati → "Crea pratica" la scrive nel gestionale.
 * Usa le funzioni di Importa.gs (analizzaCartellaImport, salvaImportCartella).
 */

const MIME_CARTELLA = 'application/vnd.google-apps.folder';

function onDriveHomepage() {
  return CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader().setTitle('Pratiche Siram').setSubtitle('Da cartella Drive a pratica'))
    .addSection(CardService.newCardSection()
      .addWidget(CardService.newTextParagraph().setText('Seleziona la <b>cartella di un lavoro</b> (dentro "LAVORI in P2" o "Lavori IN CORSO"): qui compaiono i dati letti dai documenti e crei la pratica nel gestionale con un clic.'))
      .addWidget(campoTesto_('link', 'Oppure incolla il link della cartella', '', false, 'Il gestionale lo copia per te con "Completa dai documenti"'))
      .addWidget(CardService.newTextButton().setText('Apri cartella').setOnClickAction(CardService.newAction().setFunctionName('onDriveLink'))))
    .addSection(CardService.newCardSection()
      .addWidget(CardService.newTextButton().setText('Apri il gestionale').setOpenLink(CardService.newOpenLink().setUrl(cfg_('GESTIONALE_URL')))))
    .build();
}

function onDriveItemsSelected(e) {
  const items = (e.drive && e.drive.selectedItems) || [];
  const cart = items.find(x => x.mimeType === MIME_CARTELLA);
  if (!cart) {
    return CardService.newCardBuilder()
      .setHeader(CardService.newCardHeader().setTitle('Pratiche Siram'))
      .addSection(CardService.newCardSection().addWidget(CardService.newTextParagraph().setText(
        items.length ? 'Hai selezionato un file: seleziona la <b>cartella</b> del lavoro.' : 'Seleziona la cartella di un lavoro.')))
      .build();
  }
  return cardCartella_(cart.id, cart.title);
}

function onDriveLink(e) {
  const x = input_(e, 'link');
  const id = idDaLink_(x ? x[0] : '');
  if (!id) return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText('Link della cartella non valido')).build();
  try { return aggiornaCardPush_(cardCartella_(id, DriveApp.getFolderById(id).getName())); }
  catch (er) { return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText('⚠️ ' + er.message)).build(); }
}
function aggiornaCardPush_(card) {
  return CardService.newActionResponseBuilder().setNavigation(CardService.newNavigation().pushCard(card)).build();
}

// Prima schermata: nome letto, stato di collegamento, pulsante per leggere i documenti
function cardCartella_(id, titolo) {
  let n = leggiNomeCartella_(titolo, '');
  let tipo = /^\d{8}\s*PS/i.test(titolo) ? 'Extra' : 'Gestione';
  try {
    const f = DriveApp.getFolderById(id);
    const padri = f.getParents();
    while (padri.hasNext()) {
      const p = padri.next();
      if (p.getId() === cfg_('CARTELLA_EXTRA_ID')) { tipo = 'Extra'; break; }
      if (normK_(p.getName()) && titolo.indexOf(p.getName() + '_') === 0) { tipo = 'Gestione'; n = leggiNomeCartella_(titolo, p.getName()); }
    }
  } catch (er) { /* sola lettura del nome */ }
  let collegata = null, avviso = '';
  try { collegata = (server_('get', '/addon/contesto').cartelleCollegate || []).find(c => c.cartella === id) || null; }
  catch (er) { avviso = '⚠️ ' + er.message; }

  const sez = CardService.newCardSection()
    .addWidget(CardService.newDecoratedText().setTopLabel(tipo === 'Extra' ? 'Lavoro Extra' : 'Lavoro in gestione (P2)').setText(n.titolo || titolo).setWrapText(true))
    .addWidget(CardService.newDecoratedText().setTopLabel('Impianto').setText(((n.k || 'K?') + ' ' + (n.impianto || '')).trim()).setWrapText(true));
  if (n.data) sez.addWidget(CardService.newDecoratedText().setTopLabel('Data').setText(isoAIT_(n.data)));
  if (n.stato) sez.addWidget(CardService.newDecoratedText().setTopLabel('Stato nel nome').setText(n.stato).setWrapText(true));
  if (avviso) sez.addWidget(CardService.newTextParagraph().setText(avviso));

  const az = CardService.newAction().setFunctionName('onDriveAnalizza').setParameters({ id, pratica: collegata ? collegata.id : '' });
  if (collegata) {
    sez.addWidget(CardService.newTextParagraph().setText(`<font color="#8fbe7a">✓ Già nel gestionale: <b>${collegata.titolo || ''}</b> — ${collegata.stato || ''}</font>`));
    sez.addWidget(CardService.newTextButton().setText('📥 Completa dai documenti').setOnClickAction(az));
  } else {
    sez.addWidget(CardService.newTextButton().setText('⚡ Leggi i documenti e prepara la pratica').setTextButtonStyle(CardService.TextButtonStyle.FILLED).setOnClickAction(az));
  }
  sez.addWidget(CardService.newTextButton().setText('Apri il gestionale').setOpenLink(CardService.newOpenLink().setUrl(cfg_('GESTIONALE_URL'))));
  return CardService.newCardBuilder().setHeader(CardService.newCardHeader().setTitle('Pratiche Siram').setSubtitle(tronca_(titolo, 70))).addSection(sez).build();
}

function onDriveAnalizza(e) {
  const id = e.parameters.id, pratica = e.parameters.pratica || '';
  try {
    const prev = analizzaCartellaImport(id);
    CacheService.getUserCache().put('imp_' + id, JSON.stringify(prev), 1800);
    return aggiornaCard_(cardAnteprimaDrive_(prev, pratica));
  } catch (err) {
    return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText('⚠️ ' + err.message)).build();
  }
}

function cardAnteprimaDrive_(p, idPratica) {
  const card = CardService.newCardBuilder().setHeader(CardService.newCardHeader()
    .setTitle(idPratica ? 'Completa la pratica' : 'Nuova pratica dalla cartella').setSubtitle(`${p.documenti.length} documenti · ${p.numFoto} foto`));
  if (p.avvisi.length) card.addSection(CardService.newCardSection().addWidget(CardService.newTextParagraph().setText(p.avvisi.join('<br>'))));

  const selTL = CardService.newSelectionInput().setType(CardService.SelectionInputType.RADIO_BUTTON).setFieldName('tipoLavoro').setTitle('Tipo lavoro');
  selTL.addItem('In gestione (P2)', 'Gestione', p.tipoLavoro !== 'Extra').addItem('Extra canone', 'Extra', p.tipoLavoro === 'Extra');
  card.addSection(CardService.newCardSection().setHeader('Pratica')
    .addWidget(selTL)
    .addWidget(campoTesto_('titolo', 'Titolo lavoro *', p.titolo))
    .addWidget(campoTesto_('codiceImpianto', 'Codice impianto *', p.codiceImpianto))
    .addWidget(campoTesto_('nomeImpianto', 'Impianto', p.nomeImpianto))
    .addWidget(campoTesto_('dataChiamata', 'Data apertura (gg/mm/aaaa)', isoAIT_(p.dataChiamata)))
    .addWidget(campoTesto_('ubicazione', 'Ubicazione', p.ubicazione || ''))
    .addWidget(campoTesto_('problema', 'Problema / note', p.problema || (p.statoCartella ? 'Stato indicato nella cartella: ' + p.statoCartella : ''), true)));

  p.offerte.forEach((o, i) => {
    const inc = CardService.newSelectionInput().setType(CardService.SelectionInputType.CHECK_BOX).setFieldName('off_ok_' + i).addItem('Importa questa offerta', 'si', true);
    card.addSection(CardService.newCardSection().setHeader('🏷️ Offerta ' + (p.offerte.length > 1 ? i + 1 : '')).setCollapsible(p.offerte.length > 1)
      .addWidget(CardService.newTextParagraph().setText('📄 ' + tronca_(o.file, 60)))
      .addWidget(inc)
      .addWidget(campoTesto_('off_forn_' + i, 'Fornitore *', o.fornitore))
      .addWidget(campoTesto_('off_num_' + i, 'N. offerta', o.numeroOfferta))
      .addWidget(campoTesto_('off_imp_' + i, 'Importo € IVA escl.', o.importo))
      .addWidget(campoTesto_('off_data_' + i, 'Data (gg/mm/aaaa)', isoAIT_(o.data))));
  });
  if (p.bdo) {
    card.addSection(CardService.newCardSection().setHeader("📋 Buono d'ordine")
      .addWidget(CardService.newTextParagraph().setText('📄 ' + tronca_(p.bdo.file, 60)))
      .addWidget(CardService.newSelectionInput().setType(CardService.SelectionInputType.CHECK_BOX).setFieldName('bdo_ok').addItem("Importa il buono d'ordine", 'si', true))
      .addWidget(campoTesto_('bdo_num', 'N. ordine', p.bdo.numeroBdo))
      .addWidget(campoTesto_('bdo_data', 'Data (gg/mm/aaaa)', isoAIT_(p.bdo.dataBdo)))
      .addWidget(campoTesto_('bdo_forn', 'Fornitore', p.bdo.fornitoreBdo))
      .addWidget(campoTesto_('bdo_imp', 'Totale € IVA escl.', p.bdo.importoBdo))
      .addWidget(campoTesto_('bdo_cons', 'Consegna entro (gg/mm/aaaa)', isoAIT_(p.bdo.consegnaEntro))));
  }
  if (p.ddt) {
    card.addSection(CardService.newCardSection().setHeader('🚚 DDT — materiale consegnato')
      .addWidget(CardService.newTextParagraph().setText('📄 ' + tronca_(p.ddt.file, 60)))
      .addWidget(CardService.newSelectionInput().setType(CardService.SelectionInputType.CHECK_BOX).setFieldName('ddt_ok').addItem('Importa il DDT', 'si', true))
      .addWidget(campoTesto_('ddt_num', 'N. DDT', p.ddt.numeroDdt))
      .addWidget(campoTesto_('ddt_data', 'Data (gg/mm/aaaa)', isoAIT_(p.ddt.data)))
      .addWidget(campoTesto_('ddt_note', 'Note', p.ddt.note)));
  }
  if (!p.offerte.length && !p.bdo && !p.ddt) {
    card.addSection(CardService.newCardSection().addWidget(CardService.newTextParagraph().setText('Nessuna offerta, buono d\'ordine o DDT riconosciuto: la pratica viene creata con i soli dati del lavoro.')));
  }
  if (p.altri.length) {
    card.addSection(CardService.newCardSection().setHeader('Altri documenti').setCollapsible(true)
      .addWidget(CardService.newTextParagraph().setText(p.altri.map(x => '· ' + tronca_(x, 60)).join('<br>'))));
  }
  card.setFixedFooter(CardService.newFixedFooter().setPrimaryButton(CardService.newTextButton()
    .setText(idPratica ? '✅ Completa la pratica' : '✅ Crea pratica')
    .setOnClickAction(CardService.newAction().setFunctionName('onDriveSalva').setParameters({ id: p.cartella.id, pratica: idPratica || '' }))));
  return card.build();
}

function onDriveSalva(e) {
  const id = e.parameters.id, idPratica = e.parameters.pratica || '';
  const c = CacheService.getUserCache().get('imp_' + id);
  if (!c) return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText('Sessione scaduta: seleziona di nuovo la cartella')).build();
  const p = JSON.parse(c);
  const v = k => { const x = input_(e, k); return x ? x[0] : ''; };
  const spunta = k => !!input_(e, k);
  p.tipoLavoro = v('tipoLavoro') || p.tipoLavoro;
  ['titolo', 'codiceImpianto', 'nomeImpianto', 'ubicazione', 'problema'].forEach(k => { p[k] = v(k); });
  p.dataChiamata = dataITaISO_(v('dataChiamata')) || p.dataChiamata;
  p.offerte.forEach((o, i) => {
    o.escludi = !spunta('off_ok_' + i);
    o.fornitore = v('off_forn_' + i); o.numeroOfferta = v('off_num_' + i); o.importo = v('off_imp_' + i);
    o.data = dataITaISO_(v('off_data_' + i)) || o.data;
  });
  if (p.bdo) {
    p.bdo.escludi = !spunta('bdo_ok');
    p.bdo.numeroBdo = v('bdo_num'); p.bdo.fornitoreBdo = v('bdo_forn'); p.bdo.importoBdo = v('bdo_imp');
    p.bdo.dataBdo = dataITaISO_(v('bdo_data')) || p.bdo.dataBdo; p.bdo.consegnaEntro = dataITaISO_(v('bdo_cons')) || '';
  }
  if (p.ddt) {
    p.ddt.escludi = !spunta('ddt_ok');
    p.ddt.numeroDdt = v('ddt_num'); p.ddt.note = v('ddt_note'); p.ddt.data = dataITaISO_(v('ddt_data')) || p.ddt.data;
  }
  if (p.offerte.some(o => !o.escludi && !o.fornitore)) {
    return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText("Manca il fornitore di un'offerta (o togli la spunta)")).build();
  }
  try {
    const r = salvaImportCartella(p, idPratica);
    CacheService.getUserCache().remove('imp_' + id);
    const sez = CardService.newCardSection()
      .addWidget(CardService.newDecoratedText().setText('✓ ' + (idPratica ? 'Pratica completata' : 'Pratica creata') + ': ' + r.titolo).setWrapText(true))
      .addWidget(CardService.newDecoratedText().setTopLabel('Stato').setText(r.stato || ''))
      .addWidget(CardService.newTextButton().setText('Apri il gestionale').setOpenLink(CardService.newOpenLink().setUrl(cfg_('GESTIONALE_URL'))));
    return aggiornaCard_(CardService.newCardBuilder().setHeader(CardService.newCardHeader().setTitle('Fatto')).addSection(sez).build(), '✅ Fatto');
  } catch (err) {
    return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText('⚠️ ' + err.message)).build();
  }
}
