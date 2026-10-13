# Pratiche Siram — installazione

Tre pezzi: **server su Render** (pratiche.js), **gestionale** (responsabile.html), **pannello in Gmail** (questa cartella).
Il foglio Google non va toccato: alla prima chiamata il server aggiunge da solo le colonne nuove (Pratiche U→AH, Offerte J→L).
Le pratiche vecchie restano e vengono lette con i nuovi stati.

## 1. Server (Render)

1. Unisci la PR `pratiche-v2` del repo **siram-proxy** → Render ridistribuisce.
2. Render → servizio → *Environment* → aggiungi `ADDON_TOKEN` = una stringa a caso (es. quella che ti ho dato in chat).
   È la password tra il pannello Gmail e il server.
3. Se il piano Render è gratuito, il server si addormenta dopo 15 minuti: la prima analisi della giornata può dare
   "server non raggiungibile" → aspetta 30 secondi e premi **Riprova**.

## 2. Gestionale

Unisci la PR `pratiche-v2` del repo **siram-manutenzione** → Cloudflare pubblica la nuova sezione Pratiche.

## 3. Pannello Gmail (account Veolia)

1. Vai su <https://script.google.com> **con l'account davide.cori@veolia.com** → *Nuovo progetto* → chiamalo `Pratiche Siram`.
2. ⚙️ *Impostazioni progetto* → spunta **"Mostra il file manifest appsscript.json nell'editor"**.
3. Nell'editor:
   - sostituisci il contenuto di `appsscript.json` con quello di questa cartella;
   - sostituisci `Codice.gs` con `Code.gs` di questa cartella.
4. ⚙️ *Impostazioni progetto* → **Proprietà script** → aggiungi:
   | Proprietà | Valore |
   |---|---|
   | `ADDON_TOKEN` | la stessa stringa messa su Render |
   | `ANTHROPIC_API_KEY` | la tua chiave Claude (la stessa della pipeline DDT va bene) |

   Facoltative (hanno già il valore giusto): `PROXY_URL`, `CARTELLA_RADICE_ID`, `EMAIL_SEGRETERIA`, `CLAUDE_MODEL`, `GESTIONALE_URL`.
5. Seleziona la funzione **`provaInstallazione`** → *Esegui* → autorizza i permessi.
   Nel registro devi vedere il numero di cartelle impianto, la risposta del server e "chiave presente".
6. *Esegui il deployment* → **Deployment di prova** → *Installa*.
7. Ricarica Gmail: a destra compare l'icona **Pratiche Siram**.

Se Veolia blocca i componenti aggiuntivi non approvati (messaggio di errore all'installazione), dimmelo:
il ripiego è una pagina del gestionale dove si incolla il link della mail.

## Uso

Apri la mail → icona **Pratiche Siram** → **⚡ Analizza** → controlla → **✅ Conferma**.

| Mail | Cosa fa |
|---|---|
| Chiamata / segnalazione | crea la pratica e la sottocartella `Kxxx_Impianto_Titolo` |
| Tua richiesta d'offerta ai fornitori | segna "offerte richieste" e collega la conversazione |
| Risposta con offerta PDF | salva il PDF come `data_OFFERTA_Fornitore_nome.pdf` e registra fornitore, numero, importo |
| Buono d'ordine della segreteria | legge numero, data, fornitore, importo, consegna; aggancia la pratica tramite il n. d'offerta |
| Fornitore: "materiale disponibile" | segna il materiale come disponibile |

Nel gestionale, dalla pratica, assegni l'intervento: la nota per l'operaio è già compilata con lavoro, ubicazione,
problema, fornitore e stato del materiale. Quando l'operaio chiude l'intervento, la pratica si chiude da sola.
