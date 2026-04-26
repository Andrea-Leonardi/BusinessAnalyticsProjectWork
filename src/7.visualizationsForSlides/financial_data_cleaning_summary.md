# Data Cleaning Su Dati FMP E Prezzi Azionari

## Scopo

Questo file riassume i punti del codice in cui viene fatta pulizia, normalizzazione o validazione dei dati finanziari FMP e dei dati di prezzo delle stock.

Nota: non elenco tutta la feature engineering del progetto. Mi concentro sulle operazioni che hanno natura di data cleaning, quality check, riallineamento, deduplica o gestione dei missing/outlier plausibili.

## 1. Selezione e pulizia dell'universo aziende FMP

### File
`src/1.dataExtraction/pipeline/1.FMP_companySelection.py`

### Attivita di pulizia

- Converte `date` e `marketCap` della market cap storica con `pd.to_datetime(..., errors="coerce")` e `pd.to_numeric(..., errors="coerce")`, trasformando i valori non validi in missing.
- Elimina le righe senza `date` o `marketCap` validi prima di scegliere la market cap storica di riferimento.
- Sui candidati attivi elimina righe con `symbol`, `companyName` o `sector` mancanti.
- Elimina i ticker vuoti o fatti solo di spazi.
- Sui delisted converte `ipoDate` e `delistedDate` a datetime con coercion degli errori.
- Filtra i delisted tenendo solo:
  - exchange USA (`NASDAQ`, `NYSE`)
  - aziende gia quotate alla data di riferimento
  - aziende delistate dopo la data di riferimento
- Elimina righe delisted con `symbol` o `companyName` mancanti.
- Deduplica i delisted per `symbol`.
- Deduplica l'universo combinato attivi + delisted per `symbol`.
- Pulisce il testo del `sector` con `strip()`.
- Mappa i settori a un codice stabile (`SectorCode`) e blocca il processo se trova settori sconosciuti.
- Converte `SectorCode` a numerico nullable (`Int64`).
- Normalizza i ticker finali con `strip().upper()`.
- Elimina ticker vuoti dopo la normalizzazione.
- Esclude manualmente ticker noti per creare problemi downstream.
- Deduplica per `companyName`, tenendo la riga con `marketCap` piu alta.

### Significato

Qui la pulizia serve soprattutto a costruire un universo coerente di aziende FMP, evitando ticker sporchi, duplicati, settori non mappati e market cap storiche non valide.

## 2. Pulizia dei ticker e dello schema dei prezzi

### File
`src/1.dataExtraction/pipeline/2.priceDataGathering.py`

### Attivita di pulizia

- Legge i ticker da `enterprises.csv` e li pulisce con:
  - rimozione dei missing
  - conversione a stringa
  - `strip()`
  - rimozione delle stringhe vuote
  - deduplica
- Valida i file prezzi gia esistenti verificando che siano leggibili e che contengano tutte le colonne attese.
- Se il download Yahoo Finance restituisce un dataset vuoto, il ticker viene scartato.
- Rimuove il timezone dall'indice con `tz_localize(None)`.
- Trasforma le date giornaliere nel calendario settimanale `WeekEndingFriday`.
- Ordina per `ActualTradingDate`, raggruppa per settimana e tiene solo l'ultima seduta disponibile della settimana.
- Dopo la costruzione di lag, return e rolling features applica `dropna()`.

### Effetto pratico del `dropna()`

Questo `dropna()` elimina le settimane che non hanno ancora abbastanza storico per:

- lag `t-1` e `t-2`
- target `t+1`
- rendimento a 4 settimane
- momentum a 12 settimane
- volatilita a 4 e 12 settimane
- drawdown a 12 settimane

Di fatto quindi il prezzo finale settimanale e gia ripulito da righe incomplete rispetto alle feature di mercato richieste.

## 3. Pulizia e armonizzazione del raw FMP prima del processing

### File
`src/1.dataExtraction/pipeline/3.FMP_financialsDataGathering.py`

### Attivita di pulizia

- Normalizza i ticker target con `strip().upper()`, rimuove vuoti e deduplica.
- Se esiste un raw storico, normalizza `requested_symbol` con `astype(str).str.strip().str.upper()`.
- Filtra il raw storico tenendo solo i ticker ancora presenti nell'universo valido.
- Gestisce colonne data miste (`date`, `filingDate`, `acceptedDate`, `fillingDate`) con una funzione dedicata che prova parsing multipli e converte gli errori in missing.
- Per ogni payload FMP inserisce sempre `requested_symbol`.
- Se il payload non contiene `symbol`, lo ricostruisce usando il ticker richiesto.
- Ordina ogni statement raw per `date` decrescente.
- Prima del merge deduplica ogni statement (`income`, `balance`, `cash_flow`, `enterprise_values`) sulle chiavi disponibili, tenendo la versione piu recente secondo `acceptedDate`, `filingDate`, `fillingDate`, `date`.
- Uniforma nomi di colonne alternative di FMP, ad esempio:
  - `netIncome` oppure `bottomLineNetIncome`
  - `totalStockholdersEquity` oppure `totalEquity`
  - `marketCapitalization` oppure `marketCap`
- Se uno statement fondamentale manca o e vuoto, il ticker viene considerato incompleto e non viene salvato.
- Il file raw finale viene riordinato per `requested_symbol` e `date`.

### Significato

Questo script fa soprattutto pulizia di schema e di coerenza del raw FMP: ticker uniformi, date parsate in modo robusto, campi equivalenti ricondotti a un unico nome e duplicati rimossi prima del merge.

## 4. Pulizia principale dei financial statements FMP allineati ai prezzi

### File
`src/1.dataExtraction/pipeline/4.FMP_financialsDataProcessing.py`

### Attivita di pulizia e normalizzazione

- Normalizza `requested_symbol` nel raw con `strip().upper()`.
- Filtra il raw FMP lasciando solo i ticker presenti nell'universo valido.
- Parsa robustamente tutte le colonne data raw (`date`, `filingDate`, `acceptedDate`).
- Valida i file prezzi usati come calendario settimanale, controllando presenza di `WeekEndingFriday` e `ClosePrice`.
- Costruisce il calendario settimanale comune usando le date prezzo ordinate e univoche.
- Per ogni statement sceglie una `public date` plausibile:
  - preferisce `acceptedDate`
  - poi `filingDate`
  - poi la massima tra le date note
- Se una `public date` risulta precedente alla `statement date`, la considera non affidabile e passa al fallback successivo.
- Se una riga non puo essere allineata a una `WeekEndingFriday`, viene eliminata con `dropna(subset=["WeekEndingFriday"])`.
- Deduplica i quarterly statements per `date`, tenendo la versione piu recente in base ai metadati di filing.
- Converte a numerico con `errors="coerce"` tutte le principali colonne contabili, trasformando valori sporchi in missing.
- Standardizza `capitalExpenditure` come flusso negativo usando `-abs(...)`, per correggere cambi di segno inconsistenti di FMP tra trimestri.
- Nelle divisioni usa `safe_divide()`, che trasforma i denominatori pari a zero in missing anziche generare infiniti o valori fuorvianti.
- Quando calcola l'average assets per le metriche TTM, trasforma in missing i valori minori o uguali a zero.
- Crea flag di zeri sospetti lato provider:
  - `CapexReportedZeroFlag`
  - `FreeCashFlowZeroFlag`
  - `FreeCashFlowTTMZeroFlag`
  - `SuspiciousTotalDebtZeroFlag`
- Tratta come sospetti i trimestri con `totalDebt = 0` ma con debito positivo sia nel trimestre precedente sia in quello successivo.
- Converte a missing alcune metriche derivate quando derivano da zeri sospetti:
  - `InvestmentIntensity`
  - `DebtToAssets`
  - `FreeCashFlowYield`
  - `FreeCashFlowYield_TTM`
- Quando piu righe finiscono sulla stessa `WeekEndingFriday`, deduplica tenendo il filing piu recente.
- Se la prima settimana del calendario prezzi non ha ancora valori finanziari, la riempie con l'ultima osservazione finanziaria precedente disponibile.
- Compila il ticker su tutte le righe settimanali con `fillna(ticker)`.
- Propaga `company_name` sulle righe settimanali quando disponibile.
- Elimina colonne tecniche o metadata non voluti nell'output finale:
  - `requested_symbol`
  - `date`
  - `fiscalYear`
  - `period`
  - `filingDate`
  - `acceptedDate`
  - `reportedCurrency`
  - `cik`
- Converte in blocco tutte le colonne finanziarie a numerico con coercion e poi applica `ffill()` per propagare l'ultimo dato trimestrale valido sulle settimane successive.
- Sui lag trimestrali usa flag espliciti di missing (`L1QMissingFlag`, `L2QMissingFlag`) per impedire che il forward fill faccia sembrare validi lag che in realta non esistono.
- Alla fine taglia il pannello finale al periodo di analisi da `2021-01-01` in poi.

### Significato

Questo e il punto in cui avviene la pulizia piu importante dei financials FMP: date credibili, numerici coerenti, gestione robusta dei missing, deduplica, correzione di segni incoerenti e neutralizzazione di zeri sospetti del provider.

## 5. Pulizia nel merge finale tra prezzi e financials

### File
`src/1.dataExtraction/pipeline/5.FMP_dataMerge.py`

### Attivita di pulizia

- Rilegge i ticker da `enterprises.csv` con rimozione missing, `strip()`, rimozione vuoti e deduplica.
- Deduplica il metadata enterprise per ticker.
- Sul blocco prezzi elimina le colonne con close non adjusted:
  - `ClosePrice`
  - `ClosePrice_t-1`
  - `ClosePrice_t-2`
  - `ClosePrice_t+1`
- Se il file prezzi non ha `Ticker`, lo crea; se ce l'ha ma con missing, li riempie con il ticker corrente.
- Se il file financials ha `symbol`, lo rinomina in `Ticker`.
- Se nel financial file manca il ticker, lo crea; se ci sono missing, li riempie con il ticker corrente.
- Deduplica sia prezzi sia financials per chiave `WeekEndingFriday + Ticker`.
- Ordina entrambi i dataset per ticker e data prima del merge.
- Dopo il merge elimina le colonne descrittive duplicate del nome azienda (`company_name`, `companyName`).
- Riordina le colonne finali mettendo davanti le variabili chiave di data, ticker e target.

### Significato

Qui la pulizia e soprattutto di schema finale: ticker coerenti, una sola riga per settimana-ticker, rimozione di colonne ridondanti e scelta esplicita della versione adjusted dei prezzi per il modeling.

## 6. Pulizia in lettura per dashboard e visualizzazioni

### File
`src/7.visualizationsForSlides/company_market_news_dashboard.py`

### Attivita di pulizia

- Normalizza il ticker in input con `upper().strip()`.
- Converte `WeekEndingFriday` a datetime con `errors="coerce"`.
- Elimina righe con `WeekEndingFriday` non valido.
- Ordina i prezzi per `WeekEndingFriday`.
- Converte `AdjClosePrice` o `ClosePrice` a numerico con `errors="coerce"`.
- Converte `WeeklyReturn_1W` a numerico con `errors="coerce"`.
- Se `WeeklyReturn_1W` non esiste, lo ricostruisce da `DisplayPrice.pct_change()`.
- Converte `DisplayPrice` e `WeeklyReturn` a `float`.
- Elimina righe senza `DisplayPrice`.

### Significato

Questa non e pulizia della pipeline dati principale, ma una pulizia locale di consumo per evitare che grafici e dashboard si rompano su date o prezzi non validi.

## 7. Operazioni trovate ma non classificate come cleaning principale

Ho trovato anche altri punti in cui i dataset finali vengono convertiti a datetime, ordinati o filtrati con `dropna()` nelle fasi di modeling/evaluation, ad esempio:

- `src/4.modeling/splitters.py`
- `src/4.modeling/classic_ML_model/split_data.py`
- `src/6.evaluation/evaluation_data_prep.py`

Non li considero pulizia specifica di raw FMP o raw prezzi, ma piuttosto preparazione del dataset finale per training e test.

## Sintesi finale

Le famiglie principali di data cleaning individuate nel codice sono:

1. Normalizzazione di ticker, stringhe e chiavi (`strip`, `upper`, rename di colonne equivalenti).
2. Parsing robusto di date e numerici con conversione degli errori in missing.
3. Eliminazione di record incompleti o non credibili (`dropna`, filtri su universo, filtri su date).
4. Deduplica di ticker, statement e righe settimana-ticker.
5. Riallineamento temporale tra dati trimestrali FMP e calendario settimanale dei prezzi.
6. Forward fill controllato dei financials per passare da trimestrale a settimanale.
7. Correzione di anomalie provider-specifiche, soprattutto su segni e zeri sospetti.
8. Pulizia dello schema finale per modeling, con rimozione di colonne ridondanti o non desiderate.
