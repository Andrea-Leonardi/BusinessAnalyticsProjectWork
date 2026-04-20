# Evaluation

La cartella `src/6.evaluation` contiene la fase finale di valutazione out-of-sample dei modelli addestrati in `src/4.modeling`, organizzata per settore.

L'obiettivo operativo di questa sezione non e` riaddestrare i modelli, ma:

- ricostruire in modo coerente il test set di ciascun settore;
- identificare il miglior modello settoriale sulla base dei risultati gia` prodotti in modeling;
- generare le predizioni out-of-sample a livello di ticker e data;
- esportare tali risultati in un CSV per l'analisi e la reportistica.

## Logica generale

La logica comune e` centralizzata nel file `src/6.evaluation/evaluation_data_prep.py`.

Questo modulo:

- carica il dataset di modeling tramite `cfg.MODELING_DATASET`;
- converte `WeekEndingFriday` in formato datetime;
- filtra il dataset sul `SectorCode` richiesto;
- rimuove le osservazioni con valori mancanti;
- ricostruisce il test set usando `split_temporal_dataframes(...)`;
- costruisce `X_test` eliminando target e variabili non utilizzate in prediction.

In particolare, nella costruzione delle feature vengono escluse:

- `AdjClosePrice_t+1_Up`, che e` il target;
- `WeekEndingFriday`, `Ticker` e `AdjClosePrice_t+1`;
- tutte le colonne che contengono i tag `EMO` o `TEXTBLOB`.

Questa scelta rende la valutazione coerente con la struttura usata nella pipeline di modeling e garantisce che il test set rimanga strettamente out-of-sample grazie allo split temporale.

## Organizzazione per settore

Ogni sottocartella settoriale, ad esempio `src/6.evaluation/11.Utilities`, contiene due script principali:

- `best_model.py`
- `predict_best_model_per_company.py`

La stessa struttura e` presente per tutti gli 11 settori:

1. `1.Basic Materials`
2. `2.Communication Services`
3. `3.Consumer Cyclical`
4. `4.Consumer Defensive`
5. `5.Energy`
6. `6.Financial Services`
7. `7.Healthcare`
8. `8.Industrials`
9. `9.Real Estate`
10. `10.Technology`
11. `11.Utilities`

## Selezione del miglior modello

Gli script `best_model.py` leggono il file `model_comparison.json` prodotto in modeling, nella cartella settoriale corrispondente sotto `src/4.modeling/classic_ML_model/.../orchestrator_results/`.

La selezione del best model segue questa regola:

- vengono esclusi dalla scelta finale `null_model`, `always_zero` e `always_one`;
- tra i modelli rimanenti viene selezionato quello con `test_accuracy` massima;
- il modello selezionato viene poi caricato dal relativo file salvato su disco.

I modelli supportati dal loader sono:

- `null_model`
- `lasso_logistic`
- `logistic_regression`
- `random_forest`
- `xgboost`
- `neural_network`

Per la rete neurale, `best_model.py` ricostruisce esplicitamente l'architettura PyTorch e carica i pesi dal checkpoint `.pt`.

## Benchmark considerati

Anche se i benchmark non possono essere eletti come best model finale, restano parte integrante del confronto.

Ogni `best_model.py` stampa infatti:

- il nome del miglior modello settoriale;
- la sua `test_accuracy`;
- il confronto con i benchmark `null_model`, `always_one` e `always_zero`;
- il delta di accuracy tra miglior modello e ciascun benchmark.

Di conseguenza, nella fase di evaluation la metrica decisionale effettivamente usata per scegliere il modello vincente e` la `test_accuracy`.

## Generazione delle predizioni out-of-sample

Gli script `predict_best_model_per_company.py`:

- richiamano `get_best_model_bundle()` dal corrispondente `best_model.py`;
- ricostruiscono il test set del settore tramite `build_sector_test_data(sector_code)`;
- applicano il modello migliore a `X_test`;
- assemblano un dataframe finale con `Ticker`, `WeekEndingFriday`, `AdjClosePrice_t+1_Up` e `predicted_AdjClosePrice_t+1_Up`.

La logica di prediction gestisce i diversi casi implementativi:

- per `null_model` usa direttamente `predict(X_test)`;
- per i modelli sklearn con `feature_names_in_` usa l'ordine delle feature salvato nel modello;
- se necessario legge le variabili selezionate dal file `lasso_model/selected_variables.csv`;
- per `neural_network` ricostruisce anche lo scaling a partire dai parametri salvati nel checkpoint.

## Output prodotti

Ogni script `predict_best_model_per_company.py` ora produce due output:

- stampa a schermo del dataframe finale di confronto;
- salvataggio del medesimo dataframe in `best_model_predictions_per_company.csv` nella cartella del settore.

Esempi:

- `src/6.evaluation/1.Basic Materials/best_model_predictions_per_company.csv`
- `src/6.evaluation/10.Technology/best_model_predictions_per_company.csv`
- `src/6.evaluation/11.Utilities/best_model_predictions_per_company.csv`

Questo rende la fase di evaluation immediatamente utilizzabile sia per controllo manuale sia per analisi successive nel report.

## Sintesi metodologica

In termini pratici, `src/6.evaluation` implementa il seguente flusso:

1. si ricostruisce il test set temporale del singolo settore;
2. si recuperano i risultati comparativi prodotti in modeling;
3. si sceglie il miglior modello non benchmark per `test_accuracy`;
4. si applica il modello al test set out-of-sample;
5. si esportano le predizioni finali per ticker e settimana.

Questa organizzazione mantiene separati modeling ed evaluation, ma li collega in modo coerente: la fase di evaluation non introduce nuove scelte arbitrarie, bensì riusa risultati, feature selection e artefatti salvati nella fase precedente per produrre una verifica finale out-of-sample a livello settoriale.
