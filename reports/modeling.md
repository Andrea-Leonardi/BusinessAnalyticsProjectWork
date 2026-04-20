# Modeling

La cartella `src/4.modeling` contiene la pipeline di addestramento, validazione e confronto dei modelli supervisionati usati nel progetto.

Nello stato attuale del codice, la modeling pipeline implementata e` concentrata in:

- `src/4.modeling/splitters.py`
- `src/4.modeling/run_all_classic_models.py`
- `src/4.modeling/classic_ML_model/`

Questa sezione documenta quindi il comportamento reale del codice presente oggi in `src/4.modeling`, senza includere modelli non implementati nella pipeline corrente.

## Obiettivo della fase di modeling

L'obiettivo e` confrontare, per ciascun settore, diversi modelli di classificazione binaria sul target `AdjClosePrice_t+1_Up`, salvando:

- il modello addestrato;
- gli eventuali iperparametri ottimali;
- le metriche di validation e test;
- un riepilogo comparativo finale per settore.

La pipeline e` organizzata per settore e produce artefatti distinti nelle sottocartelle di `src/4.modeling/classic_ML_model`, ad esempio:

- `1.Basic Materials`
- `2.Communication Services`
- `3.Consumer Cyclical`
- `4.Consumer Defensive`
- `5.Energy`
- `6.Financial Services`
- `7.Healthcare`
- `8.Industrials`
- `9.Real Estate`
- `10.Technology`
- `11.Utilities`

## Split temporale

Lo split temporale e` definito in `src/4.modeling/splitters.py`.

La suddivisione e` la seguente:

- `2021-2024`: training
- `2025`: validation
- `2026`: test

Lo split avviene sulla colonna `WeekEndingFriday`, convertita in datetime. Questa scelta mantiene la coerenza out-of-sample della valutazione ed evita leakage temporale.

## Preparazione dei dataset

La logica di costruzione dei dataset e` centralizzata in `src/4.modeling/classic_ML_model/split_data.py`.

Questo modulo:

- carica il dataset di modeling tramite `cfg.MODELING_DATASET`;
- converte `WeekEndingFriday` in datetime;
- applica il filtro per settore tramite `SECTOR_FILTER`;
- rimuove le osservazioni con valori mancanti;
- costruisce train, validation e test con lo split temporale;
- genera anche un dataset `train_full = train + validation` per il retraining finale.

Le feature escluse in modo sistematico sono:

- `AdjClosePrice_t+1_Up`, che e` il target;
- `WeekEndingFriday`;
- `Ticker`;
- `AdjClosePrice_t+1`;
- tutte le colonne che contengono `EMO` o `TEXTBLOB`.

## Bilanciamento del training set

Il bilanciamento del target viene applicato solo ai dataset usati per addestrare i modelli, non ai dataset usati per valutarli.

In particolare:

- `train` viene bilanciato;
- `train_full` viene bilanciato;
- `validation` e `test` restano nella distribuzione originale.

Il bilanciamento e` implementato con sottocampionamento deterministico della classe maggioritaria, mantenendo una copertura distribuita per `Ticker` e anno quando possibile. La selezione delle righe della classe maggioritaria avviene in modo temporalmente distribuito, non con random undersampling puro.

Questo approccio consente di:

- ridurre il bias verso la classe dominante in addestramento;
- mantenere validation e test realistici;
- preservare la struttura panel-temporale del dataset.

## Orchestrazione della pipeline

L'esecuzione complessiva e` gestita da `src/4.modeling/run_all_classic_models.py`.

Lo script:

- definisce quali modelli attivare tramite flag `INCLUDE_*`;
- esegue in sequenza gli script di ciascun modello;
- legge i rispettivi `performance.json`;
- costruisce una classifica finale per `test_accuracy`;
- salva il riepilogo in `orchestrator_results/model_comparison.json` e `model_comparison.csv`.

L'ordine di esecuzione previsto e`:

- `null_model`
- `always_zero`
- `always_one`
- `lasso_logistic`
- `logistic_regression`
- `random_forest`
- `xgboost`
- `neural_network`

Inoltre sono definite dipendenze esplicite:

- `logistic_regression` richiede `lasso_logistic`
- `random_forest` richiede `lasso_logistic`
- `xgboost` richiede `lasso_logistic`
- `neural_network` richiede `lasso_logistic`

Questo perche' i modelli downstream riusano le variabili selezionate dal LASSO.

## Modelli effettivamente implementati

La pipeline corrente implementa i seguenti modelli.

### 1. Null model

Il `null_model` e` un `DummyClassifier(strategy="most_frequent")` addestrato su `X_train_full_unbalanced, y_train_full_unbalanced`.

Dato l'attuale sbilanciamento del dataset, questo modello rappresenta la baseline empirica che predice sempre la classe piu' frequente nel training full non bilanciato.

Artefatti principali:

- `null_model.joblib`
- `performance.json`

### 2. Always-zero benchmark

`always_zero_model` non richiede training: genera direttamente predizioni costanti pari a `0` sul test set e salva la performance.

Artefatto principale:

- `performance.json`

### 3. Always-one benchmark

`always_one_model` non richiede training: genera direttamente predizioni costanti pari a `1` sul test set e salva la performance.

Artefatto principale:

- `performance.json`

### 4. LASSO logistic

Il modello `lasso_model` usa una `LogisticRegression` con:

- penalizzazione `l1`
- solver `saga`
- standardizzazione tramite `StandardScaler`

La validation cerca il miglior `C` su una griglia prefissata, ottimizzando `accuracy` sul validation set.

La scelta del miglior candidato non usa solo lo score, ma anche:

- preferisce modelli con almeno una variabile selezionata rispetto a soluzioni completamente nulle;
- a parita' di score privilegia configurazioni piu' parsimoniose;
- in ulteriore parita' sceglie il `C` piu' piccolo.

Dopo la validation:

- il modello viene riaddestrato su `train_full` bilanciato;
- vengono estratte le variabili con coefficiente non nullo;
- le variabili selezionate vengono salvate in `selected_variables.csv`.

Artefatti principali:

- `best_C.json`
- `lasso_logistic_model.pkl`
- `selected_variables.csv`
- `performance.json`

### 5. Logistic regression

`logistic_regression` usa una regressione logistica standard con `StandardScaler`, ma non sull'intero set di feature: usa solo le variabili selezionate dal LASSO.

Il modello viene:

- addestrato su `train_full` bilanciato;
- salvato in `logistic_model.joblib`;
- valutato su test usando esclusivamente le feature selezionate dal LASSO.

E' presente anche `interpretation.py`, che stampa i coefficienti ordinati per valore assoluto per supportare una lettura economica del modello.

Artefatti principali:

- `logistic_model.joblib`
- `performance.json`

### 6. Random Forest

`random_forest` usa solo le variabili selezionate dal LASSO.

La validation esplora una griglia manuale su:

- `n_estimators`
- `max_depth`
- `min_samples_leaf`
- `max_features`

La scelta avviene in base a `accuracy` su validation. Il modello finale viene poi riaddestrato su `train_full` bilanciato e valutato su test.

Artefatti principali:

- `best_params.json`
- `random_forest_model.joblib`
- `performance.json`

### 7. XGBoost

`XGBoost` usa anch'esso solo le variabili selezionate dal LASSO.

La validation esplora una griglia manuale su:

- `n_estimators`
- `learning_rate`
- `max_depth`
- `min_child_weight`
- `subsample`
- `colsample_bytree`

Il classificatore usato e` `XGBClassifier` con:

- `objective="binary:logistic"`
- `eval_metric="logloss"`

Anche qui la selezione finale degli iperparametri usa `accuracy` su validation, seguita da retraining su `train_full` bilanciato e valutazione su test.

Artefatti principali:

- `best_params.json`
- `xgboost_model.joblib`
- `performance.json`

### 8. Neural network

La rete neurale e` una MLP PyTorch a due hidden layer con:

- due layer fully connected;
- attivazione `ReLU`;
- `Dropout`;
- output binario a 2 classi.

La validation usa Optuna con:

- `TPESampler(seed=42)`
- `MedianPruner`
- `100` trial

Gli iperparametri ottimizzati sono:

- `hidden_dim_1`
- `hidden_dim_2`
- `dropout`
- `learning_rate`
- `weight_decay`
- `batch_size`

Il training in validation include:

- standardizzazione delle feature;
- uso opzionale delle variabili selezionate dal LASSO, che nel codice attuale e` attivo;
- early stopping sulla validation loss con `patience = 10`.

Dopo la validation, il modello finale viene riaddestrato su `train_full` bilanciato per il numero di epoche ottimale osservato in validation e salvato insieme ai parametri dello scaler.

Artefatti principali:

- `best_params.json`
- `best_model_state.pt`
- `neural_network_model.pt`
- `performance.json`

## Metrica usata nella pipeline corrente

Nel codice attuale, la metrica decisionale effettivamente usata per validation, test e ranking finale e` `accuracy`.

Questo vale per:

- scelta del miglior `C` nel LASSO;
- selezione degli iperparametri di Random Forest;
- selezione degli iperparametri di XGBoost;
- ottimizzazione Optuna della rete neurale;
- classifica finale prodotta dall'orchestrator.

I file `performance.json` e `model_comparison.json` riportano infatti `metric: "accuracy"`.

## Output salvati per settore

Per ciascun settore, la pipeline salva una struttura di output come questa:

- `null_model/`
- `always_zero_model/`
- `always_one_model/`
- `lasso_model/`
- `logistic_regression/`
- `random_forest/`
- `XGBoost/`
- `neural network/`
- `orchestrator_results/`

In particolare:

- ogni modello salva il proprio `performance.json`;
- i modelli con tuning salvano anche `best_C.json` o `best_params.json`;
- i modelli addestrati salvano anche il file del modello;
- `orchestrator_results/model_comparison.json` contiene ranking finale, modello migliore, best accuracy, dataset sizes ed execution order;
- `orchestrator_results/model_comparison.csv` contiene un riepilogo tabellare delle performance.

## Sintesi operativa

Il flusso completo di `src/4.modeling` e` il seguente:

1. si carica il dataset di modeling;
2. si applica il filtro settoriale;
3. si costruisce lo split temporale train-validation-test;
4. si bilancia il training set in memoria;
5. si valida il LASSO e si salvano le variabili selezionate;
6. i modelli successivi riusano quelle variabili selezionate;
7. ogni modello viene addestrato su `train_full` bilanciato e valutato su test;
8. l'orchestrator produce il ranking finale per settore.

In altre parole, `src/4.modeling` implementa oggi una pipeline settoriale di classificazione tabellare che combina benchmark semplici, modelli lineari, ensemble tree-based e una rete neurale, con feature selection LASSO come passaggio comune per gran parte dei modelli non banali.
