# Data Preparation

La preparazione dei dati per modeling ed evaluation usa uno split temporale definito nel file `src/4.modeling/splitters.py`.

## Logica dello split

La funzione principale e` `split_temporal_dataframes(df, date_col="WeekEndingFriday")`.

Questa funzione:

- crea una copia del dataframe;
- converte la colonna data in formato datetime;
- rimuove le righe con valori mancanti;
- divide il dataset in tre blocchi temporali distinti.

Lo split applicato e` il seguente:

- `2021-2024`: training set
- `2025`: validation set
- `2026`: test set

In formula:

- `df_train`: osservazioni con anno tra 2021 e 2024 inclusi
- `df_validation`: osservazioni con anno uguale a 2025
- `df_test`: osservazioni con anno uguale a 2026

## Motivazione metodologica

Questo approccio evita leakage temporale e mantiene la valutazione coerente con un contesto predittivo reale: i modelli vengono addestrati sul passato, calibrati su un periodo successivo e testati su dati ancora piu' futuri.

Lo split temporale e` particolarmente importante in questo progetto perche':

- le osservazioni hanno una struttura panel `azienda-settimana`;
- molte feature incorporano dinamiche persistenti nel tempo;
- osservazioni vicine temporalmente non possono essere trattate come indipendenti in uno split casuale classico.

## Funzioni disponibili in `splitters.py`

Il modulo espone tre funzioni.

### `split_temporal_dataframes(...)`

Restituisce direttamente tre dataframe:

- `df_train`
- `df_validation`
- `df_test`

Viene usata quando serve mantenere la struttura completa del dataset.

### `split_dataframe_by_date(...)`

Riceve un dataframe gia` caricato e costruisce:

- `X_train`, `y_train`
- `X_validation`, `y_validation`
- `X_test`, `y_test`

Questa funzione:

- richiama internamente `split_temporal_dataframes(...)`;
- separa target e covariate;
- permette di escludere colonne tramite `exclude_vars`.

### `split_data_by_date(...)`

E` una funzione wrapper che:

- legge un CSV da disco;
- richiama `split_dataframe_by_date(...)`;
- restituisce direttamente i sei oggetti `X/y` per train, validation e test.

## Uso nella pipeline

Nella pipeline di modeling, questo split viene poi riusato da `src/4.modeling/classic_ML_model/split_data.py`, che aggiunge:

- filtro settoriale;
- esclusione delle variabili non utilizzate;
- bilanciamento del training set;
- costruzione di `train_full = train + validation` per il retraining finale.

Anche la fase di evaluation rimane coerente con questa impostazione: in `src/6.evaluation/evaluation_data_prep.py` viene infatti riutilizzata la stessa logica di split temporale per ricostruire il test set out-of-sample.

## Sintesi

La regola di split del progetto e` quindi semplice e stabile:

1. si ordina logicamente il dataset nel tempo tramite `WeekEndingFriday`;
2. si usa il periodo `2021-2024` per addestrare;
3. si usa `2025` per validare;
4. si usa `2026` per il test finale.

Questo garantisce coerenza tra data preparation, modeling ed evaluation.
