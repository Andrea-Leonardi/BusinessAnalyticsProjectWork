# Modelli da testare e motivazione

Obiettivo: valutare diversi modelli variando il trade-off interpretabilità-performance.
Individuati 6 modelli appartenenti a famiglie metodologiche diverse.
---


## 0. Modello nullo (predizione costante “up”)
**Motivazione:** rappresenta una baseline minima (“zero-skill model”) che consente di verificare che i modelli proposti aggiungano effettivamente valore predittivo rispetto a una strategia banale.

Questo modello permette di:
- controllare se esiste reale contenuto informativo nelle feature
- confrontare correttamente le performance dei modelli più complessi
- evidenziare eventuali problemi di dataset sbilanciato


## 1. Regressione Fama-MacBeth characteristics-based
**Motivazione:** rappresenta il benchmark econometrico classico nell’asset pricing. Consente di stimare relazioni cross-sectional tra caratteristiche fondamentali e rendimenti attesi mantenendo elevata interpretabilità dei coefficienti. È utile come baseline “scientifica” per verificare la presenza di segnale informativo nelle variabili fondamentali.

---

## 2. Regression logistic penalizzata (LASSO / Elastic Net)
**Motivazione:** permette selezione automatica delle variabili e gestione della multicollinearità tra indicatori finanziari. Produce modelli parsimoniosi e interpretabili, mantenendo una struttura lineare ma più robusta rispetto alla regressione tradizionale.

---

## 3. GAM (Generalized Additive Model) <- Regression logistic
**Motivazione:** introduce non-linearità mantenendo interpretabilità elevata. Ogni variabile contribuisce tramite una funzione univariata stimabile graficamente, consentendo di individuare soglie, saturazioni o effetti non monotoni tipici delle relazioni finanziarie.


---

## 4. Random Forest
**Motivazione:** benchmark non lineare robusto basato su ensemble di alberi decisionali. È in grado di catturare interazioni e relazioni complesse senza richiedere forti assunzioni sulla distribuzione dei dati. Fornisce una misura di riferimento per valutare il beneficio della non-linearità.

---

## 5. XGBoost (Gradient Boosted Trees)
**Motivazione:** rappresenta uno standard moderno per dati tabellari, spesso caratterizzato da elevate performance predittive. Viene utilizzato come benchmark ad alta capacità per stimare il limite superiore di accuratezza raggiungibile rispetto ai modelli più interpretabili.

---

## 6. Rete neurale (MLP – Multilayer Perceptron)

**Motivazione:** rappresenta un benchmark aggiuntivo ad alta flessibilità capace di modellare relazioni non lineari complesse tra variabili fondamentali, tecniche e di sentiment. Tuttavia, per dataset tabellari, le reti neurali non garantiscono necessariamente performance superiori rispetto a metodi come Gradient Boosted Trees e comportano maggiore complessità di tuning e minore interpretabilità.

Il modello viene incluso principalmente come riferimento di performance per verificare se architetture più flessibili riescano a estrarre ulteriore segnale predittivo rispetto agli altri metodi testati.





# PREPARAZIONE TRAINING SET
 guardare **Strategia di split del dataset in evaluation.md** 

---

### Split principale
| periodo | uso |
|--------|-----|
| 2021–2024 | training (stima modelli per diversi iperparametri) |
| 2025 | scelta iperparametro migliore (validation) |
| 2026 | test finale |

---


### Ottimizzazione degli iperparametri
Balanced Accuracy = (Sensitivity + Specificity) / 2

assegna lo stesso peso alle due classi e consente di:
- valutare la capacità del modello di distinguere entrambe le direzioni del rendimento
- evitare soluzioni che funzionano bene solo sulla classe dominante

---





# ADDESTRAMENTO MODELLI   PARAMETRI DI CONTROLLO E LIBERIE PRINCIPALI

## 1. Regressione two-pass (Fama-MacBeth):
    Usare funzione FamaMacBeth(y,x) from linearmodels.asset_pricing.
    
    ATTENZIONE!!!
    convertire rendimenti in "UP" o "DOWN", 
    
    comparazione: threshold 0 (approcio econometrico standard) vs  threshold ottimizzato. ??

---

## 2. Regression logistic penalizzata (LASSO / Elastic Net)
    Usare libreria sklearn.linear_model

    variabili standardizzate ----> usare funzione apposita
    2 parametri da ottimizzare: parametro di penalità e tipo di penalità

---

# BILANCIAMENTO DELLA VARIABILE TARGET

La variabile target del progetto è `AdjClosePrice_t+1_Up`, cioè una variabile binaria che vale:
- `1` se il prezzo adjusted della settimana successiva sale
- `0` se il prezzo adjusted della settimana successiva non sale

Nel file `data/modeling/modeling.csv` il dataset originale era sbilanciato a favore della classe `1`.
Per evitare che i modelli imparassero una preferenza eccessiva per la classe dominante, il bilanciamento è stato inserito direttamente in `src/modeling/classic_ML_model/split_data.py`, prima della costruzione di train, validation e test.

## Criterio usato

Il riequilibrio non è stato fatto eliminando osservazioni casualmente.
È stato invece adottato un sottocampionamento deterministico della classe maggioritaria con tre vincoli:
- il bilanciamento viene fatto separatamente nei tre blocchi temporali del progetto: `2021-2024`, `2025`, `2026`
- si mantiene la copertura per `Ticker` e anno, così da non concentrare la riduzione solo su alcune imprese o su pochi periodi
- all'interno di ogni gruppo `Ticker`-anno, le osservazioni della classe maggioritaria vengono scelte in modo distribuito nel tempo, prendendo righe equispaziate dopo ordinamento per data

## Logica implementata in split_data.py

La procedura è la seguente:
1. si carica `modeling.csv` e si converte `WeekEndingFriday` in formato data
2. si divide il dataset nei tre periodi già usati per lo split del progetto
3. in ciascun periodo si identifica la classe minoritaria e si mantengono tutte le sue osservazioni
4. per la classe maggioritaria si calcola quante righe tenere in ogni gruppo `Ticker`-anno, usando un'allocazione proporzionale e deterministica
5. dentro ogni gruppo si selezionano osservazioni distribuite lungo la sequenza temporale, invece di estrarle a caso
6. il dataset bilanciato viene riscritto in `data/modeling/modeling.csv` e solo dopo viene applicata la funzione `split_data_by_date(...)`

Questa scelta rende il dataset più coerente con la struttura panel-temporale del progetto rispetto a un semplice random undersampling.

## Risultato finale

Dopo il riequilibrio, il numero di `0` e `1` è identico:
- dataset totale: `0 = 11530`, `1 = 11530`
- training `2021-2024`: `0 = 8884`, `1 = 8884`
- validation `2025`: `0 = 2126`, `1 = 2126`
- test `2026`: `0 = 520`, `1 = 520`

In questo modo anche le metriche usate per il tuning, in particolare la Balanced Accuracy, diventano più informative e meno influenzate dalla prevalenza iniziale della classe `1`.
