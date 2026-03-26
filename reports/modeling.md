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


## 1. Regressione two-pass (Fama-MacBeth characteristics-based)
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




# ADDESTRAMENTO MODELLI   PARAMETRI DI CONTROLLO E LIBERIE PRINCIPALI

## Ottimizzazione degli iperparametri
Balanced Accuracy --> motivo **evaluation.md in metriche di valutazione**


## 1. Regressione two-pass (Fama-MacBeth):
    Usare funzione FamaMacBeth(y,x) from linearmodels.asset_pricing.
    
    ATTENZIONE!!!
    convertire rendimenti in "UP" o "DOWN", 
    
    comparazione: threshold 0.5(approcio econometrico standard) vs  threshold ottimizzato. ??

---

## 2. Regression logistic penalizzata (LASSO / Elastic Net)
    Usare libreria sklearn.linear_model

    variabili standardizzate ----> usare funzione apposita
    2 parametri da ottimizzare: parametro di penalità e tipo di penalità