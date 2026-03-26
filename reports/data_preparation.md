## 1. Regressione two-pass (Fama-MacBeth characteristics-based)

Per implementare la regressione **Fama-MacBeth characteristics-based** è necessario disporre di un **dataset panel** con struttura:

**una riga = (azienda, settimana)**

| date       | symbol | 
|------------|--------|
| 2020-01-03 | AAPL   |
| 2020-01-03 | MSFT   |
| 2020-01-10 | AAPL   | 


## 2. Regression logistic penalizzata (LASSO / Elastic Net)
standardizzare le variabili -----> farlo direttamente in fase di implementazione del modello 

## 3. GAM (Generalized Additive Model) <- Regression logistic
attenzione correlazione tra variabili

## 6. Rete neurale (MLP – Multilayer Perceptron)
standardizzare le variabili -----> farlo direttamente in fase di implementazione del modello 