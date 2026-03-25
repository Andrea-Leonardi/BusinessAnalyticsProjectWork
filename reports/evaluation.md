# OBIETTIVI
focus sulla capacità predittiva out-of-sample.
Modelli più complessi migliori rispetto a benchmark semplici?
Da valutare con gli altri(focus secondario: capacità del modello di predire "DOWN" quando il prezzo effettivamente scende) ---> renderebbe le cose un pò più complesse




# Strategia di split del dataset

## Approccio

Adottiamo uno **split temporale** del dataset

---

## Motivazione

Anche se i modelli utilizzati nel progetto non modellano esplicitamente la dinamica temporale, il dataset presenta comunque una struttura temporale implicita:

- i fundamentals variano lentamente nel tempo
- gli indicatori price-based dipendono da prezzi recenti
- le variabili di sentiment mostrano persistenza
- alcuni ratios dipendono dal market cap, aggiornato settimanalmente tramite il prezzo

Di conseguenza, osservazioni vicine nel tempo non sono indipendenti.

Lo split temporale garantisce che i modelli vengano valutati su **dati futuri non osservati durante l’addestramento**, fornendo una stima più realistica della capacità predittiva out-of-sample.


In questo modo il confronto tra modelli con diverso trade-off interpretabilità–performance risulta coerente e metodologicamente corretto.

---

## Perché non utilizzare uno split casuale classico

Uno split casuale standard presuppone che le osservazioni siano indipendenti e identicamente distribuite (i.i.d.).





# METRICHE DI VALUTAZIONE

accuracy: Balanced Accuracy (variabile risposta sbilanciata) 
          AUC (capacità discriminante modello)
          1-error rate

curve roc per mostrare trade-off sensibilità vs specificità (da valutare)




# BENCHMARK DI CONFRONTO
null model


# CRITERIO DI SCELTA MODELLO FINALE 
(da valutare)
