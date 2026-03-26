# OBIETTIVI
focus sulla capacità predittiva out-of-sample.
Modelli più complessi migliori rispetto a benchmark semplici?



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

 La variabile risposta è  sbilanciata, l’accuracy standard può risultare fuorviante. 
Per questo motivo viene utilizzata la **Balanced Accuracy** come metrica principale, in quanto tiene conto in modo equilibrato della capacità del modello di prevedere entrambe le classi (up/down).

L’**AUC (Area Under the ROC Curve)** viene utilizzata come metrica complementare per valutare la capacità discriminante del modello indipendentemente dalla soglia di classificazione.

L’**Accuracy (1 − error rate)** viene riportata a fini interpretativi, per fornire una misura facilmente leggibile della percentuale di previsioni corrette.




# BENCHMARK DI CONFRONTO
Modello nullo (benchmark predittivo)
Fama-MacBeth characteristics (benchmark econometrico)


# CRITERIO DI SCELTA MODELLO FINALE 
(da valutare)
