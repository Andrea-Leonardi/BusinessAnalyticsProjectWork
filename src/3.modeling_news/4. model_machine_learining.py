# %%
import pandas as pd
import numpy as np 
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 

training_tf_idf_df = pd.read_csv(cfg.VECTORIZATION_TFIDF_FINANCIAL_PHRASEBANK)
training_bag_of_words_df = pd.read_csv(cfg.VECTORIZATION_BAG_OF_WORDS_FINANCIAL_PHRASEBANK)

# %%
# ===== SPLITTING DEI DATI =====
# Questo blocco divide i dataset in training (80%) e test (20%)

# Importa la funzione train_test_split da scikit-learn per dividere i dati
from sklearn.model_selection import train_test_split
# Importa il modulo math per usare la funzione ceil (arrotondamento per eccesso)
import math

# ===== CALCOLO DIMENSIONI DEL DATASET TF-IDF =====
# Conta il numero totale di righe nel dataset TF-IDF
n_rows_tfidf = len(training_tf_idf_df)
# Calcola il numero di righe per il test set (20%) arrotondando per eccesso con ceil
n_test_tfidf = math.ceil(n_rows_tfidf * 0.2)
# Calcola il numero di righe per il training set sottraendo il test dal totale
n_train_tfidf = n_rows_tfidf - n_test_tfidf

# ===== CALCOLO DIMENSIONI DEL DATASET BAG OF WORDS =====
# Conta il numero totale di righe nel dataset Bag of Words
n_rows_bow = len(training_bag_of_words_df)
# Calcola il numero di righe per il test set (20%) arrotondando per eccesso
n_test_bow = math.ceil(n_rows_bow * 0.2)
# Calcola il numero di righe per il training set sottraendo il test dal totale
n_train_bow = n_rows_bow - n_test_bow

# ===== SEPARAZIONE FEATURES E TARGET PER TF-IDF =====
# Estrae tutte le colonne come features eccetto 'label_id' (variabile target)
# X_tfidf contiene i vettori TF-IDF (feature matrix)
X_tfidf = training_tf_idf_df.drop(columns=['label_id'])
# Estrae la colonna 'label_id' come variabile target (0=negative, 1=neutral, 2=positive)
y_tfidf = training_tf_idf_df['label_id']

# ===== SEPARAZIONE FEATURES E TARGET PER BAG OF WORDS =====
# Estrae tutte le colonne come features eccetto 'label_id'
# X_bow contiene i vettori Bag of Words (feature matrix)
X_bow = training_bag_of_words_df.drop(columns=['label_id'])
# Estrae la colonna 'label_id' come variabile target
y_bow = training_bag_of_words_df['label_id']

# ===== SPLIT TF-IDF DATASET =====
# Divide il dataset TF-IDF in training e test set
# test_size=n_test_tfidf: usa il numero calcolato in precedenza (20%)
# random_state=42: fissa il seed per riproducibilità (ogni volta darai gli stessi risultati)
# stratify=y_tfidf: mantiene le proporzioni delle classi nei subset (importante per classi squilibrate)
X_train_tfidf, X_test_tfidf, y_train_tfidf, y_test_tfidf = train_test_split(
    X_tfidf, y_tfidf, test_size=n_test_tfidf, random_state=42, stratify=y_tfidf
)

# ===== SPLIT BAG OF WORDS DATASET =====
# Divide il dataset Bag of Words in training e test set con gli stessi parametri
# test_size=n_test_bow: usa il numero calcolato precedentemente (20%)
# random_state=42: stesso seed per riproducibilità
# stratify=y_bow: mantiene le proporzioni delle classi
X_train_bow, X_test_bow, y_train_bow, y_test_bow = train_test_split(
    X_bow, y_bow, test_size=n_test_bow, random_state=42, stratify=y_bow
)

# ===== STAMPA RIEPILOGO DIMENSIONI DATASET =====
# Stampa il numero di righe nel training set TF-IDF
print(f"TF-IDF Dataset:")
print(f"  Training: {len(X_train_tfidf)} righe")
# Stampa il numero di righe nel test set TF-IDF
print(f"  Test: {len(X_test_tfidf)} righe")
# Stampa il numero di righe nel training set Bag of Words
print(f"\nBag of Words Dataset:")
print(f"  Training: {len(X_train_bow)} righe")
# Stampa il numero di righe nel test set Bag of Words
print(f"  Test: {len(X_test_bow)} righe")

# %%
# ===== GRID SEARCH CON CROSS-VALIDATION 10-FOLD =====
# Questo blocco implementa Grid Search per trovare i migliori iperparametri per ogni modello

# Importa GridSearchCV per fare ricerca sistematica dei migliori iperparametri
# Importa cross_val_score per calcolare lo score using cross-validation
from sklearn.model_selection import GridSearchCV, cross_val_score
# Importa KNeighborsClassifier (modello KNN - K-Nearest Neighbors)
from sklearn.neighbors import KNeighborsClassifier
# Importa SVC (Support Vector Classifier - macchina a vettori di supporto)
from sklearn.svm import SVC
# Importa RandomForestClassifier (ensemble di alberi decisionali)
from sklearn.ensemble import RandomForestClassifier
# Importa GaussianNB (Naive Bayes Gaussiano - modello probabilistico)
from sklearn.naive_bayes import GaussianNB
# Importa metriche di valutazione: accuracy_score per calcolare l'accuratezza
# classification_report per stampare metriche dettagliate (precision, recall, f1-score)
# confusion_matrix per calcolare la matrice di confusione
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
# Importa matplotlib.pyplot per creare e visualizzare grafici
import matplotlib.pyplot as plt
# Importa seaborn per grafici stilizzati (opzionale, non usato in questo codice)
import seaborn as sns
from sklearn.naive_bayes import ComplementNB # o MultinomialNB

# ===== DEFINIZIONE DEI MODELLI E IPERPARAMETRI =====
# Questo dizionario contiene la configurazione di tutti i modelli da testare
models_config = {
    # Configurazione per il modello KNN (K-Nearest Neighbors)
    'KNN': {
        # Istanza del modello KNN
        'model': KNeighborsClassifier(),
        # Griglia di iperparametri da testare
        # n_neighbors: numero di vicini più prossimi da considerare (da 3 a 20)
        'params': {'n_neighbors': list(range(1, 70, 2))}
    },
    # Configurazione per il modello SVM (Support Vector Machine)
    'SVM_linear': {
    'model': SVC(kernel='linear', random_state=42, probability=True),
    'params': {
        'C': np.logspace(-5, 5, 30).tolist()}
    },
    'SVM_rbf': {
    'model': SVC(kernel='rbf', random_state=42, probability=True),
    'params': {
        'C': np.logspace(-3, 5, 20).tolist(),
        'gamma': np.logspace(-6, 1, 20).tolist()}
    },
    'SVM_poly': {
    'model': SVC(kernel='poly', random_state=42, probability=True),
    'params': {
        'C': np.logspace(-3, 5, 15).tolist(),
        'degree': [2, 3, 4, 5],
        'gamma': ['scale', 'auto'],
        'coef0': [0, 0.1, 0.5, 1]}
    },
    # Configurazione per il modello Random Forest
    'Random Forest': {
        # Istanza del modello Random Forest
        # random_state=42: seed per riproducibilità
        # n_jobs=-1: usa tutti i processori disponibili per parallelizzare il calcolo
        'model': RandomForestClassifier(random_state=42, n_jobs=-1),
        # Griglia di iperparametri da testare
        # n_estimators: numero di alberi nella foresta
        # max_depth: profondità massima di ogni albero (None = no limit)
        'params': {'n_estimators': list(range(1, 500, 20)), 'max_depth': [list(range(0, 150, 5)), None]}
    },
    # Configurazione per il modello Naive Bayes Gaussiano
    # Genera 20 valori distribuiti logaritmicamente tra 1e-10 e 1e-5
    'Naive Bayes Gaussiano': {
        # Istanza del modello Naive Bayes Gaussiano
        'model': GaussianNB(),
        # Griglia di iperparametri da testare
        # var_smoothing: termine di smorzamento per la varianza (evita divisioni per zero)
        'params': {'var_smoothing': np.logspace(-10, -5, num=50).tolist()}
    },
    'Naive Bayes ComplementNB': {
        'model': ComplementNB(), 
        'params': {
        # Alpha controlla quanto "spalmare" la probabilità sui termini rari
        # Una griglia fitta tra 0.001 e 1.0 è l'ideale
        'alpha': np.logspace(-3, 0, num=50).tolist(),
        # norm=True è specifico per ComplementNB e spesso aiuta con TF-IDF
        'norm': [True, False] 
        }
    }
} 


# Dizionario per salvare i migliori modelli (non usato in questo codice ma utile per usi futuri)
best_models = {}
# Dizionario per salvare tutti i risultati della cross-validation (non usato ma utile per usi futuri)
cv_results_all = {}

# ===== DEFINIZIONE DELLA FUNZIONE PER GRID SEARCH =====
# Questa funzione esegue Grid Search con 10-fold cross-validation per tutti i modelli
def perform_grid_search(X_train, y_train, X_test, y_test, dataset_name, models_config):
    """
    Esegue Grid Search e Cross-Validation per tutti i modelli configurati.
    
    Parametri:
    - X_train: training features (matrice con features vettorizzate)
    - y_train: training labels (etichette delle classi: 0, 1, 2)
    - X_test: test features (matrice con features vettorizzate del test set)
    - y_test: test labels (etichette delle classi del test set)
    - dataset_name: nome del dataset (es. "TF-IDF" o "Bag of Words") per stampe informative
    - models_config: dizionario con configurazione modelli e iperparametri
    
    Ritorna:
    - results: dizionario con i risultati (modelli, parametri, accuratezza) per ogni modello
    """
    # Inizializza un dizionario vuoto per salvare i risultati di tutti i modelli
    results = {}
    
    # Itera su ogni modello nella configurazione
    for model_name, config in models_config.items():
        # Stampa una riga di separazione per leggibilità
        print(f"\n{'='*60}")
        # Stampa il nome del dataset e del modello che sta processando
        print(f"{dataset_name} - {model_name}")
        # Stampa un'altra riga di separazione
        print(f"{'='*60}")
        
        # ===== CREAZIONE E CONFIGURAZIONE GRID SEARCH =====
        # GridSearchCV esegue la ricerca sistematica dei migliori iperparametri
        grid_search = GridSearchCV(
            # estimator: il modello da testare (es. KNeighborsClassifier)
            estimator=config['model'],
            # param_grid: dizionario con i parametri da testare e loro valori
            param_grid=config['params'],
            # cv=10: usa 10-fold cross-validation (divide i dati in 10 parti)
            cv=5,
            # scoring='accuracy': usa l'accuratezza come metrica di valutazione
            scoring='accuracy',
            # n_jobs=-1: parallelizza il calcolo su tutti i processori disponibili
            n_jobs=-1,
            # verbose=1: stampa il progresso durante l'esecuzione
            verbose=1
        )
        
        # ===== ADDESTRAMENTO DEL MODELLO =====
        # Esegue il grid search: testa tutte le combinazioni di iperparametri
        # con 10-fold cross-validation sul training set
        grid_search.fit(X_train, y_train)
        
        # ===== ESTRAZIONE DEI MIGLIORI RISULTATI =====
        # Estrae il miglior modello dalla grid search (con i migliori parametri trovati)
        best_model = grid_search.best_estimator_
        # Estrae il dizionario dei migliori iperparametri
        best_params = grid_search.best_params_
        # Estrae l'accuratezza media ottenuta con cross-validation (10-fold)
        best_cv_score = grid_search.best_score_
        
        # ===== PREVISIONI E VALUTAZIONE SU TEST SET =====
        # Genera previsioni sul test set usando il miglior modello
        y_pred = best_model.predict(X_test)
        # Calcola l'accuratezza confrontando le predizioni con i valori reali
        # accuracy = (numero di predizioni corrette) / (numero totale di predizioni)
        test_accuracy = accuracy_score(y_test, y_pred)
        
        # ===== SALVATAGGIO DEI RISULTATI =====
        # Salva tutti i risultati in un dizionario per questo modello
        results[model_name] = {
            # Miglior modello addestrato
            'best_model': best_model,
            # Migliori iperparametri trovati
            'best_params': best_params,
            # Accuratezza media con 10-fold cross-validation
            'best_cv_score': best_cv_score,
            # Accuratezza sul test set
            'test_accuracy': test_accuracy,
            # Tutti i risultati dettagliati della cross-validation (valori medi, std, ecc)
            'cv_results': grid_search.cv_results_,
            # Previsioni fatte dal modello sul test set
            'y_pred': y_pred
        }
        
        # ===== STAMPA DEI RISULTATI =====
        # Stampa i migliori iperparametri trovati
        print(f"Migliori parametri: {best_params}")
        # Stampa l'accuratezza media ottenuta con 10-fold cross-validation (4 decimali)
        print(f"Cross-validation accuracy (10-fold): {best_cv_score:.4f}")
        # Stampa l'accuratezza ottenuta sul test set (4 decimali)
        print(f"Test set accuracy: {test_accuracy:.4f}")
        
    # Ritorna il dizionario completo con tutti i risultati
    return results

# ===== ESECUZIONE GRID SEARCH SU TF-IDF =====
# Stampa un header informativo
print("\n" + "="*80)
print("GRID SEARCH SU TF-IDF DATASET")
print("="*80)
# Esegue il Grid Search sul dataset TF-IDF
# Lo train su X_train_tfidf e y_train_tfidf, valuta su X_test_tfidf e y_test_tfidf
results_tfidf = perform_grid_search(X_train_tfidf, y_train_tfidf, X_test_tfidf, y_test_tfidf, 
                                    "TF-IDF", models_config)

# ===== ESECUZIONE GRID SEARCH SU BAG OF WORDS =====
# Stampa un header informativo
print("\n" + "="*80)
print("GRID SEARCH SU BAG OF WORDS DATASET")
print("="*80)
# Esegue il Grid Search sul dataset Bag of Words
# Lo train su X_train_bow e y_train_bow, valuta su X_test_bow e y_test_bow
results_bow = perform_grid_search(X_train_bow, y_train_bow, X_test_bow, y_test_bow, 
                                  "Bag of Words", models_config)

# %%
# ===== VISUALIZZAZIONE DELLA VARIAZIONE DELL'ERRORE =====
# Questo blocco crea grafici per mostrare come l'errore varia al variare degli iperparametri

# ===== GRAFICO PER TF-IDF DATASET =====
# Crea una figura con 6 subplot (3x2) per i 6 modelli
# figsize=(15, 18) specifica le dimensioni della figura in pollici (più grande per 6 subplot)
fig, axes = plt.subplots(3, 2, figsize=(15, 18))
# Imposta il titolo della figura
fig.suptitle('Variazione dell\'Errore al Variare degli Iperparametri - TF-IDF', fontsize=16)

# Estrae una lista con i nomi di tutti i modelli
model_names = list(models_config.keys())

# Itera su ogni modello e crea un grafico
for idx, model_name in enumerate(model_names):
    # Seleziona il subplot corretto (idx // 2 = riga, idx % 2 = colonna)
    ax = axes[idx // 2, idx % 2]
    
    # Estrae i risultati della cross-validation per questo modello dal dataset TF-IDF
    cv_results = results_tfidf[model_name]['cv_results']
    # Estrae l'accuratezza media per ogni combinazione di iperparametri
    mean_test_scores = cv_results['mean_test_score']
    # Estrae la deviazione standard dell'accuratezza (utile per le barre di errore)
    std_test_scores = cv_results['std_test_score']
    # Estrae la lista di tutti i parametri testati (non usato direttamente nel grafico)
    params = cv_results['params']
    
    # Converte l'accuratezza in errore (errore = 1 - accuracy)
    # Questo permette di visualizzare l'errore anziché l'accuratezza
    mean_test_errors = 1 - mean_test_scores
    
    # Crea una lista di posizioni x per posizionare le barre (0, 1, 2, ...)
    x_pos = np.arange(len(mean_test_errors))
    
    # ===== CREAZIONE DEL GRAFICO A BARRE =====
    # Crea un grafico a barre con:
    # x_pos: posizioni delle barre sull'asse x
    # mean_test_errors: altezza delle barre (errore medio)
    # yerr=std_test_scores: deviazione standard (barre di errore verticali)
    # capsize=5: lunghezza dei cappelli delle barre di errore
    # alpha=0.7: trasparenza delle barre (0=trasparente, 1=opaco)
    # color='steelblue': colore delle barre per TF-IDF
    ax.bar(x_pos, mean_test_errors, yerr=std_test_scores, capsize=5, alpha=0.7, color='steelblue')
    # Etichetta dell'asse x (combinazioni di iperparametri)
    ax.set_xlabel('Combinazione di Iperparametri')
    # Etichetta dell'asse y (errore)
    ax.set_ylabel('Errore (1 - Accuracy)')
    # Titolo del subplot (nome del modello)
    ax.set_title(f'{model_name}')
    # Posiziona i tick dell'asse x
    ax.set_xticks(x_pos)
    # Etichette dei tick (numeri da 1 a n combinazioni)
    ax.set_xticklabels(range(1, len(mean_test_errors) + 1), fontsize=8)
    # Aggiunge una griglia sull'asse y per leggibilità
    ax.grid(axis='y', alpha=0.3)

# Regola lo spazio tra i subplot
plt.tight_layout()
# Salva la figura come file PNG con alta risoluzione (300 dpi)
plt.savefig(cfg.REPORTS / 'error_analysis_tfidf.png', dpi=300, bbox_inches='tight')
# Mostra il grafico nella finestra
plt.show()

# ===== GRAFICO PER BAG OF WORDS DATASET =====
# Crea una figura con 6 subplot (3x2) per i 6 modelli
fig, axes = plt.subplots(3, 2, figsize=(15, 18))
# Imposta il titolo della figura
fig.suptitle('Variazione dell\'Errore al Variare degli Iperparametri - Bag of Words', fontsize=16)

# Itera su ogni modello e crea un grafico
for idx, model_name in enumerate(model_names):
    # Seleziona il subplot corretto
    ax = axes[idx // 2, idx % 2]
    
    # Estrae i risultati della cross-validation per questo modello dal dataset Bag of Words
    cv_results = results_bow[model_name]['cv_results']
    # Estrae l'accuratezza media per ogni combinazione di iperparametri
    mean_test_scores = cv_results['mean_test_score']
    # Estrae la deviazione standard dell'accuratezza
    std_test_scores = cv_results['std_test_score']
    # Estrae la lista di parametri testati
    params = cv_results['params']
    
    # Converte l'accuratezza in errore
    mean_test_errors = 1 - mean_test_scores
    
    # Crea liste di posizioni x per le barre
    x_pos = np.arange(len(mean_test_errors))
    
    # Crea il grafico a barre con colore 'coral' per Bag of Words (diverso da TF-IDF)
    ax.bar(x_pos, mean_test_errors, yerr=std_test_scores, capsize=5, alpha=0.7, color='coral')
    # Etichetta dell'asse x
    ax.set_xlabel('Combinazione di Iperparametri')
    # Etichetta dell'asse y
    ax.set_ylabel('Errore (1 - Accuracy)')
    # Titolo del subplot
    ax.set_title(f'{model_name}')
    # Posiziona i tick
    ax.set_xticks(x_pos)
    # Etichette dei tick
    ax.set_xticklabels(range(1, len(mean_test_errors) + 1), fontsize=8)
    # Aggiunge griglia per leggibilità
    ax.grid(axis='y', alpha=0.3)

# Regola lo spazio tra i subplot
plt.tight_layout()
# Salva il grafico come file PNG
plt.savefig(cfg.REPORTS / 'error_analysis_bow.png', dpi=300, bbox_inches='tight')
# Mostra il grafico
plt.show()

# %%
# ===== RIEPILOGO ACCURATEZZA MIGLIOR MODELLI =====
# Questo blocco crea tabelle di riepilogo con le performance di tutti i modelli

# ===== RIEPILOGO PER TF-IDF DATASET =====
# Stampa una riga di separazione
print("\n" + "="*80)
# Stampa il titolo della sezione
print("RIEPILOGO ACCURATEZZA - TF-IDF DATASET")
# Stampa un'altra riga di separazione
print("="*80)

# Inizializza una lista vuota per i dati del riepilogo TF-IDF
summary_tfidf = []
# Itera su tutti i modelli e i loro risultati
for model_name, result in results_tfidf.items():
    # Aggiunge un dizionario con le informazioni del modello alla lista
    summary_tfidf.append({
        # Nome del modello
        'Modello': model_name,
        # Accuratezza media ottenuta con 10-fold cross-validation, formattata a 4 decimali
        'CV Accuracy': f"{result['best_cv_score']:.4f}",
        # Accuratezza sul test set, formattata a 4 decimali
        'Test Accuracy': f"{result['test_accuracy']:.4f}",
        # Migliori iperparametri trovati (convertiti in stringa per visualizzazione)
        'Iperparametri': str(result['best_params'])
    })

# Converte la lista di dizionari in un DataFrame di pandas
summary_df_tfidf = pd.DataFrame(summary_tfidf)
# Stampa il DataFrame senza indice (index=False) per una visualizzazione pulita
print(summary_df_tfidf.to_string(index=False))

# ===== RIEPILOGO PER BAG OF WORDS DATASET =====
# Stampa una riga di separazione
print("\n" + "="*80)
# Stampa il titolo della sezione
print("RIEPILOGO ACCURATEZZA - BAG OF WORDS DATASET")
# Stampa un'altra riga di separazione
print("="*80)

# Inizializza una lista vuota per i dati del riepilogo Bag of Words
summary_bow = []
# Itera su tutti i modelli e i loro risultati
for model_name, result in results_bow.items():
    # Aggiunge un dizionario con le informazioni del modello alla lista
    summary_bow.append({
        # Nome del modello
        'Modello': model_name,
        # Accuratezza media con 10-fold cross-validation, formattata a 4 decimali
        'CV Accuracy': f"{result['best_cv_score']:.4f}",
        # Accuratezza sul test set, formattata a 4 decimali
        'Test Accuracy': f"{result['test_accuracy']:.4f}",
        # Migliori iperparametri trovati (convertiti in stringa)
        'Iperparametri': str(result['best_params'])
    })

# Converte la lista di dizionari in un DataFrame di pandas
summary_df_bow = pd.DataFrame(summary_bow)
# Stampa il DataFrame senza indice per una visualizzazione pulita
print(summary_df_bow.to_string(index=False))

# %%
# ===== DETAILED CLASSIFICATION REPORTS =====
# Questo blocco stampa report dettagliati con metriche di performance per ogni modello

# ===== CLASSIFICATION REPORT PER TF-IDF DATASET =====
# Stampa una riga di separazione
print("\n" + "="*80)
# Stampa il titolo della sezione
print("DETAILED CLASSIFICATION REPORT - TF-IDF")
# Stampa un'altra riga di separazione
print("="*80)

# Itera su tutti i modelli e i loro risultati
for model_name, result in results_tfidf.items():
    # Stampa il nome del modello
    print(f"\n{model_name}:")
    # Stampa il classification report dettagliato
    # y_test_tfidf: valori effettivi delle classi
    # result['y_pred']: previsioni del modello
    # target_names: nomi leggibili delle classi (Negative, Neutral, Positive)
    # Il report include per ogni classe:
    # - Precision: quante delle predizioni positive erano corrette
    # - Recall: quante dei veri positivi sono stati identificati
    # - F1-score: media armonica di precision e recall
    # - Support: numero di esempi per ogni classe
    print(classification_report(y_test_tfidf, result['y_pred'], 
                               target_names=['Negative', 'Neutral', 'Positive']))

# ===== CLASSIFICATION REPORT PER BAG OF WORDS DATASET =====
# Stampa una riga di separazione
print("\n" + "="*80)
# Stampa il titolo della sezione
print("DETAILED CLASSIFICATION REPORT - BAG OF WORDS")
# Stampa un'altra riga di separazione
print("="*80)

# Itera su tutti i modelli e i loro risultati
for model_name, result in results_bow.items():
    # Stampa il nome del modello
    print(f"\n{model_name}:")
    # Stampa il classification report dettagliato
    # y_test_bow: valori effettivi delle classi
    # result['y_pred']: previsioni del modello
    # target_names: nomi leggibili delle classi
    print(classification_report(y_test_bow, result['y_pred'], 
                               target_names=['Negative', 'Neutral', 'Positive']))




#%%
tf_idf_df = pd.read_csv(cfg.VECTORIZATION_TFIDF_ARTICLES)
bag_of_words_df = pd.read_csv(cfg.VECTORIZATION_BAG_OF_WORDS_ARTICLES)



























































































































#%%
import pandas as pd
import numpy as np 
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 


bag_of_words_df = pd.read_csv(cfg.VECTORIZATION_BAG_OF_WORDS_ARTICLES)
tf_idf_df = pd.read_csv(cfg.VECTORIZATION_TFIDF_ARTICLES)

financial_phrasebank_bag_of_words_df = pd.read_csv(cfg.VECTORIZATION_BAG_OF_WORDS_FINANCIAL_PHRASEBANK)
financial_phrasebank_tf_idf_df = pd.read_csv(cfg.VECTORIZATION_TFIDF_FINANCIAL_PHRASEBANK)
"""
da eliminare serve solo per una prova, per vedere se il codice funziona
"""
bag_of_words_df = bag_of_words_df.iloc[0:10, :]
tf_idf_df = tf_idf_df.iloc[0:10, :]





# -----------------------------------------------------------------------------
#%%
import torch
import torch.nn as nn
import torch.optim as optim
import optuna
import pandas as pd
from torch.utils.data import DataLoader, TensorDataset
from torchsummary import summary
from sklearn.preprocessing import StandardScaler
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 


df = pd.read_csv(cfg.FULL_DATA)
variabili =[
    "AdjClosePrice",
    "AdjClosePrice_t-1",
    "AdjClosePrice_t-2",
    "BookToMarket",
    "MarketCap",
    "EarningsYield",
    "EarningsYield_TTM"]
"""
    "BookToMarket_L1W",
    "MarketCap_L1W",
    "EarningsYield_L1W",
    "EarningsYield_TTM_L1W",
    "BookToMarket_L2W",
    "EarningsYield_TTM_L2W",
    "GrossProfitability",
    "GrossProfitability_TTM",
    "ROA",
    "ROA_TTM",
    "GrossProfitability_TTM_L1Q",
    "ROA_L1Q",
    "ROA_TTM_L1Q",
    "Accruals_TTM_L1Q",
    "DebtToAssets_L1Q",
    "WorkingCapitalScaled_L1Q",
    "ROA_L2Q",
    "InvestmentIntensity_L2Q",
    "Accruals_TTM_L2Q",
    "DebtToAssets_L2Q",
    "WorkingCapitalScaled_L2Q"
]
"""
# 1. Trasformiamo le etichette
# df['Direction'] = df['Direction'].map({'Down': 0, 'Up': 1})
# Seleziona le colonne per indice e le elimina

# 2. Rimuoviamo 'Today' subito da tutto il dataframe
# axis=1 significa "rimuovi la colonna"
# df = df.drop(columns=['Today'])

    
# 3. Ora facciamo lo split per anno (Year è ancora la prima colonna)
# Usiamo iloc[:, 1:] per togliere 'Year' dalle variabili del modello
test = df[df['WeekEndingFriday'] >= "2025-01-01"]#.drop(df.columns[[0, 1, 2, 4]], axis=1)
training = df[df['WeekEndingFriday'] < "2025-01-01"]#.drop(df.columns[[0, 1, 2, 4]], axis=1)
"""
# 4. NORMALIZZAZIONE (Indispensabile!)
scaler = StandardScaler()
x_train_scaled = scaler.fit_transform(training)
x_test_scaled = scaler.transform(test)

# convertito il dataset in un tensore il dataset di test in un tensore
x_test_tensor = torch.tensor(x_test_scaled, dtype=torch.float32)
y_test_tensor = torch.tensor(test.iloc[:, 0].values, dtype=torch.long)

# convertito il dataset in un tensore il dataset di training in un tensore
x_train_tensor = torch.tensor(x_train_scaled, dtype=torch.float32)
y_train_tensor = torch.tensor(training.iloc[:, 0].values, dtype=torch.long)
"""
# 4. SEPARAZIONE E NORMALIZZAZIONE
# Prendiamo Y (Target) - colonna 0
"""
y_train = training.iloc[:, 0].values
y_test = test.iloc[:, 0].values

y_train = training.iloc[0:50, "AdjClosePrice_t+1_Up"].values
y_test = test.iloc[0:50, "AdjClosePrice_t+1_Up"].values
# Prendiamo X (Feature) - tutto tranne la colonna 0
# X_train_raw = training.iloc[:, 1:].values
# X_test_raw = test.iloc[:, 1:].values
# SBAGLIATO: X_train_raw = training.iloc[:, 1:].values
# SBAGLIATO: X_train_raw = X_train_raw[[variabili]] 

X_train_raw = training.iloc[0:50, variabili].values
X_test_raw = test.iloc[0:50, variabili].values
"""


# Oppure, ancora più semplice (selezioni prima la colonna e poi le righe):
y_train = training["AdjClosePrice_t+1_Up"].iloc[0:10000].values
y_test = test["AdjClosePrice_t+1_Up"].iloc[0:10000].values

X_train_raw = training[variabili].iloc[0:10000].values
X_test_raw = test[variabili].iloc[0:10000].values


# Ora scali solo le feature
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train_raw)
X_test_scaled = scaler.transform(X_test_raw)

# 5. CONVERSIONE IN TENSORI
x_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
y_train_tensor = torch.tensor(y_train, dtype=torch.long)

x_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32)
y_test_tensor = torch.tensor(y_test, dtype=torch.long)

# Controlla quante colonne di input hai davvero
input_dim = x_train_tensor.shape[1]
print(f"Numero di feature in ingresso: {input_dim}")

# definiamo la funzione che ci calocla l'accuratezza del modello sul dataset di test 
def accuratezza_test(model, x_test_tensor, y_test_tensor):
    
    # 1. Metti il modello in modalità valutazione
    model.eval()
    
    # 2. Blocca il calcolo dei gradienti (risparmia memoria e calcolo)
    with torch.no_grad():

        # 3. Ottieni le previsioni del modello sui dati di test
        outputs_test = model(x_test_tensor)

        # 4. Trova quale classe ha il valore più alto (0 o 1)
        _, predicted = torch.max(outputs_test, 1)

        # 5. Confronta con le etichette reali e fai la media
        accuratezza = (predicted == y_test_tensor).sum().item() / len(y_test_tensor)

    print(f"Accuratezza sul test set: {accuratezza:.2%}")
    
    return accuratezza 

#%%
def objective(trial):
    # --- 1. SUGGERIMENTI DI OPTUNA (Architettura variabile) ---
    # Chiediamo a Optuna di decidere la grandezza di ogni singolo "piano" della rete
    h1 = trial.suggest_int('units_l1', 32, 64) # Primo strato nascosto
    h2 = trial.suggest_int('units_l2', 5, 32)  # Secondo strato nascosto
    # h3 = trial.suggest_int('units_l3', 40, 70) # Terzo strato nascosto
    # h4 = trial.suggest_int('units_l4', 10, 40) # Quarto strato nascosto
    
    # Suggerimento per il Learning Rate (passo di apprendimento)
    lr = trial.suggest_float('learning_rate', 1e-4, 1e-2, log=True)
    # --- 2. COSTRUZIONE DELLA RETE PROFONDA ---
    # Usiamo nn.Sequential per impilare i 4 blocchi di calcolo (3 nascosti + 1 output)
    model = nn.Sequential(
        # Strato 1: Da 1000 ingressi a h1 neuroni
        nn.Linear(7, h1),
        nn.BatchNorm1d(h1), # Normalizza l'output di questo strato per stabilizzare l'apprendimento
        nn.LeakyReLU(0.1), # Meglio di ReLU
        nn.Dropout(0.2),   # Spegne il 20% dei neuroni a caso per forzare l'apprendimento        
        
        # Strato 2: Da h1 neuroni a h2 neuroni
        nn.Linear(h1, h2),
        nn.BatchNorm1d(h2),
        nn.LeakyReLU(0.1),
        
        # Strato 3: Da h2 neuroni a h3 neuroni
        nn.Linear(h2, 2),
        # nn.BatchNorm1d(h3),
        # nn.LeakyReLU(0.1),
        
        # Strato 4: Da h3 neuroni a h4 neuroni
        # nn.Linear(h3, h4),
        # nn.BatchNorm1d(h4),
        # nn.LeakyReLU(0.1),
        
        # Strato 4 (Output): Da h4 neuroni alle 10 classi finali
        # nn.Linear(h4, 2)
        
    )

    # --- 3. CONFIGURAZIONE MATEMATICA ---
    # Funzione di perdita per classificazione (include la Softmax internamente)
    criterion = nn.CrossEntropyLoss()
    
    # Ottimizzatore Adam: gestisce l'aggiornamento dei pesi in modo dinamico
    optimizer = optim.Adam(model.parameters(), lr=lr)
    
    # faccimao lo shuffle e il batch dei dati di training 
    train_loader = DataLoader(TensorDataset(x_train_tensor, y_train_tensor), batch_size=50, shuffle=True)
    
    
    
    # --- 4. TRAINING LOOP (Ciclo di apprendimento) ---
    for epoch in range(200): # Aumentiamo un po' le epoche per la profondità
        model.train() # <--- AGGIUNTO: Mette il modello in modalità addestramento 
        
        for inputs, labels in train_loader:

            # A) Reset: pulisce i gradienti del giro precedente
            optimizer.zero_grad()
            
            # B) Forward: i dati attraversano i 4 strati
            outputs = model(inputs)
            
            # C) Loss: calcola la "distanza" tra previsione e realtà
            loss = criterion(outputs, labels)
            
            # D) Backward: calcola la derivata (il gradiente) per ogni strato
            loss.backward()
            
            # E) Step: modifica i pesi seguendo la direzione del gradiente
            optimizer.step()
    
        acc = accuratezza_test(model, x_test_tensor, y_test_tensor) # Calcoliamo l'accuratezza sul test set per ogni combinazione di iperparametri
    
    # Restituiamo il valore finale dell'errore (da minimizzare)
    return acc

# --- 5. LANCIO DELL'ESPERIMENTO ---
study = optuna.create_study(direction='maximize') # Vogliamo massimizzare l'accuratezza, quindi minimizziamo la loss
study.optimize(objective, n_trials=30) # Prova 30 diverse combinazioni di h1, h2, h3, h4 e lr

# %%

print(study)
print(study.best_params)

# %%
# 1. Recuperiamo i parametri migliori
best_p = study.best_params

# 2. Ricreiamo il modello finale
final_model = nn.Sequential(
    nn.Linear(63, best_p['units_l1']), nn.ReLU(),
    nn.Linear(best_p['units_l1'], best_p['units_l2']), nn.ReLU(),
    nn.Linear(best_p['units_l2'], best_p['units_l3']), nn.ReLU(),
    nn.Linear(best_p['units_l3'], best_p['units_l4']), nn.ReLU(),
    nn.Linear(best_p['units_l4'], 2)
)

# 3. Visualizziamo la struttura
print("--- STRUTTURA DELLA RETE ---")
print(final_model)

# Se vuoi vedere anche il numero di parametri (installa torchsummary se non l'hai)
summary(final_model, (63,))
# %%
#%%
# ... (tutta la tua parte di import e preparazione dei tensori rimane uguale) ...

# Controlliamo il bilanciamento delle classi nel test set
class_1_ratio = (y_test == 1).sum() / len(y_test)
print(f"Percentuale della classe 1 nel test set: {class_1_ratio:.2%}")
print(f"Percentuale della classe 0 nel test set: {1 - class_1_ratio:.2%}")
print("Se l'accuratezza rimane bloccata su uno di questi due numeri, la rete sta prevedendo una sola classe.")

def accuratezza_test(model, x_test_tensor, y_test_tensor, silenzioso=False):
    model.eval()
    with torch.no_grad():
        outputs_test = model(x_test_tensor)
        _, predicted = torch.max(outputs_test, 1)
        accuratezza = (predicted == y_test_tensor).sum().item() / len(y_test_tensor)
    
    if not silenzioso:
        print(f"Accuratezza sul test set: {accuratezza:.2%}")
    return accuratezza 

def objective(trial):
    h1 = trial.suggest_int('units_l1', 16, 64) 
    h2 = trial.suggest_int('units_l2', 8, 32)  
    
    # ATTENZIONE 1: Learning rate abbassato. Adam lavora bene tra 1e-4 e 1e-2.
    lr = trial.suggest_float('learning_rate', 1e-4, 1e-2, log=True)
    
    # ATTENZIONE 2: Usiamo input_dim (che nel tuo caso è 7) invece di hardcodare il numero
    model = nn.Sequential(
        nn.Linear(input_dim, h1),
        nn.ReLU(),        
        nn.Linear(h1, h2),
        nn.ReLU(),
        
        nn.Linear(h2, 2)
    )

    # Se le classi sono sbilanciate, potresti dover aggiungere dei pesi qui:
    # criterion = nn.CrossEntropyLoss(weight=torch.tensor([peso_0, peso_1]))
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5) # weight_decay è una L2 regularization
    
    train_loader = DataLoader(TensorDataset(x_train_tensor, y_train_tensor), batch_size=64, shuffle=True)
    
    for epoch in range(150): # 150 epoche sono sufficienti per iniziare
        model.train() 
        for inputs, labels in train_loader:
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
    
    # ATTENZIONE 3: Calcoliamo l'accuratezza SOLO alla fine di tutte le epoche per questo trial
    acc = accuratezza_test(model, x_test_tensor, y_test_tensor, silenzioso=True) 
    
    return acc

study = optuna.create_study(direction='maximize') 
study.optimize(objective, n_trials=30) 

print("Migliori parametri trovati:", study.best_params)
print("Migliore accuratezza:", study.best_value)

#%%
# --- ATTENZIONE 4: CORREZIONE DEL MODELLO FINALE ---
best_p = study.best_params

# Deve rispecchiare esattamente la struttura che avevi dentro 'objective'
final_model = nn.Sequential(
    nn.Linear(input_dim, best_p['units_l1']), # input_dim = 7 (le tue variabili)
    nn.BatchNorm1d(best_p['units_l1']),
    nn.LeakyReLU(0.1),
    nn.Dropout(0.2),
    
    nn.Linear(best_p['units_l1'], best_p['units_l2']),
    nn.BatchNorm1d(best_p['units_l2']),
    nn.LeakyReLU(0.1),
    nn.Dropout(0.2),
    
    nn.Linear(best_p['units_l2'], 2)
)

print("--- STRUTTURA DELLA RETE ---")
print(final_model)

# La summary richiede il numero di input corretto (es: 7, non 63)
summary(final_model, (input_dim,))


# %%
