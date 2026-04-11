# %%
import pandas as pd
import numpy as np 
import sys 
import shutil
import subprocess
import time
from pathlib import Path
from scipy import sparse
import torch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg

try:
    from xgboost import XGBClassifier, XGBRFClassifier
except ImportError:
    XGBClassifier = None
    XGBRFClassifier = None

PROJECT_ROOT = cfg.ROOT
VECTORIZATION_INPUTS = [
    cfg.VECTORIZATION_TFIDF_FINANCIAL_PHRASEBANK,
    cfg.VECTORIZATION_BAG_OF_WORDS_FINANCIAL_PHRASEBANK,
    cfg.VECTORIZATION_TFIDF_ARTICLES,
    cfg.VECTORIZATION_BAG_OF_WORDS_ARTICLES,
]


def resolve_dvc_command() -> list[str]:
    candidates = [
        Path(sys.executable).with_name("dvc.exe"),
        Path(sys.executable).with_name("dvc"),
        PROJECT_ROOT / ".venv" / "Scripts" / "dvc.exe",
    ]

    for candidate in candidates:
        if candidate.exists():
            return [str(candidate)]

    dvc_on_path = shutil.which("dvc") or shutil.which("dvc.exe")
    if dvc_on_path:
        return [dvc_on_path]

    raise FileNotFoundError(
        "DVC non trovato. Installa DVC nella .venv oppure aggiungilo al PATH."
    )


def ensure_vectorization_inputs_available() -> None:
    missing_files = [Path(path) for path in VECTORIZATION_INPUTS if not Path(path).exists()]
    if not missing_files:
        return

    dvc_command = resolve_dvc_command()
    relative_targets = [str(path.relative_to(PROJECT_ROOT)) for path in missing_files]

    print("File di vettorizzazione mancanti in locale. Eseguo 'dvc pull'...")
    subprocess.run(
        dvc_command + ["pull", *relative_targets],
        cwd=PROJECT_ROOT,
        check=True,
    )

    still_missing = [str(path) for path in missing_files if not path.exists()]
    if still_missing:
        raise FileNotFoundError(
            "Download DVC completato ma questi file sono ancora assenti:\n"
            + "\n".join(still_missing)
        )


ensure_vectorization_inputs_available()

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


def build_feature_views(X_train_df, X_test_df):
    X_train_dense = X_train_df.to_numpy(dtype=np.float32, copy=False)
    X_test_dense = X_test_df.to_numpy(dtype=np.float32, copy=False)
    return {
        "dense": (X_train_dense, X_test_dense),
        "sparse": (
            sparse.csr_matrix(X_train_dense),
            sparse.csr_matrix(X_test_dense),
        ),
    }


feature_views_tfidf = build_feature_views(X_train_tfidf, X_test_tfidf)
feature_views_bow = build_feature_views(X_train_bow, X_test_bow)

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
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
# Importa metriche di valutazione: accuracy_score per calcolare l'accuratezza
# classification_report per stampare metriche dettagliate (precision, recall, f1-score)
# confusion_matrix per calcolare la matrice di confusione
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
# Importa matplotlib.pyplot per creare e visualizzare grafici
import matplotlib.pyplot as plt
# Importa seaborn per grafici stilizzati (opzionale, non usato in questo codice)
import seaborn as sns
from sklearn.naive_bayes import ComplementNB # o MultinomialNB


def count_param_combinations(param_grid):
    total = 1
    for values in param_grid.values():
        total *= len(values)
    return total

NUM_CLASSES = len(np.unique(y_tfidf))
CLASSICAL_GPU_AVAILABLE = (
    XGBClassifier is not None
    and XGBRFClassifier is not None
    and torch.cuda.is_available()
)


def make_xgb_classifier(num_classes, **overrides):
    base_params = {
        'device': 'cuda',
        'tree_method': 'hist',
        'objective': 'multi:softprob',
        'num_class': num_classes,
        'eval_metric': 'mlogloss',
        'random_state': 42,
        'verbosity': 0,
        'n_jobs': 1,
    }
    base_params.update(overrides)
    return XGBClassifier(**base_params)


def make_xgbrf_classifier(num_classes, **overrides):
    base_params = {
        'device': 'cuda',
        'tree_method': 'hist',
        'objective': 'multi:softprob',
        'num_class': num_classes,
        'eval_metric': 'mlogloss',
        'random_state': 42,
        'verbosity': 0,
        'n_jobs': 1,
    }
    base_params.update(overrides)
    return XGBRFClassifier(**base_params)


def build_models_config(num_classes):
    baseline_models = {
        'KNN': {
            'model': KNeighborsClassifier(),
            'params': {'n_neighbors': list(range(1, 70, 2))},
            'input_kind': 'sparse',
            'uses_gpu': False,
        },
        'Naive Bayes Gaussiano': {
            'model': GaussianNB(),
            'params': {'var_smoothing': np.logspace(-10, -5, num=50).tolist()},
            'input_kind': 'dense',
            'uses_gpu': False,
        },
        'Naive Bayes ComplementNB': {
            'model': ComplementNB(),
            'params': {
                'alpha': np.logspace(-3, 0, num=50).tolist(),
                'norm': [True, False]
            },
            'input_kind': 'sparse',
            'uses_gpu': False,
        },
        'LDA': {
            'model': LinearDiscriminantAnalysis(),
            'params': {'solver': ['svd', 'lsqr']},
            'input_kind': 'dense',
            'uses_gpu': False,
        },
    }

    if CLASSICAL_GPU_AVAILABLE:
        gpu_models = {
            'XGBoost Lineare (GPU)': {
                'model': make_xgb_classifier(
                    num_classes,
                    booster='gblinear',
                    n_estimators=200,
                    learning_rate=0.1,
                    reg_alpha=0.0,
                ),
                'params': {
                    'reg_lambda': np.logspace(-5, 5, 30).tolist(),
                },
                'input_kind': 'sparse',
                'uses_gpu': True,
            },
            'XGBoost Alberi (GPU)': {
                'model': make_xgb_classifier(
                    num_classes,
                    booster='gbtree',
                    n_estimators=200,
                    max_depth=6,
                    subsample=0.9,
                    colsample_bytree=0.9,
                ),
                'params': {
                    'reg_lambda': np.logspace(-3, 5, 20).tolist(),
                    'gamma': np.logspace(-6, 1, 20).tolist(),
                },
                'input_kind': 'sparse',
                'uses_gpu': True,
            },
            'XGBoost Alberi Profondi (GPU)': {
                'model': make_xgb_classifier(
                    num_classes,
                    booster='gbtree',
                    n_estimators=200,
                    subsample=0.9,
                    colsample_bytree=0.9,
                ),
                'params': {
                    'reg_lambda': np.logspace(-3, 5, 15).tolist(),
                    'max_depth': [2, 3, 4, 5],
                    'min_child_weight': [1, 3],
                    'gamma': [0, 0.1, 0.5, 1],
                },
                'input_kind': 'sparse',
                'uses_gpu': True,
            },
            'Random Forest GPU (XGBRF)': {
                'model': make_xgbrf_classifier(num_classes),
                'params': {
                    'n_estimators': list(range(1, 500, 20)),
                    'max_depth': list(range(5, 150, 5)) + [0],
                },
                'input_kind': 'sparse',
                'uses_gpu': True,
            },
        }
        return {**gpu_models, **baseline_models}

    cpu_models = {
        'SVM_linear': {
            'model': SVC(kernel='linear', random_state=42),
            'params': {'C': np.logspace(-5, 5, 30).tolist()},
            'input_kind': 'sparse',
            'uses_gpu': False,
        },
        'SVM_rbf': {
            'model': SVC(kernel='rbf', random_state=42),
            'params': {
                'C': np.logspace(-3, 5, 20).tolist(),
                'gamma': np.logspace(-6, 1, 20).tolist(),
            },
            'input_kind': 'sparse',
            'uses_gpu': False,
        },
        'SVM_poly': {
            'model': SVC(kernel='poly', random_state=42),
            'params': {
                'C': np.logspace(-3, 5, 15).tolist(),
                'degree': [2, 3, 4, 5],
                'gamma': ['scale', 'auto'],
                'coef0': [0, 0.1, 0.5, 1],
            },
            'input_kind': 'sparse',
            'uses_gpu': False,
        },
        'Random Forest': {
            'model': RandomForestClassifier(random_state=42, n_jobs=-1),
            'params': {
                'n_estimators': list(range(1, 500, 20)),
                'max_depth': list(range(5, 150, 5)) + [None],
            },
            'input_kind': 'sparse',
            'uses_gpu': False,
        },
    }
    return {**cpu_models, **baseline_models}


# ===== DEFINIZIONE DEI MODELLI E IPERPARAMETRI =====
# Questo dizionario contiene la configurazione di tutti i modelli da testare
models_config = build_models_config(NUM_CLASSES)


for model_name, config in models_config.items():
    combinations = count_param_combinations(config['params'])
    print(f"{model_name}: {combinations} combinazioni ({combinations * 5} fit con cv=5)")

print(
    "Backend modelli classici: "
    + (
        "GPU con XGBoost (i modelli più pesanti usano CUDA)."
        if CLASSICAL_GPU_AVAILABLE
        else "CPU fallback sklearn (XGBoost/CUDA non disponibile in questo ambiente)."
    )
)


# Dizionario per salvare i migliori modelli (non usato in questo codice ma utile per usi futuri)
best_models = {}
# Dizionario per salvare tutti i risultati della cross-validation (non usato ma utile per usi futuri)
cv_results_all = {}

# ===== DEFINIZIONE DELLA FUNZIONE PER GRID SEARCH =====
# Questa funzione esegue Grid Search con 10-fold cross-validation per tutti i modelli
def perform_grid_search(feature_views, y_train, y_test, dataset_name, models_config):
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
        input_kind = config.get('input_kind', 'dense')
        X_train, X_test = feature_views[input_kind]
        uses_gpu = config.get('uses_gpu', False)
        search_n_jobs = 1 if uses_gpu else -1
        pre_dispatch = 1 if uses_gpu else 'n_jobs'
        # Stampa una riga di separazione per leggibilità
        print(f"\n{'='*60}")
        # Stampa il nome del dataset e del modello che sta processando
        print(f"{dataset_name} - {model_name}")
        # Stampa un'altra riga di separazione
        print(f"{'='*60}")
        print(f"Input usato: {input_kind}")
        print(f"Backend: {'GPU' if uses_gpu else 'CPU'}")
        start_time = time.perf_counter()
        
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
            n_jobs=search_n_jobs,
            pre_dispatch=pre_dispatch,
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
        print(f"Tempo modello: {time.perf_counter() - start_time:.1f} secondi")
        
    # Ritorna il dizionario completo con tutti i risultati
    return results

# ===== ESECUZIONE GRID SEARCH SU TF-IDF =====
# Stampa un header informativo
print("\n" + "="*80)
print("GRID SEARCH SU TF-IDF DATASET")
print("="*80)
# Esegue il Grid Search sul dataset TF-IDF
# Lo train su X_train_tfidf e y_train_tfidf, valuta su X_test_tfidf e y_test_tfidf
results_tfidf = perform_grid_search(feature_views_tfidf, y_train_tfidf, y_test_tfidf, 
                                    "TF-IDF", models_config)

# ===== ESECUZIONE GRID SEARCH SU BAG OF WORDS =====
# Stampa un header informativo
print("\n" + "="*80)
print("GRID SEARCH SU BAG OF WORDS DATASET")
print("="*80)
# Esegue il Grid Search sul dataset Bag of Words
# Lo train su X_train_bow e y_train_bow, valuta su X_test_bow e y_test_bow
results_bow = perform_grid_search(feature_views_bow, y_train_bow, y_test_bow, 
                                  "Bag of Words", models_config)

# %%
# ===== VISUALIZZAZIONE DELLA VARIAZIONE DELL'ERRORE =====
# Questo blocco crea grafici per mostrare come l'errore varia al variare degli iperparametri

def plot_error_analysis(results, title, color, save_path=None):
    model_names = list(results.keys())
    n_cols = 2
    n_rows = int(np.ceil(len(model_names) / n_cols))
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 6 * n_rows))
    axes = np.atleast_1d(axes).flatten()
    fig.suptitle(title, fontsize=16)

    for idx, model_name in enumerate(model_names):
        ax = axes[idx]
        cv_results = results[model_name]['cv_results']
        mean_test_errors = 1 - cv_results['mean_test_score']
        std_test_scores = cv_results['std_test_score']
        x_pos = np.arange(len(mean_test_errors))

        ax.bar(x_pos, mean_test_errors, yerr=std_test_scores, capsize=5, alpha=0.7, color=color)
        ax.set_xlabel('Combinazione di Iperparametri')
        ax.set_ylabel('Errore (1 - Accuracy)')
        ax.set_title(model_name)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(range(1, len(mean_test_errors) + 1), fontsize=8)
        ax.grid(axis='y', alpha=0.3)

    for ax in axes[len(model_names):]:
        ax.set_visible(False)

    plt.tight_layout(rect=(0, 0, 1, 0.97))
    if save_path is not None:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()


plot_error_analysis(
    results_tfidf,
    "Variazione dell'Errore al Variare degli Iperparametri - TF-IDF",
    color='steelblue',
    save_path=cfg.PROJECT_ROOT / 'reports' / 'error_analysis_tfidf.png',
)

plot_error_analysis(
    results_bow,
    "Variazione dell'Errore al Variare degli Iperparametri - Bag of Words",
    color='coral',
    save_path=cfg.PROJECT_ROOT / 'reports' / 'error_analysis_bow.png',
)

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



















































































































if False: 



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

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Dispositivo PyTorch: {device}")


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
    x_test_tensor_device = x_test_tensor.to(device)
    y_test_tensor_device = y_test_tensor.to(device)

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
            outputs_test = model(x_test_tensor_device)

            # 4. Trova quale classe ha il valore più alto (0 o 1)
            _, predicted = torch.max(outputs_test, 1)

            # 5. Confronta con le etichette reali e fai la media
            accuratezza = (predicted == y_test_tensor_device).sum().item() / len(y_test_tensor_device)

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
            
        ).to(device)

        # --- 3. CONFIGURAZIONE MATEMATICA ---
        # Funzione di perdita per classificazione (include la Softmax internamente)
        criterion = nn.CrossEntropyLoss()
        
        # Ottimizzatore Adam: gestisce l'aggiornamento dei pesi in modo dinamico
        optimizer = optim.Adam(model.parameters(), lr=lr)
        
        # faccimao lo shuffle e il batch dei dati di training 
        train_loader = DataLoader(
            TensorDataset(x_train_tensor, y_train_tensor),
            batch_size=50,
            shuffle=True,
            pin_memory=(device.type == "cuda"),
        )
        
        
        
        # --- 4. TRAINING LOOP (Ciclo di apprendimento) ---
        for epoch in range(200): # Aumentiamo un po' le epoche per la profondità
            model.train() # <--- AGGIUNTO: Mette il modello in modalità addestramento 
            
            for inputs, labels in train_loader:
                inputs = inputs.to(device, non_blocking=(device.type == "cuda"))
                labels = labels.to(device, non_blocking=(device.type == "cuda"))

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
            outputs_test = model(x_test_tensor_device)
            _, predicted = torch.max(outputs_test, 1)
            accuratezza = (predicted == y_test_tensor_device).sum().item() / len(y_test_tensor_device)
        
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
        ).to(device)

        # Se le classi sono sbilanciate, potresti dover aggiungere dei pesi qui:
        # criterion = nn.CrossEntropyLoss(weight=torch.tensor([peso_0, peso_1]))
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5) # weight_decay è una L2 regularization
        
        train_loader = DataLoader(
            TensorDataset(x_train_tensor, y_train_tensor),
            batch_size=64,
            shuffle=True,
            pin_memory=(device.type == "cuda"),
        )
        
        for epoch in range(150): # 150 epoche sono sufficienti per iniziare
            model.train() 
            for inputs, labels in train_loader:
                inputs = inputs.to(device, non_blocking=(device.type == "cuda"))
                labels = labels.to(device, non_blocking=(device.type == "cuda"))
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
