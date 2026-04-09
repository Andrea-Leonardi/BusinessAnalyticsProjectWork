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
    "EarningsYield_TTM",
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
    lr = trial.suggest_float('learning_rate', 1e-3, 1e-1, log=True)
    # --- 2. COSTRUZIONE DELLA RETE PROFONDA ---
    # Usiamo nn.Sequential per impilare i 4 blocchi di calcolo (3 nascosti + 1 output)
    model = nn.Sequential(
        # Strato 1: Da 1000 ingressi a h1 neuroni
        nn.Linear(28, h1),
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


