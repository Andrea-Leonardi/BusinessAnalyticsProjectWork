# %%
import yfinance as yf
from datetime import datetime, timedelta

# Definisci il periodo: 10 anni fa fino a oggi
end_date = datetime.now()
start_date = end_date - timedelta(days=365*10)  # Circa 10 anni

# Scarica i dati mensili per un titolo (es. AAPL)
ticker = 'AAPL'  # Sostituisci con il simbolo desiderato
data = yf.download(ticker, start=start_date, end=end_date, interval='1mo')

# Verifica se i dati sono disponibili
if data.empty:
    print(f"Nessun dato disponibile per {ticker} negli ultimi 10 anni.")
else:
    print(f"Dati disponibili per {ticker}: da {data.index[0].date()} a {data.index[-1].date()}")
    print(f"Numero di mesi con dati: {len(data)}")
    # Mostra i primi e ultimi prezzi di chiusura (Close)
    print(data[['Close']].head())
    print(data[['Close']].tail())
# %%
import yfinance as yf
from datetime import datetime, timedelta

# Definisci il periodo: ultimi 10 anni
end_date = datetime.now()
start_date = end_date - timedelta(days=365*10)

# Crea un oggetto Ticker per il titolo
ticker = 'AAPL'  # Sostituisci con il simbolo desiderato
stock = yf.Ticker(ticker)

# Ottieni gli split storici
splits = stock.splits

# Filtra gli split negli ultimi 10 anni
splits_last_10_years = splits[splits.index >= start_date]

# Verifica e mostra i risultati
if splits_last_10_years.empty:
    print(f"Nessuno split rilevato per {ticker} negli ultimi 10 anni.")
else:
    print(f"Split rilevati per {ticker} negli ultimi 10 anni:")
    for date, ratio in splits_last_10_years.items():
        print(f"- Data: {date.date()}, Rapporto: {ratio} (es. {ratio}:1)")
    print(f"Totale split: {len(splits_last_10_years)}")
# %%
