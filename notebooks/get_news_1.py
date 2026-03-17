import pandas as pd
import requests
from datetime import datetime

# ==========================================
# 1. API Keys
# ==========================================
API_KEY = "PKVESCM6H235I3XWBT25AYP7JC"
SECRET_KEY = "6MPjbdQyG6PmWP1niRAAGPFM3HQw2SvAWMGWL3r5Q3FM"

NEWS_URL = "https://data.alpaca.markets/v1beta1/news"

# ==========================================
# 2. Tickers (can change to others)
# ==========================================
tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"] # test 5 companies first, then can change to others)

headers = {
    "Apca-Api-Key-Id": API_KEY,
    "Apca-Api-Secret-Key": SECRET_KEY,
    "accept": "application/json"
}

params = {
    "symbols": ",".join(tickers),
    "start": "2021-01-01T00:00:00Z",
    "end": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
    "limit": 50
}

print(f"🚀 Direct API Request: Fetching news for {len(tickers)} companies...")

# ==========================================
# 3. Send request
# ==========================================
try:
    response = requests.get(NEWS_URL, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        articles = data.get('news', [])
        
        news_list = []
        for art in articles:
            news_list.append({
                "Date": art.get('created_at'),
                "Tickers": art.get('symbols'),
                "Headline": art.get('headline')
            })
        
        df = pd.DataFrame(news_list)
        
        if not df.empty:
            df.to_csv("sp500_news_final_test.csv", index=False)
            print("-" * 30)
            print(f"✅ Success! Retrieved {len(df)} articles.")
            print(f"📂 Saved to: sp500_news_final_test.csv")
        else:
            print("⚠️ No news found for these tickers.")
    else:
        print(f"❌ API Error: {response.status_code} - {response.text}")

except Exception as e:
    print(f"❌ Runtime Error: {e}")