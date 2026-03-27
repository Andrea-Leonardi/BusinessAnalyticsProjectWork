#%%
import json
from urllib.request import urlopen

import certifi


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
FMP_ARTICLES_URL = (
    "https://financialmodelingprep.com/stable/fmp-articles"
    f"?page=0&limit=20&apikey={FMP_API_KEY}"
)


def get_jsonparsed_data(url: str):
    """Download one JSON payload and convert it to Python objects."""
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)


# ---------------------------------------------------------------------------
# Download And Print News
# ---------------------------------------------------------------------------

articles = get_jsonparsed_data(FMP_ARTICLES_URL)

if not isinstance(articles, list) or not articles:
    print("No news articles were returned by the API.")
else:
    print(f"Downloaded {len(articles)} news articles.\n")

    for index, article in enumerate(articles, start=1):
        title = article.get("title", "No title")
        article_link = article.get("link") or article.get("url") or "No URL"

        print(f"{index}. {title}")
        print(f"   {article_link}")
        print()

# %%
