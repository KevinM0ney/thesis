import os
from newsapi import NewsApiClient
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('NEWS_API_KEY')

api = NewsApiClient(api_key=API_KEY)

sources = api.get_sources(language='it')
for source in sources['sources']:
    print(f"ID: {source['id']}")
    print(f"name: {source['name']}")
    print(f"url: {source['url']}")

news = api.get_everything(
    q='intelligenza artificiale',
    sources='ansa',
    page_size=100
)

articles = news.get('articles', [])
if articles:
    for i, article in enumerate(articles, start=1):
        print(f"Articolo {i}:")
        print(f"Titolo: {article['title']}")
        print(f"Autore: {article.get('author', 'Non specificato')}")
        print(f"Fonte: {article['source']['name']}")
        print(f"URL: {article['url']}")
        print(f"Descrizione: {article.get('description', 'Nessuna descrizione disponibile')}\n")
else:
    print("Nessun articolo trovato.")