import requests
from datetime import datetime
import pandas as pd
import time
import os
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class NYTimesScraper:
    def __init__(self):
        self.api_key = os.getenv('NYT_API_KEY')
        print("\n✨ Initializing New York Times Scraper...")
        print(f"🔑 API Key loaded: {'Yes' if self.api_key else 'No'}")
        self.base_url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
        self.start_date = '20220601'  # Format YYYYMMDD
        self.end_date = datetime.now().strftime('%Y%m%d')
        print(f"📅 Date range: {self.start_date} to {self.end_date}")
        print("=" * 100)

    def clean_html(self, text):
        # Remove HTML tags
        clean = re.compile('<.*?>')
        text = re.sub(clean, '', text)
        # Remove multiple spaces
        text = ' '.join(text.split())
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s.,!?-]', '', text)
        return text.strip()

    def fetch_articles(self, page=0):
        print(f"\n🔄 Preparing to fetch page {page}...")
        params = {
            'api-key': self.api_key,
            'q': '"artificial intelligence" OR "AI"',
            'begin_date': self.start_date,
            'end_date': self.end_date,
            'page': page,
            'sort': 'newest',
            'fq': 'document_type:("article")'
        }
        print("📝 Query parameters set:")
        print(f"    - Search terms: 'artificial intelligence' OR 'AI'")
        print(f"    - Page: {page}")
        print(f"    - Sorting: Newest first")

        try:
            print("\n📡 Sending request to NYT API...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            print("✅ Request successful!")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching articles: {e}")
            return None

    def process_articles(self, data):
        if not data or 'response' not in data or 'docs' not in data['response']:
            print("❌ No data to process")
            return []

        print("\n📊 Processing articles...")
        articles = []
        total_articles = len(data['response']['docs'])
        print(f"📚 Found {total_articles} articles on this page")

        for idx, article in enumerate(data['response']['docs'], 1):
            print(f"\n📰 Processing article {idx}/{total_articles}")
            print("=" * 100)

            # Get and clean the snippet
            full_text = article.get('lead_paragraph', '') or article.get('abstract', '') or article.get('snippet', '')
            full_text = self.clean_html(full_text)
            words = full_text.split()[:50]
            snippet = ' '.join(words)

            # Get the author(s)
            byline = article.get('byline', {}).get('original', '') or ''
            if byline.startswith('By '):
                byline = byline[3:]

            article_data = {
                'title': article.get('headline', {}).get('main', ''),
                'topic': article.get('section_name', ''),
                'author': byline,
                'snippet': snippet,
                'date': article.get('pub_date', ''),
                'newspaper': 'The New York Times'
            }

            # Detailed article information
            print(f"📌 Title: {article_data['title']}")
            print(f"📍 Topic: {article_data['topic']}")
            print(f"✍️ Author: {article_data['author']}")
            print(f"📅 Date: {article_data['date']}")
            print("\n📄 Snippet preview (first 50 words):")
            print(f"{snippet}...")
            print(f"\n🔗 Source: The New York Times")
            print("-" * 100)

            # Data validation checks
            print("\n✔️ Data validation:")
            print(f"  - Title present: {'Yes' if article_data['title'] else 'No'}")
            print(f"  - Author present: {'Yes' if article_data['author'] else 'No'}")
            print(f"  - Topic present: {'Yes' if article_data['topic'] else 'No'}")
            print(f"  - Date present: {'Yes' if article_data['date'] else 'No'}")
            print(f"  - Content present: {'Yes' if article_data['snippet'] else 'No'}")

            articles.append(article_data)
            print(f"\n✅ Article {idx} processed successfully")

        return articles

    def scrape_all_articles(self):
        print("\n🚀 Starting full scraping process...")
        all_articles = []
        page = 0
        max_pages = 100  # NYT API limit is 100 pages

        while page < max_pages:
            print(f"\n📑 Processing page {page}...")
            print("=" * 100)

            data = self.fetch_articles(page)

            if not data:
                print("❌ No data received, stopping scraping process")
                break

            if page == 0:
                total_hits = data['response'].get('meta', {}).get('hits', 0)
                estimated_pages = min(max_pages, (total_hits + 9) // 10)  # NYT returns 10 articles per page
                print(f"\n📊 Scraping Statistics:")
                print(f"    - Total articles found: {total_hits}")
                print(f"    - Estimated pages to fetch: {estimated_pages}")
                print(f"    - Estimated time: {estimated_pages * 12} seconds (12 second delay per page)")

            articles = self.process_articles(data)
            if not articles:
                break

            all_articles.extend(articles)

            print(f"\n📈 Progress: Page {page + 1}/{estimated_pages} completed")
            print(f"📚 Total articles collected so far: {len(all_articles)}")

            # NYT API rate limit is 5 calls per minute = 12 seconds between calls
            if page < max_pages - 1:
                print("\n⏳ Waiting 12 seconds before next request (respecting rate limits)...")
                time.sleep(12)

            page += 1

        print(f"\n🎉 Scraping completed! Total articles collected: {len(all_articles)}")
        return all_articles

    def save_to_csv(self, articles, filename='D:/PycharmProjects/Thesis/data/nyt_ai_articles.csv'):
        print(f"\n💾 Saving {len(articles)} articles to CSV...")
        print(f"📂 File path: {filename}")

        try:
            df = pd.DataFrame(articles)
            print("\n📊 DataFrame created with columns:")
            for col in df.columns:
                print(f"    - {col}: {len(df[col].unique())} unique values")

            df.to_csv(filename, index=False)
            print(f"\n✅ Successfully saved to {filename}")
            print(f"📊 File size: {os.path.getsize(filename) / (1024 * 1024):.2f} MB")

        except Exception as e:
            print(f"\n❌ Error saving to CSV: {e}")


def main():
    print("\n🚀 Starting The New York Times AI Articles Scraper")
    print("=" * 100)

    try:
        scraper = NYTimesScraper()
        print("\n🔍 Beginning article collection...")
        articles = scraper.scrape_all_articles()
        scraper.save_to_csv(articles)
        print("\n✨ Script completed successfully!")

    except Exception as e:
        print(f"\n❌ An error occurred during execution: {e}")

    finally:
        print("\n👋 Script execution ended")
        print("=" * 100)


if __name__ == "__main__":
    main()