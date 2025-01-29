import requests
from datetime import datetime
import pandas as pd
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class GuardianScraper:
    def __init__(self):
        self.api_key = os.getenv('GUARDIAN_API_KEY')
        print("\n✨ Initializing Guardian Scraper...")
        print(f"🔑 API Key loaded: {'Yes' if self.api_key else 'No'}")
        self.base_url = 'https://content.guardianapis.com/search'
        self.start_date = '2022-06-01'
        self.end_date = datetime.now().strftime('%Y-%m-%d')
        print(f"📅 Date range: {self.start_date} to {self.end_date}")
        print("=" * 100)

    def fetch_articles(self, page=1):
        print(f"\n🔄 Preparing to fetch page {page}...")
        params = {
            'api-key': self.api_key,
            'q': '"artificial intelligence" OR "AI"',
            'from-date': self.start_date,
            'to-date': self.end_date,
            'page': page,
            'page-size': 50,
            'show-fields': 'headline,byline,wordcount,publicationDate,body',
            'order-by': 'newest'
        }
        print("📝 Query parameters set:")
        print(f"    - Search terms: 'artificial intelligence' OR 'AI'")
        print(f"    - Page size: 50 articles")
        print(f"    - Ordering: Newest first")

        try:
            print("\n📡 Sending request to The Guardian API...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            print("✅ Request successful!")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching articles: {e}")
            return None

    def process_articles(self, data):
        if not data or 'response' not in data:
            print("❌ No data to process")
            return []

        print("\n📊 Processing articles...")
        articles = []
        total_articles = len(data['response']['results'])
        print(f"📚 Found {total_articles} articles on this page")

        for idx, article in enumerate(data['response']['results'], 1):
            print(f"\n📰 Processing article {idx}/{total_articles}")
            print("=" * 100)

            import re

            def clean_html(text):
                # Remove HTML tags
                clean = re.compile('<.*?>')
                text = re.sub(clean, '', text)
                # Remove multiple spaces
                text = ' '.join(text.split())
                # Remove special characters but keep basic punctuation
                text = re.sub(r'[^\w\s.,!?-]', '', text)
                return text.strip()

            # Get the full body text and clean it
            full_body = article['fields'].get('body', '')
            full_body = clean_html(full_body)

            # Split into words and take first 50
            words = full_body.split()[:50]
            # Join back into text
            snippet = ' '.join(words)

            article_data = {
                'title': article['fields'].get('headline', ''),
                'topic': article.get('sectionName', ''),
                'author': article['fields'].get('byline', ''),
                'snippet': snippet,
                'date': article.get('webPublicationDate', ''),
                'newspaper': 'The Guardian'
            }

            # Detailed article information
            print(f"📌 Title: {article_data['title']}")
            print(f"📍 Topic: {article_data['topic']}")
            print(f"✍️ Author: {article_data['author']}")
            print(f"📅 Date: {article_data['date']}")
            print("\n📄 Snippet preview (first 50 words):")
            # Split into words and take first 50
            words = article_data['snippet'].split()[:50]
            preview = ' '.join(words)
            print(f"{preview}...")
            print(f"\n🔗 Source: The Guardian")
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

        print(f"\n📊 Processed {len(articles)} articles in total for this page")
        return articles

    def scrape_all_articles(self):
        print("\n🚀 Starting full scraping process...")
        all_articles = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            print(f"\n📑 Processing page {page}...")
            print("=" * 100)

            data = self.fetch_articles(page)

            if not data:
                print("❌ No data received, stopping scraping process")
                break

            if page == 1:
                total_pages = data['response']['pages']
                total_results = data['response']['total']
                print(f"\n📊 Scraping Statistics:")
                print(f"    - Total pages to fetch: {total_pages}")
                print(f"    - Total articles found: {total_results}")
                print(f"    - Estimated time: {total_pages} seconds (1 second per page)")

            articles = self.process_articles(data)
            all_articles.extend(articles)

            print(f"\n📈 Progress: Page {page}/{total_pages} completed")
            print(f"📚 Total articles collected so far: {len(all_articles)}")

            if page < total_pages:
                print("\n⏳ Waiting 1 second before next request (respecting rate limits)...")
                time.sleep(1)

            page += 1

        print(f"\n🎉 Scraping completed! Total articles collected: {len(all_articles)}")
        return all_articles

    def save_to_csv(self, articles, filename='D:/PycharmProjects/Thesis/data/guardian_ai_articles.csv'):
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
    print("\n🚀 Starting The Guardian AI Articles Scraper")
    print("=" * 100)

    try:
        scraper = GuardianScraper()
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