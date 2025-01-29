import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
BASE_URL = 'https://www.ai4business.it/intelligenza-artificiale/page/'
TOTAL_PAGES = 273
OUTPUT_FILE = '../data/test/ai4business.csv'
# Delay range in seconds for requests
MIN_DELAY = 200
MAX_DELAY = 800


def setup_session():
    """Configure requests session with appropriate headers"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    })
    return session


def scrape_page(session, page_number):
    """Scrape a single page and return its data"""
    page_url = f"{BASE_URL}{page_number}"

    # Add random delay before request
    delay = random.uniform(MIN_DELAY, MAX_DELAY) / 100
    time.sleep(delay)

    print(f"Scraping page {page_number}/{TOTAL_PAGES} (delay: {delay:.2f}s)")

    try:
        response = session.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all required elements
        elements = {
            'topics': soup.find_all('p', class_='card-post__occhiello p-text'),
            'titles': soup.find_all('h2', class_='card-post__title gd-text'),
            'dates': soup.find_all('p', class_='card-post__data gl-text'),
            'authors': soup.find_all('p', class_='card-post__firma'),
            'urls': soup.find_all('a', class_="full-absolute z-10")
        }

        return elements

    except requests.exceptions.RequestException as e:
        logging.error(f"Error scraping page {page_number}: {str(e)}")
        return None


def main():
    # Load existing data if file exists
    try:
        df = pd.read_csv(OUTPUT_FILE)
        logging.info(f"Loaded existing data with {len(df)} records")
    except FileNotFoundError:
        df = pd.DataFrame(columns=['testata', 'topic', 'title', 'date', 'author', 'snippet'])
        logging.info("Created new DataFrame")

    session = setup_session()
    start_time = time.time()
    articles_count = 0

    for page in range(1, TOTAL_PAGES + 1):
        elements = scrape_page(session, page)

        if elements is None:
            print("NO ELEMENTS.")
            continue

        # Process page data
        for topic, title, date, author, url in zip(
                elements['topics'],
                elements['titles'],
                elements['dates'],
                elements['authors'],
                elements['urls']
        ):
            # Extract text and clean data
            row = {
                "testata": 'AI4Business',
                "topic": topic.get_text().strip(),
                "title": title.get_text().strip(),
                "date": date.get_text().strip(),
                "author": author.get_text().strip(),
                "snippet": url.get('href', '')
            }

            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            articles_count += 1

        # Save progress every 10 pages
        if page % 10 == 0:
            df.to_csv(OUTPUT_FILE, index=False)
            logging.info(f"Progress saved - {articles_count} articles collected so far")

    # Final save
    df.to_csv(OUTPUT_FILE, index=False)

    # Print summary
    elapsed_time = time.time() - start_time
    print("\nScraping completed!")
    print(f"Total articles collected: {articles_count}")
    print(f"Total time elapsed: {elapsed_time / 60:.2f} minutes")
    print(f"Average time per page: {elapsed_time / TOTAL_PAGES:.2f} seconds")
    print(f"Data saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()