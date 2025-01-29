import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import time


class AINewsScraper:
    def __init__(self, csv_path):
        """
        Initialize the scraper with the path to the CSV file.

        Args:
            csv_path (str): Path to the CSV file for storing scraped data
        """
        self.csv_path = csv_path
        self.base_url = 'https://ainews.it/'
        self.categories = ['finanza', 'istruzione', 'governance', 'etica', 'militare',
                           'legal', 'sanità', 'sociale', 'mercato', 'lavoro', 'sicurezza',
                           'infrastrutture', 'editoria-media', 'psicologia', 'cultura',
                           'ambiente', 'innovazione', 'politica', 'sport', 'trasporti']
        self.driver = None
        self.df = None

    def load_existing_data(self):
        """Load existing data from CSV file."""
        try:
            self.df = pd.read_csv(self.csv_path)
            print(f"Initial DataFrame shape: {self.df.shape}")
        except FileNotFoundError:
            self.df = pd.DataFrame()
            print("No existing file found. Starting with empty DataFrame.")

    def setup_driver(self):
        """Initialize the Chrome webdriver."""
        self.driver = webdriver.Chrome()

    def scroll_page(self):
        """Scroll the page to load all content."""
        scroll_pause_time = 5
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        scroll_count = 0

        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause_time)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_count += 1
            print(f"Scroll {scroll_count} completed")

            if new_height == last_height:
                print("Reached end of page")
                break
            last_height = new_height

    def extract_article_data(self, element):
        """
        Extract data from a single article element.

        Args:
            element (BeautifulSoup): HTML element containing article data

        Returns:
            dict: Extracted article data
        """
        return {
            'testata': 'AI News',
            'topic': self.current_category,
            'date': element.find('span', class_='d-block pt-3 mb-0 opacity-50').text,
            'title': element.find('a', class_='title animation').text,
            'snippet': element.find('div', class_='col-12 col-md-10 pe-3').text,
            'author': ''
        }

    def scrape_category(self, category):
        """
        Scrape articles from a specific category.

        Args:
            category (str): Category to scrape

        Returns:
            list: List of scraped article data
        """
        self.current_category = category
        print(f"\nScraping category: {category}")

        full_url = f"{self.base_url}{category}/"
        self.driver.get(full_url)
        self.scroll_page()

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        elements = soup.find_all('div', class_='news')
        print(f"Found {len(elements)} articles in {category}")

        return [self.extract_article_data(element) for element in elements]

    def process_data(self, new_rows):
        """
        Process and deduplicate the scraped data.

        Args:
            new_rows (list): List of newly scraped articles
        """
        new_df = pd.DataFrame(new_rows)
        print(f"New scraped data shape: {new_df.shape}")

        self.df = pd.concat([self.df, new_df], ignore_index=True)
        print(f"Shape after concatenation: {self.df.shape}")

        columns_for_dedup = ['testata', 'date', 'title', 'snippet', 'author']
        self.df = self.df.drop_duplicates(subset=columns_for_dedup, keep='first')
        print(f"Final shape after removing duplicates: {self.df.shape}")

    def save_data(self):
        """Save the processed data to CSV file."""
        self.df.to_csv(self.csv_path, index=True)
        print("Data saved to CSV successfully")

    def print_statistics(self):
        """Print final statistics about the scraped data."""
        print("\nFinal Statistics:")
        print(f"Total number of articles: {len(self.df)}")
        print("\nArticles per topic:")
        print(self.df['topic'].value_counts())

    def run(self):
        """Run the complete scraping process."""
        print("Starting the scraping process...")

        self.load_existing_data()
        self.setup_driver()

        new_rows = []
        for category in self.categories:
            new_rows.extend(self.scrape_category(category))

        self.driver.quit()
        print("\nWeb scraping completed")

        self.process_data(new_rows)
        self.save_data()
        self.print_statistics()


# Usage example:
if __name__ == "__main__":
    scraper = AINewsScraper('/data/test/ainews.csv')
    scraper.run()