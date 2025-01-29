from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime


class WiredScraper:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.url = 'https://www.wired.it/search/?q=INTELLIGENZA+ARTIFICIALE&sort=publishdate+desc'
        self.target_date = datetime.strptime('01.06.2022', '%d.%m.%Y')
        self.df = None
        self.driver = None
        self.start_time = time.time()  # Add start time
        print(f"Scraper initialized at {datetime.now().strftime('%H:%M:%S')}")

    def get_elapsed_time(self):
        """Return elapsed time since start in minutes and seconds"""
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        return f"{minutes}m {seconds}s"

    def parse_date(self, date_str):
        """Parse date string into datetime object"""
        try:
            date_str = date_str.strip()
            # Converting date format from DD.MM.YYYY to a datetime object
            return datetime.strptime(date_str, '%d.%m.%Y')
        except Exception as e:
            print(f"Error parsing date '{date_str}': {e}")
            return None

    def load_existing_data(self):
        print(f"\n[{self.get_elapsed_time()}] Loading existing data...")
        try:
            self.df = pd.read_csv(self.csv_path)
            print(f"Initial DataFrame shape: {self.df.shape}")
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.df = pd.DataFrame(columns=[
                'testata', 'topic', 'date', 'title', 'snippet', 'author'
            ])
            print("Starting with new empty DataFrame")

    def setup_driver(self):
        print(f"\n[{self.get_elapsed_time()}] Setting up Chrome driver...")
        options = webdriver.ChromeOptions()
        options.add_argument('--start-maximized')
        self.driver = webdriver.Chrome(options=options)
        self.driver.get(self.url)
        print("Driver initialized and page loaded")
        time.sleep(5)

    def handle_cookie_popup(self):
        print(f"\n[{self.get_elapsed_time()}] Checking for cookie popup...")
        try:
            cookie_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
            )
            cookie_button.click()
            print("Cookie popup handled")
            time.sleep(1)
        except:
            print("No cookie popup found or couldn't be handled")

    def click_load_more(self, num_clicks=5):
        print(f"\n[{self.get_elapsed_time()}] Starting click sequence ({num_clicks} clicks)...")
        for i in range(num_clicks):
            try:
                button = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "button--primary"))
                )

                self.driver.execute_script("arguments[0].scrollIntoView({ behavior: 'smooth', block: 'center' });",
                                           button)
                time.sleep(2)

                try:
                    button.click()
                except ElementClickInterceptedException:
                    self.driver.execute_script("arguments[0].click();", button)

                print(f"Click {i + 1}/{num_clicks} completed at {self.get_elapsed_time()}")
                time.sleep(3)

            except Exception as e:
                print(f"Error clicking button: {e}")
                break

    def extract_article_data(self, article):
        try:
            topic = article.find('div', class_='SummaryItemRubricWrapper-jjNbqu')
            topic = topic.text.strip() if topic else ""

            title = article.find('h2', class_='summary-item__hed')
            title = title.text.strip() if title else ""

            snippet = article.find('div', class_='summary-item__dek')
            snippet = snippet.text.strip() if snippet else ""

            author = article.find('span', class_='byline__name')
            author = author.text.strip() if author else ""

            date = article.find('time', class_='summary-item__publish-date')
            date = date.text.strip() if date else ""

            if all([title, date]):
                article_data = {
                    'testata': 'Wired',
                    'topic': topic,
                    'date': date,
                    'title': title,
                    'snippet': snippet,
                    'author': author
                }
                print(f"\nArticle found at {self.get_elapsed_time()}:")
                print(f"Testata: Wired")
                print(f"Topic: {topic}")
                print(f"Date: {date}")
                print(f"Title: {title}")
                print(f"Author: {author}")
                print(f"Snippet: {snippet[:100]}...")  # Print first 100 chars of snippet
                return article_data
            return None
        except Exception as e:
            print(f"Error extracting article data: {e}")
            return None

    def scrape_articles(self):
        """
        Scrape articles from the website until reaching the target date.

        Returns:
            list: List of dictionaries containing article data
        """
        print(f"\n[{self.get_elapsed_time()}] Starting article scraping...")
        new_rows = []
        reached_target_date = False

        try:
            # Handle any cookie consent popup that might appear
            self.handle_cookie_popup()

            while not reached_target_date:
                # Click the "Load More" button 5 times to load more content
                self.click_load_more(5)

                try:
                    # Get the current page source and parse it
                    page_source = self.driver.page_source
                    print(f"\n[{self.get_elapsed_time()}] Parsing current page...")
                    soup = BeautifulSoup(page_source, 'html.parser')

                    # Find all article elements on the page
                    articles = soup.find_all('div', class_='summary-item--article')

                    if not articles:
                        print("No more articles found")
                        break

                    print(f"Found {len(articles)} articles on current page")

                    # Process each article
                    for article in articles:
                        article_data = self.extract_article_data(article)

                        if article_data:
                            # Parse the article date and check if we've reached our target date
                            date_obj = self.parse_date(article_data['date'])

                            if date_obj and date_obj < self.target_date:
                                reached_target_date = True
                                print(f"\nReached target date ({self.target_date}) at {self.get_elapsed_time()}")
                                break

                            # Add the article data to our collection
                            new_rows.append(article_data)

                    if reached_target_date:
                        break

                except Exception as e:
                    # Log any errors that occur during page parsing but continue with next iteration
                    print(f"Error parsing page: {e}")
                    continue

        except Exception as e:
            # Log any critical errors that occur during scraping
            print(f"Critical error in scrape_articles: {e}")
            return new_rows
        finally:
            # Log the final count of scraped articles
            print(f"\n[{self.get_elapsed_time()}] Scraping completed. Found {len(new_rows)} articles")

        return new_rows

    def process_data(self, new_rows):
        print(f"\n[{self.get_elapsed_time()}] Processing scraped data...")
        if not new_rows:
            print("No new data to process")
            return

        new_df = pd.DataFrame(new_rows)
        print(f"New scraped data shape: {new_df.shape}")

        self.df = pd.concat([self.df, new_df], ignore_index=True)
        print(f"Shape after concatenation: {self.df.shape}")

        self.df = self.df.drop_duplicates(subset=['title', 'date'], keep='first')
        print(f"Final shape after removing duplicates: {self.df.shape}")

    def save_data(self):
        print(f"\n[{self.get_elapsed_time()}] Saving data to CSV...")
        self.df.to_csv(self.csv_path, index=False)
        print(f"Data saved successfully to {self.csv_path}")

    def print_statistics(self):
        print(f"\n[{self.get_elapsed_time()}] Final Statistics:")
        print(f"Total number of articles: {len(self.df)}")
        print("\nArticles per topic:")
        print(self.df['topic'].value_counts())

    def run(self):
        print(f"\n[{self.get_elapsed_time()}] Starting the scraping process...")

        self.load_existing_data()
        self.setup_driver()

        try:
            new_rows = self.scrape_articles()
            if new_rows:
                self.process_data(new_rows)
                self.save_data()
                self.print_statistics()
        except Exception as e:
            print(f"Error during scraping: {e}")
            raise
        finally:
            if self.driver:
                print(f"\n[{self.get_elapsed_time()}] Closing browser...")
                try:
                    self.driver.quit()
                    print(f"Browser closed successfully")
                except Exception as e:
                    print(f"Error closing browser: {e}")
                finally:
                    print(f"Total execution time: {self.get_elapsed_time()}")


if __name__ == "__main__":
    scraper = WiredScraper('/data/test/wired.csv')
    scraper.run()
