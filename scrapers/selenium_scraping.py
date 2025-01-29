import re

from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
import pandas as pd
from utils import is_valid_date_format, ProxyManager, UserAgentManager, parse_date_from_url, extract_title_from_url
import time
import sys
from datetime import datetime
import random
import logging
from typing import Tuple, Optional, Dict, Any


class WebScraping:
    def __init__(self, log_file: str = 'scraper.log'):
        """Initialize WebScraping class with logging configuration"""
        # Configure logging
        self.setup_logging(log_file)

        # Initialize managers
        self.user_manager = UserAgentManager()
        self.proxy_manager = ProxyManager()

        # Initialize driver as None
        self.driver = None
        self.df = None

    def setup_logging(self, log_file: str) -> None:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )

    def random_delay(self, min_seconds: float = 1.0, max_seconds: float = 5.0) -> None:
        """Add random delay to avoid detection"""
        delay = random.uniform(min_seconds, max_seconds)
        logging.debug(f"Adding random delay of {delay:.2f} seconds")
        time.sleep(delay)

    def setup_driver(self, max_retries: int = 5) -> webdriver.Chrome:
        """Initialize webdriver with enhanced retry mechanism and random delays"""
        for attempt in range(max_retries):
            try:
                chrome_options = Options()
                chrome_options.add_argument('--disable-blink-features=AutomationControlled')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--start-maximized')
                chrome_options.add_argument('--window-size=1920,1080')

                # Additional stealth settings
                chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                chrome_options.add_experimental_option('useAutomationExtension', False)

                # Random User Agent
                user_agent = self.user_manager.get_random_user_agent()
                chrome_options.add_argument(f'user-agent={user_agent}')

                driver = webdriver.Chrome(options=chrome_options)
                driver.set_page_load_timeout(45)

                # Enhanced anti-detection measures
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})
                driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                    "source": """
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        });
                        Object.defineProperty(navigator, 'plugins', {
                            get: () => [1, 2, 3, 4, 5]
                        });
                    """
                })

                logging.info(f"[SUCCESS] Chrome driver initialized successfully (attempt {attempt + 1})")
                self.random_delay(2, 4)
                return driver

            except Exception as e:
                logging.error(f"[ERROR] Driver initialization attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                self.random_delay(5, 10)

    def load_existing_data(self, filepath: str = "data/ai4business.csv") -> pd.DataFrame:
        """Load existing CSV with enhanced error handling"""
        try:
            df = pd.read_csv(filepath)
            logging.info(f"[INFO] Successfully loaded existing data: {len(df)} records")
            return df
        except FileNotFoundError:
            logging.info("[INFO] No existing CSV found. Creating new DataFrame")
            return pd.DataFrame(columns=['testata', 'topic', 'date', 'title', 'snippet', 'author'])
        except Exception as e:
            logging.error(f"[ERROR] Error loading CSV: {str(e)}")
            raise

    def wait_for_element(self, by: By, value: str, timeout: int = 15, retries: int = 4) -> Any:
        """Wait for element with enhanced retry mechanism and random delays"""
        last_exception = None
        for attempt in range(retries):
            try:
                self.random_delay(0.5, 2)
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((by, value))
                )
                # Additional check for visibility
                if element.is_displayed():
                    self.random_delay(1, 3)
                    return element
                raise TimeoutException("Element found but not visible")

            except (TimeoutException, StaleElementReferenceException) as e:
                last_exception = e
                logging.warning(f"[WARNING] Attempt {attempt + 1} failed to find element {value}: {str(e)}")
                if attempt == retries - 1:
                    raise last_exception
                logging.info("Refreshing page and adding delay...")
                self.random_delay(3, 7)
                self.driver.refresh()
                self.random_delay(2, 5)

    def process_article(self, lines: list, index: int, source: str = "ilsole24") -> Tuple[
        Optional[Dict[str, str]], int]:
        """
        Process single article with enhanced validation

        Args:
            lines: List of text lines from the article
            index: Current index in the lines list
            source: Source identifier ('ilsole24' or other sources)

        Returns:
            Tuple of (article data dict or None, next index to process)
        """
        if source == "ilsole24":
            if index + 3 >= len(lines):
                return None, index + 1

            try:
                topic = lines[index].strip()
                date = lines[index + 1].strip()

                if not is_valid_date_format(date):
                    logging.warning(f"[WARNING] Invalid date format: {date}")
                    return None, index + 1

                title = lines[index + 2].strip()
                snippet = lines[index + 3].strip()

                # Enhanced author detection
                author = ""
                next_index = index + 4
                if index + 4 < len(lines):
                    potential_author = lines[index + 4].strip().lower()
                    if potential_author.startswith('di ') or potential_author.startswith('by '):
                        author = lines[index + 4].strip()
                        next_index = index + 5

                return {
                    'testata': "IlSole24ORE",
                    'topic': topic,
                    'date': date,
                    'title': title,
                    'snippet': snippet,
                    'author': author
                }, next_index

            except Exception as e:
                logging.error(f"[ERROR] Error processing article at index {index}: {str(e)}")
                return None, index + 1

        elif source == "corriere":
            # TODO: Implementare la logica per il Corriere
            pass
        else:
            logging.error(f"[ERROR] Unknown source: {source}")
            return None, index + 1

    def scrape_ilsole24(self, output_file: str = 'data/ilsole24.csv', start_page: int = 1, end_page: int = 900):
        """Main scraping function for IlSole24"""
        try:
            logging.info("\n" + "=" * 50)
            logging.info("STARTING SCRAPING PROCESS")
            logging.info("=" * 50 + "\n")

            self.df = self.load_existing_data()
            self.driver = self.setup_driver()

            base_url = ("https://www.ricerca24.ilsole24ore.com/?cmd=static&chId=30&path=/search/search_engine"
                        ".jsp&keyWords=intelligenza+artificiale&field=&id=&maxDocs=&criteria=0&pageNumber=1&simili=&a"
                        "ction=&chiaviSelezionate=&description=&flagPartialResult=&senv=r24&layout=r24&disable_user_rqq"
                        "=false&orderBy=data+desc&pageSize=10&fromDate=01/06/2022&toDate=19/12/2024&filter=all")

            logging.info("\n[INFO] Accessing base URL...")
            self.random_delay(3, 7)

            logging.info(f"[INFO] Will process pages from {start_page} to {end_page}")

            for page in range(start_page, end_page + 1):
                try:
                    logging.info(f"\n{'=' * 30}")
                    logging.info(f"Processing page {page}/{end_page}")
                    logging.info(f"{'=' * 30}")

                    current_url = base_url.replace("pageNumber=1", f"pageNumber={page}")

                    for attempt in range(4):
                        try:
                            self.driver.get(current_url)
                            self.random_delay(2, 5)

                            articles = self.wait_for_element(By.ID, 's_main')
                            self.random_delay(1, 3)

                            articles_in_lines = articles.text.strip().splitlines()

                            i = 0
                            articles_in_page = 0
                            while i < len(articles_in_lines):
                                if not articles_in_lines[i].strip():
                                    i += 1
                                    continue

                                article_data, next_index = self.process_article(articles_in_lines, i)
                                if article_data:
                                    self.df = pd.concat([self.df, pd.DataFrame([article_data])], ignore_index=True)
                                    logging.info(f"[SUCCESS] Added: {article_data['title'][:50]}...")
                                    articles_in_page += 1
                                    self.random_delay(0.5, 1.5)
                                i = next_index

                            logging.info(f"\n[INFO] Found {articles_in_page} articles on page {page}")
                            break

                        except (TimeoutException, WebDriverException) as e:
                            if attempt == 3:
                                raise
                            logging.warning(f"[WARNING] Page {page} attempt {attempt + 1} failed: {str(e)}")
                            logging.info("Waiting before retry...")
                            self.random_delay(5, 15)
                            self.driver.refresh()

                    # Save progress more frequently
                    if page % 3 == 0 or page == end_page:
                        self.df.to_csv(output_file, index=False)
                        logging.info(f"\n[INFO] Progress saved. Total articles: {len(self.df)}")

                    logging.info("\nWaiting before next page...")
                    self.random_delay(3, 8)

                except Exception as e:
                    logging.error(f"[ERROR] Error processing page {page}: {str(e)}")
                    logging.info("Continuing with next page...")
                    self.random_delay(5, 10)
                    continue

        except Exception as e:
            logging.critical(f"\n[CRITICAL ERROR] {str(e)}")
            raise

        finally:
            if self.driver:
                self.driver.quit()
            if self.df is not None:
                self.df.to_csv(output_file, index=False)
            logging.info("\n" + "=" * 50)
            logging.info("SCRAPING COMPLETED")
            logging.info(f"Total articles collected: {len(self.df) if self.df is not None else 0}")
            logging.info("=" * 50 + "\n")

    def scrape_ilcorrieredellasera(self, output_file: str = 'data/il_corriere_della_sera.csv',
                                   start_page: int = 1, end_page: int = 100):
        """Main scraping function for Il Corriere della Sera"""
        try:
            logging.info("\n" + "=" * 50)
            logging.info("STARTING SCRAPING PROCESS")
            logging.info("=" * 50 + "\n")

            self.df = self.load_existing_data(output_file)
            self.driver = self.setup_driver()

            base_url = "https://www.corriere.it/ricerca/?refresh_ce=&q=intelligenza%2520artificiale&page=1"

            logging.info("\n[INFO] Accessing base URL...")
            self.random_delay(3, 7)

            logging.info(f"[INFO] Will process pages from {start_page} to {end_page}")

            for page in range(start_page, end_page + 1):
                try:
                    logging.info(f"\n{'=' * 30}")
                    logging.info(f"Processing page {page}/{end_page}")
                    logging.info(f"{'=' * 30}")

                    current_url = base_url.replace("page=1", f"page={page}")

                    for attempt in range(4):
                        try:
                            self.driver.get(current_url)
                            self.random_delay(2, 5)

                            # Base XPath for articles
                            base_xpath = "/html/body/main/div/div/section/div/div/div[2]/div"
                            articles_data = []

                            # Try to find articles from index 1 to 20 (assuming max 20 articles per page)
                            for i in range(1, 21):
                                try:
                                    # Full XPath for current article
                                    article_xpath = f"{base_xpath}[{i}]"

                                    # Try to find article
                                    article = WebDriverWait(self.driver, 15).until(
                                        EC.presence_of_element_located((By.XPATH, article_xpath))
                                    )
                                    x_path_href = "/div/div[2]/div/div/h3/a"
                                    # Get article link with class has-text-black
                                    link_element = article.find_element(By.XPATH, article_xpath + x_path_href)
                                    url = link_element.get_attribute("href")
                                    title = article.text.splitlines()[0]

                                    # Extract and process data
                                    if url and len(title) > 7:
                                        # Process URL with existing content
                                        url_parts = url.split('.it/')[1].split('/') if '.it/' in url else []

                                        # Extract topic based on URL structure (handles both 2 and 3 level paths)
                                        if len(url_parts) >= 4:
                                            topic = '/'.join(
                                                url_parts[:-1])  # Join all parts except the last (date+filename)
                                        else:  # URL with 2 levels like tecnologia
                                            topic = '/'.join(url_parts[:-1])

                                        date = parse_date_from_url(url)

                                        # Process text content for author and snippet
                                        if article.text.lower().splitlines()[1].startswith("di "):
                                            author = article.text.splitlines()[1]
                                            snippet = "\n".join(article.text.splitlines()[2:])
                                        elif article.text.splitlines()[1]:
                                            author = ""
                                            snippet = "\n".join(article.text.splitlines()[1:])
                                        else:
                                            author = ""
                                            snippet = ""

                                        article_data = {
                                            'testata': "Corriere della Sera",

                                            'topic': topic,

                                            'date': date,

                                            'title': title,

                                            'snippet': snippet,

                                            'author': author
                                        }
                                        articles_data.append(article_data)
                                        logging.info(f"[SUCCESS] Found article: {title[:50]}...")

                                    elif url and (title is None or len(
                                            title) <= 7):  # URL exists but title is missing or invalid

                                        url_parts = url.split('.it/')[1].split('/') if '.it/' in url else []

                                        # Extract topic based on URL structure (handles both 2 and 3 level paths)

                                        if len(url_parts) >= 4:  # URL with 3 levels

                                            topic = '/'.join(url_parts[:-1])

                                        else:  # URL with 2 levels

                                            topic = '/'.join(url_parts[:-1])

                                        date = parse_date_from_url(url)

                                        article_data = {

                                            'testata': "Corriere della Sera",

                                            'topic': topic,

                                            'date': date,

                                            'title': extract_title_from_url(url),  # Empty as not available/valid

                                            'snippet': "",  # Empty as not available

                                            'author': ""  # Empty as not available

                                        }

                                        articles_data.append(article_data)

                                        logging.info(f"[SUCCESS] Found article with URL only: {url[:100]}...")

                                except TimeoutException:
                                    # No more articles found, break the loop
                                    break
                                except Exception as e:
                                    logging.warning(f"Error processing article {i} on page {page}: {str(e)}")
                                    continue

                            if articles_data:
                                # Add all found articles to DataFrame
                                self.df = pd.concat([self.df, pd.DataFrame(articles_data)], ignore_index=True)
                                logging.info(f"\n[INFO] Found {len(articles_data)} articles on page {page}")
                                break
                            else:
                                raise TimeoutException("No articles found on page")

                        except Exception as e:
                            if attempt == 3:  # Last attempt
                                raise
                            logging.warning(f"[WARNING] Page {page} attempt {attempt + 1} failed: {str(e)}")
                            logging.info("Waiting before retry...")
                            self.random_delay(5, 15)
                            self.driver.refresh()

                    # Save progress more frequently
                    if page % 3 == 0 or page == end_page:
                        self.df.to_csv(output_file, index=False)
                        logging.info(f"\n[INFO] Progress saved. Total articles: {len(self.df)}")

                    logging.info("\nWaiting before next page...")
                    self.random_delay(3, 8)

                except Exception as e:
                    logging.error(f"[ERROR] Error processing page {page}: {str(e)}")
                    logging.info("Continuing with next page...")
                    self.random_delay(5, 10)
                    continue

        finally:
            if self.driver:
                self.driver.quit()
            if self.df is not None:
                self.df.to_csv(output_file, index=False)
            logging.info("\n" + "=" * 50)
            logging.info("SCRAPING COMPLETED")
            logging.info(f"Total articles collected: {len(self.df) if self.df is not None else 0}")
            logging.info("=" * 50 + "\n")

    def scrape_ai4business(self, output_file: str = 'data/ai4business.csv',
                           start_page: int = 1, end_page: int = 270):
        """Main function for scraping AI4Business website."""
        try:
            logging.info("\n" + "=" * 50)
            logging.info("STARTING SCRAPING PROCESS")
            logging.info("=" * 50 + "\n")

            self.df = self.load_existing_data(output_file)
            self.driver = self.setup_driver()

            base_url = "https://www.ai4business.it/intelligenza-artificiale/page/1"

            logging.info("\n[INFO] Accessing base URL...")
            self.random_delay(3, 7)

            logging.info(f"[INFO] Will process pages from {start_page} to {end_page}")

            for page in range(start_page, end_page + 1):
                try:
                    logging.info(f"\n{'=' * 30}")
                    logging.info(f"Processing page {page}/{end_page}")
                    logging.info(f"{'=' * 30}")

                    current_url = base_url.replace("page/1", f"page/{page}")

                    for attempt in range(4):
                        try:
                            self.driver.get(current_url)
                            self.random_delay(2, 5)
                            # Fixed class name selector
                            elements = self.driver.find_elements(By.CLASS_NAME, "large-margin-bottom")

                            if elements:
                                break  # Successfully found elements, exit retry loop
                        except Exception as e:
                            if attempt == 3:  # Last attempt
                                logging.error(f"Failed to process page {page} after all attempts: {e}")
                                continue
                            logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                            self.random_delay(5, 10)  # Longer delay between retries

                    for element in elements:
                        try:
                            print(element.text)
                            # Add your processing logic here
                        except Exception as e:
                            logging.error(f"Error processing element: {e}")

                except Exception as e:
                    logging.error(f"Error processing page {page}: {e}")
                    continue

        except Exception as e:
            logging.error(f"Critical error in scraping process: {e}")
            raise
        finally:
            if hasattr(self, 'driver'):
                self.driver.quit()

    def scrape_wired(self, output_file: str = 'data/wired.csv', max_clicks: int = 50):
        """Main scraping function for Wired.it articles about AI using specific XPath"""
        try:
            logging.info("\n" + "=" * 50)
            logging.info("STARTING WIRED.IT SCRAPING PROCESS")
            logging.info("=" * 50 + "\n")

            self.df = self.load_existing_data(output_file)
            self.driver = self.setup_driver()

            base_url = 'https://www.wired.it/search/?q=INTELLIGENZA+ARTIFICIALE&sort=publishdate+desc'

            logging.info("\n[INFO] Accessing Wired.it...")
            self.driver.get(base_url)
            self.random_delay(3, 7)

            clicks = 0
            total_articles = 0

            # Base XPath for articles
            base_xpath = "/html/body/div[1]/div/div/main/div[2]/div/div/section/div/div[1]/div/div/div"

            while clicks < max_clicks:
                try:
                    # Process articles currently loaded
                    for i in range(1, 21):  # Assuming max 20 articles per load
                        try:
                            # Construct full XPath for current article
                            article_xpath = f"{base_xpath}[{i}]/div[2]"

                            # Wait for article to be present
                            article = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, article_xpath))
                            )

                            # Extract title and link
                            try:
                                title_element = article.find_element(By.TAG_NAME, "h3")
                                link_element = title_element.find_element(By.TAG_NAME, "a")
                                title = title_element.text.strip()
                                link = link_element.get_attribute("href")
                            except Exception as e:
                                logging.warning(f"Could not extract title/link for article {i}: {str(e)}")
                                continue

                            # Extract date from URL
                            date = self.extract_date_from_wired_url(link)

                            # Extract description (dek)
                            try:
                                description = article.find_element(By.CLASS_NAME, "summary-item__dek").text.strip()
                            except:
                                description = ""

                            # Extract author
                            try:
                                author = article.find_element(By.CLASS_NAME, "summary-item__byline").text.strip()
                            except:
                                author = ""

                            article_data = {
                                'testata': "Wired.it",
                                'topic': "tech/intelligenza-artificiale",
                                'date': date,
                                'title': title,
                                'snippet': description,
                                'author': author
                            }

                            # Add to DataFrame if not duplicate
                            if not self.is_duplicate_article(article_data):
                                self.df = pd.concat([self.df, pd.DataFrame([article_data])], ignore_index=True)
                                total_articles += 1
                                logging.info(f"[SUCCESS] Added: {title[:50]}...")
                                self.random_delay(0.5, 1.5)

                        except TimeoutException:
                            # No more articles in current load
                            break
                        except Exception as e:
                            logging.error(f"Error processing article {i}: {str(e)}")
                            continue

                    # Try to click "Load More" button
                    try:
                        load_more = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.CLASS_NAME, "ButtonLabel-cjAuJN.hzwRuG.button__label"))
                        )
                        self.driver.execute_script("arguments[0].click();", load_more)
                        clicks += 1
                        logging.info(f"[INFO] Clicked 'Load More' button ({clicks}/{max_clicks})")
                        self.random_delay(2, 4)
                    except TimeoutException:
                        logging.info("No more 'Load More' button found. Breaking loop.")
                        break

                    # Save progress periodically
                    if clicks % 5 == 0:
                        self.df.to_csv(output_file, index=False)
                        logging.info(f"\n[INFO] Progress saved. Total articles: {len(self.df)}")

                except Exception as e:
                    logging.error(f"Error during main loop: {str(e)}")
                    break

        except Exception as e:
            logging.critical(f"\n[CRITICAL ERROR] {str(e)}")
            raise

        finally:
            if self.driver:
                self.driver.quit()
            if self.df is not None:
                self.df.to_csv(output_file, index=False)
            logging.info("\n" + "=" * 50)
            logging.info("WIRED.IT SCRAPING COMPLETED")
            logging.info(f"Total new articles collected: {total_articles}")
            logging.info("=" * 50 + "\n")

    def extract_date_from_wired_url(self, url: str) -> str:
        """Extract and format date from Wired.it URL"""
        try:
            # Extract date pattern from URL (assuming format like /YYYY/MM/DD/)
            pattern = r'/(\d{4})/(\d{2})/(\d{2})/'
            match = re.search(pattern, url)
            if match:
                year, month, day = match.groups()
                return f"{day}/{month}/{year}"
        except Exception as e:
            logging.error(f"Error extracting date from URL {url}: {str(e)}")
        return ""

    def is_duplicate_article(self, article_data: Dict[str, str]) -> bool:
        """Check if article already exists in DataFrame based on title and date"""
        if self.df is None or len(self.df) == 0:
            return False

        mask = (self.df['title'] == article_data['title']) & (self.df['date'] == article_data['date'])
        return mask.any()


def main():
    try:
        scraper = WebScraping()
        scraper.scrape_wired()
    except KeyboardInterrupt:
        logging.info("\n[INFO] Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"\n[CRITICAL ERROR] Unhandled exception: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()