import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
import pandas as pd
from bs4 import BeautifulSoup
import requests
import spacy
import warnings
from fake_useragent import UserAgent

warnings.filterwarnings("ignore")


def is_valid_date_format(date_string):
    """
    Check if date matches format: dd month(in Italian) yyyy
    Example: '19 dicembre 2024'
    """
    # List of Italian months
    italian_months = [
        'gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
        'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre'
    ]

    # Try to split the date string
    try:
        parts = date_string.strip().split()
        if len(parts) != 3:  # Must have exactly 3 parts
            return False

        day, month, year = parts

        # Check day (1-31)
        if not day.isdigit() or not (1 <= int(day) <= 31):
            return False

        # Check month (must be in Italian)
        if month.lower() not in italian_months:
            return False

        # Check year (4 digits)
        if not year.isdigit() or len(year) != 4:
            return False

        return True

    except:
        return False


def parse_date_from_url(url: str) -> Optional[str]:
    """
    Parse date from Corriere URL format (e.g., '.../24_dicembre_20/...' -> '20 dicembre 2024')

    Args:
        url: Article URL containing the date

    Returns:
        Formatted date string or None if parsing fails
    """
    try:
        # Extract the date part using regex
        import re
        date_pattern = r'/(\d{2})_([a-z]+)_(\d{2})/'
        match = re.search(date_pattern, url.lower())

        if not match:
            return None

        year, month, day = match.groups()

        # Convert 2-digit year to 4-digit year
        full_year = f"20{year}"  # Assumes years 2000-2099

        # Italian months mapping (if needed)
        month_mapping = {
            'gennaio': 'gennaio',
            'febbraio': 'febbraio',
            'marzo': 'marzo',
            'aprile': 'aprile',
            'maggio': 'maggio',
            'giugno': 'giugno',
            'luglio': 'luglio',
            'agosto': 'agosto',
            'settembre': 'settembre',
            'ottobre': 'ottobre',
            'novembre': 'novembre',
            'dicembre': 'dicembre'
        }

        # Validate month
        if month not in month_mapping:
            return None

        # Format the date
        formatted_date = f"{day} {month_mapping[month]} {full_year}"

        return formatted_date

    except Exception as e:
        return None


def extract_title_from_url(url: str) -> str:
    """
    Extract title from URL by getting the part between date and ID
    Example: from '.../24_dicembre_20/la-repubblica-del-congo...-80e29e36-...'
    extracts 'la-repubblica-del-congo...'
    """
    try:
        # Split URL by '/' and get the last part before the file extension
        last_part = url.split('/')[-1].split('.')[0]

        # Find where the date pattern ends (XX_month_XX/)
        date_parts = last_part.split('-', 1)  # Split on first hyphen
        if len(date_parts) < 2:
            return ""

        # Get everything after the first hyphen until the last segment that looks like an ID
        title_parts = date_parts[1].split('-')

        # Remove the last segment if it looks like an ID (has letters and numbers and is long)
        if len(title_parts[-1]) >= 8 and any(c.isdigit() for c in title_parts[-1]) and any(
                c.isalpha() for c in title_parts[-1]):
            title_parts = title_parts[:-1]

        # Join remaining parts and replace hyphens with spaces
        title = ' '.join(title_parts).replace('-', ' ').strip()

        return title.capitalize()

    except Exception as e:
        print(e)
        return ""


class ProxyManager:
    def __init__(self):
        self.proxy_sources = [
            'https://free-proxy-list.net/',
            'https://www.sslproxies.org/',
        ]
        self.proxies: List[Dict[str, str]] = []
        self.working_proxies: List[Dict[str, str]] = []
        self.last_fetch_time = 0
        self.fetch_interval = 300  # 5 minutes

    def fetch_proxies(self) -> None:
        """Fetch new proxies from all sources"""
        proxies = []
        for url in self.proxy_sources:
            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                proxy_list = soup.find('table', {'id': 'proxylisttable'}).find_all('tr')[1:]

                for row in proxy_list:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        ip = cols[0].text.strip()
                        port = cols[1].text.strip()
                        proxy = f'{ip}:{port}'
                        proxies.append({
                            'http': f'http://{proxy}',
                            'https': f'https://{proxy}'
                        })
                print(f"[INFO] Fetched {len(proxy_list)} proxies from {url}")
            except Exception as e:
                print(f"[ERROR] Error fetching proxies from {url}: {e}")

        self.proxies = proxies
        self.last_fetch_time = time.time()
        print(f"[INFO] Total proxies fetched: {len(self.proxies)}")

    def test_proxy(self, proxy: Dict[str, str]) -> bool:
        """Test if proxy is working"""
        try:
            response = requests.get(
                'https://www.google.com',
                proxies=proxy,
                timeout=5
            )
            return response.status_code == 200
        except:
            return False

    def verify_proxies(self) -> None:
        """Verify which proxies are working using parallel testing"""
        print("[INFO] Testing proxies...")
        working_proxies = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(self.test_proxy, self.proxies))

        self.working_proxies = [proxy for proxy, is_working in zip(self.proxies, results) if is_working]
        print(f"[INFO] Found {len(self.working_proxies)} working proxies")

    def get_random_proxy(self) -> Dict[str, str]:
        """Get a random working proxy, fetching new ones if necessary"""
        current_time = time.time()

        # Refresh proxies if needed
        if current_time - self.last_fetch_time > self.fetch_interval or not self.working_proxies:
            print("[INFO] Fetching new proxies...")
            self.fetch_proxies()
            self.verify_proxies()

        if not self.working_proxies:
            raise Exception("No working proxies available")

        return random.choice(self.working_proxies)

    def save_proxies(self, filename: str = 'working_proxies.txt') -> None:
        """Save working proxies to file"""
        with open(filename, 'w') as f:
            for proxy in self.working_proxies:
                f.write(f"{proxy['http'].replace('http://', '')}\n")
        print(f"[INFO] Saved {len(self.working_proxies)} proxies to {filename}")

    def load_proxies(self, filename: str = 'working_proxies.txt') -> None:
        """Load proxies from file"""
        try:
            with open(filename, 'r') as f:
                proxies = []
                for line in f:
                    proxy = line.strip()
                    if proxy:
                        proxies.append({
                            'http': f'http://{proxy}',
                            'https': f'https://{proxy}'
                        })
            self.proxies = proxies
            print(f"[INFO] Loaded {len(self.proxies)} proxies from {filename}")
        except FileNotFoundError:
            print(f"[WARNING] No proxy file found at {filename}")


class UserAgentManager:
    def __init__(self):
        self.ua = UserAgent(browsers=['chrome', 'firefox', 'edge'])
        self.recent_agents = set()
        self.max_recent = 5

    def get_random_user_agent(self) -> str:
        """Get random user agent avoiding recent ones"""
        while True:
            agent = self.ua.random
            if agent not in self.recent_agents:
                self.recent_agents.add(agent)
                if len(self.recent_agents) > self.max_recent:
                    self.recent_agents.pop()
                return agent


def get_random_proxy() -> Dict[str, str]:
    """
    Get a random working proxy from the list
    Returns a dict with http and https proxy settings
    """
    # Read all proxies
    with open("working_proxies.txt", "r") as f:
        proxies = f.read().splitlines()

    # Shuffle the list to test them in random order
    random.shuffle(proxies)

    print(f"Testing proxies from a pool of {len(proxies)} proxies...")

    # Test proxies until we find a working one
    for proxy in proxies:
        proxy_dict = {
            'http': f'http://{proxy}',
            'https': f'https://{proxy}'
        }

        try:
            # Test the proxy with a 5 second timeout
            print(f"Testing proxy: {proxy}")
            response = requests.get(
                'https://www.google.com',
                proxies=proxy_dict,
                timeout=5
            )

            if response.status_code == 200:
                print(f"Found working proxy: {proxy}")
                return proxy_dict

        except Exception as e:
            print(f"Proxy {proxy} failed: {str(e)[:100]}...")
            continue

    raise Exception("No working proxy found!")