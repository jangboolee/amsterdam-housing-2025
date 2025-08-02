import re
from time import sleep

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from sqlalchemy import select
from tqdm import tqdm

from src.db.db_handler import DBHandler
from src.db.schema import City


class ParariusScraper:
    def __init__(self) -> None:
        self.db_handler = DBHandler()
        self.cities = None
        self.base_url = "https://www.pararius.nl/koopwoningen/{}/page-{}"
        self.max_pg_num = -1
        self.headers = {"User-Agent": UserAgent(os="Linux").firefox}
        self.scraped_data = None
        # Initialize scraper's parameters
        self._get_cities()

    def _get_cities(self) -> bool:
        """Helper method to retrieve a dictionary of enabled cities for
        scraping.

        Returns:
            bool: True after completion.
        """

        # Create SELECT statement for enabled cities
        stmt = select(City).where(City.is_enabled)
        # Retrieve enabled cities from city table
        cities_raw = self.db_handler.read_table(City, stmt)
        # Create and save dictionary of cities
        cities = {i.id: i.name for i in cities_raw}
        self.cities = cities

        return True

    def _parse_webpage(self, url: str) -> BeautifulSoup | None:
        """Helper method to get the contents of a URL, then parse the contents
        with BeautifulSoup.

        Args:
            url (str): URL to parse.

        Returns:
            BeautifulSoup | None: BeautifulSoup parser of the webpage if
                scraping is successful, None if not.
        """

        try:
            # Get request for URL
            res = requests.get(url)
            res.raise_for_status()
            # Parse content of webpage with BeautifulSoup
            soup = BeautifulSoup(res.text, "html.parser")
        except requests.exceptions.RequestException:
            return None
        return soup

    def _get_max_pg_num(self, city: str) -> bool:
        """Helper method to retrieve the maximum page number available for the
        city's listing page.

        Returns:
            bool: True if successful, False if not.
        """

        # Parse content of webpage with BeautifulSoup
        city_url = self.base_url.format(city.lower(), "1")
        soup = self._parse_webpage(city_url)
        if soup:
            pg_links = soup.find_all(name="li", class_="pagination__item")
            if pg_links:
                # Extract number from page links
                pattern = re.compile(r"\d+")
                pg_nums = []
                for pg_link in pg_links:
                    match = re.search(pattern, pg_link.text)
                    if match:
                        pg_nums.append(int(match.group()))
                # Get and save maximum page number
                max_pg_num = max([int(pg_num) for pg_num in pg_nums])
                self.max_pg_num = max_pg_num
                return True
        # If scraping fails
        return False

    def _scrape_webpage(self, url: str) -> bool:
        soup = self._parse_webpage(url)
        listings = soup.find_all(
            name="li", class_="search-list__item search-list__item--listing"
        )

        pass

    def _scrape_city(self, city: str) -> bool:
        city_max_pg_num = self._get_max_pg_num(city.lower())
        if city_max_pg_num:
            for pg_num in tqdm(range(1, self.city_max_pg_num)):
                pg_url = self.base_url + str(pg_num)
                self._scrape_webpage(pg_url)
                sleep(2)

    def _create_dataframe(self) -> bool:
        pass

    def run(self) -> bool:
        for c_id, c_name in self.cities.items():
            print(f"Scraping pararius for {c_name}...")
            self._scrape_city(c_name)
        return False


if __name__ == "__main__":
    scraper = ParariusScraper()
    scraper.run()
