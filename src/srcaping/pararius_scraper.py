import re
from time import sleep

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tqdm import tqdm


class ParariusScraper:
    def __init__(self, city: str = "amsterdam") -> None:
        self.base_url = f"https://www.pararius.nl/koopwoningen/{city}/page-"
        self.max_pg_num = -1
        self.headers = {"User-Agent": UserAgent(os="Linux").firefox}
        self.scraped_data = None

    def _get_max_pg_num(self) -> bool:
        """Helper method to retrieve the maximum page number available for the
        city's listing page.

        Returns:
            bool: True if successful, False if not.
        """

        # Try scraping the first page
        try:
            # Get request for first page of the URL
            res = requests.get(self.base_url + str(1))
            res.raise_for_status()
            # Parse content of webpage with BeautifulSoup
            soup = BeautifulSoup(res.text, "html.parser")
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
        # If scraping the first page fails
        except requests.exceptions.RequestException:
            return False
        # For all other failures
        return False

    def _scrape_webpage(self, url: str) -> bool:
        pass

    def _create_dataframe(self) -> bool:
        pass

    def run(self) -> bool:
        if self._get_max_pg_num():
            for pg_num in tqdm(range(1, self.max_pg_num)):
                pg_url = self.base_url + str(pg_num)
                self._scrape_webpage(pg_url)
                sleep(2)
            if self._create_dataframe():
                return True
        return False


if __name__ == "__main__":
    scraper = ParariusScraper()
    scraper.run()
