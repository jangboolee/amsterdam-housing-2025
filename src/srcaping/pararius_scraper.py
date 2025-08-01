from time import sleep

import requests
from fake_useragent import UserAgent
from tqdm import tqdm


class ParariusScraper:
    def __init__(self, city: str = "amsterdam") -> None:
        self.base_url = f"https://www.pararius.nl/koopwoningen/{city}/page-"
        self.max_pg_num = -1
        self.headers = {"User-Agent": UserAgent(os="Linux").firefox}
        self.scraped_data = None

    def _get_max_pg_num(self) -> bool:
        # Get request for first page of the URL
        res = requests.get(self.base_url + str(1))
        res.raise_for_status()
        pass

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
