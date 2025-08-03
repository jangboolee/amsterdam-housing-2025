import re
from datetime import datetime
from time import sleep

import requests
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent
from tqdm import tqdm

from src.db.db_handler import DBHandler
from src.db.schema import Listing


class CityScraper:
    def __init__(self, city_id: int, city_name: str) -> None:
        self.log_id = None
        self.city_id = city_id
        self.city_name = city_name.lower()
        self.db_handler = DBHandler()
        self.base_url = (
            f"https://www.pararius.nl/koopwoningen/{self.city_name}/page-"
        )
        self.max_pg_num = -1
        self.headers = {"User-Agent": UserAgent(os="Linux").firefox}
        self.scraped_data = []

    @staticmethod
    def _get_current_time() -> datetime:
        """Static helper method to get the current time for timestamps.

        Returns:
            datetime: Datetime object of the current time.
        """

        return datetime.now()

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

    def _get_max_pg_num(self) -> bool:
        """Helper method to retrieve the maximum page number available for the
        city's listing page.

        Returns:
            bool: True if successful, False if not.
        """

        # Parse content of city's landing page with BeautifulSoup
        city_url = self.base_url + "1"
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

    def _scrape_listing(self, listing: Tag) -> dict:
        def get_label(element: Tag) -> str | None:
            label = element.find("div", class_="listing-search-item__label")
            if label:
                return label.text.strip()
            return None

        def get_address(element: Tag) -> str | int:
            address = element.h2.a.text.strip()
            if "Project:" in address:
                return -1
            else:
                return address

        def get_postcode(element: Tag) -> str:
            # Find the text containing the postcode
            text = element.find(
                "div", class_="listing-search-item__sub-title"
            ).text.strip()
            # Use regex for postcode extraction
            pattern = re.compile(r"(\d{4}\s\w{2})")
            return re.search(pattern, text).group()

        def get_buurt(element: Tag) -> str:
            # Find the text containing the postcode
            text = element.find(
                "div", class_="listing-search-item__sub-title"
            ).text.strip()
            # Use regex for buurt extraction
            pattern = re.compile(r"\(([^)]+)\)")
            return re.search(pattern, text).group(1)

        def get_asking_price(element: Tag) -> int:
            # Find the text containing the asking price
            text = listing.find(
                "div", class_="listing-search-item__price"
            ).text.strip()
            # Use regex for price extraction
            pattern = re.compile(r"\d+(\.\d+)*")
            match = re.search(pattern, text)
            # Return price if available
            if match:
                price_str = match.group()
                price_int = int(price_str.replace(".", ""))
                return price_int
            # Return -1 if price is not available (prijs op aanvraag)
            return -1

        def get_features(element: Tag) -> tuple[int, int, int]:
            # Find ul container holding the key listing features
            feature_container = listing.find_all(
                "ul",
                class_="illustrated-features illustrated-features--compact",
            )
            # Extract li elements of the key listing features
            feature_elems = feature_container[0].find_all("li")

            pattern = re.compile(r"\d+")
            features = []
            # Get size, room count, and year
            for feature in feature_elems:
                text = feature.text.strip()
                feature_str = re.search(pattern, text).group()
                features.append(int(feature_str))

            return tuple(features)

        def get_makelaar(element: Tag) -> str:
            # Find the text containing the makelaar information
            return listing.find(
                "div", class_="listing-search-item__info"
            ).text.strip()

        def get_pararius_link(element: Tag) -> str:
            link = listing.h2.find("a")["href"]
            return f"https://www.pararius.nl{link}"

        def get_gmaps_link(address: str) -> str:
            base = "https://www.google.com/maps/place/{}"
            query = address.replace(" ", "+")

            return base.format(query)

        # Check if the listing is for a property or a project
        address = get_address(listing)
        # Skip real estate projects
        if address == -1:
            return None

        # Scrape other listing metadata if listing is a property
        label = get_label(listing)
        postcode = get_postcode(listing)
        buurt = get_buurt(listing)
        asking_price_eur = get_asking_price(listing)
        size_sqm, room_count, year = get_features(listing)
        makelaar = get_makelaar(listing)
        pararius_link = get_pararius_link(listing)
        gmaps_link = get_gmaps_link(address)

        # Save listing metadata
        row = {
            "log_id": self.log_id,
            "city_id": self.city_id,
            "label": label,
            "address": address,
            "postcode": postcode,
            "buurt": buurt,
            "asking_price_eur": asking_price_eur,
            "size_sqm": size_sqm,
            "room_count": room_count,
            "year": year,
            "makelaar": makelaar,
            "pararius_link": pararius_link,
            "gmaps_link": gmaps_link,
            "scrape_time": self._get_current_time(),
        }

        return row

    def _scrape_webpage(self, url: str) -> bool:
        soup = self._parse_webpage(url)
        listings = soup.find_all(
            name="li", class_="search-list__item search-list__item--listing"
        )

        scraped = []
        for listing in listings:
            listing_data = self._scrape_listing(listing)
            if listing_data:
                scraped.append(listing_data)

        # Insert scraped data into DB
        self.db_handler.bulk_insert(Listing, scraped)
        # Save scraped data as instance variable
        self.scraped_data.extend(scraped)

    def _create_dataframe(self) -> bool:
        pass

    def run(self) -> bool:
        max_pg_num = self._get_max_pg_num()
        if max_pg_num:
            for pg_num in tqdm(range(1, self.max_pg_num)):
                pg_url = self.base_url + str(pg_num)
                self._scrape_webpage(pg_url)
                sleep(2)
        return False


if __name__ == "__main__":
    scraper = CityScraper(1, "Amsterdam")
    scraper.run()
