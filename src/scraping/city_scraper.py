import re
from datetime import datetime
from time import sleep

import requests
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent
from tqdm import tqdm

from src.db.db_handler import DBHandler
from src.db.schema import Listing, StatusDict


class CityScraper:
    def __init__(self, city_id: int, city_name: str) -> None:
        self.log_id = None
        self.city_id = city_id
        self.city_name = city_name.lower().replace(" ", "-")
        self.db_handler = DBHandler()
        self.status_mapper = None
        self.base_url = (
            f"https://www.pararius.nl/koopwoningen/{self.city_name}/page-"
        )
        self.max_pg_num = -1
        self.headers = {"User-Agent": UserAgent(os="Linux").firefox}
        self.scraped_data = []
        # Populate instance variable for status mapper
        self._get_status_mapper()

    @staticmethod
    def _get_current_time() -> datetime:
        """Static helper method to get the current time for timestamps.

        Returns:
            datetime: Datetime object of the current time.
        """

        return datetime.now()

    def _get_status_mapper(self) -> bool:
        """Helper method to create a dictionary for status ID mapping.

        Returns:
            bool: True if mapper is created, False if not.
        """

        status_raw = self.db_handler.read_table(StatusDict)
        self.status_mapper = {status.name: status.id for status in status_raw}
        return self.status_mapper is not None

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
            return soup
        except requests.exceptions.RequestException:
            return None

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
        """Helper method to scrape metadata for a single property listing.

        Args:
            listing (Tag): HTML element of a single listing.

        Returns:
            dict: Scraped metadata to insert as a row into the DB.
        """

        def get_status_id(element: Tag) -> int:
            """Helper function to extract the listing's label and map the
            corresponding status ID.

            Args:
                element (Tag): HTML element to parse.

            Returns:
                int: ID of the property status label.
            """

            status = element.find("div", class_="listing-search-item__label")
            if status:
                status_text = status.text.strip()
                # Map the status ID while defaulting to -1 for unseen values
                status_id = self.status_mapper.get(status_text, -1)
                return status_id
            # Use 0 for listings without a status
            return 0

        def get_address(element: Tag) -> str | int:
            """Helper function to extract the listing's address and to check if
            the listing is for a real estate project or a single property.

            Args:
                element (Tag): HTML element to parse.

            Returns:
                str | int: The address for a property, or -1 for a project.
            """

            address = element.h2.a.text.strip()
            if "Project:" in address:
                return -1
            else:
                return address

        def get_postcode(element: Tag) -> str:
            """Helper function to get the postcode of a property listing.

            Args:
                element (Tag): HTML element of a single listing.

            Returns:
                str: Postcode of the property listing.
            """

            # Find the text containing the postcode
            text = element.find(
                "div", class_="listing-search-item__sub-title"
            ).text.strip()
            # Use regex for postcode extraction
            pattern = re.compile(r"(\d{4}\s\w{2})")
            return re.search(pattern, text).group()

        def get_buurt(element: Tag) -> str:
            """Helper function to get the neighborhood (buurt) of a property
            listing.

            Args:
                element (Tag): HTML element of a single listing.

            Returns:
                str: Neighborhood (buurt) of the property listing.
            """

            # Find the text containing the postcode
            text = element.find(
                "div", class_="listing-search-item__sub-title"
            ).text.strip()
            # Use regex for buurt extraction
            pattern = re.compile(r"\(([^)]+)\)")
            return re.search(pattern, text).group(1)

        def get_asking_price(element: Tag) -> int:
            """Helper function to get the asking price of a property listing.

            Args:
                element (Tag): HTML element of a single listing.

            Returns:
                int: Asking price of the property listing, if available.
                    Returns -1 if asking price is unavailable due to "prijs op
                    aanvraag".
            """

            # Find the text containing the asking price
            text = element.find(
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
            """Helper function to extract the following key listing features:
                1. Size in square meters
                2. Number of rooms
                3. Construction year

            Args:
                element (Tag): HTML element of a single listing

            Returns:
                tuple[int, int, int]: The three properties of the listing.
            """

            # Find ul container holding the key listing features
            feature_container = element.find_all(
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
            """Helper function to extract the name of the makelaar selling the
            property.

            Args:
                element (Tag): HTML element of a single listing.

            Returns:
                str: Name of the makelaar selling the property.
            """

            # Find the text containing the makelaar information
            return element.find(
                "div", class_="listing-search-item__info"
            ).text.strip()

        def get_pararius_link(element: Tag) -> str:
            """Helper function to get the Pararius link of the property.

            Args:
                element (Tag): HTML element of a single listing.

            Returns:
                str: URL of the property's listing page on Pararius.
            """

            link = listing.h2.find("a")["href"]
            return f"https://www.pararius.nl{link}"

        def get_gmaps_link(address: str) -> str:
            """Helper function to get the search results of the address on
            Google Maps.

            Args:
                address (str): Extracted address of the property.

            Returns:
                str: Google Maps search results page for the address.
            """

            base = "https://www.google.com/maps/place/{}"
            query = address.replace(" ", "+")

            return base.format(query)

        # Check if the listing is for a property or a project
        address = get_address(listing)
        # Skip real estate projects
        if address == -1:
            return None

        # Scrape other listing metadata if listing is a property
        status_id = get_status_id(listing)
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
            "status_id": status_id,
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
        # Parse the listings webpage
        soup = self._parse_webpage(url)
        # Find all listings on the webpage
        listings = soup.find_all(
            name="li", class_="search-list__item search-list__item--listing"
        )

        scraped = []
        for listing in listings:
            # Scrape a single listing from the webpage
            listing_data = self._scrape_listing(listing)
            # Save the listing data if the listing is a property
            if listing_data:
                scraped.append(listing_data)

        # Insert scraped data into DB
        self.db_handler.bulk_insert(Listing, scraped)
        # Save scraped data as instance variable
        self.scraped_data.extend(scraped)

    def run(self) -> bool:
        """Main method to run the core operations for the CityScraper.

        Returns:
            bool: True if scraping is successful, False if not.
        """

        # Extract the maximum page number for the city's listing landing page
        max_pg_num = self._get_max_pg_num()
        # If maximum page number extraction is successful
        if max_pg_num:
            # Scrape each city's listing webpage
            for pg_num in tqdm(range(1, self.max_pg_num + 1)):
                # Generate webpage URL and scrape webpage
                pg_url = self.base_url + str(pg_num)
                self._scrape_webpage(pg_url)
                # Add delay for polite scraping
                sleep(2)
            return True
        return False


if __name__ == "__main__":
    scraper = CityScraper(1, "Amsterdam")
    scraper.run()
