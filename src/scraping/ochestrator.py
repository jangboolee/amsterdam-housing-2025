from datetime import datetime

from sqlalchemy import select

from src.db.db_handler import DBHandler
from src.db.schema import City, Log
from src.scraping.city_scraper import CityScraper


class Orchestrator:
    def __init__(self) -> None:
        self.db_handler = DBHandler()
        self.cities = None
        self.scrapers = None
        self.times = None
        # Initialize scraper's parameters
        self._get_cities()
        self._create_scrapers()

    @staticmethod
    def _get_current_time() -> datetime:
        return datetime.now()

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

    def _create_scrapers(self) -> bool:
        self.scrapers = {
            c_id: CityScraper(c_id, c_name)
            for c_id, c_name in self.cities.items()
        }
        return True

    def _start_scraper(self, scraper: CityScraper) -> int:
        # Record start time of scraper
        start_time = self._get_current_time()
        self.times = {scraper.city_id: {"start": start_time, "end": None}}
        # Get maximum page number of city page
        scraper._get_max_pg_num()
        # Insert log record of scraper start and get the inserted row's key
        data = {
            "city_id": scraper.city_id,
            "max_pg": scraper.max_pg_num,
            "start_time": start_time,
        }
        row_id = self.db_handler.insert_row(Log, data)
        # Run the scraper if log record was inserted
        if row_id:
            scraper.run()
        return row_id

    def _end_scraper(self, row_id: int, scraper: CityScraper) -> int:
        # Record end time of scraper
        end_time = self._get_current_time()
        self.times[scraper.city_id]["end"] = end_time
        # Update log record of scraper end
        data = {"end_time": end_time}
        return self.db_handler.update_row(Log, row_id, data)

    def run(self) -> bool:
        for c_id, scraper in self.scrapers.items():
            # Extract city name
            c_name = self.cities[c_id]
            # Instantiate instance of CityScraper for the city
            scraper = CityScraper(c_id, c_name)
            # Start the scraper
            row_id = self._start_scraper(scraper)
            # End the scraper
            self._end_scraper(row_id, scraper)

        return True


if __name__ == "__main__":
    o = Orchestrator()
    o.run()
