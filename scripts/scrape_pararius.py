from prettytable import PrettyTable
from prompt_toolkit import prompt
from sqlalchemy import select

from src.db.db_handler import DBHandler
from src.db.schema import City
from src.scraping.ochestrator import Orchestrator


def print_city_table(
    handler: DBHandler, cols: list[str] = None, enabled_only: bool = False
) -> tuple[int] | None:
    """Function to print out the contents of the DB's table to the terminal
    using prettyprint.

    Args:
        handler (DBHandler): Singleton instance of the DBHandler class.
        cols (list[str]): Columns to print.
        enabled_only (bool, optional): Flag to toggle to print only the enabled
            cities.

    Returns:
        bool: True if successful, False if not.
    """

    # Read contents of city table
    if enabled_only:
        stmt = select(City).where(City.is_enabled)
        data = handler.read_table(City, stmt)
    else:
        data = handler.read_table(City)

    # Print contents of table if data exists
    if data:
        table = PrettyTable()
        cols = cols if cols else [col.name for col in City.__table__.columns]
        table.field_names = cols

        for row in data:
            table.add_row([getattr(row, column) for column in cols])

        print(table)
        return tuple(row.id for row in data)

    return None


def add_cities(handler: DBHandler, flag: bool = False):
    """Function to prompt the user to add new cities for scraping.

    Args:
        handler (DBHandler): Instance of the DBHandler class.
        flag (bool, optional): Flag for adding new cities. Defaults to False.
    """

    def prompt_city_addition() -> list[str]:
        """Prompt user to add new cities outside of the top 10 Dutch cities.

        Returns:
            list[str]: List of city names added by the user.
        """

        added_names = []
        while True:
            # Give user prompt to add a city name
            usr_name = prompt(
                "Enter valid name of Dutch city to add ('done' to finish): "
            )
            if usr_name.lower() == "done":
                break
            if usr_name not in added_names:
                added_names.append(usr_name)
                print(f"{usr_name} added.")
            else:
                print(f"{usr_name} already added.")

        return added_names

    if flag:
        # Print 10 cities in DB by default
        print("Currently added cities:")
        print_city_table(handler, cols=["id", "name"])

        # Prompt user to add new cities
        names = prompt_city_addition()
        # Add cities to DB Iif user has added cities
        if names:
            for name in names:
                handler.insert_row(City, {"name": name, "is_enabled": False})
                print(
                    "Warning: Scrapers will not work for invalid city names!"
                )
            return True

    return False


def select_cities(handler: DBHandler) -> bool:
    """Function to have the user select cities for scraping.

    Args:
        handler (DBHandler, optional): Handler for read and write operations to
            the SQLITE DB. Defaults to DBHandler().

    Returns:
        bool: True if more than one city is selected, False if not.
    """

    def prompt_city_selection(val_ids: tuple[int]) -> list[int]:
        """Helper function to prompt the user to select cities to run scrapers
        for.

        Args:
            val_ids (tuple[int]): Valid city IDs based on the contents of city
                table.

        Returns:
            list[int]: Tuple of all city IDs found in the city table.
        """

        sel_ids = []
        while True:
            # Give user prompt to select a city ID
            print(f"\nCurrently selected IDs: {str(sel_ids)}")
            usr_id = prompt(
                "Enter city ID to enable scraping for ('done' to finish): "
            )
            # User is finished
            if usr_id.lower() == "done":
                break
            try:
                usr_id = int(usr_id)
                # User entered an invalid ID
                if usr_id not in val_ids:
                    print("Invalid ID.")
                    print(f"Choose a valid ID from: {str(val_ids)}.")
                else:
                    # User entered a new ID
                    if usr_id not in sel_ids:
                        sel_ids.append(usr_id)
                        print(f"City ID {usr_id} enabled.")
                    # User entered a duplicate ID
                    else:
                        print(f"City ID {usr_id} already enabled.")
            # User entered non-numeric input that is not "done"
            except ValueError:
                print("Invalid input")
                print(f"Choose a valid ID from: {str(val_ids)}.")

        return sel_ids

    def enable_cities(val_ids: list[int], sel_ids: list[int]) -> int:
        """Helper function to enable scraping for the user-selected cities.

        Args:
            val_ids (list[int]): Valid city IDs from the city table.
            sel_ids (list[int]): User-selected city IDs to enable scraping for.

        Returns:
            int: Count of user-enabled cities.
        """

        count = 0
        # Initially disable all cities
        for val_id in val_ids:
            handler.update_row(City, val_id, {"is_enabled": False})
        # Enable selected cities
        for sel_id in sel_ids:
            handler.update_row(City, sel_id, {"is_enabled": True})
            count += 1

        return count

    print("Cities available for scraping: ")
    # Print ID and name from City table and get valid IDs
    city_ids = print_city_table(handler, cols=["id", "name"])
    if city_ids:
        # Prompt user for choice of cities
        selected_ids = prompt_city_selection(city_ids)
        # Enable user-selected cities
        enabled_count = enable_cities(city_ids, selected_ids)
        if enabled_count > 0:
            # Print enabled Cities after update
            print("\nEnabled cities for scraping:")
            print_city_table(handler, enabled_only=True)
            return True

    return False


def prompt_user(
    handler: DBHandler, reset: bool = False, add: bool = False
) -> bool:
    """Function to prompt the user to:
        1. Add new cities if `add` flag is enabled.
        2. Select cities to scrape.

    Args:
        handler (DBHandler): Singleton instance of the DBHandler class.
        reset (bool, optional): Flag to reset the DB before scraping.
            Defaults to False.
        add (bool, optional): Flag to have the user manually add cities for
            scraping. Defaults to False.

    Returns:
        bool: True if user selected more than one city, False if not.
    """

    # Clear all data from DB and re-create table schema if reset is toggled
    if reset:
        handler.reset()

    # Ask user to add cities
    add_cities(handler, add)
    # Ask user to select cities
    if select_cities(handler):
        return True

    return False


def main() -> None:
    """Main script function"""

    # Instantiate singleton DB Handler
    handler = DBHandler()

    # If the user selected more than one city for scraping
    if prompt_user(handler=handler, reset=True, add=True):
        # Instantiate and have orchestrator run scrapers for selected cities
        o = Orchestrator()
        o.run()


if __name__ == "__main__":
    main()
