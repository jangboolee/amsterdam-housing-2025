from prettytable import PrettyTable
from prompt_toolkit import prompt
from sqlalchemy.orm import DeclarativeBase

from src.db.db_handler import DBHandler
from src.db.schema import City
from src.scraping.ochestrator import Orchestrator


def print_table(
    orm: DeclarativeBase, cols: list[str] = None
) -> tuple[int] | None:
    """Function to print out the contents of the DB's table to the terminal
    using prettyprint.

    Args:
        orm (DeclarativeBase): ORM of the table class to print.

    Returns:
        bool: True if successful, False if not.
    """

    # Instantiate DBHandler and read contents of city table
    handler = DBHandler()
    data = handler.read_table(orm)

    # Print contents of table if data exists
    if data:
        table = PrettyTable()
        cols = cols if cols else [col.name for col in orm.__table__.columns]
        table.field_names = cols

        for row in data:
            table.add_row([getattr(row, column) for column in cols])

        print(table)
        return tuple(row.id for row in data)

    return None


def select_cities(
    handler: DBHandler = DBHandler(), reset: bool = False
) -> bool:
    """Function to have the user select cities for scraping.

    Args:
        handler (DBHandler, optional): Handler for read and write operations to
            the SQLITE DB. Defaults to DBHandler().
        reset (bool, optional): Flag to reset contents of the DB.
            Defaults to False.

    Returns:
        bool: True if more than one city is selected, False if not.
    """

    def prompt_cities(val_ids: tuple[int]) -> list[int]:
        """Helper function to use prompt-toolkit to prompt the user to select
        cities to run scrapers for.

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

    # Clear all data from DB and re-create table schema
    if reset:
        handler.reset()

    print("Cities available for scraping: ")
    # Print ID and name from City table and get valid IDs
    city_ids = print_table(orm=City, cols=["id", "name"])
    if city_ids:
        # Prompt user for choice of cities
        selected_ids = prompt_cities(city_ids)
        # Enable user-selected cities
        enabled_count = enable_cities(city_ids, selected_ids)
        # Print all contents of City table after update
        print("\nEnabled cities for scraping:")
        print_table(City)
        if enabled_count > 0:
            return True

    return False


def main() -> None:
    # If the user selected more than one city for scraping
    if select_cities(reset=True):
        # Instantiate and run orchestrator to scrape the selected cities
        o = Orchestrator()
        o.run()


if __name__ == "__main__":
    main()
