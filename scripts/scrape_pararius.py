from prettytable import PrettyTable
from sqlalchemy.orm import DeclarativeBase

from src.db.db_handler import DBHandler
from src.db.schema import City


def print_table(orm: DeclarativeBase) -> tuple[int] | None:
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
        cols = [col.name for col in orm.__table__.columns]
        table.field_names = cols

        for row in data:
            table.add_row([getattr(row, column) for column in cols])

        print(table)
        return tuple(row.id for row in data)

    return None


def select_cities() -> list[int]:
    print("Cities available and enabled for scraping: ")
    city_ids = print_table(City)
    if city_ids:
        pass
    pass


def main() -> None:
    select_cities()


if __name__ == "__main__":
    main()
