from pathlib import Path

from pandas import read_csv
from sqlalchemy import Select, create_engine, insert, select, update
from sqlalchemy.orm import (
    DeclarativeBase,
    Session,
    configure_mappers,
    sessionmaker,
)

from src.db.schema import Base, City


class DBHandler:
    def __init__(self) -> None:
        self.uri = "sqlite:///data/pararius_scrape.db"
        self.engine = create_engine(self.uri)
        self.Session = sessionmaker(bind=self.engine)
        self.orms = {City: Path(".") / "data" / "city.csv"}
        self._create_all()

    def get_session(self) -> Session:
        """Method to retrieve a Session object to connect to the DB.

        Returns:
            Session: Session object to connect to the DB.
        """

        return self.Session()

    def _create_all(self) -> bool:
        """Helper method to create all tables as defined in the schema.

        Returns:
            bool: True after completion.
        """

        Base.metadata.create_all(self.engine)
        configure_mappers()
        return True

    def _drop_all(self) -> bool:
        """Helper method to drop all tables defined in the schema.

        Returns:
            bool: True after completion.
        """

        Base.metadata.drop_all(self.engine)
        return True

    def _load_tables(self) -> bool:
        """Helper method to load all CSV tables into the DB for data tables.

        Returns:
            bool: True after completion.
        """

        for orm, file_path in self.orms.items():
            # Read the CSV file as a dataframe
            df = read_csv(file_path)
            # Write contents of the dataframe into the DB
            rows = df.to_dict(orient="records")
            self.bulk_insert(orm, rows)

        return True

    def reset(self) -> bool:
        """Method to drop all tables, re-create all tables, and load all data
        tables.

        Returns:
            bool: True after completion.
        """

        return all([self._drop_all(), self._create_all(), self._load_tables()])

    def read_table(
        self, orm: DeclarativeBase, stmt: Select | None = None
    ) -> list[DeclarativeBase]:
        """Method to read a table in the DB, using an optional SELECT statement
        object for custom reading.

        Args:
            orm (DeclarativeBase): ORM class of the table to read.
            stmt (Select | None, optional): Custom SELECT statement object.
                Defaults to None.

        Returns:
            list[DeclarativeBase]: List of ORM objects for the table's rows.
        """

        with self.get_session() as session:
            stmt = stmt if stmt is not None else select(orm)
            return session.scalars(stmt).all()

    def insert_row(self, orm: DeclarativeBase, data: dict) -> int | None:
        """Method to insert a single row into a table in the DB.

        Args:
            orm (DeclarativeBase): ORM class of the table to insert row into.
            data (dict): Data to insert into the table.

        Returns:
            int | None: Row ID of the inserted row if successful, None if not.
        """

        stmt = insert(orm).returning(orm.id)
        try:
            with self.get_session() as session:
                result = session.execute(stmt, data)
                session.commit()
                return result.scalar()
        except Exception as e:
            print(e)
            session.rollback()
        return None

    def update_row(
        self, orm: DeclarativeBase, row_id: int, data: dict
    ) -> bool:
        """Method to update an existing row of a table in the DB.

        Args:
            orm (DeclarativeBase): ORM class of the table to update a row for.
            row_id (int): ID of the row to update.
            data (dict): Data to update.

        Returns:
            bool: True if successful, False if not.
        """

        stmt = update(orm).where(orm.id == row_id).values(data)
        try:
            with self.get_session() as session:
                session.execute(stmt)
                session.commit()
                return True
        except Exception:
            session.rollback()
        return False

    def bulk_insert(self, orm: DeclarativeBase, data: list[dict]) -> bool:
        """Method to bulk insert rows into a table in the DB.

        Args:
            orm (DeclarativeBase): ORM class of the table to insert rows into.
            data (list[dict]): Data to bulk insert.

        Returns:
            bool: True if successful, False if not.
        """

        try:
            with self.get_session() as session:
                session.execute(insert(orm), data)
                session.commit()
                return True
        except Exception:
            return False
