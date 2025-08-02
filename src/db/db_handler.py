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
        return self.Session()

    def _create_all(self) -> bool:
        Base.metadata.create_all(self.engine)
        configure_mappers()
        return True

    def _drop_all(self) -> bool:
        Base.metadata.drop_all(self.engine)
        return True

    def _load_tables(self) -> bool:
        for orm, file_path in self.orms.items():
            # Read the CSV file as a dataframe
            df = read_csv(file_path)
            # Write contents of the dataframe into the DB
            with self.get_session() as session:
                rows = df.to_dict(orient="records")
                session.execute(
                    insert(orm),
                    rows,
                )
                session.commit()

        return True

    def reset(self) -> bool:
        return all([self._drop_all(), self._create_all(), self._load_tables()])

    def read_table(
        self, orm: DeclarativeBase, stmt: Select | None = None
    ) -> list[DeclarativeBase]:
        with self.get_session() as session:
            stmt = stmt if stmt is not None else select(orm)
            return session.scalars(stmt).all()

    def insert_row(self, orm: DeclarativeBase, data: dict) -> int | None:
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
        try:
            with self.get_session() as session:
                session.execute(insert(orm), data)
                session.commit()
                return True
        except Exception:
            return False
