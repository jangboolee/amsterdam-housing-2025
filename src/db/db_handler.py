from sqlalchemy import create_engine
from sqlalchemy.orm import Session, configure_mappers, sessionmaker

from src.db.schema import Base


class DBHandler:
    def __init__(self) -> None:
        self.uri = "sqlite:///data/pararius_scrape.db"
        self.engine = create_engine(self.uri)
        self.Session = sessionmaker(bind=self.engine)
        self._create_all()

    def _create_all(self) -> bool:
        Base.metadata.create_all(self.engine)
        configure_mappers()
        return True

    def _drop_all(self) -> bool:
        Base.metadata.drop_all(self.engine)
        return True

    def reset(self) -> bool:
        return all([self._drop_all(), self._create_all()])

    def get_session(self) -> Session:
        return self.Session()
