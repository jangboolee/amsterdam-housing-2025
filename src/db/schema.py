from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class City(Base):
    __tablename__ = "city"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        return f"City(id={self.id!r}, name={self.city!r}"


class Log(Base):
    __tablename__ = "log"

    id: Mapped[int] = mapped_column(primary_key=True)
    city_id: Mapped[int] = mapped_column(ForeignKey("city.id"), nullable=False)
    max_pg: Mapped[int] = mapped_column(nullable=False)
    scrape_time: Mapped[datetime] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        return (
            f"ScrapeLog(id={self.id!r}, "
            f"city={self.city!r}, "
            f"max_pg={self.max_pg!r})"
        )


class Listing(Base):
    __tablename__ = "listing"

    id: Mapped[int] = mapped_column(primary_key=True)
    log_id: Mapped[int] = mapped_column(ForeignKey("log.id"), nullable=False)
    city_id: Mapped[int] = mapped_column(ForeignKey("city.id"), nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    postcode: Mapped[str] = mapped_column(nullable=False)
    buurt: Mapped[str] = mapped_column(nullable=False)
    asking_price_eur: Mapped[int] = mapped_column(nullable=False)
    size_sqm: Mapped[int] = mapped_column(nullable=False)
    room_count: Mapped[int] = mapped_column(nullable=False)
    year: Mapped[int] = mapped_column(nullable=False)
    makelaar: Mapped[str] = mapped_column(nullable=False)
