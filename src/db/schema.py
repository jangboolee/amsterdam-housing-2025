from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class City(Base):
    __tablename__ = "city"

    # Columns
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    is_enabled: Mapped[bool] = mapped_column(nullable=False)
    # Relationships
    logs: Mapped[list["Log"]] = relationship(back_populates="city")
    listings: Mapped[list["Listing"]] = relationship(back_populates="city")

    def __repr__(self) -> str:
        return f"City(id={self.id!r}, name={self.name!r})"


class StatusDict(Base):
    __tablename__ = "status_dict"

    # Columns
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=True)
    # Relationships
    listings: Mapped[list["Listing"]] = relationship(back_populates="status")

    def __repr__(self) -> str:
        return f"StatusDict(id={self.id!r}, name={self.name!r})"


class Log(Base):
    __tablename__ = "log"

    # Columns
    id: Mapped[int] = mapped_column(primary_key=True)
    city_id: Mapped[int] = mapped_column(ForeignKey("city.id"), nullable=False)
    max_pg: Mapped[int] = mapped_column(nullable=False)
    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=True)
    # Relationships
    city: Mapped["City"] = relationship(back_populates="logs")
    listings: Mapped[list["Listing"]] = relationship(back_populates="log")

    def __repr__(self) -> str:
        return (
            f"Log(id={self.id!r}, "
            f"city_id={self.city_id!r}, "
            f"max_pg={self.max_pg!r}, "
            f"start_time={self.start_time!r}, "
            f"end_time={self.end_time!r})"
        )


class Listing(Base):
    __tablename__ = "listing"

    id: Mapped[int] = mapped_column(primary_key=True)
    log_id: Mapped[int] = mapped_column(ForeignKey("log.id"), nullable=False)
    city_id: Mapped[int] = mapped_column(ForeignKey("city.id"), nullable=False)
    status_id: Mapped[int] = mapped_column(
        ForeignKey("status_dict.id"), nullable=True
    )
    address: Mapped[str] = mapped_column(nullable=False)
    postcode: Mapped[str] = mapped_column(nullable=False)
    buurt: Mapped[str] = mapped_column(nullable=False)
    asking_price_eur: Mapped[int] = mapped_column(nullable=False)
    size_sqm: Mapped[int] = mapped_column(nullable=False)
    room_count: Mapped[int] = mapped_column(nullable=False)
    year: Mapped[int] = mapped_column(nullable=False)
    makelaar: Mapped[str] = mapped_column(nullable=False)
    pararius_link: Mapped[str] = mapped_column(nullable=False)
    gmaps_link: Mapped[str] = mapped_column(nullable=False)
    scrape_time: Mapped[datetime] = mapped_column(nullable=False)
    # Relationships
    status: Mapped["StatusDict"] = relationship(back_populates="listings")
    log: Mapped["Log"] = relationship(back_populates="listings")
    city: Mapped["City"] = relationship(back_populates="listings")

    def __repr__(self) -> str:
        return (
            f"Listing(id={self.id!r}, "
            f"city_id={self.city_id!r}, "
            f"address={self.address!r}, "
            f"postcode={self.postcode!r}, "
            f"asking_price_eur={self.asking_price_eur!r}, "
            f"size_sqm={self.size_sqm!r}, "
            f"room_count={self.room_count!r}, "
            f"year={self.year!r})"
        )
