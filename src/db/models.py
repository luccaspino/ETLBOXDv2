from __future__ import annotations

from datetime import date, datetime

try:
    from sqlalchemy import (
        Boolean,
        CheckConstraint,
        Date,
        DateTime,
        ForeignKey,
        Integer,
        Numeric,
        String,
        Text,
        UniqueConstraint,
        text,
    )
    from sqlalchemy.dialects.postgresql import UUID
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
except Exception as err:  # pragma: no cover - modelo opcional
    raise RuntimeError(
        "SQLAlchemy nao instalado. Adicione `sqlalchemy` ao requirements quando for usar ORM."
    ) from err


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    letterboxd_username: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))


class Film(Base):
    __tablename__ = "films"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    year: Mapped[int | None] = mapped_column(Integer)
    runtime_min: Mapped[int | None] = mapped_column(Integer)
    original_language: Mapped[str | None] = mapped_column(String(10))
    overview: Mapped[str | None] = mapped_column(Text)
    tagline: Mapped[str | None] = mapped_column(String(500))
    poster_url: Mapped[str | None] = mapped_column(String(500))
    letterboxd_url: Mapped[str] = mapped_column(String(500), nullable=False, unique=True)
    letterboxd_avg_rating: Mapped[float | None] = mapped_column(Numeric(3, 2))
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))


class UserFilm(Base):
    __tablename__ = "user_films"
    __table_args__ = (
        UniqueConstraint("user_id", "film_id", "watched_date", name="user_films_user_id_film_id_watched_date_key"),
        CheckConstraint("rating BETWEEN 0.5 AND 5.0", name="user_films_rating_check"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[str] = mapped_column(UUID(as_uuid=False), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    film_id: Mapped[int] = mapped_column(Integer, ForeignKey("films.id", ondelete="CASCADE"), nullable=False)
    rating: Mapped[float | None] = mapped_column(Numeric(3, 1))
    watched_date: Mapped[date | None] = mapped_column(Date)
    log_date: Mapped[date | None] = mapped_column(Date)
    is_rewatch: Mapped[bool] = mapped_column(Boolean, default=False)
    review_text: Mapped[str | None] = mapped_column(Text)
    tags: Mapped[str | None] = mapped_column(String(500))
