from __future__ import annotations

import zipfile
from io import BytesIO

from src.ingestion.parser import parse_zip


def _csv(rows: list[list[str]]) -> str:
    return "\n".join([",".join(row) for row in rows]) + "\n"


def _sample_zip_bytes() -> bytes:
    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "profile.csv",
            _csv([
                ["username", "date_joined", "given_name", "family_name", "email_address"],
                ["ppino", "2024-01-01", "Lucca", "Spino", "ppino@example.com"],
            ]),
        )
        zf.writestr(
            "diary.csv",
            _csv([
                ["name", "year", "letterboxd_uri", "rating", "watched_date", "date", "rewatch", "tags"],
                ["Film A", "2020", "https://letterboxd.com/film/film-a", "", "2026-01-10", "2026-01-10", "NO", "tag1"],
            ]),
        )
        zf.writestr(
            "ratings.csv",
            _csv([
                ["name", "year", "letterboxd_uri", "rating"],
                ["Film A", "2020", "https://boxd.it/aaaa", "4.0"],
                ["Film B", "2021", "https://boxd.it/bbbb", "3.5"],
            ]),
        )
        zf.writestr(
            "reviews.csv",
            _csv([
                ["letterboxd_uri", "watched_date", "review"],
                ["https://letterboxd.com/film/film-a", "2026-01-10", "Great"],
            ]),
        )
        zf.writestr(
            "watchlist.csv",
            _csv([
                ["name", "year", "letterboxd_uri", "date"],
                ["Film C", "2022", "https://boxd.it/cccc", "2026-02-01"],
            ]),
        )
    return mem.getvalue()


def test_parse_zip_basic_outputs() -> None:
    result = parse_zip(_sample_zip_bytes())

    assert result["user"].iloc[0]["username"] == "ppino"
    assert len(result["user_films"]) == 2
    assert len(result["watchlist"]) == 1
    assert len(result["scrape_queue"]) == 3



def test_parse_zip_filters_existing() -> None:
    existing_uris = {"https://letterboxd.com/film/film-a"}
    existing_keys = {("film c", 2022)}

    result = parse_zip(
        _sample_zip_bytes(),
        existing_uris=existing_uris,
        existing_film_keys=existing_keys,
    )

    queued = set(result["scrape_queue"]["letterboxd_uri"].tolist())
    assert "https://letterboxd.com/film/film-a" not in queued
    assert "https://boxd.it/cccc" not in queued
