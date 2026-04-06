from __future__ import annotations

import pandas as pd

from src.db.repository_write import _insert_watchlist


class _FakeCursor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.rowcount = 0

    def executemany(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.executemany_calls.append((sql, list(rows)))

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.execute_calls.append((sql, params))
        self.rowcount = 1


def test_insert_watchlist_skips_delete_when_some_rows_are_unmapped() -> None:
    cur = _FakeCursor()
    watchlist_df = pd.DataFrame(
        [
            {
                "film_name": "Mapped",
                "film_year": 2000,
                "letterboxd_uri": "https://boxd.it/mapped",
                "added_date": None,
            },
            {
                "film_name": "Unmapped",
                "film_year": 2001,
                "letterboxd_uri": "https://boxd.it/unmapped",
                "added_date": None,
            },
        ]
    )

    inserted = _insert_watchlist(
        cur,
        "user-1",
        watchlist_df,
        {"https://boxd.it/mapped": 10},
        {},
        {},
    )

    assert inserted == 1
    assert not cur.execute_calls


def test_insert_watchlist_deletes_missing_rows_after_full_resolution() -> None:
    cur = _FakeCursor()
    watchlist_df = pd.DataFrame(
        [
            {
                "film_name": "Mapped",
                "film_year": 2000,
                "letterboxd_uri": "https://boxd.it/mapped",
                "added_date": None,
            }
        ]
    )

    _insert_watchlist(
        cur,
        "user-1",
        watchlist_df,
        {"https://boxd.it/mapped": 10},
        {},
        {},
    )

    assert len(cur.execute_calls) == 1
    sql, params = cur.execute_calls[0]
    assert "DELETE FROM watchlist" in sql
    assert params == ("user-1", [10])
