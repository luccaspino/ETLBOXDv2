from __future__ import annotations

from src.text_filters import is_show_all_placeholder, normalize_text_token


def test_normalize_text_token_collapses_whitespace() -> None:
    assert normalize_text_token("  Show   All...  ") == "Show All..."


def test_is_show_all_placeholder_matches_letterboxd_placeholder() -> None:
    assert is_show_all_placeholder("Show All...")
    assert is_show_all_placeholder("Show All…")
    assert not is_show_all_placeholder("Showgirls")
