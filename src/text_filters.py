from __future__ import annotations

import re
import unicodedata

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text_token(value: object) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def is_show_all_placeholder(value: object) -> bool:
    normalized = normalize_text_token(value).casefold()
    return normalized.startswith("show all")
