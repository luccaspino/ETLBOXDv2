from __future__ import annotations

import csv
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

import httpx

from src.ingestion.scraper_parser import _is_review_like_page, _looks_like_review_title, _parse_film_page
from src.ingestion.scraper_urls import _is_letterboxd_url, _normalize_film_url, _to_global_film_url

logger = logging.getLogger(__name__)

RETRYABLE_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}
TRANSIENT_FETCH_ERRORS = (
    TimeoutError,
    OSError,
)


@dataclass
class FilmScrapeResult:
    letterboxd_url: str
    requested_url: str | None = None
    title: str | None = None
    year: int | None = None
    runtime_min: int | None = None
    original_language: str | None = None
    overview: str | None = None
    tagline: str | None = None
    poster_url: str | None = None
    letterboxd_avg_rating: float | None = None
    genres: list[str] = field(default_factory=list)
    directors: list[str] = field(default_factory=list)
    cast: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    scraped_at: datetime | None = None
    scrape_error: str | None = None
    attempts: int = 0

    @property
    def ok(self) -> bool:
        return self.scrape_error is None


class _GlobalRateLimiter:
    def __init__(self, min_interval_s: float) -> None:
        self.min_interval_s = max(0.0, float(min_interval_s))
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        if self.min_interval_s <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait_s = self._next_allowed - now
            if wait_s > 0:
                time.sleep(wait_s)
                now = time.monotonic()
            self._next_allowed = now + self.min_interval_s


class LetterboxdScraper:
    def __init__(
        self,
        max_workers: int = 20,
        timeout_s: int = 10,
        retries: int = 1,
        retry_backoff_s: float = 0.25,
        request_interval_s: float = 0.0,
        progress_every: int = 50,
    ) -> None:
        self.max_workers = max(1, int(max_workers))
        self.timeout_s = max(1, int(timeout_s))
        self.retries = max(0, int(retries))
        self.retry_backoff_s = max(0.0, float(retry_backoff_s))
        self.progress_every = max(1, int(progress_every))
        self._rate_limiter = _GlobalRateLimiter(request_interval_s)
        self._user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
        self._client_local = threading.local()
        self._clients_lock = threading.Lock()
        self._clients_to_close: list[httpx.Client] = []

    def _build_http_client(self) -> httpx.Client:
        return httpx.Client(
            timeout=httpx.Timeout(self.timeout_s),
            limits=httpx.Limits(
                max_connections=max(20, self.max_workers * 2),
                max_keepalive_connections=max(10, self.max_workers),
                keepalive_expiry=30.0,
            ),
            follow_redirects=True,
            headers={
                "User-Agent": self._user_agent,
                "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8",
            },
        )

    def _get_http_client(self) -> httpx.Client:
        client = getattr(self._client_local, "client", None)
        if client is None:
            client = self._build_http_client()
            self._client_local.client = client
            with self._clients_lock:
                self._clients_to_close.append(client)
        return client

    def close(self) -> None:
        with self._clients_lock:
            clients = list(self._clients_to_close)
            self._clients_to_close.clear()

        for client in clients:
            try:
                client.close()
            except Exception:
                pass

    def _fetch_html(self, url: str) -> tuple[str, str]:
        self._rate_limiter.acquire()
        response = self._get_http_client().get(url)
        response.raise_for_status()
        return response.text, str(response.url)

    def _retry_sleep(self, attempt: int) -> None:
        if self.retry_backoff_s <= 0:
            return
        backoff = self.retry_backoff_s * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, self.retry_backoff_s)
        time.sleep(backoff + jitter)

    def _can_use_review_fallback(self, result: FilmScrapeResult) -> bool:
        title = (result.title or "").strip()
        low = title.lower()
        is_review_title = (
            _looks_like_review_title(title)
            or low.startswith("review of ")
            or low.startswith("diary entry for ")
        )
        return bool(title) and not is_review_title

    def scrape_one(self, uri: str) -> FilmScrapeResult:
        url = _normalize_film_url(uri)
        if not _is_letterboxd_url(url):
            return FilmScrapeResult(
                letterboxd_url=url,
                requested_url=url,
                scrape_error=f"URL invalida para Letterboxd: {url}",
            )

        attempts = self.retries + 1
        for attempt in range(1, attempts + 1):
            try:
                html, final_url = self._fetch_html(url)
                result = _parse_film_page(final_url, html)
                result.requested_url = url
                result.attempts = attempt

                global_film_url = _to_global_film_url(final_url)
                must_refetch_canonical = _is_review_like_page(final_url) or _looks_like_review_title(result.title)
                if global_film_url != final_url.rstrip("/") or must_refetch_canonical:
                    try:
                        html2, final2 = self._fetch_html(global_film_url)
                        canonical = _parse_film_page(final2, html2)
                        canonical.requested_url = url
                        canonical.attempts = attempt
                        result = canonical
                    except Exception as err:
                        logger.debug("fallback canonical fetch falhou para %s: %s", url, err)
                        if must_refetch_canonical:
                            if self._can_use_review_fallback(result):
                                result.letterboxd_url = global_film_url
                                logger.debug(
                                    "usando dados do review/log para %s apos falha no fetch canônico",
                                    url,
                                )
                                return result
                            return FilmScrapeResult(
                                letterboxd_url=global_film_url,
                                requested_url=url,
                                scrape_error="canonical fetch failed for review/log URL",
                                attempts=attempt,
                            )
                return result
            except httpx.HTTPStatusError as err:
                status_code = err.response.status_code
                if status_code not in RETRYABLE_HTTP_STATUS or attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=f"HTTP {status_code}",
                        attempts=attempt,
                    )
                self._retry_sleep(attempt)
            except httpx.RequestError as err:
                if attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=str(err),
                        attempts=attempt,
                    )
                self._retry_sleep(attempt)
            except TRANSIENT_FETCH_ERRORS as err:
                if attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=str(err),
                        attempts=attempt,
                    )
                self._retry_sleep(attempt)
            except ValueError as err:
                if attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=str(err),
                        attempts=attempt,
                    )
                self._retry_sleep(attempt)
            except Exception as err:
                logger.exception("erro inesperado no scraping de %s", url)
                return FilmScrapeResult(
                    letterboxd_url=url,
                    requested_url=url,
                    scrape_error=f"unexpected: {err}",
                    attempts=attempt,
                )

        return FilmScrapeResult(
            letterboxd_url=url,
            requested_url=url,
            scrape_error="falha nao mapeada",
            attempts=attempts,
        )

    def scrape_many(self, uris: Iterable[str]) -> list[FilmScrapeResult]:
        uri_list = [str(uri).strip() for uri in uris if str(uri).strip()]
        total = len(uri_list)
        if total == 0:
            return []

        started = time.perf_counter()
        done = ok = err = 0
        out: list[FilmScrapeResult | None] = [None] * total

        logger.info(
            "scraping: iniciado | total=%s | workers=%s | timeout=%ss | retries=%s | progress_every=%s",
            total,
            self.max_workers,
            self.timeout_s,
            self.retries,
            self.progress_every,
        )

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_idx = {executor.submit(self.scrape_one, uri): i for i, uri in enumerate(uri_list)}
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        result = future.result()
                    except Exception as err_msg:
                        result = FilmScrapeResult(
                            letterboxd_url=uri_list[idx],
                            requested_url=uri_list[idx],
                            scrape_error=f"future error: {err_msg}",
                        )
                    out[idx] = result
                    done += 1
                    if result.ok:
                        ok += 1
                    else:
                        err += 1

                    if done % self.progress_every == 0 or done == total:
                        elapsed = max(1e-6, time.perf_counter() - started)
                        rate = done / elapsed
                        eta = (total - done) / rate if rate > 0 else 0.0
                        logger.info(
                            "scraping: %s/%s | ok=%s erro=%s | %.2f url/s | ETA %.1fs",
                            done,
                            total,
                            ok,
                            err,
                            rate,
                            eta,
                        )
        finally:
            self.close()

        return [row for row in out if row is not None]


def write_scrape_failures(results: list[FilmScrapeResult], output_csv: str) -> int:
    failures = [row for row in results if not row.ok]
    if not failures:
        return 0
    with open(output_csv, "w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=["letterboxd_url", "requested_url", "scrape_error", "attempts", "title", "year"],
        )
        writer.writeheader()
        for row in failures:
            writer.writerow(
                {
                    "letterboxd_url": row.letterboxd_url,
                    "requested_url": row.requested_url,
                    "scrape_error": row.scrape_error,
                    "attempts": row.attempts,
                    "title": row.title,
                    "year": row.year,
                }
            )
    return len(failures)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    if len(sys.argv) < 2:
        print("Uso: python -m src.ingestion.scraper <url1> [url2] [...]")
        raise SystemExit(1)

    scraper = LetterboxdScraper()
    rows = scraper.scrape_many(sys.argv[1:])
    for item in rows:
        print(
            {
                "requested_url": item.requested_url,
                "url": item.letterboxd_url,
                "title": item.title,
                "year": item.year,
                "directors": item.directors,
                "cast": item.cast,
                "genres": item.genres,
                "countries": item.countries,
                "rating": item.letterboxd_avg_rating,
                "error": item.scrape_error,
            }
        )
