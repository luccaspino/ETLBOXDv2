from __future__ import annotations

import csv
import logging
import math
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
    latency_ms: float | None = None
    http_status: int | None = None
    http_version: str | None = None
    used_fallback: bool = False

    @property
    def ok(self) -> bool:
        return self.scrape_error is None


@dataclass
class ScrapeMetrics:
    total: int = 0
    ok: int = 0
    errors: int = 0
    timeouts: int = 0
    retries_fired: int = 0
    fallbacks_used: int = 0
    http2_responses: int = 0
    http11_responses: int = 0
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    elapsed_s: float = 0.0
    urls_per_s: float = 0.0

    @staticmethod
    def _fmt_ms(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value:.0f}ms"

    def log(self) -> None:
        logger.info(
            (
                "scraping metrics | total=%s ok=%s err=%s timeouts=%s retries=%s fallbacks=%s"
                " | http2=%s http1.1=%s | p50=%s p95=%s p99=%s | %.2f url/s em %.1fs"
            ),
            self.total,
            self.ok,
            self.errors,
            self.timeouts,
            self.retries_fired,
            self.fallbacks_used,
            self.http2_responses,
            self.http11_responses,
            self._fmt_ms(self.p50_ms),
            self._fmt_ms(self.p95_ms),
            self._fmt_ms(self.p99_ms),
            self.urls_per_s,
            self.elapsed_s,
        )


def _http2_dependencies_available() -> bool:
    try:
        import h2  # noqa: F401
    except ImportError:
        return False
    return True


def _pool_sizes(max_workers: int) -> tuple[int, int]:
    return max(64, max_workers * 4), max(32, max_workers * 2)


def _percentile(data: list[float], percentile_rank: int) -> float | None:
    if not data:
        return None
    rank = max(1, math.ceil((percentile_rank / 100) * len(data)))
    return data[rank - 1]


def _compute_metrics(results: list[FilmScrapeResult], elapsed_s: float) -> ScrapeMetrics:
    latencies = sorted(row.latency_ms for row in results if row.latency_ms is not None)
    ok = sum(1 for row in results if row.ok)
    errors = len(results) - ok
    timeouts = sum(
        1
        for row in results
        if not row.ok and row.scrape_error and "timeout" in row.scrape_error.lower()
    )
    retries_fired = sum(max(0, row.attempts - 1) for row in results)
    fallbacks_used = sum(1 for row in results if row.used_fallback)
    http2_responses = sum(1 for row in results if row.http_version == "HTTP/2")
    http11_responses = sum(1 for row in results if row.http_version == "HTTP/1.1")

    return ScrapeMetrics(
        total=len(results),
        ok=ok,
        errors=errors,
        timeouts=timeouts,
        retries_fired=retries_fired,
        fallbacks_used=fallbacks_used,
        http2_responses=http2_responses,
        http11_responses=http11_responses,
        p50_ms=_percentile(latencies, 50),
        p95_ms=_percentile(latencies, 95),
        p99_ms=_percentile(latencies, 99),
        elapsed_s=elapsed_s,
        urls_per_s=(len(results) / elapsed_s) if elapsed_s > 0 else 0.0,
    )


def _iter_exception_chain(err: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = err
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        chain.append(current)
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return chain


def _looks_like_http2_transport_issue(err: BaseException) -> bool:
    for item in _iter_exception_chain(err):
        name = type(item).__name__
        module = type(item).__module__
        if isinstance(item, KeyError):
            return True
        if module.startswith("h2.") and name in {"ProtocolError", "StreamClosedError"}:
            return True
        if module.startswith("httpcore.") and name in {"RemoteProtocolError", "ReadError", "WriteError"}:
            return True
    return False


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
        self.last_metrics: ScrapeMetrics | None = None

        self._user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
        self._max_connections, self._max_keepalive_connections = _pool_sizes(self.max_workers)
        self.http2_enabled = _http2_dependencies_available()
        if not self.http2_enabled:
            logger.debug("Dependencia 'h2' ausente; scraper usara HTTP/1.1.")
        self._client_lock = threading.Lock()
        self._stale_clients: list[httpx.Client] = []
        self._client = self._build_http_client(http2_enabled=self.http2_enabled)

    def _build_http_client(self, *, http2_enabled: bool) -> httpx.Client:
        return httpx.Client(
            timeout=httpx.Timeout(self.timeout_s),
            limits=httpx.Limits(
                max_connections=self._max_connections,
                max_keepalive_connections=self._max_keepalive_connections,
                keepalive_expiry=60.0,
            ),
            follow_redirects=True,
            http2=http2_enabled,
            headers={
                "User-Agent": self._user_agent,
                "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8",
            },
        )

    def _downgrade_client_to_http11(self, err: BaseException) -> None:
        with self._client_lock:
            if not self.http2_enabled:
                return
            old_client = self._client
            self.http2_enabled = False
            self._client = self._build_http_client(http2_enabled=False)
            self._stale_clients.append(old_client)

        logger.warning(
            "Instabilidade no transporte HTTP/2 detectada (%s). "
            "Scraper fara downgrade para HTTP/1.1 nas proximas requisicoes.",
            type(err).__name__,
        )

    def close(self) -> None:
        clients = [self._client]
        with self._client_lock:
            clients.extend(self._stale_clients)
            self._stale_clients.clear()
        for client in clients:
            try:
                client.close()
            except Exception:
                pass

    def _fetch_html(self, url: str) -> tuple[str, str, int, str | None]:
        self._rate_limiter.acquire()
        try:
            response = self._client.get(url)
        except Exception as err:
            if self.http2_enabled and _looks_like_http2_transport_issue(err):
                self._downgrade_client_to_http11(err)
                response = self._client.get(url)
            else:
                raise
        response.raise_for_status()
        return (
            response.text,
            str(response.url),
            response.status_code,
            getattr(response, "http_version", None),
        )

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

    def _try_scrape_canonical_from_blocked_review(
        self,
        err: httpx.HTTPStatusError,
        requested_url: str,
        attempt: int,
        started_at: float,
    ) -> FilmScrapeResult | None:
        response = err.response
        blocked_url = str(response.url).rstrip("/")
        if response.status_code != 403 or not _is_review_like_page(blocked_url):
            return None

        canonical_url = _to_global_film_url(blocked_url)
        if canonical_url == blocked_url:
            return None

        logger.debug("403 em review/log %s; tentando URL canonica %s", blocked_url, canonical_url)

        try:
            html, final_url, status_code, http_version = self._fetch_html(canonical_url)
            result = _parse_film_page(final_url, html)
            result.requested_url = requested_url
            result.attempts = attempt
            result.http_status = status_code
            result.http_version = http_version
            result.used_fallback = True
            result.latency_ms = (time.perf_counter() - started_at) * 1000
            return result
        except Exception as canonical_err:
            logger.debug(
                "fallback canonico apos 403 falhou para %s: %s",
                canonical_url,
                canonical_err,
            )
            return None

    def scrape_one(self, uri: str) -> FilmScrapeResult:
        url = _normalize_film_url(uri)
        if not _is_letterboxd_url(url):
            return FilmScrapeResult(
                letterboxd_url=url,
                requested_url=url,
                scrape_error=f"URL invalida para Letterboxd: {url}",
            )

        attempts = self.retries + 1
        last_status: int | None = None
        last_http_version: str | None = None
        started_at = time.perf_counter()

        for attempt in range(1, attempts + 1):
            try:
                html, final_url, status_code, http_version = self._fetch_html(url)
                last_status = status_code
                last_http_version = http_version

                result = _parse_film_page(final_url, html)
                result.requested_url = url
                result.attempts = attempt
                result.http_status = status_code
                result.http_version = http_version

                global_film_url = _to_global_film_url(final_url)
                must_refetch_canonical = _is_review_like_page(final_url) or _looks_like_review_title(result.title)
                if global_film_url != final_url.rstrip("/") or must_refetch_canonical:
                    try:
                        html2, final2, status_code2, http_version2 = self._fetch_html(global_film_url)
                        canonical = _parse_film_page(final2, html2)
                        canonical.requested_url = url
                        canonical.attempts = attempt
                        canonical.http_status = status_code2
                        canonical.http_version = http_version2
                        canonical.used_fallback = True
                        result = canonical
                    except Exception as err:
                        logger.debug("fallback canonical fetch falhou para %s: %s", url, err)
                        if must_refetch_canonical:
                            if self._can_use_review_fallback(result):
                                result.letterboxd_url = global_film_url
                                result.used_fallback = True
                                result.latency_ms = (time.perf_counter() - started_at) * 1000
                                logger.debug(
                                    "usando dados do review/log para %s apos falha no fetch canonico",
                                    url,
                                )
                                return result
                            return FilmScrapeResult(
                                letterboxd_url=global_film_url,
                                requested_url=url,
                                scrape_error="canonical fetch failed for review/log URL",
                                attempts=attempt,
                                http_status=last_status,
                                http_version=last_http_version,
                                used_fallback=True,
                                latency_ms=(time.perf_counter() - started_at) * 1000,
                            )

                result.latency_ms = (time.perf_counter() - started_at) * 1000
                return result
            except httpx.HTTPStatusError as err:
                status_code = err.response.status_code
                last_status = status_code
                last_http_version = getattr(err.response, "http_version", None)
                recovered = self._try_scrape_canonical_from_blocked_review(
                    err,
                    requested_url=url,
                    attempt=attempt,
                    started_at=started_at,
                )
                if recovered is not None:
                    return recovered
                if status_code not in RETRYABLE_HTTP_STATUS or attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=f"HTTP {status_code}",
                        attempts=attempt,
                        http_status=status_code,
                        http_version=last_http_version,
                        latency_ms=(time.perf_counter() - started_at) * 1000,
                    )
                self._retry_sleep(attempt)
            except httpx.TimeoutException as err:
                if attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=f"timeout: {err}",
                        attempts=attempt,
                        http_status=last_status,
                        http_version=last_http_version,
                        latency_ms=(time.perf_counter() - started_at) * 1000,
                    )
                self._retry_sleep(attempt)
            except httpx.RequestError as err:
                if attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=str(err),
                        attempts=attempt,
                        http_status=last_status,
                        http_version=last_http_version,
                        latency_ms=(time.perf_counter() - started_at) * 1000,
                    )
                self._retry_sleep(attempt)
            except TRANSIENT_FETCH_ERRORS as err:
                if attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=str(err),
                        attempts=attempt,
                        http_status=last_status,
                        http_version=last_http_version,
                        latency_ms=(time.perf_counter() - started_at) * 1000,
                    )
                self._retry_sleep(attempt)
            except ValueError as err:
                if attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=str(err),
                        attempts=attempt,
                        http_status=last_status,
                        http_version=last_http_version,
                        latency_ms=(time.perf_counter() - started_at) * 1000,
                    )
                self._retry_sleep(attempt)
            except Exception as err:
                logger.exception("erro inesperado no scraping de %s", url)
                return FilmScrapeResult(
                    letterboxd_url=url,
                    requested_url=url,
                    scrape_error=f"unexpected: {err}",
                    attempts=attempt,
                    http_status=last_status,
                    http_version=last_http_version,
                    latency_ms=(time.perf_counter() - started_at) * 1000,
                )

        return FilmScrapeResult(
            letterboxd_url=url,
            requested_url=url,
            scrape_error="falha nao mapeada",
            attempts=attempts,
            http_status=last_status,
            http_version=last_http_version,
            latency_ms=(time.perf_counter() - started_at) * 1000,
        )

    def scrape_many(self, uris: Iterable[str]) -> list[FilmScrapeResult]:
        uri_list = [str(uri).strip() for uri in uris if str(uri).strip()]
        total = len(uri_list)
        if total == 0:
            self.last_metrics = None
            return []

        started = time.perf_counter()
        done = ok = err = 0
        out: list[FilmScrapeResult | None] = [None] * total

        logger.info(
            (
                "scraping: iniciado | total=%s | workers=%s | timeout=%ss | retries=%s"
                " | http2=%s | pool=%s/%s | progress_every=%s"
            ),
            total,
            self.max_workers,
            self.timeout_s,
            self.retries,
            self.http2_enabled,
            self._max_connections,
            self._max_keepalive_connections,
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

        results = [row for row in out if row is not None]
        self.last_metrics = _compute_metrics(results, max(1e-6, time.perf_counter() - started))
        self.last_metrics.log()
        return results


def write_scrape_failures(results: list[FilmScrapeResult], output_csv: str) -> int:
    failures = [row for row in results if not row.ok]
    if not failures:
        return 0
    with open(output_csv, "w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "letterboxd_url",
                "requested_url",
                "scrape_error",
                "attempts",
                "title",
                "year",
                "http_status",
                "http_version",
                "latency_ms",
                "used_fallback",
            ],
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
                    "http_status": row.http_status,
                    "http_version": row.http_version,
                    "latency_ms": round(row.latency_ms, 1) if row.latency_ms is not None else None,
                    "used_fallback": row.used_fallback,
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
                "http_status": item.http_status,
                "http_version": item.http_version,
                "latency_ms": item.latency_ms,
                "used_fallback": item.used_fallback,
            }
        )
