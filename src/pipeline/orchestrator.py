from __future__ import annotations

import logging
import time

from src.db import (
    fetch_existing_film_keys,
    fetch_existing_film_urls,
    load_all_to_db,
)
from src.ingestion.parser import parse_zip
from src.ingestion.scraper import LetterboxdScraper, write_scrape_failures


def run(
    zip_path: str,
    workers: int = 20,
    timeout: int = 10,
    retries: int = 1,
    retry_backoff: float = 0.25,
    request_interval: float = 0.0,
    progress_every: int = 50,
    errors_out: str | None = "scrape_errors.csv",
    auto_retry_failed: bool = True,
    retry_failed_passes: int = 6,
    require_complete_scrape: bool = True,
    max_failed_ratio: float = 0.0,
) -> dict[str, int | str]:
    started_at = time.perf_counter()
    logging.info("Pipeline iniciado para ZIP: %s", zip_path)

    existing = fetch_existing_film_urls()
    existing_keys = fetch_existing_film_keys()
    logging.info("Cache DB carregado: %s URL(s) ja conhecidas.", len(existing))
    logging.info("Cache DB carregado: %s chave(s) nome+ano conhecidas.", len(existing_keys))

    parse_started = time.perf_counter()
    parsed = parse_zip(
        zip_path,
        existing_uris=existing,
        existing_film_keys=existing_keys,
    )
    logging.info("Parser finalizado em %.1fs.", time.perf_counter() - parse_started)
    queue_df = parsed["scrape_queue"]
    uris = queue_df["letterboxd_uri"].dropna().astype(str).tolist()
    logging.info("Fila de scraping: %s URL(s).", len(uris))

    scraper = LetterboxdScraper(
        max_workers=workers,
        timeout_s=timeout,
        retries=retries,
        retry_backoff_s=retry_backoff,
        request_interval_s=request_interval,
        progress_every=progress_every,
    )
    scrape_started = time.perf_counter()
    scrape_results = scraper.scrape_many(uris)
    ok_count = sum(1 for item in scrape_results if item.ok)
    err_count = len(scrape_results) - ok_count
    logging.info(
        "Scraping finalizado: ok=%s erro=%s em %.1fs",
        ok_count,
        err_count,
        time.perf_counter() - scrape_started,
    )

    if auto_retry_failed and err_count > 0:
        for attempt in range(1, retry_failed_passes + 1):
            failed_idx = [i for i, item in enumerate(scrape_results) if not item.ok]
            if not failed_idx:
                break

            failed_uris = [scrape_results[i].letterboxd_url for i in failed_idx]
            logging.info(
                "Retry automatico de falhas (%s/%s): %s URL(s).",
                attempt,
                retry_failed_passes,
                len(failed_uris),
            )

            retry_scraper = LetterboxdScraper(
                max_workers=min(6, max(2, workers)),
                timeout_s=max(10, timeout),
                retries=max(2, retries),
                retry_backoff_s=max(0.4, retry_backoff),
                request_interval_s=max(0.05, request_interval),
                progress_every=progress_every,
            )
            retried = retry_scraper.scrape_many(failed_uris)

            for idx, retried_item in zip(failed_idx, retried):
                if retried_item.ok:
                    scrape_results[idx] = retried_item

            ok_count = sum(1 for item in scrape_results if item.ok)
            err_count = len(scrape_results) - ok_count
            logging.info(
                "Pos-retry (%s/%s): ok=%s erro=%s",
                attempt,
                retry_failed_passes,
                ok_count,
                err_count,
            )

    if errors_out and err_count > 0:
        written = write_scrape_failures(scrape_results, errors_out)
        logging.info("Falhas de scraping exportadas: %s -> %s", written, errors_out)

    failed_ratio = (err_count / len(scrape_results)) if scrape_results else 0.0
    if require_complete_scrape and err_count > 0 and failed_ratio > max_failed_ratio:
        raise RuntimeError(
            f"Scraping incompleto: {err_count} URL(s) falharam "
            f"({failed_ratio:.2%}, limite {max_failed_ratio:.2%}). "
            f"Carga no DB abortada para evitar dados parciais."
        )

    logging.info("Iniciando carga no banco...")
    load_started = time.perf_counter()
    stats = load_all_to_db(parsed, scrape_results)
    logging.info("Carga no banco finalizada em %.1fs.", time.perf_counter() - load_started)
    logging.info("Pipeline finalizado com sucesso em %.1fs.", time.perf_counter() - started_at)
    return stats
