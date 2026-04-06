from __future__ import annotations

import logging
import time
from collections import Counter, defaultdict

from src.db import (
    fetch_existing_film_keys,
    fetch_existing_film_urls,
    load_all_to_db,
)
from src.ingestion.parser import parse_zip
from src.ingestion.scraper import LetterboxdScraper, write_scrape_failures


def _summarize_scrape_failures(scrape_results: list) -> list[tuple[str, int, list[str]]]:
    reasons = Counter()
    samples_by_reason: dict[str, list[str]] = defaultdict(list)

    for item in scrape_results:
        if item.ok:
            continue

        reason = (item.scrape_error or "erro desconhecido").strip()
        reasons[reason] += 1
        if len(samples_by_reason[reason]) < 5:
            samples_by_reason[reason].append(item.letterboxd_url)

    summary: list[tuple[str, int, list[str]]] = []
    for reason, count in reasons.most_common():
        summary.append((reason, count, samples_by_reason[reason]))
    return summary


def _log_scrape_failure_summary(scrape_results: list, stage: str) -> None:
    summary = _summarize_scrape_failures(scrape_results)
    if not summary:
        return

    failed_total = sum(count for _, count, _ in summary)
    logging.info(
        "Resumo das falhas de scraping (%s): %s URL(s) com %s motivo(s).",
        stage,
        failed_total,
        len(summary),
    )
    for reason, count, sample_urls in summary[:5]:
        samples_text = ", ".join(sample_urls) if sample_urls else "-"
        logging.info(
            "Falha de scraping (%s): motivo=%s | total=%s | amostra=%s",
            stage,
            reason,
            count,
            samples_text,
        )


def _format_failure_reason_summary(scrape_results: list, limit: int = 3) -> str:
    summary = _summarize_scrape_failures(scrape_results)
    if not summary:
        return ""
    return "; ".join(f"{reason} ({count})" for reason, count, _ in summary[:limit])


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
    if err_count > 0:
        _log_scrape_failure_summary(scrape_results, stage="tentativa inicial")

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
            cooldown_s = min(3.0, 0.5 * attempt)
            if cooldown_s > 0:
                logging.info("Aguardando %.1fs antes do retry das falhas.", cooldown_s)
                time.sleep(cooldown_s)

            retry_scraper = LetterboxdScraper(
                max_workers=min(3, max(1, workers // 4)),
                timeout_s=max(10, timeout),
                retries=max(2, retries),
                retry_backoff_s=max(0.75, retry_backoff),
                request_interval_s=max(0.2, request_interval),
                progress_every=progress_every,
            )
            retried = retry_scraper.scrape_many(failed_uris)
            recovered_count = 0

            for idx, retried_item in zip(failed_idx, retried):
                if retried_item.ok:
                    scrape_results[idx] = retried_item
                    recovered_count += 1

            ok_count = sum(1 for item in scrape_results if item.ok)
            err_count = len(scrape_results) - ok_count
            logging.info(
                "Pos-retry (%s/%s): ok=%s erro=%s recuperados=%s",
                attempt,
                retry_failed_passes,
                ok_count,
                err_count,
                recovered_count,
            )
            if err_count > 0:
                _log_scrape_failure_summary(scrape_results, stage=f"apos retry {attempt}")
            if recovered_count == 0:
                logging.info(
                    "Retry automatico encerrado antes do limite: nenhuma URL foi recuperada nesta passada."
                )
                break

    if errors_out and err_count > 0:
        written = write_scrape_failures(scrape_results, errors_out)
        logging.info("Falhas de scraping exportadas: %s -> %s", written, errors_out)

    failed_ratio = (err_count / len(scrape_results)) if scrape_results else 0.0
    if require_complete_scrape and err_count > 0 and failed_ratio > max_failed_ratio:
        reason_summary = _format_failure_reason_summary(scrape_results)
        reason_suffix = f" Motivos mais comuns: {reason_summary}." if reason_summary else ""
        raise RuntimeError(
            f"Scraping incompleto: {err_count} URL(s) falharam "
            f"({failed_ratio:.2%}, limite {max_failed_ratio:.2%}). "
            f"Carga no DB abortada para evitar dados parciais."
            f"{reason_suffix}"
        )

    logging.info("Iniciando carga no banco...")
    load_started = time.perf_counter()
    stats = load_all_to_db(parsed, scrape_results)
    logging.info("Carga no banco finalizada em %.1fs.", time.perf_counter() - load_started)
    logging.info("Pipeline finalizado com sucesso em %.1fs.", time.perf_counter() - started_at)
    return stats
