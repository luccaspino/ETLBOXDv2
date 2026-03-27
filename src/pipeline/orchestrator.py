from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from src.db.repository import (
    fetch_existing_film_keys,
    fetch_existing_film_urls,
    load_all_to_db,
)
from src.ingestion.parser import parse_zip
from src.scraper.scraper import LetterboxdScraper, write_scrape_failures


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
) -> dict:
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

    if require_complete_scrape and err_count > 0:
        raise RuntimeError(
            f"Scraping incompleto: {err_count} URL(s) falharam. "
            f"Carga no DB abortada para evitar dados parciais."
        )

    logging.info("Iniciando carga no banco...")
    load_started = time.perf_counter()
    stats = load_all_to_db(parsed, scrape_results)
    logging.info("Carga no banco finalizada em %.1fs.", time.perf_counter() - load_started)
    logging.info("Pipeline finalizado com sucesso em %.1fs.", time.perf_counter() - started_at)
    return stats


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Executa parser + scraper + carga no PostgreSQL")
    parser.add_argument("zip_path", type=str, help="Caminho para o ZIP exportado pelo Letterboxd")
    parser.add_argument("--workers", type=int, default=20, help="Workers paralelos de scraping")
    parser.add_argument("--timeout", type=int, default=10, help="Timeout por request (segundos)")
    parser.add_argument("--retries", type=int, default=1, help="Quantidade de retries por URL")
    parser.add_argument("--retry-backoff", type=float, default=0.25, help="Backoff base entre retries")
    parser.add_argument("--request-interval", type=float, default=0.0, help="Intervalo minimo global entre requests")
    parser.add_argument("--progress-every", type=int, default=50, help="Log de progresso a cada N URLs")
    parser.add_argument("--errors-out", type=str, default="scrape_errors.csv", help="CSV com URLs que falharam")
    parser.add_argument("--auto-retry-failed", action="store_true", default=True, help="Ativa retry automatico de falhas")
    parser.add_argument("--no-auto-retry-failed", dest="auto_retry_failed", action="store_false", help="Desativa retry automatico de falhas")
    parser.add_argument("--retry-failed-passes", type=int, default=6, help="Numero de passadas de retry automatico")
    parser.add_argument("--allow-partial", action="store_true", default=False, help="Permite continuar para carga mesmo com falhas de scraping")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    args = build_arg_parser().parse_args()

    zip_file = Path(args.zip_path)
    if not zip_file.exists():
        raise SystemExit(f"ZIP nao encontrado: {zip_file}")

    summary = run(
        zip_path=str(zip_file),
        workers=args.workers,
        timeout=args.timeout,
        retries=args.retries,
        retry_backoff=args.retry_backoff,
        request_interval=args.request_interval,
        progress_every=args.progress_every,
        errors_out=args.errors_out,
        auto_retry_failed=args.auto_retry_failed,
        retry_failed_passes=args.retry_failed_passes,
        require_complete_scrape=not args.allow_partial,
    )
    print(summary)


if __name__ == "__main__":
    main()
