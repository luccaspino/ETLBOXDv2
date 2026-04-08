from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.pipeline.orchestrator import run as run_pipeline


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
    parser.add_argument(
        "--max-failed-ratio",
        type=float,
        default=0.0,
        help="Quando --allow-partial NAO for usado, aborta so se a taxa de falha ultrapassar esse valor (0.0 a 1.0)",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    args = build_arg_parser().parse_args()

    zip_file = Path(args.zip_path)
    if not zip_file.exists():
        raise SystemExit(f"ZIP nao encontrado: {zip_file}")

    summary = run_pipeline(
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
        max_failed_ratio=max(0.0, min(1.0, args.max_failed_ratio)),
    )
    print(summary)


if __name__ == "__main__":
    main()
