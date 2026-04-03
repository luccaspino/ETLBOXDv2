from __future__ import annotations

import argparse
import csv
import logging
import random
import time
from pathlib import Path

from src.ingestion.parser import parse_zip
from src.ingestion.scraper import LetterboxdScraper


def _pick_sample(uris: list[str], sample_size: int, seed: int) -> list[str]:
    unique = list(dict.fromkeys([uri.strip() for uri in uris if uri and str(uri).strip()]))
    if len(unique) <= sample_size:
        return unique
    rng = random.Random(seed)
    return rng.sample(unique, sample_size)


def run_benchmark(zip_path: str, sample_size: int = 200, seed: int = 42) -> list[dict]:
    parsed = parse_zip(zip_path, existing_uris=None)
    uris = parsed["scrape_queue"]["letterboxd_uri"].dropna().astype(str).tolist()
    sample = _pick_sample(uris, sample_size, seed)
    logging.info("Benchmark sample: %s URL(s)", len(sample))

    configs = [
        {"name": "safe", "workers": 8, "timeout": 10, "retries": 1, "backoff": 0.25, "interval": 0.03},
        {"name": "balanced", "workers": 12, "timeout": 8, "retries": 1, "backoff": 0.20, "interval": 0.01},
        {"name": "aggressive", "workers": 20, "timeout": 8, "retries": 0, "backoff": 0.0, "interval": 0.0},
    ]

    results: list[dict] = []
    for cfg in configs:
        logging.info("Rodando config: %s", cfg["name"])
        scraper = LetterboxdScraper(
            max_workers=cfg["workers"],
            timeout_s=cfg["timeout"],
            retries=cfg["retries"],
            retry_backoff_s=cfg["backoff"],
            request_interval_s=cfg["interval"],
            progress_every=50,
        )
        start = time.perf_counter()
        out = scraper.scrape_many(sample)
        elapsed = time.perf_counter() - start
        ok = sum(1 for row in out if row.ok)
        err = len(out) - ok
        rate = (len(out) / elapsed) if elapsed > 0 else 0.0
        result = {
            "name": cfg["name"],
            "workers": cfg["workers"],
            "timeout": cfg["timeout"],
            "retries": cfg["retries"],
            "interval": cfg["interval"],
            "sample": len(out),
            "ok": ok,
            "err": err,
            "err_rate_pct": round((err / max(1, len(out))) * 100, 2),
            "elapsed_s": round(elapsed, 2),
            "urls_per_s": round(rate, 2),
        }
        results.append(result)
        logging.info("Resultado %s: %s", cfg["name"], result)

    return results


def _save_results(rows: list[dict], out_csv: str) -> None:
    if not rows:
        return
    with open(out_csv, "w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark de performance do scraper")
    parser.add_argument("zip_path", type=str, help="Caminho para ZIP do Letterboxd")
    parser.add_argument("--sample-size", type=int, default=200, help="Quantidade de URLs para benchmark")
    parser.add_argument("--seed", type=int, default=42, help="Seed para amostragem")
    parser.add_argument("--out", type=str, default="scraper_benchmark.csv", help="CSV de resultados")
    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    args = _build_arg_parser().parse_args()

    zip_file = Path(args.zip_path)
    if not zip_file.exists():
        raise SystemExit(f"ZIP nao encontrado: {zip_file}")

    rows = run_benchmark(str(zip_file), sample_size=args.sample_size, seed=args.seed)
    _save_results(rows, args.out)
    print(rows)
    print(f"Benchmark salvo em: {args.out}")
