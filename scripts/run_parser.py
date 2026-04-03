from __future__ import annotations

import sys

from src.ingestion.parser import parse_zip


def main() -> None:
    if len(sys.argv) < 2:
        print("Uso: python scripts/run_parser.py <caminho_para_o_zip>")
        raise SystemExit(1)

    result = parse_zip(sys.argv[1])

    print("\n--- user ---")
    print(result["user"].to_string(index=False))

    print("\n--- user_films (primeiras 5 linhas) ---")
    print(result["user_films"].head().to_string(index=False))

    print("\n--- watchlist (primeiras 5 linhas) ---")
    print(result["watchlist"].head().to_string(index=False))

    print("\n--- scrape_queue (primeiras 5 linhas) ---")
    print(result["scrape_queue"].head().to_string(index=False))

    print(f"\nTotal user_films : {len(result['user_films'])}")
    print(f"Total watchlist  : {len(result['watchlist'])}")
    print(f"Total scrape_queue: {len(result['scrape_queue'])}")


if __name__ == "__main__":
    main()
