import zipfile
import logging
import pandas as pd
import warnings
import re
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas: lowercase, espaços -> underscore, strip."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    return df


def _require_columns(df: pd.DataFrame, filename: str, required: list[str]) -> None:
    """Valida colunas obrigatórias para falhar com erro claro."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(
            f"{filename}: coluna(s) obrigatória(s) ausente(s): {missing}. "
            f"Colunas encontradas: {list(df.columns)}"
        )


def _optional_str_column(df: pd.DataFrame, col_name: str) -> pd.Series:
    """
    Retorna coluna opcional como string stripada e alinhada ao índice do df.

    Evita desalinhamento quando a coluna não existe (Series de tamanho 1).
    """
    if col_name not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype="object")
    return df[col_name].astype("string").str.strip()


def _parse_date(series: pd.Series) -> pd.Series:
    """Converte para datetime, retorna NaT em vez de explodir."""
    return pd.to_datetime(series, errors="coerce").dt.date


def _read_csv_from_zip(zf: zipfile.ZipFile, filename: str) -> pd.DataFrame:
    """Lê um CSV de dentro do ZIP e já normaliza as colunas."""
    try:
        with zf.open(filename) as f:
            df = pd.read_csv(f, dtype=str)
    except KeyError as err:
        raise FileNotFoundError(
            f"Arquivo obrigatório '{filename}' não encontrado no ZIP do Letterboxd."
        ) from err
    return _normalize_columns(df)


# ---------------------------------------------------------------------------
# Parsers individuais
# ---------------------------------------------------------------------------

def _parse_profile(zf: zipfile.ZipFile) -> pd.DataFrame:
    """
    Extrai dados do usuário a partir de profile.csv.

    Colunas usadas: username, given_name, family_name, email_address, date_joined
    Colunas ignoradas: location, website, bio, pronoun, favorite_films
    """
    df = _read_csv_from_zip(zf, "profile.csv")
    _require_columns(df, "profile.csv", ["username", "date_joined"])

    user_df = pd.DataFrame({
        "username":     _optional_str_column(df, "username"),
        "given_name":   _optional_str_column(df, "given_name"),
        "family_name":  _optional_str_column(df, "family_name"),
        "email":        _optional_str_column(df, "email_address"),
        "date_joined":  _parse_date(df["date_joined"]),
    })

    # Substitui strings vazias por None (será NULL no banco)
    user_df = user_df.replace("", None)

    logger.info(f"profile.csv: usuário '{user_df['username'].iloc[0]}' carregado.")
    return user_df


def _parse_diary(zf: zipfile.ZipFile) -> pd.DataFrame:
    """
    Lê diary.csv e mapeia para o schema de user_films.

    diary.csv é a fonte principal: tem watched_date, rewatch, tags.
    URIs aqui são longas (ex: https://boxd.it/4xZFbP) — usadas como
    chave de cache para o scraper.
    """
    df = _read_csv_from_zip(zf, "diary.csv")
    _require_columns(
        df,
        "diary.csv",
        ["name", "year", "letterboxd_uri", "rating", "watched_date", "date", "rewatch"],
    )

    diary_df = pd.DataFrame({
        "film_name":        _optional_str_column(df, "name"),
        "film_year":        pd.to_numeric(df["year"], errors="coerce").astype("Int64"),
        "letterboxd_uri":   _optional_str_column(df, "letterboxd_uri"),
        "rating":           pd.to_numeric(df["rating"], errors="coerce"),
        "watched_date":     _parse_date(df["watched_date"]),
        "log_date":         _parse_date(df["date"]),
        "is_rewatch":       _optional_str_column(df, "rewatch").str.upper().eq("YES"),
        "tags":             _optional_str_column(df, "tags").replace("", None),
        "review_text":      None,   # preenchido depois via merge com reviews.csv
    })

    logger.debug("diary.csv carregado: %s linha(s).", len(diary_df))
    return diary_df


def _parse_ratings(zf: zipfile.ZipFile) -> pd.DataFrame:
    """
    Lê ratings.csv para cobrir filmes com nota mas sem entrada no diário.

    ATENÇÃO: as URIs aqui são curtas (ex: https://boxd.it/1YKY), formato
    diferente do diary. O merge com diary deve ser feito por name + year,
    não por letterboxd_uri.
    """
    df = _read_csv_from_zip(zf, "ratings.csv")
    _require_columns(df, "ratings.csv", ["name", "year", "letterboxd_uri", "rating"])

    ratings_df = pd.DataFrame({
        "film_name":      _optional_str_column(df, "name"),
        "film_year":      pd.to_numeric(df["year"], errors="coerce").astype("Int64"),
        "letterboxd_uri": _optional_str_column(df, "letterboxd_uri"),
        "rating":         pd.to_numeric(df["rating"], errors="coerce"),
    })

    logger.debug("ratings.csv carregado: %s linha(s).", len(ratings_df))
    return ratings_df


def _parse_reviews(zf: zipfile.ZipFile) -> pd.DataFrame:
    """
    Lê reviews.csv para extrair textos de review.

    Chave de join com user_films: letterboxd_uri + watched_date
    (ambos no mesmo formato longo de URI do diary).
    """
    df = _read_csv_from_zip(zf, "reviews.csv")
    _require_columns(df, "reviews.csv", ["letterboxd_uri", "watched_date", "review"])

    reviews_df = pd.DataFrame({
        "letterboxd_uri": _optional_str_column(df, "letterboxd_uri"),
        "watched_date":   _parse_date(df["watched_date"]),
        "review_text":    _optional_str_column(df, "review").replace("", None),
    })

    logger.debug("reviews.csv carregado: %s linha(s).", len(reviews_df))
    return reviews_df


def _parse_watchlist(zf: zipfile.ZipFile) -> pd.DataFrame:
    """
    Lê watchlist.csv e mapeia para o schema da tabela watchlist.

    URIs aqui são curtas (mesmo formato do ratings.csv).
    """
    df = _read_csv_from_zip(zf, "watchlist.csv")
    _require_columns(df, "watchlist.csv", ["name", "year", "letterboxd_uri", "date"])

    watchlist_df = pd.DataFrame({
        "film_name":      _optional_str_column(df, "name"),
        "film_year":      pd.to_numeric(df["year"], errors="coerce").astype("Int64"),
        "letterboxd_uri": _optional_str_column(df, "letterboxd_uri"),
        "added_date":     _parse_date(df["date"]),
    })

    logger.debug("watchlist.csv carregado: %s linha(s).", len(watchlist_df))
    return watchlist_df


# ---------------------------------------------------------------------------
# Merge: diary + ratings + reviews -> user_films
# ---------------------------------------------------------------------------

def _build_user_films(
    diary_df: pd.DataFrame,
    ratings_df: pd.DataFrame,
    reviews_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Consolida diary + ratings + reviews em um único DataFrame de user_films.

    Estratégia:
    1. Parte do diary (fonte mais rica: tem watched_date, rewatch, tags).
    2. Faz merge com ratings por (film_name, film_year) para preencher
       ratings faltantes E para incluir filmes que só estão no ratings
       (watched_date = NULL nesses casos).
    3. Faz merge com reviews por (letterboxd_uri, watched_date) para
       anexar review_text.
    """

    # --- 1. Enriquecer diary com ratings faltantes ---
    # Alguns filmes no diary não têm nota (ex: só logou sem avaliar).
    # O ratings tem a nota consolidada mais recente.
    ratings_lookup = ratings_df[["film_name", "film_year", "rating"]].copy()
    ratings_lookup = ratings_lookup.rename(columns={"rating": "rating_from_ratings"})

    diary_enriched = diary_df.merge(
        ratings_lookup,
        on=["film_name", "film_year"],
        how="left",
    )
    # Prioriza nota do diary; usa ratings como fallback
    diary_enriched["rating"] = diary_enriched["rating"].fillna(
        diary_enriched["rating_from_ratings"]
    )
    diary_enriched = diary_enriched.drop(columns=["rating_from_ratings"])

    # --- 2. Adicionar filmes que só existem em ratings (não estão no diary) ---
    # Identifica quais filmes do ratings não têm nenhuma entrada no diary
    diary_keys = set(
        zip(diary_enriched["film_name"], diary_enriched["film_year"])
    )
    ratings_only = ratings_df[
        ~ratings_df.apply(
            lambda r: (r["film_name"], r["film_year"]) in diary_keys, axis=1
        )
    ].copy()

    if len(ratings_only) > 0:
        logger.debug(
            "%s filme(s) encontrados só no ratings.csv (watched_date = NULL).",
            len(ratings_only),
        )

        ratings_only_mapped = pd.DataFrame({
            "film_name":      ratings_only["film_name"],
            "film_year":      ratings_only["film_year"],
            "letterboxd_uri": ratings_only["letterboxd_uri"],
            "rating":         ratings_only["rating"],
            "watched_date":   None,
            "log_date":       None,
            "is_rewatch":     False,
            "tags":           None,
            "review_text":    None,
        })

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated",
                category=FutureWarning,
            )
            user_films_df = pd.concat(
                [diary_enriched, ratings_only_mapped],
                ignore_index=True,
            )
    else:
        user_films_df = diary_enriched.copy()

    # --- 3. Anexar reviews ---
    user_films_df = user_films_df.merge(
        reviews_df,
        on=["letterboxd_uri", "watched_date"],
        how="left",
        suffixes=("", "_from_reviews"),
    )
    # Consolida review_text (vem None do diary, preenchido pelo merge)
    if "review_text_from_reviews" in user_films_df.columns:
        user_films_df["review_text"] = user_films_df["review_text"].fillna(
            user_films_df["review_text_from_reviews"]
        )
        user_films_df = user_films_df.drop(columns=["review_text_from_reviews"])

    # --- 4. Warning para watched_date = NULL ---
  #  null_date_mask = user_films_df["watched_date"].isna()
    # if null_date_mask.any():
    #    logger.warning(
    #        "%s registro(s) com watched_date = NULL (não cobertos por UNIQUE com DATE).",
  #          int(null_date_mask.sum()),
  #      )

    logger.debug("user_films consolidado: %s registro(s).", len(user_films_df))
    return user_films_df



def _build_scrape_queue(
    user_films_df: pd.DataFrame,
    watchlist_df: pd.DataFrame,
    existing_uris: set[str] | None = None,
    existing_film_keys: set[tuple[str, int | None]] | None = None,
) -> pd.DataFrame:
    """
    Retorna a lista única de letterboxd_uri que ainda precisam ser scrapadas.

    Combina URIs de user_films e watchlist, remove duplicatas e filtra
    as que já existem no banco (passadas via existing_uris).

    existing_uris: conjunto de letterboxd_url já presentes na tabela films.
                   Passar None (padrão) assume banco vazio — scrapa tudo.
    """
    def _clean_uri(uri: str | None) -> str | None:
        if not isinstance(uri, str):
            return None
        cleaned = uri.strip()
        if not cleaned:
            return None
        # Remove querystring para evitar duplicata artificial por tracking.
        cleaned = cleaned.split("?", 1)[0].rstrip("/")
        if not cleaned:
            return None

        # Canonicaliza URLs de review/log para URL global do filme:
        # https://letterboxd.com/<user>/film/<slug>/ -> https://letterboxd.com/film/<slug>
        try:
            parsed = urlparse(cleaned)
            host = (parsed.hostname or "").lower()
            path = parsed.path or ""
            film_idx = path.find("/film/")
            if host in {"letterboxd.com", "www.letterboxd.com"} and film_idx >= 0:
                canonical_path = path[film_idx:].rstrip("/")
                # Remove sufixo de review/log: /film/<slug>/<n>
                m = re.match(r"^(/film/[^/]+)(?:/\d+)?$", canonical_path)
                if m:
                    canonical_path = m.group(1)
                return f"https://letterboxd.com{canonical_path}"
        except ValueError:
            pass

        return cleaned

    uris_user_films = user_films_df[["film_name", "film_year", "letterboxd_uri"]].copy()
    uris_watchlist = watchlist_df[["film_name", "film_year", "letterboxd_uri"]].copy()

    uris_user_films["letterboxd_uri"] = uris_user_films["letterboxd_uri"].apply(_clean_uri)
    uris_watchlist["letterboxd_uri"] = uris_watchlist["letterboxd_uri"].apply(_clean_uri)

    all_uris = pd.concat([uris_user_films, uris_watchlist], ignore_index=True)
    all_uris = all_uris.dropna(subset=["letterboxd_uri"])

    # Se houver URI longa e curta para o mesmo filme, prioriza a mais longa.
    all_uris["_uri_len"] = all_uris["letterboxd_uri"].str.len()
    all_uris = all_uris.sort_values(
        by=["film_name", "film_year", "_uri_len"],
        ascending=[True, True, False],
    ).drop_duplicates(subset=["film_name", "film_year"], keep="first")
    all_uris = all_uris.drop_duplicates(subset=["letterboxd_uri"]).drop(columns=["_uri_len"])

    if existing_uris:
        normalized_existing_uris = {_clean_uri(uri) for uri in existing_uris}
        normalized_existing_uris.discard(None)
        before = len(all_uris)
        all_uris = all_uris[~all_uris["letterboxd_uri"].isin(normalized_existing_uris)]
        logger.info(
            f"scrape_queue: {before - len(all_uris)} URI(s) já existem no banco "
            f"e foram removidas da fila."
        )

    if existing_film_keys:
        before = len(all_uris)
        mask_existing_key = all_uris.apply(
            lambda r: (str(r["film_name"]).strip().lower(), None if pd.isna(r["film_year"]) else int(r["film_year"]))
            in existing_film_keys,
            axis=1,
        )
        all_uris = all_uris[~mask_existing_key]
        logger.info(
            f"scrape_queue: {before - len(all_uris)} filme(s) já existem no banco "
            f"por chave nome+ano e foram removidos da fila."
        )

    logger.debug("scrape_queue: %s filme(s).", len(all_uris))
    return all_uris.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Entrada principal
# ---------------------------------------------------------------------------

def parse_zip(
    zip_source: str | Path | bytes,
    existing_uris: set[str] | None = None,
    existing_film_keys: set[tuple[str, int | None]] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Ponto de entrada do parser. Aceita caminho para o ZIP ou bytes (upload web).

    Parâmetros:
        zip_source    : caminho para o arquivo ZIP ou bytes do arquivo.
        existing_uris : URIs já presentes na tabela films do banco.
                        Passar None assume banco vazio.

    Retorna dict com as chaves:
        "user"         -> pd.DataFrame (1 linha)
        "user_films"   -> pd.DataFrame
        "watchlist"    -> pd.DataFrame
        "scrape_queue" -> pd.DataFrame
    """
    if isinstance(zip_source, (str, Path)):
        zf = zipfile.ZipFile(zip_source, "r")
    else:
        zf = zipfile.ZipFile(BytesIO(zip_source), "r")

    logger.info("Iniciando parser do ZIP do Letterboxd...")

    with zf:
        user_df      = _parse_profile(zf)
        diary_df     = _parse_diary(zf)
        ratings_df   = _parse_ratings(zf)
        reviews_df   = _parse_reviews(zf)
        watchlist_df = _parse_watchlist(zf)

    user_films_df = _build_user_films(diary_df, ratings_df, reviews_df)
    scrape_queue = _build_scrape_queue(
        user_films_df,
        watchlist_df,
        existing_uris,
        existing_film_keys=existing_film_keys,
    )

    films_sem_data = int(user_films_df["watched_date"].isna().sum())
    logger.info("Filmes logados: %s", len(diary_df))
    logger.info("Filmes sem data: %s", films_sem_data)
    logger.info("Filmes na watchlist: %s", len(watchlist_df))
    logger.info("Total de filmes a serem scrappados: %s", len(scrape_queue))

    logger.info("Parser concluído.")

    return {
        "user":         user_df,
        "user_films":   user_films_df,
        "watchlist":    watchlist_df,
        "scrape_queue": scrape_queue,
    }




if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python parser.py <caminho_para_o_zip>")
        sys.exit(1)

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
