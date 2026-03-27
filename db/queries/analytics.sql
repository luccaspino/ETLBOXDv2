-- ============================================================
-- Letterboxd Analytics Queries
-- Param style: psycopg named params (ex: %(user_id)s)
-- ============================================================

-- ------------------------------------------------------------
-- Base CTE reutilizavel (filtros para tabela/sorteador)
-- ------------------------------------------------------------
-- Params opcionais:
-- %(user_id)s                UUID (obrigatorio)
-- %(min_rating)s             NUMERIC
-- %(max_rating)s             NUMERIC
-- %(director_name)s          TEXT
-- %(actor_name)s             TEXT
-- %(country_code)s           TEXT (ISO2)
-- %(genre_name)s             TEXT
-- %(min_runtime)s            INT
-- %(max_runtime)s            INT
-- %(decade_start)s           INT (ex: 1990)
WITH filtered_films AS (
    SELECT
        uf.user_id,
        uf.id AS user_film_id,
        uf.rating AS user_rating,
        uf.watched_date,
        uf.log_date,
        uf.is_rewatch,
        uf.review_text,
        uf.tags,
        f.id AS film_id,
        f.title,
        f.year,
        f.runtime_min,
        f.original_language,
        f.overview,
        f.tagline,
        f.poster_url,
        f.letterboxd_url,
        f.letterboxd_avg_rating
    FROM user_films uf
    JOIN films f ON f.id = uf.film_id
    WHERE uf.user_id = %(user_id)s
      AND (%(min_rating)s IS NULL OR uf.rating >= %(min_rating)s)
      AND (%(max_rating)s IS NULL OR uf.rating <= %(max_rating)s)
      AND (%(min_runtime)s IS NULL OR f.runtime_min >= %(min_runtime)s)
      AND (%(max_runtime)s IS NULL OR f.runtime_min <= %(max_runtime)s)
      AND (
          %(decade_start)s IS NULL
          OR (f.year IS NOT NULL AND f.year BETWEEN %(decade_start)s AND %(decade_start)s + 9)
      )
      AND (
          %(director_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_people fp
              JOIN people p ON p.id = fp.person_id
              WHERE fp.film_id = f.id
                AND fp.role = 'director'
                AND p.name ILIKE %(director_name)s
          )
      )
      AND (
          %(actor_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_people fp
              JOIN people p ON p.id = fp.person_id
              WHERE fp.film_id = f.id
                AND fp.role = 'actor'
                AND p.name ILIKE %(actor_name)s
          )
      )
      AND (
          %(country_code)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_countries fc
              WHERE fc.film_id = f.id
                AND fc.country_code = %(country_code)s
          )
      )
      AND (
          %(genre_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_genres fg
              JOIN genres g ON g.id = fg.genre_id
              WHERE fg.film_id = f.id
                AND g.name ILIKE %(genre_name)s
          )
      )
)
SELECT 1;


-- ============================================================
-- 1) KPIs principais (total filmes, media nota, total horas)
-- ============================================================
SELECT
    COUNT(*)::INT AS total_filmes,
    ROUND(AVG(uf.rating)::NUMERIC, 2) AS media_nota_pessoal,
    ROUND(SUM(COALESCE(f.runtime_min, 0)) / 60.0, 2) AS total_horas
FROM user_films uf
JOIN films f ON f.id = uf.film_id
WHERE uf.user_id = %(user_id)s;


-- ============================================================
-- 2) KPI diferenca nota pessoal vs media Letterboxd
-- ============================================================
SELECT
    ROUND(AVG(uf.rating - f.letterboxd_avg_rating)::NUMERIC, 2) AS diferenca_media,
    ROUND(AVG(uf.rating)::NUMERIC, 2) AS media_pessoal,
    ROUND(AVG(f.letterboxd_avg_rating)::NUMERIC, 2) AS media_letterboxd
FROM user_films uf
JOIN films f ON f.id = uf.film_id
WHERE uf.user_id = %(user_id)s
  AND uf.rating IS NOT NULL
  AND f.letterboxd_avg_rating IS NOT NULL;


-- ============================================================
-- 3) KPI ano medio de lancamento
-- ============================================================
SELECT ROUND(AVG(f.year)::NUMERIC, 1) AS ano_medio_lancamento
FROM user_films uf
JOIN films f ON f.id = uf.film_id
WHERE uf.user_id = %(user_id)s
  AND f.year IS NOT NULL;


-- ============================================================
-- 4) Filme sorteado (sem filtros)
-- ============================================================
SELECT f.*
FROM user_films uf
JOIN films f ON f.id = uf.film_id
WHERE uf.user_id = %(user_id)s
ORDER BY random()
LIMIT 1;


-- ============================================================
-- 5) Filmes logados por mes
-- ============================================================
SELECT
    DATE_TRUNC('month', uf.watched_date)::DATE AS mes,
    COUNT(*)::INT AS total
FROM user_films uf
WHERE uf.user_id = %(user_id)s
  AND uf.watched_date IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- 6) Filmes logados por ano
-- ============================================================
SELECT
    EXTRACT(YEAR FROM uf.watched_date)::INT AS ano,
    COUNT(*)::INT AS total
FROM user_films uf
WHERE uf.user_id = %(user_id)s
  AND uf.watched_date IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- 7) Distribuicao por rating pessoal
-- ============================================================
SELECT
    uf.rating,
    COUNT(*)::INT AS total
FROM user_films uf
WHERE uf.user_id = %(user_id)s
  AND uf.rating IS NOT NULL
GROUP BY uf.rating
ORDER BY uf.rating;


-- ============================================================
-- 8) N filmes por pais
-- ============================================================
SELECT
    fc.country_code,
    COUNT(DISTINCT uf.film_id)::INT AS total_filmes
FROM user_films uf
JOIN film_countries fc ON fc.film_id = uf.film_id
WHERE uf.user_id = %(user_id)s
GROUP BY fc.country_code
ORDER BY total_filmes DESC, fc.country_code;


-- ============================================================
-- 9) N filmes por genero
-- ============================================================
SELECT
    g.name AS genero,
    COUNT(DISTINCT uf.film_id)::INT AS total_filmes
FROM user_films uf
JOIN film_genres fg ON fg.film_id = uf.film_id
JOIN genres g ON g.id = fg.genre_id
WHERE uf.user_id = %(user_id)s
GROUP BY g.name
ORDER BY total_filmes DESC, g.name;


-- ============================================================
-- 10) Diretores mais assistidos e melhores (min 3 filmes)
-- ============================================================
SELECT
    p.name AS diretor,
    COUNT(DISTINCT uf.film_id)::INT AS filmes_assistidos,
    ROUND(AVG(uf.rating)::NUMERIC, 2) AS media_nota_pessoal
FROM user_films uf
JOIN film_people fp ON fp.film_id = uf.film_id AND fp.role = 'director'
JOIN people p ON p.id = fp.person_id
WHERE uf.user_id = %(user_id)s
  AND uf.rating IS NOT NULL
GROUP BY p.name
HAVING COUNT(DISTINCT uf.film_id) >= 3
ORDER BY filmes_assistidos DESC, media_nota_pessoal DESC, p.name;


-- ============================================================
-- 11) Atores mais assistidos e melhores (min 3 filmes)
-- ============================================================
SELECT
    p.name AS ator,
    COUNT(DISTINCT uf.film_id)::INT AS filmes_assistidos,
    ROUND(AVG(uf.rating)::NUMERIC, 2) AS media_nota_pessoal
FROM user_films uf
JOIN film_people fp ON fp.film_id = uf.film_id AND fp.role = 'actor'
JOIN people p ON p.id = fp.person_id
WHERE uf.user_id = %(user_id)s
  AND uf.rating IS NOT NULL
GROUP BY p.name
HAVING COUNT(DISTINCT uf.film_id) >= 3
ORDER BY filmes_assistidos DESC, media_nota_pessoal DESC, p.name;


-- ============================================================
-- 12) Tabela filtravel (rating, diretor, ator, pais, genero, runtime, decada)
-- Usa CTE filtered_films
-- ============================================================
WITH filtered_films AS (
    SELECT
        uf.user_id,
        uf.rating AS user_rating,
        uf.watched_date,
        uf.review_text,
        f.id AS film_id,
        f.title,
        f.year,
        f.runtime_min,
        f.tagline,
        f.letterboxd_avg_rating,
        f.letterboxd_url
    FROM user_films uf
    JOIN films f ON f.id = uf.film_id
    WHERE uf.user_id = %(user_id)s
      AND (%(min_rating)s IS NULL OR uf.rating >= %(min_rating)s)
      AND (%(max_rating)s IS NULL OR uf.rating <= %(max_rating)s)
      AND (%(min_runtime)s IS NULL OR f.runtime_min >= %(min_runtime)s)
      AND (%(max_runtime)s IS NULL OR f.runtime_min <= %(max_runtime)s)
      AND (
          %(decade_start)s IS NULL
          OR (f.year IS NOT NULL AND f.year BETWEEN %(decade_start)s AND %(decade_start)s + 9)
      )
      AND (
          %(director_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_people fp
              JOIN people p ON p.id = fp.person_id
              WHERE fp.film_id = f.id AND fp.role = 'director' AND p.name ILIKE %(director_name)s
          )
      )
      AND (
          %(actor_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_people fp
              JOIN people p ON p.id = fp.person_id
              WHERE fp.film_id = f.id AND fp.role = 'actor' AND p.name ILIKE %(actor_name)s
          )
      )
      AND (
          %(country_code)s IS NULL
          OR EXISTS (
              SELECT 1 FROM film_countries fc
              WHERE fc.film_id = f.id AND fc.country_code = %(country_code)s
          )
      )
      AND (
          %(genre_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_genres fg
              JOIN genres g ON g.id = fg.genre_id
              WHERE fg.film_id = f.id AND g.name ILIKE %(genre_name)s
          )
      )
)
SELECT
    ff.film_id,
    ff.title,
    ff.year,
    ff.runtime_min,
    ff.user_rating,
    ff.letterboxd_avg_rating,
    ff.watched_date,
    ff.tagline,
    ff.letterboxd_url
FROM filtered_films ff
ORDER BY ff.watched_date DESC NULLS LAST, ff.title;


-- ============================================================
-- 13) Tabela com cast_top3 e tagline
-- ============================================================
SELECT
    f.id AS film_id,
    f.title,
    f.year,
    f.tagline,
    STRING_AGG(p.name, ' | ' ORDER BY fp.cast_order) AS cast_top3
FROM films f
LEFT JOIN film_people fp
    ON fp.film_id = f.id
   AND fp.role = 'actor'
   AND fp.cast_order BETWEEN 1 AND 3
LEFT JOIN people p ON p.id = fp.person_id
GROUP BY f.id, f.title, f.year, f.tagline
ORDER BY f.title;


-- ============================================================
-- 14) Sorteador com filtros ativos (mesmos filtros da tabela)
-- ============================================================
WITH filtered_films AS (
    SELECT
        uf.user_id,
        uf.rating AS user_rating,
        uf.watched_date,
        f.id AS film_id,
        f.title,
        f.year,
        f.runtime_min,
        f.tagline,
        f.letterboxd_avg_rating,
        f.letterboxd_url
    FROM user_films uf
    JOIN films f ON f.id = uf.film_id
    WHERE uf.user_id = %(user_id)s
      AND (%(min_rating)s IS NULL OR uf.rating >= %(min_rating)s)
      AND (%(max_rating)s IS NULL OR uf.rating <= %(max_rating)s)
      AND (%(min_runtime)s IS NULL OR f.runtime_min >= %(min_runtime)s)
      AND (%(max_runtime)s IS NULL OR f.runtime_min <= %(max_runtime)s)
      AND (
          %(decade_start)s IS NULL
          OR (f.year IS NOT NULL AND f.year BETWEEN %(decade_start)s AND %(decade_start)s + 9)
      )
      AND (
          %(director_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_people fp
              JOIN people p ON p.id = fp.person_id
              WHERE fp.film_id = f.id AND fp.role = 'director' AND p.name ILIKE %(director_name)s
          )
      )
      AND (
          %(actor_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_people fp
              JOIN people p ON p.id = fp.person_id
              WHERE fp.film_id = f.id AND fp.role = 'actor' AND p.name ILIKE %(actor_name)s
          )
      )
      AND (
          %(country_code)s IS NULL
          OR EXISTS (
              SELECT 1 FROM film_countries fc
              WHERE fc.film_id = f.id AND fc.country_code = %(country_code)s
          )
      )
      AND (
          %(genre_name)s IS NULL
          OR EXISTS (
              SELECT 1
              FROM film_genres fg
              JOIN genres g ON g.id = fg.genre_id
              WHERE fg.film_id = f.id AND g.name ILIKE %(genre_name)s
          )
      )
)
SELECT *
FROM filtered_films
ORDER BY random()
LIMIT 1;
