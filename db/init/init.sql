CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- USERS
-- ============================================================
CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username      VARCHAR(100) NOT NULL UNIQUE,
    email         VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    letterboxd_username VARCHAR(100),
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_username ON users(username);

-- ============================================================
-- FILMS (cache central - 1 linha por filme)
-- ============================================================
CREATE TABLE films (
    id                    SERIAL PRIMARY KEY,
    title                 VARCHAR(500) NOT NULL,
    year                  SMALLINT,
    runtime_min           SMALLINT,
    original_language     VARCHAR(10),
    overview              TEXT,
    tagline               VARCHAR(500),
    poster_url            VARCHAR(500),
    letterboxd_url        VARCHAR(500) NOT NULL UNIQUE,
    letterboxd_avg_rating NUMERIC(3, 2),
    scraped_at            TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_films_letterboxd_url ON films(letterboxd_url);
CREATE INDEX idx_films_year ON films(year);
CREATE INDEX idx_films_title ON films(title);

-- ============================================================
-- GENRES
-- ============================================================
CREATE TABLE genres (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- ============================================================
-- FILM_GENRES (N:N filmes-generos)
-- ============================================================
CREATE TABLE film_genres (
    film_id  INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    genre_id INTEGER NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (film_id, genre_id)
);

CREATE INDEX idx_film_genres_genre ON film_genres(genre_id);

-- ============================================================
-- PEOPLE (diretores e atores)
-- ============================================================
CREATE TABLE people (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE INDEX idx_people_name ON people(name);

-- ============================================================
-- FILM_PEOPLE (N:N filmes-pessoas com role e ordem)
-- ============================================================
CREATE TABLE film_people (
    film_id    INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    person_id  INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    role       VARCHAR(20) NOT NULL CHECK (role IN ('director', 'actor')),
    cast_order SMALLINT,  -- NULL para diretores, 1-3 para atores
    PRIMARY KEY (film_id, person_id, role)
);

CREATE INDEX idx_film_people_person ON film_people(person_id);
CREATE INDEX idx_film_people_role   ON film_people(role);

-- ============================================================
-- FILM_COUNTRIES (N:N filmes-paises)
-- ============================================================
CREATE TABLE film_countries (
    film_id      INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    country_code CHAR(2) NOT NULL,
    PRIMARY KEY (film_id, country_code)
);

CREATE INDEX idx_film_countries_code ON film_countries(country_code);

-- ============================================================
-- USER_FILMS (coracao do schema - dados pessoais por usuario)
-- ============================================================
CREATE TABLE user_films (
    id           SERIAL PRIMARY KEY,
    user_id      UUID    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    film_id      INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    rating       NUMERIC(3, 1) CHECK (rating BETWEEN 0.5 AND 5.0),
    watched_date DATE,
    log_date     DATE,
    is_rewatch   BOOLEAN DEFAULT FALSE,
    review_text  TEXT,
    tags         VARCHAR(500)
);

CREATE UNIQUE INDEX uq_user_films_user_film_watched_not_null
    ON user_films(user_id, film_id, watched_date)
    WHERE watched_date IS NOT NULL;

CREATE UNIQUE INDEX uq_user_films_user_film_watched_null
    ON user_films(user_id, film_id)
    WHERE watched_date IS NULL;

CREATE INDEX idx_user_films_user       ON user_films(user_id);
CREATE INDEX idx_user_films_film       ON user_films(film_id);
CREATE INDEX idx_user_films_watched    ON user_films(watched_date);
CREATE INDEX idx_user_films_rating     ON user_films(rating);

-- ============================================================
-- WATCHLIST (filmes que o usuario quer assistir)
-- ============================================================
CREATE TABLE watchlist (
    id         SERIAL PRIMARY KEY,
    user_id    UUID    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    film_id    INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    added_date DATE,
    UNIQUE (user_id, film_id)
);

CREATE INDEX idx_watchlist_user ON watchlist(user_id);
