# ETLboxd

ETLboxd is a Letterboxd analytics stack built with FastAPI, Streamlit, PostgreSQL, and a custom scraping/ingestion pipeline.

The project lets you:

- upload a Letterboxd ZIP export
- enrich films with scraped metadata
- persist user, watchlist, logs, people, genres, and countries in PostgreSQL
- explore dashboards for summary metrics, rankings, filters, collages, watchlist analysis, and random picks
- check backend and database readiness from the initial menu, with better handling for free-tier cold starts

## Stack

- Backend API: FastAPI
- Dashboard: Streamlit
- Database: PostgreSQL
- Scraping: Python + BeautifulSoup + httpx
- Tests: pytest

## Main folders

- `src/api`: FastAPI routes and schemas
- `src/api/validators`: request/upload validation for API endpoints
- `src/dashboard`: Streamlit app and pages
- `src/db`: database access, mappings, and write/load logic
- `src/ingestion`: ZIP parsing and scraping
- `src/pipeline`: orchestration only
- `scripts`: local CLI/benchmark entrypoints
- `db/init`: local PostgreSQL bootstrap SQL
- `tests`: automated tests


## Upload flow

1. User uploads a Letterboxd ZIP in the dashboard.
2. The API validates file size and ZIP contents.
3. The parser loads `profile.csv`, `diary.csv`, `ratings.csv`, `reviews.csv`, and `watchlist.csv`.
4. The pipeline builds a scrape queue for films not already known in the database.
5. The scraper enriches film metadata.
6. The database load upserts films, people, genres, countries, user logs, and watchlist items.
7. The dashboard cache is cleared and the user can open the analytics pages.

By default the pipeline is strict: if scraping is incomplete, the database load is aborted to avoid partial data.

## Useful endpoints

- `GET /health` - lightweight liveness check for the API service itself
- `GET /health/db` - readiness check that confirms database connectivity
- `POST /pipeline/run`
- `GET /users/{username}`
- `GET /analytics/kpis/main`
- `GET /analytics/logs/monthly`
- `GET /analytics/logs/yearly`
- `GET /analytics/logs/films`
- `GET /analytics/rankings/directors/most-watched`
- `GET /analytics/rankings/actors/most-watched`
- `GET /analytics/rankings/genres/most-watched`
- `GET /analytics/rankings/countries/most-watched`
- `GET /analytics/rankings/languages/most-watched`
- `GET /analytics/watchlist`
- `GET /analytics/filters/options`


## Notes on performance

- ZIP parsing is optimized to read only the columns used by the pipeline.
- Scraping uses a shared `httpx` client with connection pooling and optional HTTP/2 support.
- Scraping logs aggregate latency metrics (`p50`, `p95`, `p99`) to help distinguish network slowness from retries/timeouts.
- The dashboard caches deterministic GET requests to reduce rerun latency.
- The initial dashboard menu checks backend and database readiness and retries transient GET failures to better absorb cold starts on free hosting tiers.
- The upload pipeline still depends on an external site, so scrape speed can vary based on network quality, rate limiting, and Letterboxd behavior.
- On the current free-tier deployment, the first request after inactivity may take longer while backend or database services wake up.

## Deployment notes

- Backend and dashboard can be deployed separately.
- If you add new API routes or new client helpers, deploy both sides when the dashboard imports new backend-aware helpers.
- Streamlit config should stay versioned in `.streamlit/config.toml`.
- It’s currently in Portuguese, but it will be translated into English soon.
