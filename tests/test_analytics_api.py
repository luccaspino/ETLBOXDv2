from __future__ import annotations

from src.api import dependencies as api_dependencies
from src.api.routes import analytics as analytics_route


def test_reviews_random_returns_selected_review(client, monkeypatch) -> None:
    monkeypatch.setattr(api_dependencies, 'get_user_id_by_username', lambda username: 'user-123')
    monkeypatch.setattr(
        analytics_route,
        'get_random_review',
        lambda user_id: {
            'film_id': 7,
            'title': 'Perfect Blue',
            'year': 1997,
            'watched_date': '2026-03-20',
            'review_text': 'absolute nightmare fuel',
            'letterboxd_url': 'https://letterboxd.com/film/perfect-blue/',
        },
    )

    response = client.get('/analytics/reviews/random', params={'username': 'ppino'})

    assert response.status_code == 200
    assert response.json()['title'] == 'Perfect Blue'
    assert response.json()['review_text'] == 'absolute nightmare fuel'


def test_country_rankings_most_watched_passes_filters(client, monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(api_dependencies, 'get_user_id_by_username', lambda username: 'user-123')

    def fake_rankings(user_id: str, order_by: str, min_films: int):
        calls['user_id'] = user_id
        calls['order_by'] = order_by
        calls['min_films'] = min_films
        return [{'nome': 'United States', 'filmes_assistidos': 12, 'media_nota_pessoal': 3.75}]

    monkeypatch.setattr(analytics_route, 'get_country_rankings', fake_rankings)

    response = client.get(
        '/analytics/rankings/countries/most-watched',
        params={'username': 'ppino', 'min_films': 2},
    )

    assert response.status_code == 200
    assert response.json() == [{'nome': 'United States', 'filmes_assistidos': 12, 'media_nota_pessoal': 3.75}]
    assert calls == {'user_id': 'user-123', 'order_by': 'most_watched', 'min_films': 2}


def test_genre_rankings_best_rated_requires_existing_user(client, monkeypatch) -> None:
    monkeypatch.setattr(api_dependencies, 'get_user_id_by_username', lambda username: None)

    response = client.get('/analytics/rankings/genres/best-rated', params={'username': 'ghost-user'})

    assert response.status_code == 404
    assert response.json()['detail'] == "Usuario 'ghost-user' nao encontrado."


def test_watchlist_route_returns_enriched_items(client, monkeypatch) -> None:
    monkeypatch.setattr(api_dependencies, 'get_user_id_by_username', lambda username: 'user-123')
    monkeypatch.setattr(
        analytics_route,
        'get_watchlist_films',
        lambda user_id: [
            {
                'film_id': 11,
                'title': 'Cure',
                'year': 1997,
                'runtime_min': 111,
                'original_language': 'ja',
                'tagline': 'Maddeningly brilliant.',
                'poster_url': 'https://img/cure.jpg',
                'letterboxd_url': 'https://letterboxd.com/film/cure/',
                'letterboxd_avg_rating': 4.2,
                'director': 'Kiyoshi Kurosawa',
                'genres': 'Crime, Horror, Mystery',
                'cast_top3': 'Koji Yakusho | Masato Hagiwara | Tsuyoshi Ujiki',
                'added_date': '2026-03-30',
            }
        ],
    )

    response = client.get('/analytics/watchlist', params={'username': 'ppino'})

    assert response.status_code == 200
    assert response.json()[0]['title'] == 'Cure'
    assert response.json()[0]['director'] == 'Kiyoshi Kurosawa'


def test_filters_options_route_returns_dropdown_payload(client, monkeypatch) -> None:
    monkeypatch.setattr(api_dependencies, 'get_user_id_by_username', lambda username: 'user-123')
    monkeypatch.setattr(
        analytics_route,
        'get_filter_options',
        lambda user_id: {
            'personal_ratings': [3.5, 4.0, 4.5],
            'letterboxd_ratings': [3.2, 3.8, 4.4],
            'watched_years': [2026, 2025],
            'watched_months': [1, 7, 12],
            'release_years': [2024, 2019, 1997],
            'release_decades': [1990, 2010, 2020],
            'genres': ['Drama', 'Thriller'],
            'countries': ['Japan', 'United States'],
            'country_options': [
                {'code': 'JP', 'name': 'Japan'},
                {'code': 'US', 'name': 'United States'},
            ],
            'directors': ['David Fincher'],
            'actors': ['Jake Gyllenhaal'],
            'runtime': {'min': 85, 'max': 180},
        },
    )

    response = client.get('/analytics/filters/options', params={'username': 'ppino'})

    assert response.status_code == 200
    payload = response.json()
    assert payload['personal_ratings'] == [3.5, 4.0, 4.5]
    assert payload['letterboxd_ratings'] == [3.2, 3.8, 4.4]
    assert payload['release_years'] == [2024, 2019, 1997]
    assert payload['release_decades'] == [1990, 2010, 2020]
    assert payload['country_options'][0] == {'code': 'JP', 'name': 'Japan'}
    assert payload['runtime'] == {'min': 85, 'max': 180}


def test_films_route_includes_poster_url_for_collage(client, monkeypatch) -> None:
    monkeypatch.setattr(api_dependencies, 'get_user_id_by_username', lambda username: 'user-123')
    monkeypatch.setattr(
        analytics_route,
        'get_filtered_films',
        lambda user_id, **filters: [
            {
                'film_id': 99,
                'title': 'Possession',
                'year': 1981,
                'runtime_min': 124,
                'user_rating': 4.5,
                'letterboxd_avg_rating': 4.1,
                'watched_date': '2026-03-15',
                'tagline': 'Inhuman ecstasy fulfilled.',
                'poster_url': 'https://img/possession.jpg',
                'letterboxd_url': 'https://letterboxd.com/film/possession/',
            }
        ],
    )

    response = client.get('/analytics/films', params={'username': 'ppino', 'watched_month': 3, 'watched_year': 2026})

    assert response.status_code == 200
    assert response.json()[0]['poster_url'] == 'https://img/possession.jpg'


def test_logs_films_route_returns_history_rows_for_collage(client, monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(api_dependencies, 'get_user_id_by_username', lambda username: 'user-123')

    def fake_logged_films(user_id: str, **filters):
        calls['user_id'] = user_id
        calls['filters'] = filters
        return [
            {
                'film_id': 99,
                'title': 'Possession',
                'year': 1981,
                'runtime_min': 124,
                'user_rating': 4.5,
                'letterboxd_avg_rating': 4.1,
                'watched_date': '2026-03-15',
                'tagline': 'Inhuman ecstasy fulfilled.',
                'poster_url': 'https://img/possession.jpg',
                'letterboxd_url': 'https://letterboxd.com/film/possession/',
                'genres': ['Drama', 'Horror'],
                'countries': ['France', 'West Germany'],
            }
        ]

    monkeypatch.setattr(analytics_route, 'get_logged_films', fake_logged_films)

    response = client.get('/analytics/logs/films', params={'username': 'ppino', 'watched_year': 2026})

    assert response.status_code == 200
    assert response.json()[0]['poster_url'] == 'https://img/possession.jpg'
    assert response.json()[0]['genres'] == ['Drama', 'Horror']
    assert response.json()[0]['countries'] == ['France', 'West Germany']
    assert calls == {'user_id': 'user-123', 'filters': {'min_rating': None, 'max_rating': None, 'min_runtime': None, 'max_runtime': None, 'decade_start': None, 'director_name': None, 'actor_name': None, 'country_code': None, 'genre_name': None, 'watched_month': None, 'watched_year': 2026}}
