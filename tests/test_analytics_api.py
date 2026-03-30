from __future__ import annotations

from src.api.routes import analytics as analytics_route


def test_reviews_random_returns_selected_review(client, monkeypatch) -> None:
    monkeypatch.setattr(analytics_route, 'get_user_id_by_username', lambda username: 'user-123')
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

    monkeypatch.setattr(analytics_route, 'get_user_id_by_username', lambda username: 'user-123')

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
    monkeypatch.setattr(analytics_route, 'get_user_id_by_username', lambda username: None)

    response = client.get('/analytics/rankings/genres/best-rated', params={'username': 'ghost-user'})

    assert response.status_code == 404
    assert response.json()['detail'] == "Usuario 'ghost-user' nao encontrado."
