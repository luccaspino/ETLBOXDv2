from __future__ import annotations

from src.db.connection import DatabaseUnavailableError
from src.api.routes import users as users_route


def test_get_user_by_username_success(client, monkeypatch) -> None:
    monkeypatch.setattr(
        users_route,
        'get_user_lookup',
        lambda username: {
            'username': username,
            'has_data': True,
            'total_filmes': 1170,
            'total_watchlist': 616,
        },
    )

    response = client.get('/users/ppino')

    assert response.status_code == 200
    assert response.json() == {
        'username': 'ppino',
        'has_data': True,
        'total_filmes': 1170,
        'total_watchlist': 616,
    }


def test_get_user_by_username_not_found(client, monkeypatch) -> None:
    monkeypatch.setattr(users_route, 'get_user_lookup', lambda username: None)

    response = client.get('/users/ghost-user')

    assert response.status_code == 404
    assert response.json()['detail'] == "Usuario 'ghost-user' nao encontrado."


def test_get_user_by_username_returns_503_when_database_is_waking_up(client, monkeypatch) -> None:
    monkeypatch.setattr(
        users_route,
        'get_user_lookup',
        lambda username: (_ for _ in ()).throw(DatabaseUnavailableError("Banco acordando.")),
    )

    response = client.get('/users/ppino')

    assert response.status_code == 503
    assert response.json()['detail'] == 'Banco acordando.'
    assert response.headers['Retry-After'] == '3'
