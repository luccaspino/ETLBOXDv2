from __future__ import annotations

import io
import zipfile

from src.api.routes import pipeline as pipeline_route


def _csv(rows: list[list[str]]) -> str:
    return '\n'.join([','.join(row) for row in rows]) + '\n'


def _sample_zip_bytes() -> bytes:
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('profile.csv', _csv([['username'], ['ppino']]))
        zf.writestr('diary.csv', _csv([['name', 'year', 'letterboxd_uri', 'rating', 'watched_date', 'date', 'rewatch', 'tags']]))
        zf.writestr('ratings.csv', _csv([['name', 'year', 'letterboxd_uri', 'rating']]))
        zf.writestr('reviews.csv', _csv([['letterboxd_uri', 'watched_date', 'review']]))
        zf.writestr('watchlist.csv', _csv([['name', 'year', 'letterboxd_uri', 'date']]))
        zf.writestr('extra-list.csv', _csv([['anything'], ['ok']]))
    return mem.getvalue()


def test_pipeline_run_accepts_valid_zip_with_extra_csv(client, monkeypatch) -> None:
    pipeline_route._PIPELINE_REQUEST_HISTORY.clear()
    if pipeline_route._PIPELINE_RUN_LOCK.locked():
        pipeline_route._PIPELINE_RUN_LOCK.release()

    monkeypatch.setenv('PIPELINE_MAX_ZIP_MB', '5')
    monkeypatch.setenv('PIPELINE_RATE_LIMIT_WINDOW_SECONDS', '900')
    monkeypatch.setenv('PIPELINE_RATE_LIMIT_MAX_REQUESTS', '5')

    captured: dict[str, object] = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        assert kwargs['zip_path'].endswith('.zip')
        return {
            'username': 'ppino',
            'films_upserted_from_scrape': 0,
            'user_films_loaded': 1170,
            'watchlist_loaded': 616,
        }

    monkeypatch.setattr(pipeline_route, 'run', fake_run)

    response = client.post(
        '/pipeline/run',
        files={'file': ('letterboxd.zip', _sample_zip_bytes(), 'application/zip')},
    )

    assert response.status_code == 200
    assert response.json() == {
        'username': 'ppino',
        'films_upserted_from_scrape': 0,
        'user_films_loaded': 1170,
        'watchlist_loaded': 616,
    }
    assert captured["workers"] == 20
    assert captured["timeout"] == 10
    assert captured["request_interval"] == 0.0
    assert captured["progress_every"] == 10
    assert captured["require_complete_scrape"] is True
    assert captured["max_failed_ratio"] == 0.0


def test_pipeline_run_rejects_non_zip_upload(client, monkeypatch) -> None:
    pipeline_route._PIPELINE_REQUEST_HISTORY.clear()
    monkeypatch.setenv('PIPELINE_RATE_LIMIT_WINDOW_SECONDS', '900')
    monkeypatch.setenv('PIPELINE_RATE_LIMIT_MAX_REQUESTS', '5')

    response = client.post(
        '/pipeline/run',
        files={'file': ('not-a-zip.txt', b'hello', 'text/plain')},
    )

    assert response.status_code == 400
    assert response.json()['detail'] == 'Envie um arquivo .zip exportado pelo Letterboxd.'


def test_pipeline_run_rate_limits_repeated_requests(client, monkeypatch) -> None:
    pipeline_route._PIPELINE_REQUEST_HISTORY.clear()
    monkeypatch.setenv('PIPELINE_MAX_ZIP_MB', '5')
    monkeypatch.setenv('PIPELINE_RATE_LIMIT_WINDOW_SECONDS', '900')
    monkeypatch.setenv('PIPELINE_RATE_LIMIT_MAX_REQUESTS', '1')
    monkeypatch.setattr(
        pipeline_route,
        'run',
        lambda **kwargs: {
            'username': 'ppino',
            'films_upserted_from_scrape': 0,
            'user_films_loaded': 1170,
            'watchlist_loaded': 616,
        },
    )

    first = client.post(
        '/pipeline/run',
        files={'file': ('letterboxd.zip', _sample_zip_bytes(), 'application/zip')},
    )
    second = client.post(
        '/pipeline/run',
        files={'file': ('letterboxd.zip', _sample_zip_bytes(), 'application/zip')},
    )

    assert first.status_code == 200
    assert second.status_code == 429
    assert 'Retry-After' in second.headers


def test_pipeline_run_exposes_runtime_error_detail(client, monkeypatch) -> None:
    pipeline_route._PIPELINE_REQUEST_HISTORY.clear()
    monkeypatch.setenv('PIPELINE_MAX_ZIP_MB', '5')
    monkeypatch.setenv('PIPELINE_RATE_LIMIT_WINDOW_SECONDS', '900')
    monkeypatch.setenv('PIPELINE_RATE_LIMIT_MAX_REQUESTS', '5')

    monkeypatch.setattr(
        pipeline_route,
        'run',
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError('Scraping incompleto: 32 URL(s) falharam.')),
    )

    response = client.post(
        '/pipeline/run',
        files={'file': ('letterboxd.zip', _sample_zip_bytes(), 'application/zip')},
    )

    assert response.status_code == 422
    assert response.json()['detail'] == 'Scraping incompleto: 32 URL(s) falharam.'
