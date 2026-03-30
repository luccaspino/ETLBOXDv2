def test_smoke_imports() -> None:
    from src.pipeline.orchestrator import run  # noqa: F401
    from src.pipeline.run_pipeline import run as cli_run  # noqa: F401

    assert callable(run)
    assert callable(cli_run)


def test_api_routes_registered() -> None:
    from src.api.main import app

    paths = {route.path for route in app.routes}
    assert "/health" in paths
    assert "/health/db" in paths
    assert "/pipeline/run" in paths
    assert "/users/{username}" in paths
    assert "/analytics/kpis/main" in paths
    assert "/analytics/rankings/countries/most-watched" in paths
    assert "/analytics/rankings/genres/best-rated" in paths
