def test_smoke_imports() -> None:
    from src.pipeline.orchestrator import run  # noqa: F401
    from src.pipeline.run_pipeline import run as legacy_run  # noqa: F401

    assert callable(run)
    assert callable(legacy_run)


def test_api_routes_registered() -> None:
    from src.api.main import app

    paths = {route.path for route in app.routes}
    assert "/health" in paths
    assert "/pipeline/run" in paths
    assert "/analytics/kpis/main" in paths
