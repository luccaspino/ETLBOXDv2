def test_smoke_imports() -> None:
    from src.pipeline.orchestrator import run  # noqa: F401
    from src.pipeline.run_pipeline import run as legacy_run  # noqa: F401

    assert callable(run)
    assert callable(legacy_run)
