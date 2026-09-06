"""Deployment-readiness configuration (M6 Task 2).

These cover the settings that only bite in a deployed environment: the database
driver a managed provider hands over, the port the platform assigns, whether the
autoreloader is left on, and which origins may call the API.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from src.config import normalise_database_url

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ── DATABASE_URL compatibility ───────────────────────────────────────────────


def test_bare_mysql_url_gains_the_installed_driver():
    """A provider's mysql:// string must select mysql-connector-python."""
    normalised = normalise_database_url("mysql://user:pw@host.internal:3306/railway")

    assert normalised == "mysql+mysqlconnector://user:pw@host.internal:3306/railway"


def test_explicit_driver_is_left_alone():
    url = "mysql+mysqlconnector://user:pw@localhost:3306/datatrust"

    assert normalise_database_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "sqlite+pysqlite:///:memory:",
        "sqlite:///data/datatrust.db",
        "postgresql://user:pw@host:5432/db",
        "mysql+pymysql://user:pw@host:3306/db",
    ],
)
def test_other_backends_are_untouched(url):
    """Only a bare mysql:// is rewritten; nothing else is second-guessed."""
    assert normalise_database_url(url) == url


def test_normalisation_preserves_query_parameters():
    url = "mysql://user:pw@host:3306/db?charset=utf8mb4"

    assert normalise_database_url(url) == (
        "mysql+mysqlconnector://user:pw@host:3306/db?charset=utf8mb4"
    )


def test_settings_normalises_the_environment_value(monkeypatch):
    """The property reads the environment at call time, so no reload is needed."""
    from src.config import Settings

    monkeypatch.setenv("DATABASE_URL", "mysql://user:pw@host:3306/railway")

    assert Settings().DATABASE_URL == "mysql+mysqlconnector://user:pw@host:3306/railway"


def test_missing_database_url_still_raises(monkeypatch):
    """The existing guard must survive the normalisation change."""
    from src.config import Settings

    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError, match="DATABASE_URL is not configured"):
        _ = Settings().DATABASE_URL


# ── CORS ─────────────────────────────────────────────────────────────────────


@pytest.fixture
def settings_for(monkeypatch):
    """Load configuration under a given CORS_ALLOW_ORIGINS, then restore it.

    The values are class attributes resolved at import, so the module has to be
    reloaded to observe a different environment.
    """
    import src.config

    def load(configured: str | None):
        if configured is None:
            monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
        else:
            monkeypatch.setenv("CORS_ALLOW_ORIGINS", configured)
        return importlib.reload(src.config).settings

    yield load

    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    importlib.reload(src.config)


@pytest.mark.parametrize(
    ("configured", "expected_origins", "expected_credentials"),
    [
        (None, ["*"], False),
        ("*", ["*"], False),
        (
            "https://dashboard.example.app",
            ["https://dashboard.example.app"],
            True,
        ),
        (
            "https://dashboard.example.app, https://admin.example.app",
            ["https://dashboard.example.app", "https://admin.example.app"],
            True,
        ),
        (
            "https://dashboard.example.app,*",
            ["https://dashboard.example.app", "*"],
            False,
        ),
    ],
    ids=[
        "unset",
        "wildcard",
        "one-explicit-origin",
        "many-explicit-origins",
        "wildcard-mixed-into-a-list",
    ],
)
def test_cors_configuration(
    settings_for, configured, expected_origins, expected_credentials
):
    """Credentials follow the presence of a wildcard, not the shape of the list."""
    settings = settings_for(configured)

    assert settings.CORS_ALLOW_ORIGINS == expected_origins
    assert settings.CORS_ALLOW_CREDENTIALS is expected_credentials


@pytest.mark.parametrize(
    "configured",
    [
        None,
        "*",
        " * ",
        "   ",
        "https://dashboard.example.app",
        "https://dashboard.example.app, https://admin.example.app",
        "https://dashboard.example.app,*",
        "*,https://dashboard.example.app",
        "https://a.example.app, *, https://b.example.app",
    ],
)
def test_a_wildcard_can_never_be_paired_with_credentials(settings_for, configured):
    """The invariant, asserted against every way a wildcard can be supplied.

    A "*" wildcard combined with credentials is invalid CORS and browsers
    reject it, so no configuration may produce that pairing.
    """
    settings = settings_for(configured)

    assert not ("*" in settings.CORS_ALLOW_ORIGINS and settings.CORS_ALLOW_CREDENTIALS)


def test_api_cors_middleware_reads_the_configured_settings():
    """The app must take its origins from configuration, not a literal."""
    from starlette.middleware.cors import CORSMiddleware

    from src.api.main import app
    from src.config import settings

    cors = next(
        middleware
        for middleware in app.user_middleware
        if middleware.cls is CORSMiddleware
    )

    assert cors.kwargs["allow_origins"] == settings.CORS_ALLOW_ORIGINS
    assert cors.kwargs["allow_credentials"] == settings.CORS_ALLOW_CREDENTIALS
    # The invalid pairing the audit flagged must be impossible.
    assert not (
        cors.kwargs["allow_origins"] == ["*"] and cors.kwargs["allow_credentials"]
    )


# ── Container start configuration ────────────────────────────────────────────
# Asserted against the Dockerfile text rather than by importing run_api.py: that
# module rebinds sys.stdout at import time, which detaches pytest's capture.


def dockerfile_text() -> str:
    return (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")


def test_image_start_command_honours_the_assigned_port():
    """A hardcoded --port would ignore the port the platform routes to."""
    content = dockerfile_text()

    assert 'CMD ["python", "run_api.py"]' in content
    assert '"--port", "8000"' not in content


def test_image_defaults_to_production_so_reload_stays_off():
    """run_api.py enables Uvicorn's autoreloader whenever APP_ENV is development."""
    assert "APP_ENV=production" in dockerfile_text()


def test_dataset_is_still_excluded_from_the_image():
    """The 45 MB CSV must not creep into the build context."""
    dockerignore = (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8")

    assert "*.csv" in dockerignore
    assert "\ndata\n" in dockerignore
