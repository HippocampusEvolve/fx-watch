from __future__ import annotations

import os
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"

#: DSN тестовой базы. Пока он не задан, интеграционные тесты пропускаются:
#: юнит-тесты должны оставаться запускаемыми на машине без Postgres.
TEST_DSN_ENV = "FXWATCH_TEST_DSN"


@pytest.fixture
def fixture_bytes():
    def _load(name: str) -> bytes:
        return (FIXTURES / name).read_bytes()

    return _load


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "integration: тест требует настоящей базы (переменная FXWATCH_TEST_DSN)"
    )
    # Настройки читаются один раз и кэшируются, поэтому DSN подставляется
    # до импорта fxwatch.db, то есть до сборки тестов.
    dsn = os.environ.get(TEST_DSN_ENV)
    if dsn:
        os.environ["FXWATCH_DB_DSN"] = dsn
