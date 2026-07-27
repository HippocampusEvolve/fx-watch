"""Проверки на настоящей базе: идемпотентность, ревизии, откаты, as_of, календарь.

Эти тесты закрывают то, что нельзя проверить чистыми функциями. Утверждения
вроде «повторный запуск за ту же дату дублей не создаёт» держатся не на коде
на Python, а на уникальном индексе, ``ON CONFLICT`` и порядке сортировки во
вьюхе - то есть ровно на том, что видно только из Postgres. Без них главные
заявления README остаются недоказанными.

Запуск: поднять Postgres и задать ``FXWATCH_TEST_DSN``. Без переменной тесты
пропускаются, чтобы юнит-набор оставался запускаемым на машине без базы:

    docker run -d --name fxtest -p 55432:5432 -e POSTGRES_PASSWORD=fxwatch \\
        -e POSTGRES_USER=fxwatch -e POSTGRES_DB=fxwatch postgres:16-alpine
    FXWATCH_TEST_DSN=postgresql+psycopg://fxwatch:fxwatch@localhost:55432/fxwatch pytest -q
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

#: Та же переменная, что читает conftest: тесты пропускаются без тестовой базы.
TEST_DSN_ENV = "FXWATCH_TEST_DSN"
ALEMBIC_INI = str(Path(__file__).resolve().parent.parent / "alembic.ini")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get(TEST_DSN_ENV),
        reason=f"нужна тестовая база: задайте {TEST_DSN_ENV}",
    ),
]

TABLES = (
    "observations", "ingest_runs", "revisions", "raw_payloads",
    "quality_checks", "quarantine", "alerts", "source_state", "heartbeats",
)

DAY = date(2026, 7, 15)


# --------------------------------------------------------------------------
# Оснастка
# --------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def migrated_database():
    """Схема накатывается теми же миграциями, что и на проде."""
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config(ALEMBIC_INI), "head")
    yield


@pytest.fixture(autouse=True)
def clean_tables(migrated_database):
    from fxwatch.db import session_scope

    with session_scope() as session:
        session.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))
    yield


class ScriptedSource:
    """Источник, которым управляет тест: отдаёт ровно то, что ему положили."""

    code = "scripted"
    title = "источник для тестов"
    supports_history = True
    min_expected_series = 1

    def __init__(self) -> None:
        self.values: dict[str, str] = {"USD/RUB": "78.5"}
        self.reported_date: date | None = DAY
        self.quarantine: list[tuple[str, str]] = []

    def fetch(self, target_date: date):
        from fxwatch.sources.base import DataPoint, FetchResult

        day = self.reported_date or target_date
        points = [
            DataPoint(series_key=key, value_date=day, value=Decimal(value), raw_value=value)
            for key, value in self.values.items()
        ]
        return FetchResult(
            points=points,
            # Тело должно быть похоже на данные: проверка на пустой ответ
            # блокирующая, и короткий ответ не дошёл бы до записи.
            raw_body=b"<scripted-source-response>" + b"." * 200,
            http_status=200,
            content_type="application/xml",
            reported_date=day,
            quarantine=list(self.quarantine),
        )


@pytest.fixture
def source():
    from fxwatch.sources import REGISTRY

    scripted = ScriptedSource()
    REGISTRY[scripted.code] = scripted
    try:
        yield scripted
    finally:
        REGISTRY.pop(scripted.code, None)


@pytest.fixture
def primary_source():
    """Тот же управляемый источник, но под кодом основного.

    Нужен там, где проверяется отчёт: покрытие и календарь считаются
    по основному источнику, поэтому подставной код тут не подходит.
    """
    from fxwatch.sources import PRIMARY_SOURCE, REGISTRY

    scripted = ScriptedSource()
    scripted.code = PRIMARY_SOURCE
    scripted.reported_date = date.today()
    original = REGISTRY[PRIMARY_SOURCE]
    REGISTRY[PRIMARY_SOURCE] = scripted
    try:
        yield scripted
    finally:
        REGISTRY[PRIMARY_SOURCE] = original


def _fetch_one(sql: str, **params):
    from fxwatch.db import session_scope

    with session_scope() as session:
        return session.execute(text(sql), params).one()


def _scalar(sql: str, **params):
    from fxwatch.db import session_scope

    with session_scope() as session:
        return session.execute(text(sql), params).scalar()


# --------------------------------------------------------------------------
# Требование 2 и 4: идемпотентность, версии, откаты
# --------------------------------------------------------------------------

def test_repeat_ingest_creates_no_duplicates(source):
    """Повторный запуск за ту же дату не плодит строк - это и есть идемпотентность."""
    from fxwatch.ingest import ingest_date

    first = ingest_date(source.code, DAY, job="test")
    second = ingest_date(source.code, DAY, job="test")

    assert first.status == "ok"
    assert first.rows_inserted == 1
    assert second.status == "ok"
    assert second.rows_inserted == 0
    assert _scalar("SELECT count(*) FROM observations") == 1
    assert _scalar("SELECT count(*) FROM revisions") == 0


def test_repeat_ingest_moves_confirmation_time(source):
    """Повтор не создаёт версии, но фиксирует, что источник подтвердил значение."""
    from fxwatch.ingest import ingest_date

    ingest_date(source.code, DAY, job="test")
    row = _fetch_one("SELECT observed_at, last_seen_at FROM observations")
    assert row.observed_at == row.last_seen_at

    ingest_date(source.code, DAY, job="test")
    row = _fetch_one("SELECT observed_at, last_seen_at FROM observations")
    assert row.last_seen_at > row.observed_at


def test_changed_value_is_stored_as_new_version(source):
    """Источник изменил значение за собранную дату - появляется версия и ревизия."""
    from fxwatch.ingest import ingest_date

    ingest_date(source.code, DAY, job="test")
    source.values["USD/RUB"] = "79.5"
    outcome = ingest_date(source.code, DAY, job="test")

    assert outcome.rows_inserted == 1
    assert outcome.rows_revised == 1
    assert _scalar("SELECT count(*) FROM observations") == 2
    assert _scalar("SELECT value_num FROM rates_current WHERE series_key = 'USD/RUB'") == Decimal("79.50000000")

    revision = _fetch_one("SELECT old_value, new_value, is_rollback FROM revisions")
    assert revision.old_value == Decimal("78.50000000")
    assert revision.new_value == Decimal("79.50000000")
    assert revision.is_rollback is False


def test_rollback_to_previous_value_is_detected(source):
    """A -> B -> A: источник вернулся к прежнему значению.

    Новой строки наблюдения при этом не появляется - такая версия уже есть, -
    поэтому без отметки подтверждения витрина навсегда осталась бы на B,
    а третье изменение источника не попало бы в историю ревизий вовсе.
    """
    from fxwatch.ingest import ingest_date

    ingest_date(source.code, DAY, job="test")  # A
    source.values["USD/RUB"] = "79.5"
    ingest_date(source.code, DAY, job="test")  # B
    source.values["USD/RUB"] = "78.5"
    outcome = ingest_date(source.code, DAY, job="test")  # снова A

    assert outcome.rows_inserted == 0, "версия уже существует, новой строки быть не должно"
    assert outcome.rows_revised == 1, "но смена текущего значения - это ревизия"
    assert _scalar("SELECT count(*) FROM observations") == 2

    current = _scalar("SELECT value_num FROM rates_current WHERE series_key = 'USD/RUB'")
    assert current == Decimal("78.50000000"), "витрина обязана показывать то, что источник отдаёт сейчас"

    last = _fetch_one(
        "SELECT old_value, new_value, is_rollback FROM revisions ORDER BY id DESC LIMIT 1"
    )
    assert last.old_value == Decimal("79.50000000")
    assert last.new_value == Decimal("78.50000000")
    assert last.is_rollback is True


def test_as_of_reconstructs_past_knowledge(source):
    """Состояние на прошлый момент считается по времени первого наблюдения."""
    from fxwatch.api import get_series
    from fxwatch.db import SessionLocal
    from fxwatch.ingest import ingest_date

    ingest_date(source.code, DAY, job="test")
    boundary = datetime.now(UTC)
    source.values["USD/RUB"] = "79.5"
    ingest_date(source.code, DAY, job="test")

    session = SessionLocal()
    try:
        now_view = get_series(code="USD/RUB", days=30, as_of=None, source=source.code, session=session)
        past_view = get_series(code="USD/RUB", days=30, as_of=boundary, source=source.code, session=session)
    finally:
        session.close()

    assert [p["value"] for p in now_view["points"]] == [79.5]
    assert [p["value"] for p in past_view["points"]] == [78.5]
    assert len(now_view["revisions"]) == 1


# --------------------------------------------------------------------------
# Требование 3: журнал не расходится с данными
# --------------------------------------------------------------------------

def test_failed_write_leaves_no_successful_run(source):
    """Если запись данных упала, прогон не может остаться успешным.

    Иначе день попал бы в календарь как «нерабочий»: успешная попытка есть,
    наблюдений нет - и окно перезапроса никогда бы его не добрало.
    """
    from fxwatch.ingest import ingest_date

    source.values = {"X" * 40 + "/RUB": "78.5"}  # длиннее колонки series_key

    with pytest.raises(DBAPIError):
        ingest_date(source.code, DAY, job="test")

    assert _scalar("SELECT count(*) FROM observations") == 0
    assert _scalar("SELECT count(*) FROM ingest_runs WHERE status IN ('ok', 'partial')") == 0


def test_calendar_separates_holidays_from_gaps(source):
    """День без данных, но с успешной попыткой, - нерабочий; неопрошенный - дыра."""
    from fxwatch.calendar import DayKind, classify_days, missing_days
    from fxwatch.db import session_scope
    from fxwatch.ingest import ingest_date

    ingest_date(source.code, DAY, job="test")
    # Источник отвечает, но данных за эту дату не отдаёт: так выглядит выходной.
    source.values = {}
    source.reported_date = None
    source.min_expected_series = 0
    ingest_date(source.code, DAY + timedelta(days=1), job="test")

    with session_scope() as session:
        kinds = classify_days(session, DAY, DAY + timedelta(days=2), source.code)
        gaps = missing_days(session, DAY, DAY + timedelta(days=2), source.code)

    assert kinds[DAY] == DayKind.BUSINESS
    assert kinds[DAY + timedelta(days=1)] == DayKind.NON_BUSINESS
    assert gaps == [DAY + timedelta(days=2)]


# --------------------------------------------------------------------------
# Требование 5: карантин и проверки
# --------------------------------------------------------------------------

def test_quarantine_merges_repeated_problem(source):
    """Одно битое значение - одна запись в карантине, а не по строке за прогон."""
    from fxwatch.ingest import ingest_date

    source.values = {"USD/RUB": "78.5", "BAD/RUB": "-1"}
    first = ingest_date(source.code, DAY, job="test")
    second = ingest_date(source.code, DAY, job="test")

    assert first.status == "partial"
    assert second.status == "partial"
    assert _scalar("SELECT count(*) FROM quarantine") == 1
    assert _scalar("SELECT seen_count FROM quarantine") == 2
    assert _scalar("SELECT count(*) FROM observations") == 1, "годное значение сохраняется"


def test_verdict_does_not_ignore_open_alerts(primary_source):
    """Вердикт отчёта обязан смотреть на весь отчёт, а не на его часть.

    Период - один сегодняшний день: так покрытие честно равно ста процентам,
    и единственное, что отличает два вердикта, - открытый алерт.
    """
    from fxwatch.alerting import raise_alert
    from fxwatch.db import session_scope
    from fxwatch.ingest import ingest_date
    from fxwatch.quality import rules
    from fxwatch.reporting import build_report

    today = date.today()
    ingest_date(primary_source.code, today, job="test")

    with session_scope() as session:
        clean = build_report(session, today, today)
        assert clean.coverage["coverage_pct"] == 100.0
        assert "вмешательство не требуется" in clean.verdict

        raise_alert(
            session, key="test:open", severity=rules.ERROR,
            title="Условие не вернулось в норму", body="проверка вердикта",
        )

    with session_scope() as session:
        report = build_report(session, today, today)

    assert "Требует внимания" in report.verdict
    assert "открытых алертов: 1" in report.verdict


# --------------------------------------------------------------------------
# Схема
# --------------------------------------------------------------------------

def test_migrations_match_models(migrated_database):
    """Модели и миграции не разъехались.

    Ровно та проверка, которой не было, пока начальная миграция создавала схему
    из ``metadata.create_all``: там расхождение невозможно по построению,
    поэтому и не находилось.
    """
    from alembic import command
    from alembic.config import Config
    from alembic.util.exc import AutogenerateDiffsDetected

    try:
        command.check(Config(ALEMBIC_INI))
    except AutogenerateDiffsDetected as exc:  # pragma: no cover - диагностика
        pytest.fail(f"схема базы разошлась с моделями: {exc}")
