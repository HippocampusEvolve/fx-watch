"""Правила качества данных (требование 5 ТЗ)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from fxwatch.quality import rules
from fxwatch.sources.base import DataPoint, FetchResult


def _point(value: str, series: str = "USD/RUB", nominal: int = 1) -> DataPoint:
    return DataPoint(
        series_key=series, value_date=date(2026, 7, 15), value=Decimal(value), nominal=nominal
    )


def _result(points: list[DataPoint], body: bytes = b"x" * 500, reported: date | None = date(2026, 7, 15)):
    return FetchResult(points=points, raw_body=body, http_status=200, reported_date=reported)


# --- уровень ответа -------------------------------------------------------

def test_empty_body_is_blocking():
    check = rules.check_response_payload(_result([], body=b""))

    assert check.status == rules.FAIL
    assert check.is_blocking


def test_incomplete_export_is_blocking():
    """Технически валидный ответ с тремя валютами вместо сорока - неполные данные."""
    check = rules.check_completeness(_result([_point("78.5")] * 3), expected_min=30)

    assert check.status == rules.FAIL
    assert check.is_blocking


def test_full_export_passes():
    check = rules.check_completeness(_result([_point("78.5")] * 44), expected_min=30)

    assert check.status == rules.PASS


def test_completeness_threshold_belongs_to_the_source():
    """Три ряда - поломка для ЦБ и норма для источника сверки.

    Порог задаётся источником, поэтому один и тот же ответ оценивается
    по-разному в зависимости от того, кто его отдал. Общая настройка
    помечала бы штатный ответ второго источника как сбой.
    """
    response = _result([_point("78.5")] * 3)

    assert rules.check_completeness(response, expected_min=30).status == rules.FAIL
    assert rules.check_completeness(response, expected_min=3).status == rules.PASS


def test_date_mismatch_is_information_not_error():
    """Несовпадение дат у ЦБ - признак нерабочего дня, а не сбоя."""
    check = rules.check_reported_date(_result([], reported=date(2026, 7, 12)), date(2026, 7, 13))

    assert check.status == rules.PASS
    assert check.severity == rules.INFO
    assert "нерабочая" in (check.message or "")


def test_missing_date_is_warning():
    check = rules.check_reported_date(_result([], reported=None), date(2026, 7, 15))

    assert check.status == rules.FAIL
    assert not check.is_blocking  # предупреждение, данные принимаем


# --- уровень значения -----------------------------------------------------

def test_negative_value_is_blocking():
    check = rules.check_value_range(_point("-10.98"))

    assert check.is_blocking


def test_zero_nominal_is_blocking():
    check = rules.check_value_range(_point("91.23", nominal=0))

    assert check.is_blocking


def test_reasonable_value_passes():
    assert rules.check_value_range(_point("78.5012")).status == rules.PASS


def test_moderate_move_is_warning_and_data_is_kept():
    """Курс может двинуться на 12% - это рынок, а не ошибка. Данные принимаем."""
    check = rules.check_jump(_point("88.0"), previous=Decimal("78.5"))

    assert check.status == rules.FAIL
    assert check.severity == rules.WARN
    assert not check.is_blocking


def test_order_of_magnitude_jump_is_blocking():
    """Курс, изменившийся в десять раз, почти всегда означает сбой источника."""
    check = rules.check_jump(_point("785.0"), previous=Decimal("78.5"))

    assert check.is_blocking


def test_jump_without_history_is_skipped_not_passed():
    """Нет предыдущего значения - проверка пропущена.

    Отметить её пройденной было бы неправдой: за галочкой не стоит сравнение.
    """
    check = rules.check_jump(_point("78.5"), previous=None)

    assert check.status == rules.SKIP


def test_small_move_passes():
    assert rules.check_jump(_point("79.0"), previous=Decimal("78.5")).status == rules.PASS
