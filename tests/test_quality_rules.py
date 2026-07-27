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


# --- уровень состояния: свежесть ------------------------------------------

def test_freshness_weekend_silence_is_not_an_alarm():
    """ЦБ не устанавливает курс на воскресенье и понедельник: 72 часа тишины - норма.

    Порог в часах от последней вставки давал бы ложную тревогу каждые выходные,
    а мониторинг, который врёт раз в неделю, перестают читать. Поэтому возраст
    данных меряется днями без новой даты, а часами - только возраст успеха.
    """
    check = rules.evaluate_freshness(
        hours_since_success=5.0, days_since_last_value=2, hours_limit=36, gap_limit_days=12
    )

    assert check.status == rules.PASS


def test_freshness_survives_long_holidays():
    """Новогодний перерыв в публикациях ЦБ - до 11 дней. Это ещё не тревога."""
    check = rules.evaluate_freshness(
        hours_since_success=6.0, days_since_last_value=11, hours_limit=36, gap_limit_days=12
    )

    assert check.status == rules.PASS


def test_freshness_fails_when_service_stopped_polling():
    """Успешных походов в источник давно не было - умер планировщик или сеть."""
    check = rules.evaluate_freshness(
        hours_since_success=40.0, days_since_last_value=1, hours_limit=36, gap_limit_days=12
    )

    assert check.status == rules.FAIL
    assert "успешных обращений" in (check.message or "")


def test_freshness_fails_when_source_frozen_despite_successful_polls():
    """Источник отвечает, но новых дат нет дольше любого известного перерыва."""
    check = rules.evaluate_freshness(
        hours_since_success=3.0, days_since_last_value=13, hours_limit=36, gap_limit_days=12
    )

    assert check.status == rules.FAIL
    assert "застыл" in (check.message or "")


def test_freshness_without_any_data_fails():
    check = rules.evaluate_freshness(
        hours_since_success=None, days_since_last_value=None, hours_limit=36, gap_limit_days=12
    )

    assert check.status == rules.FAIL
