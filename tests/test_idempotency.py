"""Отпечаток значения: механизм, на котором держатся требования 2 и 4 ТЗ.

Повторный сбор за ту же дату не должен плодить дубли, а изменение значения
источником должно сохраняться как новая версия. Обе задачи решает один
уникальный индекс по ``(источник, ряд, дата, хэш)``, поэтому его поведение
проверяется отдельно от базы.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from fxwatch.sources.base import DataPoint


def _point(value: str, day: str = "2026-07-15", series: str = "USD/RUB", nominal: int = 1) -> DataPoint:
    return DataPoint(
        series_key=series, value_date=date.fromisoformat(day), value=Decimal(value), nominal=nominal
    )


def test_same_value_produces_same_hash():
    """Повторный запрос с тем же ответом отбрасывается уникальным индексом."""
    assert _point("78.5012").payload_hash() == _point("78.5012").payload_hash()


def test_trailing_zeros_do_not_create_false_revision():
    """78.5 и 78.50 - одно и то же значение.

    Без нормализации форматирование источника выглядело бы как ревизия,
    и в отчёте копились бы несуществующие изменения.
    """
    assert _point("78.5").payload_hash() == _point("78.50000000").payload_hash()


def test_changed_value_produces_new_hash():
    """Источник изменил значение за ту же дату - это новая версия, а не дубль."""
    assert _point("78.5012").payload_hash() != _point("78.5013").payload_hash()


def test_tiny_change_is_still_a_new_version():
    """Различие в восьмом знаке фиксируется как версия.

    Отдельный вопрос - считать ли его значимым: этим занимается порог
    ``revision_epsilon_pct`` при записи ревизии, а не хэш.
    """
    assert _point("78.50120000").payload_hash() != _point("78.50120001").payload_hash()


def test_same_value_on_different_dates_is_not_a_duplicate():
    assert _point("78.5012", day="2026-07-15").payload_hash() != _point(
        "78.5012", day="2026-07-16"
    ).payload_hash()


def test_nominal_change_is_a_new_version():
    """Смена номинала при том же курсе меняет смысл данных."""
    assert _point("0.52311", nominal=100).payload_hash() != _point("0.52311", nominal=10).payload_hash()
