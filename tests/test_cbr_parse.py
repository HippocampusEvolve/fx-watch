"""Разбор ответа ЦБ, включая случаи, ради которых всё и строилось."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from fxwatch.sources.base import SourceParseError
from fxwatch.sources.cbr import CbrSource


def test_parses_normal_response(fixture_bytes):
    points, reported_date, quarantine = CbrSource.parse(fixture_bytes("cbr_ok.xml"))

    assert reported_date == date(2026, 7, 15)
    assert quarantine == []
    assert len(points) == 6

    by_key = {p.series_key: p for p in points}
    assert by_key["USD/RUB"].value == Decimal("78.5012")
    assert by_key["EUR/RUB"].value == Decimal("91.234")


def test_normalizes_by_nominal(fixture_bytes):
    """Иена котируется за 100 единиц: в историю должен попасть курс за одну.

    Без нормализации ряд JPY отличался бы от остальных на два порядка,
    и проверка диапазона считала бы это нормой.
    """
    points, _, _ = CbrSource.parse(fixture_bytes("cbr_ok.xml"))
    jpy = next(p for p in points if p.series_key == "JPY/RUB")

    assert jpy.nominal == 100
    assert jpy.value == Decimal("0.52311")


def test_weekend_response_reports_previous_business_day(fixture_bytes):
    """За выходной ЦБ отдаёт курс предыдущего рабочего дня и указывает его дату.

    Это основа календаря: значения должны лечь под датой из ответа,
    а не под запрошенной, иначе в истории появятся фиктивные выходные дни.
    """
    points, reported_date, _ = CbrSource.parse(fixture_bytes("cbr_weekend.xml"))

    assert reported_date == date(2026, 7, 17)
    assert all(p.value_date == date(2026, 7, 17) for p in points)


def test_broken_body_raises_parse_error(fixture_bytes):
    """HTML-заглушка вместо XML: источник ответил, но данных нет."""
    with pytest.raises(SourceParseError):
        CbrSource.parse(fixture_bytes("cbr_broken.html"))


def test_empty_response_raises(fixture_bytes):
    with pytest.raises(SourceParseError, match="нет ни одной валюты"):
        CbrSource.parse(fixture_bytes("cbr_empty.xml"))


def test_bad_rows_go_to_quarantine_without_losing_good_ones(fixture_bytes):
    """Одна сломанная валюта не должна отменять остальные.

    Полный откат прогона означал бы потерю корректных данных из-за чужой ошибки
    в одной строке - прямо противоположное требованию «не терять данные».
    """
    points, _, quarantine = CbrSource.parse(fixture_bytes("cbr_bad_value.xml"))

    assert [p.series_key for p in points] == ["CNY/RUB", "AUD/RUB"]
    quarantined_codes = {code for code, _ in quarantine}
    assert quarantined_codes == {"USD", "EUR"}


def test_unknown_root_element_rejected():
    with pytest.raises(SourceParseError, match="ValCurs"):
        CbrSource.parse(b"<?xml version='1.0'?><SomethingElse/>")
