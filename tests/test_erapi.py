"""Второй источник и его ограничения."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from fxwatch.sources.base import SourceError, SourceParseError
from fxwatch.sources.erapi import ErApiSource

STAMP = int(datetime(2026, 7, 15, 0, 2, tzinfo=UTC).timestamp())


def _body(rates: dict[str, object], result: str = "success") -> bytes:
    return json.dumps(
        {"result": result, "base_code": "USD", "time_last_update_unix": STAMP, "rates": rates}
    ).encode()


def test_parses_tracked_currencies():
    """Ряды пересчитываются в рублёвые кросс-курсы.

    Сверка с ЦБ возможна только на одинаковых ключах рядов, а у ЦБ все ряды -
    к рублю. Хранить «EUR за доллар» было бы данными, которые не с чем сравнить.
    """
    points, reported_date, quarantine = ErApiSource.parse(_body({"RUB": 78.9, "EUR": 0.86, "CNY": 7.1}))

    assert reported_date == date(2026, 7, 15)
    assert quarantine == []
    assert {p.series_key for p in points} == {"USD/RUB", "EUR/RUB", "CNY/RUB"}
    assert next(p for p in points if p.series_key == "USD/RUB").value == Decimal("78.9")
    eur = next(p for p in points if p.series_key == "EUR/RUB")
    assert eur.value == (Decimal("78.9") / Decimal("0.86")).quantize(Decimal("0.00000001"))


def test_missing_currency_goes_to_quarantine():
    points, _, quarantine = ErApiSource.parse(_body({"RUB": 78.9}))

    assert [p.series_key for p in points] == ["USD/RUB"]
    assert {code for code, _ in quarantine} == {"EUR/RUB", "CNY/RUB"}


def test_missing_rub_rejects_whole_response():
    """Без курса рубля пересчитывать нечего: это не одна битая строка, а битый ответ."""
    with pytest.raises(SourceParseError, match="RUB"):
        ErApiSource.parse(_body({"EUR": 0.86, "CNY": 7.1}))


def test_error_result_is_rejected():
    with pytest.raises(SourceParseError, match="result="):
        ErApiSource.parse(_body({"RUB": 78.9}, result="error"))


def test_non_json_is_rejected():
    with pytest.raises(SourceParseError, match="JSON"):
        ErApiSource.parse(b"<html>502 Bad Gateway</html>")


def test_historical_request_is_refused_explicitly():
    """Источник не умеет отдавать прошлые даты, и это должно быть видно явно.

    Молча вернуть сегодняшнее значение под вчерашней датой означало бы
    придумать данные, которых у источника нет. Допуск в один день оставлен
    сознательно - на стык таймзон планировщика (МСК) и источника (UTC).
    """
    with pytest.raises(SourceError, match="только текущее состояние"):
        ErApiSource().fetch(date(2020, 1, 1))
