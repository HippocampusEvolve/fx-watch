"""Реестр источников данных."""

from __future__ import annotations

from fxwatch.sources.base import DataPoint, FetchResult, Source, SourceError, SourceParseError
from fxwatch.sources.cbr import CbrSource
from fxwatch.sources.erapi import ErApiSource

#: Активные источники. Заглушка маркетплейса сюда намеренно не включена:
#: она существует как пример расширения, а не как рабочий источник.
REGISTRY: dict[str, Source] = {
    CbrSource.code: CbrSource(),
    ErApiSource.code: ErApiSource(),
}

#: Источник, по которому строится календарь рабочих дней и основная витрина.
PRIMARY_SOURCE = CbrSource.code

__all__ = [
    "REGISTRY",
    "PRIMARY_SOURCE",
    "DataPoint",
    "FetchResult",
    "Source",
    "SourceError",
    "SourceParseError",
    "CbrSource",
    "ErApiSource",
]


def get_source(code: str) -> Source:
    try:
        return REGISTRY[code]
    except KeyError:
        raise KeyError(f"неизвестный источник: {code!r}") from None
