"""Общий контракт источника данных.

Сервис ничего не знает про конкретные API: он умеет «сходить в источник за датой D
и получить набор точек». Всё, что специфично для ЦБ или для маркетплейса,
живёт внутри своей реализации ``Source``.

Добавление нового источника - это один класс с двумя методами; остальной конвейер
(retry, идемпотентность, ревизии, проверки качества, отчётность) переиспользуется
как есть. Пример-заглушка: :mod:`fxwatch.sources.marketplace_stub`.
"""

from __future__ import annotations

import gzip
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal


@dataclass(slots=True)
class DataPoint:
    """Одна точка данных, уже приведённая к общему виду."""

    series_key: str  # «USD/RUB»
    value_date: date  # к какой дате относится значение
    value: Decimal  # курс за одну единицу валюты
    nominal: int = 1
    raw_value: str | None = None  # исходная строка, нужна для карантина

    def payload_hash(self) -> str:
        """Отпечаток содержимого.

        Именно он отличает «источник повторил то же самое» от «источник изменил
        значение»: одинаковый хэш отбрасывается уникальным индексом, другой хэш
        ложится новой версией.
        """
        normalized = f"{self.series_key}|{self.value_date.isoformat()}|{self.value:.8f}|{self.nominal}"
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class FetchResult:
    """Результат одного обращения к источнику."""

    points: list[DataPoint]
    raw_body: bytes
    http_status: int
    content_type: str | None = None
    attempts: int = 1
    # Дата, которую источник фактически вернул. У ЦБ она может отличаться от
    # запрошенной: за выходной день отдаётся курс предыдущего рабочего дня.
    reported_date: date | None = None
    quarantine: list[tuple[str, str]] = field(default_factory=list)  # (описание, причина)

    @property
    def raw_sha256(self) -> str:
        return hashlib.sha256(self.raw_body).hexdigest()

    def raw_gzip(self) -> bytes:
        return gzip.compress(self.raw_body)


class SourceError(Exception):
    """Ошибка получения данных, из-за которой прогон считается неуспешным."""


class SourceParseError(SourceError):
    """Ответ получен, но разобрать его не удалось. Ретраить бессмысленно."""


class Source(ABC):
    """Базовый источник."""

    code: str
    title: str
    supports_history: bool = False  # умеет ли отдавать данные за прошлую дату

    #: Сколько рядов источник обязан отдать в норме. Это свойство конкретного
    #: источника, а не общая настройка: ЦБ публикует около 54 валют, и три
    #: ряда в ответе означают поломку, а второй источник по замыслу отдаёт
    #: ровно три, и для него это норма. Общий порог давал бы ложную тревогу.
    min_expected_series: int = 1

    @abstractmethod
    def fetch(self, target_date: date) -> FetchResult:
        """Забрать данные за указанную дату."""

    def describe(self) -> dict[str, object]:
        return {
            "code": self.code,
            "title": self.title,
            "supports_history": self.supports_history,
            "min_expected_series": self.min_expected_series,
        }
