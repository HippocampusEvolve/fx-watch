"""Заглушка-пример: как в этот же конвейер подключается маркетплейс.

Модуль не подключён к планировщику и не ходит в сеть. Он показывает объём работы,
который нужен для нового источника: реализовать ``fetch`` и вернуть список
``DataPoint``. Всё остальное - повторы, идемпотентность, история ревизий,
проверки качества, карантин, журнал прогонов, отчётность и дашборд -
переиспользуется без изменений.

Что в реальной интеграции добавится поверх этого файла:

* авторизация: ``Client-Id`` и ``Api-Key`` в заголовках, ключи из окружения;
* пагинация: у Ozon это курсор ``last_id``, у WB - смещение по дате;
* лимиты: у обоих маркетплейсов ограничение на число запросов в минуту,
  поэтому между страницами нужна пауза, а 429 уже обрабатывается в
  :class:`fxwatch.http.RetryingClient`;
* отчёты формируются асинхронно: запрос создаёт задачу, готовый файл забирается
  отдельным вызовом - это отдельный ``job`` в планировщике, а не один ``fetch``.

Смысл примера в том, что тема данных для сервиса не важна: «курс валюты на дату»
и «цена товара на дату» - одна и та же структура, а значит и требования ТЗ
(история изменений, ревизии, контроль качества) закрываются для них одинаково.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from fxwatch.sources.base import DataPoint, FetchResult, Source


class MarketplaceStubSource(Source):
    code = "marketplace_demo"
    title = "Пример: цены и остатки товаров (не активен)"
    supports_history = True

    #: В реальном источнике это ответ API, здесь - фиксированный набор,
    #: чтобы пример оставался запускаемым и не требовал ключей.
    SAMPLE = {"SKU-1001": Decimal("1290.00"), "SKU-1002": Decimal("2450.00")}

    def fetch(self, target_date: date) -> FetchResult:
        points = [
            DataPoint(
                series_key=f"price/{sku}",
                value_date=target_date,
                value=price,
                nominal=1,
                raw_value=str(price),
            )
            for sku, price in self.SAMPLE.items()
        ]
        return FetchResult(
            points=points,
            raw_body=b'{"demo": true}',
            http_status=200,
            content_type="application/json",
            reported_date=target_date,
        )
