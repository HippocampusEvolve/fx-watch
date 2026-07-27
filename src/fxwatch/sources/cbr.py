"""Официальные курсы валют Банка России.

Эндпоинт: ``https://www.cbr.ru/scripts/XML_daily.asp?date_req=DD/MM/YYYY``
Ключ не нужен, ограничений на частоту нет, есть архив за прошлые даты -
поэтому историю за три месяца можно залить сразу, а не ждать три месяца.

Важная особенность, на которой держатся сразу две проверки: если запросить
нерабочий день, ЦБ вернёт курсы предыдущего рабочего дня и укажет в атрибуте
``Date`` именно его. Расхождение запрошенной и возвращённой даты - это не ошибка,
а информация: так сервис узнаёт календарь рабочих дней прямо из данных
(см. :mod:`fxwatch.calendar`).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from xml.etree import ElementTree

from fxwatch.http import RetryingClient
from fxwatch.sources.base import DataPoint, FetchResult, Source, SourceParseError

log = logging.getLogger(__name__)

CBR_URL = "https://www.cbr.ru/scripts/XML_daily.asp"


class CbrSource(Source):
    code = "cbr"
    title = "Банк России, официальные курсы"
    supports_history = True
    #: ЦБ штатно публикует около 44 валют. Порог с запасом вниз: состав списка
    #: изредка меняется, но падение до трети означает поломку, а не пересмотр.
    min_expected_series = 30

    def __init__(self, client: RetryingClient | None = None) -> None:
        self._client = client or RetryingClient()

    def fetch(self, target_date: date) -> FetchResult:
        response = self._client.get(CBR_URL, params={"date_req": target_date.strftime("%d/%m/%Y")})
        points, reported_date, quarantine = self.parse(response.content)
        return FetchResult(
            points=points,
            raw_body=response.content,
            http_status=response.status_code,
            content_type=response.content_type,
            attempts=response.attempts,
            reported_date=reported_date,
            quarantine=quarantine,
        )

    @staticmethod
    def parse(body: bytes) -> tuple[list[DataPoint], date | None, list[tuple[str, str]]]:
        """Разобрать XML ЦБ.

        Возвращает точки, дату из ответа и список записей, которые не удалось
        разобрать. Битая валюта не роняет остальные 43: она уходит в карантин,
        прогон получает статус ``partial``.
        """
        try:
            root = ElementTree.fromstring(body)
        except ElementTree.ParseError as exc:
            raise SourceParseError(f"ответ не является корректным XML: {exc}") from exc

        if root.tag != "ValCurs":
            raise SourceParseError(f"неожиданный корневой элемент: {root.tag!r}, ожидался ValCurs")

        reported_date: date | None = None
        raw_date = root.attrib.get("Date")
        if raw_date:
            try:
                reported_date = datetime.strptime(raw_date, "%d.%m.%Y").date()
            except ValueError:
                raise SourceParseError(f"нераспознанная дата в ответе: {raw_date!r}") from None

        points: list[DataPoint] = []
        quarantine: list[tuple[str, str]] = []

        for node in root.findall("Valute"):
            char_code = (node.findtext("CharCode") or "").strip().upper()
            raw_value = (node.findtext("Value") or "").strip()
            raw_unit = (node.findtext("VunitRate") or "").strip()
            raw_nominal = (node.findtext("Nominal") or "1").strip()

            if not char_code:
                quarantine.append((node.attrib.get("ID", "?"), "отсутствует CharCode"))
                continue

            try:
                nominal = int(raw_nominal.replace(" ", "") or "1")
            except ValueError:
                quarantine.append((char_code, f"нечисловой Nominal: {raw_nominal!r}"))
                continue
            if nominal <= 0:
                quarantine.append((char_code, f"недопустимый Nominal: {nominal}"))
                continue

            # VunitRate - курс за одну единицу, уже нормализованный самим ЦБ.
            # Если его нет (старые выгрузки), считаем сами из Value и Nominal.
            source_value = raw_unit or raw_value
            try:
                value = Decimal(source_value.replace(",", ".").replace(" ", ""))
            except (InvalidOperation, ValueError):
                quarantine.append((char_code, f"нечисловое значение: {source_value!r}"))
                continue

            if not raw_unit:
                value = value / Decimal(nominal)

            points.append(
                DataPoint(
                    series_key=f"{char_code}/RUB",
                    value_date=reported_date or date.today(),
                    value=value,
                    nominal=nominal,
                    raw_value=source_value,
                )
            )

        if not points and not quarantine:
            raise SourceParseError("в ответе нет ни одной валюты")

        return points, reported_date, quarantine
