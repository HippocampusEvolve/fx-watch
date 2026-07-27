"""Второй, независимый источник рыночных курсов - open.er-api.com.

Нужен не ради данных, а ради контроля: два независимых источника позволяют
поймать класс ошибок, который не ловится изнутри одного (например, если ЦБ
однажды отдаст технически валидный, но неправильный на порядок курс).

Источник отдаёт курсы к доллару, поэтому ряды пересчитываются в рублёвые
кросс-курсы (EUR/RUB = RUB-за-доллар / EUR-за-доллар): сверка с ЦБ имеет смысл
только на одинаковых ключах рядов, а у ЦБ все ряды - к рублю.

Ограничение осознанное: бесплатный тариф отдаёт только текущее состояние,
истории нет. Поэтому кросс-проверка работает начиная с даты запуска сервиса,
а на забэкфилленной истории она помечается как ``skip``, а не как провал.
Пропуск проверки честнее, чем зелёная галочка, за которой ничего не стоит.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation

from fxwatch.http import RetryingClient
from fxwatch.sources.base import DataPoint, FetchResult, Source, SourceError, SourceParseError

ERAPI_URL = "https://open.er-api.com/v6/latest/USD"
#: Валюты, по которым строятся рублёвые ряды для сверки с ЦБ.
TRACKED = ("USD", "EUR", "CNY")
_PRECISION = Decimal("0.00000001")


class ErApiSource(Source):
    code = "erapi"
    title = "open.er-api.com, рыночные курсы"
    supports_history = False
    #: Из этого источника берём только ряды для сверки, поэтому норма - ровно
    #: столько рядов, сколько мы отслеживаем.
    min_expected_series = len(TRACKED)

    def __init__(self, client: RetryingClient | None = None) -> None:
        self._client = client or RetryingClient()

    def fetch(self, target_date: date) -> FetchResult:
        # Допуск в один день - на стык часовых поясов: планировщик живёт по МСК,
        # источник обновляется в полночь UTC. Всё, что дальше, - настоящий запрос
        # истории, которой у источника нет, и молчать об этом нельзя.
        today = datetime.now(UTC).date()
        if abs((target_date - today).days) > 1:
            raise SourceError(
                f"источник отдаёт только текущее состояние, запрошено {target_date.isoformat()}"
            )

        response = self._client.get(ERAPI_URL)
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
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise SourceParseError(f"ответ не является корректным JSON: {exc}") from exc

        if payload.get("result") != "success":
            raise SourceParseError(f"источник вернул result={payload.get('result')!r}")

        rates = payload.get("rates")
        if not isinstance(rates, dict) or not rates:
            raise SourceParseError("в ответе нет блока rates")

        stamp = payload.get("time_last_update_unix")
        reported_date = (
            datetime.fromtimestamp(int(stamp), tz=UTC).date()
            if isinstance(stamp, (int, float))
            else datetime.now(UTC).date()
        )

        try:
            rub_per_usd = Decimal(str(rates["RUB"]))
        except KeyError:
            raise SourceParseError("в ответе нет курса RUB, пересчитать в рубли нечего") from None
        except (InvalidOperation, ValueError):
            raise SourceParseError(f"нечисловой курс RUB: {rates.get('RUB')!r}") from None
        if rub_per_usd <= 0:
            raise SourceParseError(f"недопустимый курс RUB: {rub_per_usd}")

        points: list[DataPoint] = []
        quarantine: list[tuple[str, str]] = []
        for code in TRACKED:
            series_key = f"{code}/RUB"
            if code == "USD":
                value, raw = rub_per_usd, str(rates["RUB"])
            else:
                if code not in rates:
                    quarantine.append((series_key, "валюта отсутствует в ответе"))
                    continue
                try:
                    per_usd = Decimal(str(rates[code]))
                except (InvalidOperation, ValueError):
                    quarantine.append((series_key, f"нечисловое значение: {rates[code]!r}"))
                    continue
                if per_usd <= 0:
                    quarantine.append((series_key, f"недопустимое значение: {per_usd}"))
                    continue
                value = (rub_per_usd / per_usd).quantize(_PRECISION)
                raw = f"RUB/{code} = {rates['RUB']}/{rates[code]}"
            points.append(
                DataPoint(
                    series_key=series_key,
                    value_date=reported_date,
                    value=value,
                    nominal=1,
                    raw_value=raw,
                )
            )

        return points, reported_date, quarantine
