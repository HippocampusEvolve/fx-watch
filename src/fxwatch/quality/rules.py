"""Проверки качества данных.

Все правила собраны в одном файле намеренно: список того, что сервис считает
«нормальными данными», должен читаться целиком, а не собираться по коду.

Правила разложены на три уровня по тому, что им нужно для работы:

* **ответ**   - виден только HTTP-ответ (пусто, не тот формат, не та дата);
* **точка**   - видна одна величина и её ближайшая история (диапазон, скачок);
* **состояние** - видна вся накопленная история (свежесть, застой, сверка источников).

Реакция на провал зависит от уровня серьёзности:

* ``info``  - записываем факт, ничего не делаем;
* ``warn``  - записываем и показываем в отчёте, но данные принимаем;
* ``error`` - запись уходит в карантин и в витрину не попадает, поднимается алерт.

Провал по одной валюте не отменяет остальные: прогон получает статус ``partial``,
43 корректных курса сохраняются. Полный откат здесь означал бы терять данные
из-за одной сломанной строки, а требование ТЗ прямо обратное.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.orm import Session

from fxwatch.calendar import DayKind, classify_days
from fxwatch.config import get_settings
from fxwatch.sources.base import DataPoint, FetchResult

INFO, WARN, ERROR = "info", "warn", "error"
PASS, FAIL, SKIP = "pass", "fail", "skip"


@dataclass(slots=True)
class CheckResult:
    check_name: str
    severity: str
    status: str
    message: str | None = None
    series_key: str | None = None
    value_date: date | None = None
    observed: str | None = None
    expected: str | None = None

    @property
    def is_blocking(self) -> bool:
        """Провал такого уровня отправляет запись в карантин."""
        return self.status == FAIL and self.severity == ERROR


# --------------------------------------------------------------------------
# Уровень 1. Ответ источника
# --------------------------------------------------------------------------

def check_response_payload(result: FetchResult) -> CheckResult:
    """Пустой или подозрительно короткий ответ.

    Формально успешный HTTP 200 с пустым телом - самый частый способ источника
    «сломаться тихо», поэтому проверяем размер отдельно от кода ответа.
    """
    size = len(result.raw_body)
    if size < 100:
        return CheckResult(
            "response_payload", ERROR, FAIL,
            f"тело ответа {size} байт, это не похоже на данные",
            observed=str(size), expected=">=100",
        )
    return CheckResult("response_payload", ERROR, PASS, observed=str(size))


def check_completeness(result: FetchResult, expected_min: int) -> CheckResult:
    """Полнота выгрузки.

    ЦБ штатно отдаёт около 54 валют. Если пришло три - ответ технически корректен,
    но данные неполные, и молча принять их значит испортить историю.

    Порог приходит от источника, а не из общей настройки: у разных источников
    норма разная, и единый порог отмечал бы штатный ответ одного из них как сбой.
    """
    minimum = expected_min
    count = len(result.points)
    if count < minimum:
        return CheckResult(
            "completeness", ERROR, FAIL,
            f"получено {count} рядов вместо ожидаемых минимум {minimum}",
            observed=str(count), expected=f">={minimum}",
        )
    return CheckResult("completeness", ERROR, PASS, observed=str(count), expected=f">={minimum}")


def check_reported_date(result: FetchResult, requested: date) -> CheckResult:
    """Соответствие даты в ответе запрошенной.

    У ЦБ несовпадение - штатная ситуация: за выходной день отдаётся курс
    предыдущего рабочего дня. Поэтому это не ошибка, а информация, которую
    сервис использует для построения календаря. Ошибкой было бы обратное:
    молча записать полученные значения под запрошенной датой и получить
    дубли выходного дня в истории.
    """
    if result.reported_date is None:
        return CheckResult("reported_date", WARN, FAIL, "источник не указал дату в ответе")
    if result.reported_date == requested:
        return CheckResult("reported_date", INFO, PASS, observed=result.reported_date.isoformat())
    return CheckResult(
        "reported_date", INFO, PASS,
        f"запрошено {requested.isoformat()}, источник вернул {result.reported_date.isoformat()}: "
        f"запрошенная дата нерабочая",
        observed=result.reported_date.isoformat(), expected=requested.isoformat(),
    )


# --------------------------------------------------------------------------
# Уровень 2. Отдельное значение
# --------------------------------------------------------------------------

def check_value_range(point: DataPoint) -> CheckResult:
    """Значение физически возможно."""
    if point.value <= 0:
        return CheckResult(
            "value_range", ERROR, FAIL, f"неположительное значение {point.value}",
            series_key=point.series_key, value_date=point.value_date,
            observed=str(point.value), expected="> 0",
        )
    if point.value > Decimal("1000000"):
        return CheckResult(
            "value_range", ERROR, FAIL, f"значение вне разумного диапазона: {point.value}",
            series_key=point.series_key, value_date=point.value_date,
            observed=str(point.value), expected="<= 1000000",
        )
    if point.nominal <= 0:
        return CheckResult(
            "value_range", ERROR, FAIL, f"недопустимый номинал {point.nominal}",
            series_key=point.series_key, value_date=point.value_date,
        )
    return CheckResult("value_range", ERROR, PASS, series_key=point.series_key, value_date=point.value_date)


def check_jump(point: DataPoint, previous: Decimal | None) -> CheckResult:
    """Скачок относительно предыдущего известного значения ряда.

    Пороги разведены: умеренное движение курса - рыночная реальность и повод
    посмотреть, а не повод выбросить данные; кратное изменение почти всегда
    означает сбой на стороне источника (потерянный номинал, сдвиг разряда).
    """
    settings = get_settings()
    if previous is None or previous == 0:
        return CheckResult(
            "jump", WARN, SKIP, "нет предыдущего значения для сравнения",
            series_key=point.series_key, value_date=point.value_date,
        )

    delta_pct = abs((point.value - previous) / previous * Decimal(100))
    observed = f"{delta_pct:.2f}%"

    if delta_pct >= Decimal(str(settings.jump_error_pct)):
        return CheckResult(
            "jump", ERROR, FAIL,
            f"изменение на {observed} относительно {previous}, похоже на ошибку источника",
            series_key=point.series_key, value_date=point.value_date,
            observed=observed, expected=f"< {settings.jump_error_pct}%",
        )
    if delta_pct >= Decimal(str(settings.jump_warn_pct)):
        return CheckResult(
            "jump", WARN, FAIL,
            f"резкое изменение на {observed} относительно {previous}",
            series_key=point.series_key, value_date=point.value_date,
            observed=observed, expected=f"< {settings.jump_warn_pct}%",
        )
    return CheckResult(
        "jump", WARN, PASS, series_key=point.series_key, value_date=point.value_date, observed=observed
    )


# --------------------------------------------------------------------------
# Уровень 3. Накопленное состояние
# --------------------------------------------------------------------------

def evaluate_freshness(
    *,
    hours_since_success: float | None,
    days_since_last_value: int | None,
    hours_limit: float,
    gap_limit_days: int,
) -> CheckResult:
    """Данные вообще продолжают поступать. Чистая логика без базы.

    «Перестали поступать» - это два разных сбоя, и меряются они разными линейками:

    * сервис давно не мог успешно сходить в источник - умер планировщик, пропала
      сеть, залип предохранитель. Здесь работает порог в часах;
    * походы успешны, но новых дат не появляется. Здесь порог в часах не работает:
      ЦБ не устанавливает курс на воскресенье и понедельник, и штатная тишина
      с субботы до вторника - 72 часа, а новогодняя - до 11 дней. Мерить возраст
      последней вставки часами значило бы получать ложную тревогу каждые выходные.
      Поэтому порог - календарные дни без единой новой даты, с запасом на самый
      длинный известный перерыв в публикациях источника.
    """
    if hours_since_success is None:
        return CheckResult("freshness", ERROR, FAIL, "по источнику нет ни одного успешного обращения")
    if days_since_last_value is None:
        return CheckResult("freshness", ERROR, FAIL, "по источнику нет ни одного наблюдения")

    observed = f"успех {hours_since_success:.1f} ч назад, свежая дата {days_since_last_value} дн назад"

    if hours_since_success > hours_limit:
        return CheckResult(
            "freshness", ERROR, FAIL,
            f"успешных обращений к источнику не было {hours_since_success:.1f} ч",
            observed=observed, expected=f"успех не старше {hours_limit:.0f} ч",
        )
    if days_since_last_value > gap_limit_days:
        return CheckResult(
            "freshness", ERROR, FAIL,
            f"обращения успешны, но новых дат нет уже {days_since_last_value} дн - "
            f"источник застыл или молчит дольше любого известного перерыва",
            observed=observed, expected=f"новая дата не реже раза в {gap_limit_days} дн",
        )
    return CheckResult(
        "freshness", ERROR, PASS,
        observed=observed,
        expected=f"успех <= {hours_limit:.0f} ч, новая дата <= {gap_limit_days} дн",
    )


def check_freshness(session: Session, source_code: str, now: datetime | None = None) -> CheckResult:
    """Обёртка над :func:`evaluate_freshness`: достаёт из базы оба возраста."""
    settings = get_settings()
    now = now or datetime.now(UTC)

    last_success = session.execute(
        text(
            """
            SELECT max(started_at) FROM ingest_runs
            WHERE source_code = :src AND status IN ('ok', 'partial')
            """
        ),
        {"src": source_code},
    ).scalar()
    last_value_date = session.execute(
        text("SELECT max(value_date) FROM observations WHERE source_code = :src"),
        {"src": source_code},
    ).scalar()

    return evaluate_freshness(
        hours_since_success=(
            (now - last_success).total_seconds() / 3600 if last_success else None
        ),
        days_since_last_value=(
            (now.astimezone(settings.zone).date() - last_value_date).days
            if last_value_date
            else None
        ),
        hours_limit=settings.freshness_hours,
        gap_limit_days=settings.freshness_max_gap_days,
    )


def check_stale_series(session: Session, source_code: str, series_key: str, today: date) -> CheckResult:
    """Значение перестало меняться.

    Считаем только по рабочим дням календаря: в выходные курс ЦБ не меняется
    по определению, и без этой поправки проверка давала бы ложную тревогу
    каждую субботу. Именно поэтому календарь выводится из данных, а не
    берётся из константы.
    """
    settings = get_settings()
    window_start = today - timedelta(days=settings.stale_business_days * 3 + 7)
    kinds = classify_days(session, window_start, today, source_code)
    business = sorted(day for day, kind in kinds.items() if kind == DayKind.BUSINESS)

    if len(business) < settings.stale_business_days:
        return CheckResult(
            "stale_series", WARN, SKIP, "недостаточно рабочих дней в окне",
            series_key=series_key,
        )

    recent = business[-settings.stale_business_days:]
    rows = session.execute(
        text(
            """
            SELECT DISTINCT ON (value_date) value_date, value_num
            FROM observations
            WHERE source_code = :src AND series_key = :series AND value_date = ANY(:dates)
            ORDER BY value_date, observed_at DESC
            """
        ),
        {"src": source_code, "series": series_key, "dates": recent},
    ).all()

    if len(rows) < settings.stale_business_days:
        return CheckResult(
            "stale_series", WARN, SKIP, "не за все рабочие дни окна есть данные",
            series_key=series_key,
        )

    values = {row[1] for row in rows}
    if len(values) == 1:
        return CheckResult(
            "stale_series", WARN, FAIL,
            f"значение не менялось {settings.stale_business_days} рабочих дня подряд: {rows[0][1]}",
            series_key=series_key, value_date=recent[-1],
            observed=str(rows[0][1]), expected="значение должно меняться",
        )
    return CheckResult("stale_series", WARN, PASS, series_key=series_key, value_date=recent[-1])


def check_cross_source(session: Session, series_key: str, value_date: date) -> CheckResult:
    """Сверка двух независимых источников по одному ряду.

    Ловит класс ошибок, невидимый изнутри одного источника: технически валидный,
    но неверный на порядок курс. Если второго значения за дату нет - проверка
    пропускается, а не засчитывается пройденной.
    """
    settings = get_settings()
    rows = session.execute(
        text(
            """
            SELECT DISTINCT ON (source_code) source_code, value_num
            FROM observations
            WHERE series_key = :series AND value_date = :day
            ORDER BY source_code, observed_at DESC
            """
        ),
        {"series": series_key, "day": value_date},
    ).all()

    values = {row[0]: row[1] for row in rows}
    if len(values) < 2:
        return CheckResult(
            "cross_source", WARN, SKIP,
            "второго источника за эту дату нет, сверять не с чем",
            series_key=series_key, value_date=value_date,
        )

    primary = values.get("cbr")
    secondary = next((v for k, v in values.items() if k != "cbr"), None)
    if primary is None or secondary is None or primary == 0:
        return CheckResult("cross_source", WARN, SKIP, series_key=series_key, value_date=value_date)

    delta_pct = abs((primary - secondary) / primary * Decimal(100))
    observed = f"{delta_pct:.2f}%"
    if delta_pct > Decimal(str(settings.cross_source_warn_pct)):
        return CheckResult(
            "cross_source", WARN, FAIL,
            f"источники расходятся на {observed}: {primary} против {secondary}",
            series_key=series_key, value_date=value_date,
            observed=observed, expected=f"<= {settings.cross_source_warn_pct}%",
        )
    return CheckResult(
        "cross_source", WARN, PASS, series_key=series_key, value_date=value_date, observed=observed
    )


#: Человекочитаемые названия для отчёта и дашборда.
CHECK_TITLES = {
    "response_payload": "Ответ не пустой",
    "completeness": "Полнота выгрузки",
    "reported_date": "Дата в ответе",
    "value_range": "Значение в допустимом диапазоне",
    "jump": "Скачок относительно вчера",
    "freshness": "Данные продолжают поступать",
    "stale_series": "Значения не застыли",
    "cross_source": "Сверка с независимым источником",
}
