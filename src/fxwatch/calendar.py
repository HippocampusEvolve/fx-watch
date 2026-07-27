"""Календарь рабочих дней, выведенный из самих данных.

Проверка «данные застыли» бесполезна без календаря: курс ЦБ не меняется в
выходные и праздники, и наивный детектор застоя будет срабатывать каждую субботу.

Готовый производственный календарь РФ сюда не тащим. Внешняя библиотека - это
зависимость, которая устаревает к каждому новому году и к каждому переносу
выходных, и её всё равно пришлось бы обновлять руками. Вместо этого календарь
выводится из поведения источника:

* за дату есть наблюдения                      -> рабочий день;
* дату успешно запрашивали, наблюдений нет     -> нерабочий день;
* ни того, ни другого                          -> дыра, её надо забрать.

Третий случай - это ровно то, что нужно для догона пропущенного после простоя
(требование 3 ТЗ): сервис знает, каких дат он ещё не видел.

На холодном старте, пока истории нет, работает грубое приближение
«суббота и воскресенье нерабочие». После первичной заливки за 90 дней
календарь становится фактическим.
"""

from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from fxwatch.sources import PRIMARY_SOURCE


class DayKind:
    BUSINESS = "business"
    NON_BUSINESS = "non_business"
    UNKNOWN = "unknown"


def classify_days(session: Session, start: date, end: date, source_code: str = PRIMARY_SOURCE) -> dict[date, str]:
    """Разметить каждый день интервала одним из трёх состояний."""
    observed = {
        row[0]
        for row in session.execute(
            text(
                """
                SELECT DISTINCT value_date
                FROM observations
                WHERE source_code = :src AND value_date BETWEEN :start AND :end
                """
            ),
            {"src": source_code, "start": start, "end": end},
        )
    }
    probed = {
        row[0]
        for row in session.execute(
            text(
                """
                SELECT DISTINCT target_date
                FROM ingest_runs
                WHERE source_code = :src
                  AND target_date BETWEEN :start AND :end
                  AND status IN ('ok', 'partial')
                """
            ),
            {"src": source_code, "start": start, "end": end},
        )
        if row[0] is not None
    }

    result: dict[date, str] = {}
    cursor = start
    while cursor <= end:
        if cursor in observed:
            result[cursor] = DayKind.BUSINESS
        elif cursor in probed:
            result[cursor] = DayKind.NON_BUSINESS
        else:
            result[cursor] = DayKind.UNKNOWN
        cursor += timedelta(days=1)
    return result


def missing_days(session: Session, start: date, end: date, source_code: str = PRIMARY_SOURCE) -> list[date]:
    """Даты, которые сервис ещё ни разу успешно не запрашивал.

    Именно этот список закрывает дыры в истории: после любого простоя
    (упал контейнер, лежал источник, не было сети) сервис видит, чего он не знает,
    и добирает это на ближайшем проходе.
    """
    kinds = classify_days(session, start, end, source_code)
    return sorted(day for day, kind in kinds.items() if kind == DayKind.UNKNOWN)


def business_days(session: Session, start: date, end: date, source_code: str = PRIMARY_SOURCE) -> list[date]:
    kinds = classify_days(session, start, end, source_code)
    return sorted(day for day, kind in kinds.items() if kind == DayKind.BUSINESS)


def is_probably_business_day(day: date) -> bool:
    """Грубое приближение для холодного старта, пока истории ещё нет."""
    return day.weekday() < 5


def expected_business_days(session: Session, start: date, end: date) -> int:
    """Сколько рабочих дней ожидалось в интервале.

    Знаменатель для метрики покрытия: если история пуста, опираемся на
    приближение по дням недели, иначе на фактический календарь источника.
    """
    kinds = classify_days(session, start, end)
    known = sum(1 for kind in kinds.values() if kind != DayKind.UNKNOWN)
    if known == 0:
        cursor, count = start, 0
        while cursor <= end:
            count += int(is_probably_business_day(cursor))
            cursor += timedelta(days=1)
        return count

    unknown_days = [day for day, kind in kinds.items() if kind == DayKind.UNKNOWN]
    return sum(1 for kind in kinds.values() if kind == DayKind.BUSINESS) + sum(
        1 for day in unknown_days if is_probably_business_day(day)
    )
