"""Регулярные задачи сервиса.

Разделение на несколько задач вместо одной - это ответ на требование 1 ТЗ.
Один интервал не подходит, потому что задачи решают разное:

* ``poll``   - забрать сегодняшний курс как можно раньше после публикации;
* ``sweep``  - перезапросить последние две недели: догнать пропущенное и увидеть
  ревизии;
* ``checks`` - посмотреть на накопленную историю целиком (свежесть, застой, сверка);
* ``retention`` - не дать базе расти бесконтрольно;
* ``heartbeat`` - оставить след, что сервис вообще жив.

Обоснование частоты каждой задачи - в README, раздел «Требование 1».
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import text

from fxwatch.alerting import raise_alert, resolve_alert
from fxwatch.calendar import missing_days
from fxwatch.config import get_settings
from fxwatch.db import session_scope
from fxwatch.ingest import RunOutcome, ingest_date
from fxwatch.models import Heartbeat, QualityCheck
from fxwatch.quality import rules
from fxwatch.sources import PRIMARY_SOURCE, REGISTRY

log = logging.getLogger(__name__)

#: Пауза между запросами при массовой заливке. Ограничений у ЦБ нет,
#: но пара сотен запросов подряд без пауз - это неуважение к чужому сервису.
BACKFILL_DELAY_SEC = 0.7


def job_poll(source_code: str = PRIMARY_SOURCE) -> RunOutcome:
    """Опросить источник за сегодняшнюю дату."""
    today = datetime.now(get_settings().zone).date()
    outcome = ingest_date(source_code, today, job="poll")
    log.info(
        "poll %s за %s: %s (получено %s, записано %s, ревизий %s)",
        source_code, today, outcome.status, outcome.rows_fetched, outcome.rows_inserted, outcome.rows_revised,
    )
    _touch_heartbeat(f"poll:{source_code}", 6 * 3600)
    return outcome


def job_sweep(source_code: str = PRIMARY_SOURCE) -> list[RunOutcome]:
    """Перезапросить скользящее окно и добрать дыры.

    Здесь закрываются сразу три требования:
    источник мог исправить значение задним числом (п. 4), сервис мог простаивать
    и что-то пропустить (п. 3), а даты старше окна считаются закрытыми, и их
    изменение уже помечается как поздняя ревизия (п. 4).
    """
    settings = get_settings()
    today = datetime.now(settings.zone).date()
    window_start = today - timedelta(days=settings.sweep_days)

    with session_scope() as session:
        gaps = missing_days(session, window_start, today, source_code)

    targets = sorted({window_start + timedelta(days=i) for i in range(settings.sweep_days + 1)} | set(gaps))
    outcomes: list[RunOutcome] = []
    for target in targets:
        outcomes.append(ingest_date(source_code, target, job="sweep"))
        time.sleep(BACKFILL_DELAY_SEC)

    revised = sum(o.rows_revised for o in outcomes)
    log.info("sweep %s: %s дат, ревизий %s", source_code, len(targets), revised)
    _touch_heartbeat(f"sweep:{source_code}", 36 * 3600)
    return outcomes


def job_backfill(days: int, source_code: str = PRIMARY_SOURCE, until: date | None = None) -> list[RunOutcome]:
    """Залить историю за последние N дней.

    Нужен, чтобы не ждать три месяца ради проверки, что сервис умеет копить
    историю: архив ЦБ доступен по датам, и та же самая логика сбора
    отрабатывает на прошлых днях без единого исключения в коде.
    """
    end = until or datetime.now(get_settings().zone).date()
    outcomes: list[RunOutcome] = []
    for offset in range(days, -1, -1):
        target = end - timedelta(days=offset)
        outcomes.append(ingest_date(source_code, target, job="backfill"))
        time.sleep(BACKFILL_DELAY_SEC)
    log.info("backfill %s: обработано %s дат", source_code, len(outcomes))
    return outcomes


def job_state_checks(source_code: str = PRIMARY_SOURCE) -> list[rules.CheckResult]:
    """Проверки, которым нужна вся накопленная история, а не один ответ."""
    settings = get_settings()
    today = datetime.now(settings.zone).date()
    results: list[rules.CheckResult] = []

    with session_scope() as session:
        freshness = rules.check_freshness(session, source_code)
        results.append(freshness)

        watched = [
            row[0]
            for row in session.execute(
                text(
                    """
                    SELECT series_key FROM observations
                    WHERE source_code = :src AND series_key IN ('USD/RUB', 'EUR/RUB', 'CNY/RUB')
                    GROUP BY series_key
                    """
                ),
                {"src": source_code},
            )
        ]
        for series in watched:
            results.append(rules.check_stale_series(session, source_code, series, today))
            results.append(rules.check_cross_source(session, series, today))

        for check in results:
            session.add(
                QualityCheck(
                    run_id=0, source_code=source_code, check_name=check.check_name,
                    severity=check.severity, status=check.status, series_key=check.series_key,
                    value_date=check.value_date, observed=check.observed,
                    expected=check.expected, message=check.message,
                )
            )

        for check in results:
            key = f"check:{source_code}:{check.check_name}:{check.series_key or '-'}"
            if check.status == rules.FAIL:
                raise_alert(
                    session, key=key, severity=check.severity,
                    title=f"{rules.CHECK_TITLES.get(check.check_name, check.check_name)}: проверка не пройдена",
                    body=check.message,
                )
            elif check.status == rules.PASS:
                resolve_alert(session, key)

    failed = [c for c in results if c.status == rules.FAIL]
    log.info("проверки состояния: %s всего, не пройдено %s", len(results), len(failed))
    _touch_heartbeat("state_checks", 36 * 3600)
    return results


def job_retention() -> int:
    """Удалить сырые ответы старше срока хранения.

    Наблюдения и журнал прогонов не трогаем никогда: они и есть история,
    ради которой всё построено. Сырые тела - это отладочный материал,
    и хранить их три месяца незачем.
    """
    settings = get_settings()
    cutoff = datetime.now(UTC) - timedelta(days=settings.raw_retention_days)
    with session_scope() as session:
        deleted = session.execute(
            text("DELETE FROM raw_payloads WHERE fetched_at < :cutoff"), {"cutoff": cutoff}
        ).rowcount
    log.info("retention: удалено сырых ответов %s", deleted)
    _touch_heartbeat("retention", 48 * 3600)
    return deleted or 0


def _touch_heartbeat(name: str, expected_interval_sec: int) -> None:
    with session_scope() as session:
        beat = session.get(Heartbeat, name)
        if beat is None:
            beat = Heartbeat(name=name, expected_interval_sec=expected_interval_sec)
            session.add(beat)
        beat.expected_interval_sec = expected_interval_sec
        beat.last_ok_at = datetime.now(UTC)


def bootstrap_if_empty() -> int:
    """Первичная заливка истории, если база пуста.

    Так стенд, поднятый с нуля, сразу показывает осмысленную историю,
    а не пустой график в ожидании первого рабочего дня.
    """
    settings = get_settings()
    with session_scope() as session:
        count = session.execute(text("SELECT count(*) FROM observations")).scalar() or 0
    if count:
        return 0
    log.info("база пуста, заливаю историю за %s дней", settings.bootstrap_days)
    outcomes = job_backfill(settings.bootstrap_days)
    for source_code in REGISTRY:
        if source_code == PRIMARY_SOURCE:
            continue
        try:
            ingest_date(source_code, datetime.now(settings.zone).date(), job="backfill")
        except Exception as exc:  # noqa: BLE001
            log.warning("дополнительный источник %s недоступен при заливке: %s", source_code, exc)
    return sum(o.rows_inserted for o in outcomes)
