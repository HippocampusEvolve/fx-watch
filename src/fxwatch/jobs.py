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

Отметка живости ставится за факт отработки задачи, а не за факт похода в сеть.
Разница принципиальная: страховочный опрос в штатный день намеренно не ходит
в источник, и если считать живостью только сетевой запрос, то отметка протухнет
как раз тогда, когда всё хорошо, а ``/health`` начнёт вечно отдавать
``degraded``. Мониторинг, который врёт в штатном режиме, перестают читать.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from fxwatch.alerting import AlertOutbox, ping_watchdog, raise_alert, resolve_alert
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

#: Сколько последних дней просматривать в поисках даты, за которую есть
#: значения обоих источников. Второй источник истории не отдаёт, поэтому
#: сравнивать можно только то, что сервис успел увидеть сам.
CROSS_SOURCE_LOOKBACK_DAYS = 14


def job_poll(source_code: str = PRIMARY_SOURCE) -> RunOutcome:
    """Опросить источник за сегодняшнюю дату."""
    today = datetime.now(get_settings().zone).date()
    outcome = ingest_date(source_code, today, job="poll")
    log.info(
        "poll %s за %s: %s (получено %s, записано %s, ревизий %s)",
        source_code, today, outcome.status, outcome.rows_fetched, outcome.rows_inserted, outcome.rows_revised,
    )
    return outcome


def job_sweep(source_code: str = PRIMARY_SOURCE) -> list[RunOutcome]:
    """Перезапросить скользящее окно и добрать дыры.

    Здесь закрываются сразу три требования:
    источник мог исправить значение задним числом (п. 4), сервис мог простаивать
    и что-то пропустить (п. 3), а даты старше окна считаются закрытыми, и их
    изменение уже помечается как поздняя ревизия (п. 4).

    Ревизии ищутся только внутри окна, а дыры - на всю глубину истории
    (``bootstrap_days``). Разница намеренная: перезапрашивать значения глубже
    окна каждый день - это сотни лишних запросов ради ревизий, которых у ЦБ
    практически не бывает, а вот дыра от простоя длиннее окна без глубокого
    поиска осталась бы навсегда - «сервис не теряет данные» перестало бы
    быть правдой на пятнадцатый день простоя.
    """
    settings = get_settings()
    today = datetime.now(settings.zone).date()
    window_start = today - timedelta(days=settings.sweep_days)
    horizon_start = today - timedelta(days=settings.bootstrap_days)

    with session_scope() as session:
        gaps = missing_days(session, horizon_start, today, source_code)

    targets = sorted({window_start + timedelta(days=i) for i in range(settings.sweep_days + 1)} | set(gaps))
    outcomes: list[RunOutcome] = []
    for target in targets:
        outcomes.append(ingest_date(source_code, target, job="sweep"))
        time.sleep(BACKFILL_DELAY_SEC)

    revised = sum(o.rows_revised for o in outcomes)
    log.info("sweep %s: %s дат, ревизий %s", source_code, len(targets), revised)
    touch_heartbeat(f"sweep:{source_code}", 36 * 3600)
    if any(o.status in ("ok", "partial") for o in outcomes):
        ping_watchdog(f"sweep:{source_code}")
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
    """Проверки, которым нужна вся накопленная история, а не один ответ.

    Застой проверяется по всем рядам источника, а не по трём главным: «USD жив,
    значит всё живо» - предположение, которое ничем не обосновано, а состав
    выгрузки ЦБ меняется. Сверка с независимым источником возможна только там,
    где второй источник вообще что-то отдаёт, - это три рублёвых ряда.
    """
    settings = get_settings()
    today = datetime.now(settings.zone).date()
    results: list[rules.CheckResult] = []
    outbox = AlertOutbox()

    with session_scope() as session:
        results.append(rules.check_freshness(session, source_code))

        all_series = [
            row[0]
            for row in session.execute(
                text(
                    """
                    SELECT series_key FROM observations
                    WHERE source_code = :src GROUP BY series_key ORDER BY series_key
                    """
                ),
                {"src": source_code},
            )
        ]
        business = rules.stale_window(session, source_code, today)
        for series in all_series:
            results.append(rules.check_stale_series(session, source_code, series, today, business_days=business))

        # Сверка идёт по последней дате, за которую есть оба источника: у второго
        # источника нет истории, а у ЦБ нет курса на воскресенье и понедельник,
        # поэтому привязка к «сегодня» оставляла бы проверку вечно пропущенной.
        since = today - timedelta(days=CROSS_SOURCE_LOOKBACK_DAYS)
        for series in rules.CROSS_CHECKED_SERIES:
            comparable = rules.latest_comparable_date(session, series, since)
            if comparable is None:
                results.append(
                    rules.CheckResult(
                        "cross_source", rules.WARN, rules.SKIP,
                        f"за последние {CROSS_SOURCE_LOOKBACK_DAYS} дней нет даты, "
                        "которую отдали оба источника",
                        series_key=series,
                    )
                )
                continue
            results.append(rules.check_cross_source(session, series, comparable))

        _record_state_checks(session, source_code, results)

        for check in results:
            key = f"check:{source_code}:{check.check_name}:{check.series_key or '-'}"
            if check.status == rules.FAIL:
                raise_alert(
                    session, key=key, severity=check.severity,
                    title=f"{rules.CHECK_TITLES.get(check.check_name, check.check_name)}: проверка не пройдена",
                    body=check.message,
                    outbox=outbox,
                )
            elif check.status == rules.PASS:
                resolve_alert(session, key)

    outbox.flush()
    failed = [c for c in results if c.status == rules.FAIL]
    log.info("проверки состояния: %s всего, не пройдено %s", len(results), len(failed))
    touch_heartbeat("state_checks", 36 * 3600)
    ping_watchdog("state_checks")
    return results


def _record_state_checks(session: Session, source_code: str, results: list[rules.CheckResult]) -> None:
    """Сохранить результаты проверок состояния.

    Рядов у ЦБ полсотни, и писать по строке на каждый пройденный застой
    четыре раза в день значило бы утопить отчёт в записях «всё хорошо».
    Провалы и пропуски сохраняются поимённо - по ним разбираются; успехи
    по рядам сворачиваются в одну строку с числом проверенных рядов.
    """
    aggregated: list[rules.CheckResult] = []
    for check in results:
        if check.check_name == "stale_series" and check.status == rules.PASS:
            aggregated.append(check)
            continue
        session.add(
            QualityCheck(
                run_id=0, source_code=source_code, check_name=check.check_name,
                severity=check.severity, status=check.status, series_key=check.series_key,
                value_date=check.value_date, observed=check.observed,
                expected=check.expected, message=check.message,
            )
        )

    if aggregated:
        session.add(
            QualityCheck(
                run_id=0, source_code=source_code, check_name="stale_series",
                severity=rules.WARN, status=rules.PASS,
                observed=f"{len(aggregated)} рядов",
                message=f"значения менялись, проверено рядов: {len(aggregated)}",
            )
        )


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
    touch_heartbeat("retention", 48 * 3600)
    return deleted or 0


def touch_heartbeat(name: str, expected_interval_sec: int) -> None:
    """Отметить, что регулярная задача отработала."""
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
