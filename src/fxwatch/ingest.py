"""Сбор данных: один проход по одному источнику за одну дату.

Здесь сходятся требования 2, 3, 4 и 5 ТЗ, поэтому порядок шагов важен:

1. открыть запись прогона отдельной транзакцией - чтобы факт попытки сохранился
   даже если дальше всё упадёт;
2. проверить предохранитель - если источник признан недоступным, прогон
   помечается ``skipped``, но остаётся в журнале;
3. сходить в источник (повторы и таймауты - в :mod:`fxwatch.http`);
4. сохранить сырой ответ, чтобы парсинг можно было переиграть задним числом;
5. проверить ответ целиком, затем каждое значение по отдельности;
6. записать пригодные значения, непригодные - в карантин;
7. сравнить с тем, что уже известно, и зафиксировать ревизии;
8. закрыть прогон итоговым статусом.

Прогон никогда не завершается «наполовину»: даже при исключении статус будет
записан, поэтому в журнале не остаётся висящих ``running``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from fxwatch import breaker
from fxwatch.alerting import raise_alert, resolve_alert
from fxwatch.config import get_settings
from fxwatch.db import session_scope
from fxwatch.models import IngestRun, Observation, QualityCheck, Quarantine, RawPayload, Revision
from fxwatch.quality import rules
from fxwatch.quality.rules import CheckResult
from fxwatch.sources import get_source
from fxwatch.sources.base import DataPoint, FetchResult, SourceError

log = logging.getLogger(__name__)


@dataclass(slots=True)
class RunOutcome:
    run_id: int
    source_code: str
    target_date: date
    status: str
    rows_fetched: int = 0
    rows_inserted: int = 0
    rows_revised: int = 0
    rows_quarantined: int = 0
    checks: list[CheckResult] = field(default_factory=list)
    error: str | None = None


# --------------------------------------------------------------------------
# Журнал прогона
# --------------------------------------------------------------------------

def _open_run(source_code: str, target_date: date | None, job: str) -> int:
    """Открыть прогон отдельной транзакцией: попытка должна остаться в журнале."""
    with session_scope() as session:
        run = IngestRun(
            source_code=source_code,
            job=job,
            target_date=target_date,
            status="running",
            code_version=get_settings().code_version,
        )
        session.add(run)
        session.flush()
        return run.id


def _close_run(run_id: int, **fields: object) -> None:
    with session_scope() as session:
        run = session.get(IngestRun, run_id)
        if run is None:
            return
        for key, value in fields.items():
            setattr(run, key, value)
        if run.finished_at is None:
            run.finished_at = datetime.now(UTC)
        if run.started_at and run.finished_at and run.duration_ms is None:
            run.duration_ms = int((run.finished_at - run.started_at).total_seconds() * 1000)


def reap_stale_runs(max_age_minutes: int = 60) -> int:
    """Пометить зависшие прогоны неуспешными.

    Если контейнер убили в момент сбора, запись осталась бы в статусе ``running``
    навсегда и портила бы статистику за три месяца. Реапер вызывается при старте
    и по расписанию.
    """
    cutoff = datetime.now(UTC) - timedelta(minutes=max_age_minutes)
    with session_scope() as session:
        result = session.execute(
            text(
                """
                UPDATE ingest_runs
                SET status = 'failed',
                    finished_at = now(),
                    error_class = 'InterruptedRun',
                    error_message = 'прогон не завершился: сервис был остановлен'
                WHERE status = 'running' AND started_at < :cutoff
                """
            ),
            {"cutoff": cutoff},
        )
        return result.rowcount or 0


# --------------------------------------------------------------------------
# Вспомогательные выборки
# --------------------------------------------------------------------------

def _current_values(session: Session, source_code: str, value_date: date) -> dict[str, tuple[Decimal, datetime]]:
    """Последние известные значения за дату: основа детекции ревизий."""
    rows = session.execute(
        text(
            """
            SELECT DISTINCT ON (series_key) series_key, value_num, observed_at
            FROM observations
            WHERE source_code = :src AND value_date = :day
            ORDER BY series_key, observed_at DESC, id DESC
            """
        ),
        {"src": source_code, "day": value_date},
    ).all()
    return {row[0]: (row[1], row[2]) for row in rows}


def _previous_values(session: Session, source_code: str, before: date) -> dict[str, Decimal]:
    """Последнее значение каждого ряда до указанной даты - для проверки скачка."""
    rows = session.execute(
        text(
            """
            SELECT DISTINCT ON (series_key) series_key, value_num
            FROM observations
            WHERE source_code = :src AND value_date < :day
            ORDER BY series_key, value_date DESC, observed_at DESC
            """
        ),
        {"src": source_code, "day": before},
    ).all()
    return {row[0]: row[1] for row in rows}


def _first_observed_at(session: Session, source_code: str, series_key: str, value_date: date) -> datetime:
    stamp = session.execute(
        text(
            """
            SELECT min(observed_at) FROM observations
            WHERE source_code = :src AND series_key = :series AND value_date = :day
            """
        ),
        {"src": source_code, "series": series_key, "day": value_date},
    ).scalar()
    return stamp or datetime.now(UTC)


# --------------------------------------------------------------------------
# Основной проход
# --------------------------------------------------------------------------

def ingest_date(source_code: str, target_date: date, job: str = "poll") -> RunOutcome:
    """Забрать данные источника за дату и записать результат."""
    settings = get_settings()
    source = get_source(source_code)
    run_id = _open_run(source_code, target_date, job)

    # --- предохранитель ---------------------------------------------------
    with session_scope() as session:
        if breaker.is_open(session, source_code):
            until = breaker.opened_until(session, source_code)
            _close_run(
                run_id,
                status="skipped",
                error_class="CircuitOpen",
                error_message=f"источник признан недоступным до {until}",
            )
            return RunOutcome(run_id, source_code, target_date, "skipped", error="circuit open")

    # --- обращение к источнику -------------------------------------------
    try:
        result = source.fetch(target_date)
    except SourceError as exc:
        message = f"{type(exc).__name__}: {exc}"
        log.error("сбор %s за %s не удался: %s", source_code, target_date, message)
        with session_scope() as session:
            tripped = breaker.record_failure(session, source_code, message)
            if tripped:
                raise_alert(
                    session,
                    key=f"source_down:{source_code}",
                    severity=rules.ERROR,
                    title=f"Источник {source_code} недоступен",
                    body=f"{settings.breaker_failure_threshold} неудач подряд. Последняя ошибка: {message}",
                )
        _close_run(run_id, status="failed", error_class=type(exc).__name__, error_message=message)
        return RunOutcome(run_id, source_code, target_date, "failed", error=message)

    outcome = _persist(run_id, source_code, target_date, result, source.min_expected_series)

    with session_scope() as session:
        breaker.record_success(session, source_code)
        resolve_alert(session, f"source_down:{source_code}")

    return outcome


def _persist(
    run_id: int, source_code: str, target_date: date, result: FetchResult, min_expected_series: int
) -> RunOutcome:
    """Проверить и записать полученные данные."""
    settings = get_settings()
    checks: list[CheckResult] = []
    inserted = revised = quarantined = 0

    with session_scope() as session:
        session.add(
            RawPayload(
                run_id=run_id,
                source_code=source_code,
                content_gzip=result.raw_gzip(),
                content_type=result.content_type,
                byte_size=len(result.raw_body),
            )
        )

        # --- проверки уровня ответа --------------------------------------
        checks.append(rules.check_response_payload(result))
        checks.append(rules.check_completeness(result, min_expected_series))
        checks.append(rules.check_reported_date(result, target_date))

        blocking = [c for c in checks if c.is_blocking]
        if blocking:
            reason = "; ".join(c.message or c.check_name for c in blocking)
            for c in blocking:
                session.add(
                    Quarantine(
                        run_id=run_id, source_code=source_code, value_date=target_date,
                        raw_value=f"{len(result.raw_body)} байт", reason=reason, check_name=c.check_name,
                    )
                )
            _record_checks(session, run_id, source_code, checks)
            raise_alert(
                session,
                key=f"bad_response:{source_code}:{target_date.isoformat()}",
                severity=rules.ERROR,
                title=f"Некорректный ответ источника {source_code}",
                body=reason,
            )
            _close_run(
                run_id, status="failed", http_status=result.http_status,
                attempt_count=result.attempts, raw_sha256=result.raw_sha256,
                rows_fetched=len(result.points), rows_quarantined=len(blocking),
                error_class="QualityGate", error_message=reason,
            )
            return RunOutcome(
                run_id, source_code, target_date, "failed",
                rows_fetched=len(result.points), rows_quarantined=len(blocking),
                checks=checks, error=reason,
            )

        # Точки, не разобранные парсером, тоже фиксируем - они не должны исчезнуть.
        for label, reason in result.quarantine:
            session.add(
                Quarantine(
                    run_id=run_id, source_code=source_code, series_key=label[:32],
                    value_date=result.reported_date or target_date,
                    reason=reason, check_name="parse",
                )
            )
            quarantined += 1

        effective_date = result.reported_date or target_date
        previous = _previous_values(session, source_code, effective_date)
        current = _current_values(session, source_code, effective_date)

        accepted: list[DataPoint] = []
        for point in result.points:
            point_checks = [rules.check_value_range(point), rules.check_jump(point, previous.get(point.series_key))]
            checks.extend(point_checks)

            failed = [c for c in point_checks if c.is_blocking]
            if failed:
                reason = "; ".join(c.message or c.check_name for c in failed)
                session.add(
                    Quarantine(
                        run_id=run_id, source_code=source_code, series_key=point.series_key,
                        value_date=point.value_date, raw_value=point.raw_value,
                        reason=reason, check_name=failed[0].check_name,
                    )
                )
                quarantined += 1
                raise_alert(
                    session,
                    key=f"bad_value:{source_code}:{point.series_key}:{point.value_date.isoformat()}",
                    severity=rules.ERROR,
                    title=f"Отклонено значение {point.series_key} за {point.value_date.isoformat()}",
                    body=reason,
                )
                continue
            accepted.append(point)

        # --- запись наблюдений -------------------------------------------
        now = datetime.now(UTC)
        for point in accepted:
            stmt = (
                pg_insert(Observation)
                .values(
                    source_code=source_code,
                    series_key=point.series_key,
                    value_date=point.value_date,
                    value_num=point.value,
                    nominal=point.nominal,
                    observed_at=now,
                    run_id=run_id,
                    payload_hash=point.payload_hash(),
                )
                # Тот же хэш означает, что источник повторил уже известное значение:
                # молча пропускаем. Повторный запуск за ту же дату безопасен.
                .on_conflict_do_nothing(constraint="uq_observation_value")
                .returning(Observation.id)
            )
            new_id = session.execute(stmt).scalar()
            if new_id is None:
                continue
            inserted += 1

            known = current.get(point.series_key)
            if known is None or known[0] == point.value:
                continue

            # Значение за уже собранную дату изменилось - это ревизия источника.
            old_value = known[0]
            delta_pct = (
                abs((point.value - old_value) / old_value * Decimal(100)) if old_value else Decimal(0)
            )
            is_significant = delta_pct >= Decimal(str(settings.revision_epsilon_pct))
            is_late = (date.today() - point.value_date).days > settings.sweep_days
            session.add(
                Revision(
                    source_code=source_code, series_key=point.series_key, value_date=point.value_date,
                    old_value=old_value, new_value=point.value, delta_pct=delta_pct,
                    first_observed_at=_first_observed_at(session, source_code, point.series_key, point.value_date),
                    revised_at=now, run_id=run_id,
                    is_significant=is_significant, is_late=is_late,
                )
            )
            revised += 1

            if is_significant and is_late:
                raise_alert(
                    session,
                    key=f"late_revision:{source_code}:{point.series_key}:{point.value_date.isoformat()}",
                    severity=rules.WARN,
                    title=f"Поздняя ревизия {point.series_key} за {point.value_date.isoformat()}",
                    body=(
                        f"{old_value} заменено на {point.value} (расхождение {delta_pct:.2f}%). "
                        f"Дата вышла из окна перезапроса в {settings.sweep_days} дней."
                    ),
                )

        _record_checks(session, run_id, source_code, checks)

        status = "partial" if quarantined else "ok"
        _close_run(
            run_id, status=status, http_status=result.http_status, attempt_count=result.attempts,
            raw_sha256=result.raw_sha256, rows_fetched=len(result.points), rows_inserted=inserted,
            rows_revised=revised, rows_quarantined=quarantined,
        )

    return RunOutcome(
        run_id, source_code, target_date, status,
        rows_fetched=len(result.points), rows_inserted=inserted,
        rows_revised=revised, rows_quarantined=quarantined, checks=checks,
    )


#: Проверки, которые выполняются один раз на весь ответ.
RESPONSE_LEVEL_CHECKS = frozenset({"response_payload", "completeness", "reported_date"})


def _record_checks(session: Session, run_id: int, source_code: str, checks: list[CheckResult]) -> None:
    """Сохранить результаты проверок.

    Проверки уровня значения выполняются по сорок с лишним раз за прогон, поэтому
    писать каждый успех отдельной строкой значило бы за три месяца накопить
    миллионы записей «всё хорошо». Но и молчать о них нельзя: без успехов
    в отчёте видны только провалы, и проверка выглядит вечно не пройденной.

    Компромисс: провалы сохраняются поимённо, потому что по ним потом разбираются,
    а успехи и пропуски сворачиваются в одну строку на прогон с числом значений.
    Масштаб цифр в отчёте при этом одинаковый для всех проверок: одна запись
    на один проход.
    """
    aggregates: dict[tuple[str, str], list[CheckResult]] = {}

    for check in checks:
        if check.check_name in RESPONSE_LEVEL_CHECKS or check.status == rules.FAIL:
            session.add(
                QualityCheck(
                    run_id=run_id, source_code=source_code, check_name=check.check_name,
                    severity=check.severity, status=check.status, series_key=check.series_key,
                    value_date=check.value_date, observed=check.observed, expected=check.expected,
                    message=check.message,
                )
            )
            continue
        aggregates.setdefault((check.check_name, check.status), []).append(check)

    for (check_name, status), group in aggregates.items():
        sample = group[0]
        session.add(
            QualityCheck(
                run_id=run_id, source_code=source_code, check_name=check_name,
                severity=sample.severity, status=status,
                observed=f"{len(group)} значений",
                expected=sample.expected,
                message=(
                    f"проверка выполнена для {len(group)} значений"
                    if status == rules.PASS
                    else f"пропущена для {len(group)} значений: {sample.message or 'нет данных для сравнения'}"
                ),
            )
        )
