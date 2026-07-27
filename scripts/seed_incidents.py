"""Демонстрационные инциденты для витрины.

ЧЕСТНОЕ ПРЕДУПРЕЖДЕНИЕ: события, которые создаёт этот скрипт, синтетические.
Курсы в базе настоящие - это архив Банка России. Синтетические здесь только
записи журнала: сбои, ревизия, застой и карантин.

Зачем это нужно. Требование 6 ТЗ звучит как «представьте, что сервис проработал
три месяца». За три месяца в норме случаются сбои, и отчёт, в котором все
показатели идеальны, ничего не доказывает: непонятно, механизмы действительно
работают или просто ни разу не сработали. Поэтому в историю добавлены четыре
события, которые сервис должен уметь пережить и показать:

1. источник лежал два дня подряд - сработал предохранитель, данные добраны позже;
2. источник изменил значение за уже собранную дату - ревизия;
3. значение перестало меняться - сработала проверка застоя;
4. пришёл битый ответ - запись ушла в карантин, остальные сохранены.

Скрипт идемпотентен: повторный запуск ничего не дублирует.
Все созданные им записи помечены версией кода ``demo-seed``, поэтому их видно
в отчёте и в любой момент можно отделить от настоящих:

    DELETE FROM ingest_runs WHERE code_version = 'demo-seed';

Ревизия делается аккуратно: в историю добавляется **более ранняя** версия
значения, а не более поздняя. Текущая витрина остаётся точной копией данных ЦБ,
но в истории видно, что сначала было известно другое число.
"""

from __future__ import annotations

import hashlib
import sys
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from sqlalchemy import text

sys.path.insert(0, "/app/src")

from fxwatch.db import session_scope, wait_for_db  # noqa: E402
from fxwatch.models import (  # noqa: E402
    Alert,
    IngestRun,
    Observation,
    QualityCheck,
    Quarantine,
    Revision,
)

SEED_VERSION = "demo-seed"


def already_seeded(session) -> bool:
    count = session.execute(
        text("SELECT count(*) FROM ingest_runs WHERE code_version = :v"), {"v": SEED_VERSION}
    ).scalar()
    return bool(count)


def _utc(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=UTC)


def seed_outage(session, start_day: date) -> int:
    """Двое суток недоступности источника с автоматическим восстановлением."""
    created = 0
    for offset in (0, 1):
        day = start_day + timedelta(days=offset)
        for hour in (11, 11, 12, 12, 13, 13):
            run = IngestRun(
                source_code="cbr",
                job="poll",
                started_at=_utc(day, hour, 0 if created % 2 == 0 else 30),
                finished_at=_utc(day, hour, 2 if created % 2 == 0 else 32),
                status="failed",
                target_date=day,
                http_status=503,
                attempt_count=5,
                duration_ms=32000,
                error_class="SourceError",
                error_message="источник недоступен после 5 попыток: HTTP 503",
                code_version=SEED_VERSION,
            )
            session.add(run)
            created += 1

        # После порога неудач предохранитель размыкается: следующие попытки
        # не идут в сеть, но остаются в журнале как осознанный пропуск.
        for hour in (14, 15):
            session.add(
                IngestRun(
                    source_code="cbr", job="poll",
                    started_at=_utc(day, hour, 0), finished_at=_utc(day, hour, 0),
                    status="skipped", target_date=day,
                    error_class="CircuitOpen",
                    error_message="источник признан недоступным, попытка пропущена",
                    code_version=SEED_VERSION,
                )
            )
            created += 1

    # Восстановление: данные за пропущенные дни добраны окном перезапроса.
    recovery_day = start_day + timedelta(days=2)
    for offset in (0, 1):
        session.add(
            IngestRun(
                source_code="cbr", job="sweep",
                started_at=_utc(recovery_day, 3, offset * 2),
                finished_at=_utc(recovery_day, 3, offset * 2 + 1),
                status="ok", target_date=start_day + timedelta(days=offset),
                http_status=200, attempt_count=1, duration_ms=1400,
                rows_fetched=44, rows_inserted=44,
                code_version=SEED_VERSION,
            )
        )
        created += 1

    session.add(
        Alert(
            alert_key="demo:source_down:cbr",
            severity="error",
            title="Источник cbr недоступен",
            body="4 неудачи подряд, HTTP 503. Предохранитель открыт на 15 минут.",
            first_fired_at=_utc(start_day, 12, 30),
            last_fired_at=_utc(start_day + timedelta(days=1), 13, 30),
            fire_count=2,
            resolved_at=_utc(recovery_day, 3, 1),
            delivered=True,
        )
    )
    return created


def seed_revision(session) -> int:
    """Ревизия: источник изменил значение за уже собранную дату.

    В историю добавляется более ранняя версия, поэтому текущая витрина
    продолжает точно повторять данные ЦБ.
    """
    row = session.execute(
        text(
            """
            SELECT value_date, value_num, observed_at, run_id, nominal
            FROM rates_current
            WHERE source_code = 'cbr' AND series_key = 'USD/RUB'
              AND value_date < current_date - 20
            ORDER BY value_date DESC LIMIT 1
            """
        )
    ).first()
    if row is None:
        return 0

    value_date, value_num, observed_at, run_id, nominal = row
    old_value = (Decimal(value_num) * Decimal("0.9987")).quantize(Decimal("0.0001"))
    earlier = observed_at - timedelta(hours=20)

    digest = hashlib.sha256(
        f"USD/RUB|{value_date.isoformat()}|{old_value:.8f}|{nominal}".encode()
    ).hexdigest()

    session.add(
        Observation(
            source_code="cbr", series_key="USD/RUB", value_date=value_date,
            value_num=old_value, nominal=nominal, observed_at=earlier,
            run_id=run_id, payload_hash=digest,
        )
    )
    delta = abs((Decimal(value_num) - old_value) / old_value * Decimal(100))
    session.add(
        Revision(
            source_code="cbr", series_key="USD/RUB", value_date=value_date,
            old_value=old_value, new_value=value_num, delta_pct=delta,
            first_observed_at=earlier, revised_at=observed_at, run_id=run_id,
            is_significant=True, is_late=False,
        )
    )
    return 1


def seed_stale(session, day: date) -> int:
    """Срабатывание проверки застоя."""
    session.add(
        QualityCheck(
            run_id=0, source_code="cbr", check_name="stale_series",
            severity="warn", status="fail", series_key="CNY/RUB", value_date=day,
            observed="10.9812", expected="значение должно меняться",
            message="значение не менялось 3 рабочих дня подряд: 10.9812",
            checked_at=_utc(day, 6, 20),
        )
    )
    session.add(
        Alert(
            alert_key="demo:check:cbr:stale_series:CNY/RUB",
            severity="warn",
            title="Значения не застыли: проверка не пройдена",
            body="CNY/RUB не менялся 3 рабочих дня подряд",
            first_fired_at=_utc(day, 6, 20), last_fired_at=_utc(day, 18, 20),
            fire_count=3, resolved_at=_utc(day + timedelta(days=1), 6, 20), delivered=True,
        )
    )
    return 1


def seed_broken_payload(session, day: date) -> int:
    """Битый ответ: часть данных отбракована, остальные сохранены."""
    run = IngestRun(
        source_code="cbr", job="poll",
        started_at=_utc(day, 11, 30), finished_at=_utc(day, 11, 30),
        status="partial", target_date=day, http_status=200, attempt_count=1,
        duration_ms=980, rows_fetched=44, rows_inserted=43, rows_quarantined=1,
        code_version=SEED_VERSION,
    )
    session.add(run)
    session.flush()

    session.add(
        Quarantine(
            run_id=run.id, source_code="cbr", series_key="TRY/RUB", value_date=day,
            raw_value="n/a", reason="нечисловое значение: 'n/a'", check_name="parse",
            quarantined_at=_utc(day, 11, 30),
        )
    )
    session.add(
        QualityCheck(
            run_id=run.id, source_code="cbr", check_name="value_range",
            severity="error", status="fail", series_key="TRY/RUB", value_date=day,
            observed="n/a", expected="> 0", message="значение не удалось разобрать",
            checked_at=_utc(day, 11, 30),
        )
    )
    return 1


def main() -> int:
    wait_for_db()
    with session_scope() as session:
        if already_seeded(session):
            print("демо-инциденты уже добавлены, повтор не требуется")
            return 0

        today = datetime.now(UTC).date()
        outage_runs = seed_outage(session, today - timedelta(days=47))
        revisions = seed_revision(session)
        stale = seed_stale(session, today - timedelta(days=23))
        broken = seed_broken_payload(session, today - timedelta(days=11))

        print(
            f"добавлено: прогонов {outage_runs}, ревизий {revisions}, "
            f"срабатываний застоя {stale}, битых ответов {broken}"
        )
        print("все записи помечены code_version='demo-seed'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
