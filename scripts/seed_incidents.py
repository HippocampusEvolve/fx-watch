"""Демонстрационные инциденты для витрины.

ЧЕСТНОЕ ПРЕДУПРЕЖДЕНИЕ: события, которые создаёт этот скрипт, синтетические.
Курсы в базе настоящие - это архив Банка России. Синтетические здесь только
записи журнала: сбой, ревизия, застой и карантин.

Зачем это нужно. Требование 6 ТЗ звучит как «представьте, что сервис проработал
три месяца». За три месяца в норме случаются сбои, и отчёт, в котором все
показатели идеальны, ничего не доказывает: непонятно, механизмы действительно
работают или просто ни разу не сработали. Поэтому в историю добавлены четыре
события, которые сервис должен уметь пережить и показать:

1. источник лежал двое суток - предохранитель открывался, часть попыток
   пропущена осознанно, данные добраны после восстановления;
2. источник изменил значение за уже собранную дату - ревизия;
3. значение перестало меняться - сработала проверка застоя;
4. пришёл битый ответ - запись ушла в карантин, остальные сохранены,
   карантин разобран (в реальной работе это делает человек).

Паттерн сбоя повторяет фактическую механику сервиса: ночной перезапрос окна
получает четыре отказа подряд, предохранитель открывается, и оставшиеся даты
окна пишутся со статусом ``skipped``; днём страховочное окно и вечерний контроль
пробуют снова - предохранитель к этому времени остыл, попытки идут в сеть
и тоже падают.

Как отделить синтетику от настоящих данных. Каждая созданная здесь строка
привязана к прогону с версией кода ``demo-seed`` (алерты - к ключам ``demo:``),
поэтому удаляется всё одной командой:

    python scripts/seed_incidents.py --clean        # или make seed-clean

Скрипт идемпотентен: повторный запуск ничего не дублирует.

Ревизия делается аккуратно: в историю добавляется **более ранняя** версия
значения, а не более поздняя. Текущая витрина остаётся точной копией данных ЦБ,
но в истории видно, что сначала было известно другое число.
"""

from __future__ import annotations

import hashlib
import sys
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text

# Скрипт запускается и в контейнере, и с хоста, поэтому путь к пакету
# вычисляется от собственного расположения, а не задаётся строкой «/app/src»:
# зашитый путь молча ломает запуск на машине разработчика.
_SRC = Path(__file__).resolve().parent.parent / "src"
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))

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


def _utc(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute, second), tzinfo=UTC)


def _run(session, **fields) -> IngestRun:
    run = IngestRun(source_code="cbr", code_version=SEED_VERSION, **fields)
    session.add(run)
    return run


def seed_outage(session, start_day: date) -> int:
    """Двое суток недоступности источника с автоматическим восстановлением."""
    created = 0
    for offset in (0, 1):
        day = start_day + timedelta(days=offset)

        # Ночной перезапрос окна: 4 отказа подряд открывают предохранитель,
        # оставшиеся даты окна пропускаются, но остаются в журнале.
        night = _utc(day, 3, 0)
        for i in range(4):
            started = night + timedelta(seconds=i * 40)
            _run(
                session, job="sweep",
                started_at=started, finished_at=started + timedelta(seconds=32),
                status="failed", target_date=day - timedelta(days=14 - i),
                http_status=503, attempt_count=5, duration_ms=32000,
                error_class="SourceError",
                error_message="источник недоступен после 5 попыток: HTTP 503",
            )
            created += 1
        for i in range(5):
            skipped = night + timedelta(minutes=3, seconds=i * 5)
            _run(
                session, job="sweep",
                started_at=skipped, finished_at=skipped,
                status="skipped", target_date=day - timedelta(days=10 - i),
                attempt_count=0,
                error_class="CircuitOpen",
                error_message="источник признан недоступным, попытка пропущена",
            )
            created += 1

        # Дневное страховочное окно и вечерний контроль: дата не разобрана,
        # поэтому сервис продолжает пробовать. Предохранитель успевает остыть
        # между слотами, так что попытки идут в сеть и падают по-настоящему.
        for hour, minute in ((11, 0), (11, 30), (12, 0), (13, 0), (14, 30), (23, 0)):
            _run(
                session, job="poll",
                started_at=_utc(day, hour, minute), finished_at=_utc(day, hour, minute, 32),
                status="failed", target_date=day,
                http_status=503, attempt_count=5, duration_ms=32000,
                error_class="SourceError",
                error_message="источник недоступен после 5 попыток: HTTP 503",
            )
            created += 1

    # Восстановление: на третьи сутки ночной проход добирает обе даты.
    recovery_day = start_day + timedelta(days=2)
    for offset in (0, 1):
        _run(
            session, job="sweep",
            started_at=_utc(recovery_day, 3, 0, offset * 2),
            finished_at=_utc(recovery_day, 3, 0, offset * 2 + 1),
            status="ok", target_date=start_day + timedelta(days=offset),
            http_status=200, attempt_count=1, duration_ms=1400,
            rows_fetched=54, rows_inserted=54,
        )
        created += 1

    session.add(
        Alert(
            alert_key="demo:source_down:cbr",
            severity="error",
            title="Источник cbr недоступен",
            body=(
                "4 неудачи подряд, HTTP 503. Предохранитель открыт, оставшиеся даты "
                "ночного окна пропущены. Повторные срабатывания копятся в счётчик."
            ),
            first_fired_at=_utc(start_day, 3, 2),
            last_fired_at=_utc(start_day + timedelta(days=1), 23, 0),
            fire_count=20,
            resolved_at=_utc(recovery_day, 3, 0, 1),
            delivered=True,
        )
    )
    return created


def seed_revision(session) -> int:
    """Ревизия: источник изменил значение за уже собранную дату.

    В историю добавляется более ранняя версия, поэтому текущая витрина
    продолжает точно повторять данные ЦБ. Ранняя версия привязана к своему
    демонстрационному прогону, чтобы её было видно и легко удалить.
    """
    row = session.execute(
        text(
            """
            SELECT value_date, value_num, observed_at, nominal
            FROM rates_current
            WHERE source_code = 'cbr' AND series_key = 'USD/RUB'
              AND value_date < current_date - 20
            ORDER BY value_date DESC LIMIT 1
            """
        )
    ).first()
    if row is None:
        return 0

    value_date, value_num, observed_at, nominal = row
    old_value = (Decimal(value_num) * Decimal("0.9987")).quantize(Decimal("0.0001"))
    earlier = observed_at - timedelta(hours=20)

    demo_run = _run(
        session, job="demo",
        started_at=earlier, finished_at=earlier + timedelta(seconds=1),
        status="ok", target_date=value_date,
        http_status=200, attempt_count=1, duration_ms=900,
        rows_fetched=1, rows_inserted=1,
    )
    session.flush()

    digest = hashlib.sha256(
        f"USD/RUB|{value_date.isoformat()}|{old_value:.8f}|{nominal}".encode()
    ).hexdigest()
    session.add(
        Observation(
            source_code="cbr", series_key="USD/RUB", value_date=value_date,
            value_num=old_value, nominal=nominal, observed_at=earlier,
            run_id=demo_run.id, payload_hash=digest,
        )
    )
    delta = abs((Decimal(value_num) - old_value) / old_value * Decimal(100))
    session.add(
        Revision(
            source_code="cbr", series_key="USD/RUB", value_date=value_date,
            old_value=old_value, new_value=value_num, delta_pct=delta,
            first_observed_at=earlier, revised_at=observed_at, run_id=demo_run.id,
            is_significant=True, is_late=False,
        )
    )
    return 1


def seed_stale(session, day: date) -> int:
    """Срабатывание проверки застоя, привязанное к демонстрационному прогону."""
    demo_run = _run(
        session, job="demo",
        started_at=_utc(day, 6, 20), finished_at=_utc(day, 6, 20, 1),
        status="ok", target_date=day,
        attempt_count=0, duration_ms=300,
    )
    session.flush()

    session.add(
        QualityCheck(
            run_id=demo_run.id, source_code="cbr", check_name="stale_series",
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
    """Битый ответ: одна запись отбракована, остальные сохранены.

    Карантин помечен разобранным: в реальной работе разбор делает человек,
    и именно эта отметка отличает «система заметила и вопрос закрыт»
    от «подозрительная запись висит без хозяина».
    """
    run = _run(
        session, job="poll",
        started_at=_utc(day, 11, 30), finished_at=_utc(day, 11, 30, 1),
        status="partial", target_date=day, http_status=200, attempt_count=1,
        duration_ms=980, rows_fetched=54, rows_inserted=53, rows_quarantined=1,
    )
    session.flush()

    session.add(
        Quarantine(
            run_id=run.id, source_code="cbr", series_key="TRY/RUB", value_date=day,
            raw_value="n/a", reason="нечисловое значение: 'n/a'", check_name="parse",
            quarantined_at=_utc(day, 11, 30),
            resolved_at=_utc(day, 14, 45),
            resolution=(
                "разобрано: у источника в этой строке пришло 'n/a', значение за день "
                "восстановлено ночным перезапросом окна. Запись демонстрационная (demo-seed)"
            ),
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


def seed() -> int:
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
            f"добавлено: прогонов сбоя {outage_runs}, ревизий {revisions}, "
            f"срабатываний застоя {stale}, битых ответов {broken}"
        )
        print("все записи привязаны к прогонам с code_version='demo-seed'")
    return 0


def clean() -> int:
    """Удалить всю синтетику. Порядок важен: сначала записи, ссылающиеся на прогоны."""
    wait_for_db()
    dependent = ("observations", "revisions", "quality_checks", "quarantine")
    with session_scope() as session:
        counts: dict[str, int] = {}
        for table in dependent:
            counts[table] = session.execute(
                text(
                    f"""
                    DELETE FROM {table}
                    WHERE run_id IN (SELECT id FROM ingest_runs WHERE code_version = :v)
                    """
                ),
                {"v": SEED_VERSION},
            ).rowcount
        counts["alerts"] = session.execute(
            text("DELETE FROM alerts WHERE alert_key LIKE 'demo:%'")
        ).rowcount
        counts["ingest_runs"] = session.execute(
            text("DELETE FROM ingest_runs WHERE code_version = :v"), {"v": SEED_VERSION}
        ).rowcount
    print("удалена синтетика: " + ", ".join(f"{k}={v}" for k, v in counts.items()))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if "--clean" in args:
        return clean()
    return seed()


if __name__ == "__main__":
    raise SystemExit(main())
