"""Схема хранения.

Ядро решения - таблица ``observations``. Она append-only и битемпоральная:

* ``value_date``   - к какой дате относится значение (valid time);
* ``observed_at``  - когда мы это значение увидели впервые (transaction time);
* ``last_seen_at`` - когда источник в последний раз подтвердил это значение.

Уникальный ключ ``(source_code, series_key, value_date, payload_hash)`` делает
всю работу по требованиям 2 и 4 ТЗ:

* повторный запрос с тем же значением даёт тот же хэш, новой строки не создаёт -
  сбор идемпотентен, обновляется только ``last_seen_at``;
* повторный запрос с другим значением даёт другой хэш и ложится новой строкой -
  ревизия источника сохраняется, ничего не перезаписывается.

Две отметки времени вместо одной нужны из-за отката: если источник отдал A,
потом B, потом снова A, то строка A уже существует, и без ``last_seen_at``
витрина навсегда осталась бы на B. Текущей версией считается та, что источник
подтверждал последней, а не та, которую мы увидели последней.

Текущее состояние и «состояние на дату X» - это две выборки из одной и той же
таблицы, а не два разных хранилища.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Observation(Base):
    """Одно наблюдение: «источник S в момент T сообщил, что на дату D значение равно V»."""

    __tablename__ = "observations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_code: Mapped[str] = mapped_column(String(32), nullable=False)
    series_key: Mapped[str] = mapped_column(String(32), nullable=False)  # например USD/RUB
    value_date: Mapped[date] = mapped_column(Date, nullable=False)
    value_num: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    nominal: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Последнее подтверждение значения источником. Отличается от observed_at
    # только у значений, которые источник отдавал повторно, - и именно по нему
    # определяется текущая версия, иначе откат A -> B -> A остался бы незамеченным.
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "source_code", "series_key", "value_date", "payload_hash", name="uq_observation_value"
        ),
        Index("ix_obs_series_date", "source_code", "series_key", "value_date"),
        Index("ix_obs_observed_at", "observed_at"),
        Index("ix_obs_last_seen_at", "last_seen_at"),
    )


class IngestRun(Base):
    """Журнал каждого обращения к источнику. Основа ответа на требование 6 ТЗ."""

    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_code: Mapped[str] = mapped_column(String(32), nullable=False)
    job: Mapped[str] = mapped_column(String(32), nullable=False)  # poll | sweep | backfill | manual
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # ok - всё чисто; partial - часть записей ушла в карантин, остальные сохранены;
    # failed - источник не отдал данные; skipped - сработал circuit breaker;
    # running - прогон стартовал и не завершился (реапер пометит failed).
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    target_date: Mapped[date | None] = mapped_column(Date)
    http_status: Mapped[int | None] = mapped_column(Integer)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    rows_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_revised: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_quarantined: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_class: Mapped[str | None] = mapped_column(String(64))
    error_message: Mapped[str | None] = mapped_column(Text)
    raw_sha256: Mapped[str | None] = mapped_column(String(64))
    code_version: Mapped[str] = mapped_column(String(64), nullable=False, default="dev")

    __table_args__ = (
        Index("ix_runs_source_started", "source_code", "started_at"),
        Index("ix_runs_status", "status"),
    )


class Revision(Base):
    """Материализованное событие «источник изменил значение за уже собранную дату»."""

    __tablename__ = "revisions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_code: Mapped[str] = mapped_column(String(32), nullable=False)
    series_key: Mapped[str] = mapped_column(String(32), nullable=False)
    value_date: Mapped[date] = mapped_column(Date, nullable=False)
    old_value: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    new_value: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    delta_pct: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    first_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revised_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # Значимая ревизия = расхождение выше epsilon. Незначимые пишем, но не алертим.
    is_significant: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Ревизия периода, который уже вышел из окна перезапроса, - отдельный сигнал.
    is_late: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Источник вернулся к значению, которое уже отдавал раньше. Новой строки
    # наблюдения при этом не появляется, поэтому событие фиксируется явно.
    is_rollback: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (Index("ix_rev_date", "value_date"), Index("ix_rev_revised_at", "revised_at"))


class RawPayload(Base):
    """Сырой ответ источника в сжатом виде: позволяет переиграть парсинг задним числом."""

    __tablename__ = "raw_payloads"

    run_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_code: Mapped[str] = mapped_column(String(32), nullable=False)
    content_gzip: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    content_type: Mapped[str | None] = mapped_column(String(128))
    byte_size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (Index("ix_raw_fetched_at", "fetched_at"),)


class QualityCheck(Base):
    """Результат одной проверки в одном прогоне."""

    __tablename__ = "quality_checks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_code: Mapped[str] = mapped_column(String(32), nullable=False)
    check_name: Mapped[str] = mapped_column(String(48), nullable=False)
    severity: Mapped[str] = mapped_column(String(8), nullable=False)  # info | warn | error
    status: Mapped[str] = mapped_column(String(8), nullable=False)  # pass | fail | skip
    series_key: Mapped[str | None] = mapped_column(String(32))
    value_date: Mapped[date | None] = mapped_column(Date)
    observed: Mapped[str | None] = mapped_column(String(64))
    expected: Mapped[str | None] = mapped_column(String(64))
    message: Mapped[str | None] = mapped_column(Text)
    checked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_qc_run", "run_id"),
        Index("ix_qc_status_time", "status", "checked_at"),
    )


class Quarantine(Base):
    """Запись, не прошедшая валидацию. Не попадает в витрину, но и не теряется.

    Одна строка на одну проблему, а не на одну попытку: окно перезапроса
    трогает те же даты каждую ночь, и без склейки стабильно битое значение
    давало бы четырнадцать новых записей в сутки. Повторное срабатывание
    увеличивает ``seen_count`` и двигает ``last_seen_at``.
    """

    __tablename__ = "quarantine"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_code: Mapped[str] = mapped_column(String(32), nullable=False)
    series_key: Mapped[str | None] = mapped_column(String(32))
    value_date: Mapped[date | None] = mapped_column(Date)
    raw_value: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    check_name: Mapped[str | None] = mapped_column(String(48))
    quarantined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolution: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_quarantine_time", "quarantined_at"),
        Index("ix_quarantine_slot", "source_code", "series_key", "value_date", "check_name"),
    )


class Alert(Base):
    """Отправленные уведомления с дедупликацией по ключу.

    Без дедупа за три месяца пришло бы несколько сотен одинаковых сообщений,
    и их перестали бы читать - то есть мониторинга фактически не было бы.
    """

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    alert_key: Mapped[str] = mapped_column(String(160), nullable=False, unique=True)
    severity: Mapped[str] = mapped_column(String(8), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str | None] = mapped_column(Text)
    first_fired_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_fired_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    fire_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (Index("ix_alerts_fired", "last_fired_at"),)


class SourceState(Base):
    """Состояние источника между прогонами: circuit breaker и отметка живости."""

    __tablename__ = "source_state"

    source_code: Mapped[str] = mapped_column(String(32), primary_key=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    breaker_open_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)


class Heartbeat(Base):
    """Отметки регулярных задач для dead man's switch.

    Молчание системы не должно читаться как «всё хорошо»: если задача перестала
    отмечаться, это отдельный наблюдаемый факт, а не отсутствие сигнала.
    """

    __tablename__ = "heartbeats"

    name: Mapped[str] = mapped_column(String(48), primary_key=True)
    last_ok_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expected_interval_sec: Mapped[int] = mapped_column(Integer, nullable=False, default=86400)
    note: Mapped[str | None] = mapped_column(Text)
