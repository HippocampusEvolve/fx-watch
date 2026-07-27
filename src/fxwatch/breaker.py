"""Предохранитель на источник.

Если источник лежит, повторные обращения ничего не чинят: они тратят время
планировщика, засоряют журнал одинаковыми ошибками и создают нагрузку на
чужой сервис ровно в тот момент, когда ему плохо.

После нескольких подряд неудач источник помечается недоступным на время
остывания. Прогоны в этот период не пропадают молча - они пишутся в журнал
со статусом ``skipped``, то есть остаётся видно, что сервис работал и
сознательно не ходил в сеть.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from fxwatch.config import get_settings
from fxwatch.models import SourceState

log = logging.getLogger(__name__)


def _state(session: Session, source_code: str) -> SourceState:
    state = session.get(SourceState, source_code)
    if state is None:
        state = SourceState(source_code=source_code, consecutive_failures=0)
        session.add(state)
        session.flush()
    return state


def is_open(session: Session, source_code: str) -> bool:
    """Открыт ли предохранитель, то есть нужно ли пропустить обращение."""
    state = _state(session, source_code)
    if state.breaker_open_until is None:
        return False
    return datetime.now(UTC) < state.breaker_open_until


def opened_until(session: Session, source_code: str) -> datetime | None:
    return _state(session, source_code).breaker_open_until


def record_success(session: Session, source_code: str) -> None:
    state = _state(session, source_code)
    if state.consecutive_failures:
        log.info("источник %s восстановился после %s неудач", source_code, state.consecutive_failures)
    state.consecutive_failures = 0
    state.breaker_open_until = None
    state.last_success_at = datetime.now(UTC)
    state.last_error = None


def record_failure(session: Session, source_code: str, error: str) -> bool:
    """Учесть неудачу. Возвращает True, если предохранитель только что сработал."""
    settings = get_settings()
    state = _state(session, source_code)
    state.consecutive_failures += 1
    state.last_failure_at = datetime.now(UTC)
    state.last_error = error[:2000]

    if state.consecutive_failures >= settings.breaker_failure_threshold:
        state.breaker_open_until = datetime.now(UTC) + timedelta(
            seconds=settings.breaker_cooldown_sec
        )
        log.warning(
            "предохранитель для %s открыт до %s (%s неудач подряд)",
            source_code,
            state.breaker_open_until,
            state.consecutive_failures,
        )
        return True
    return False


def all_states(session: Session) -> list[SourceState]:
    return list(session.execute(select(SourceState)).scalars())
