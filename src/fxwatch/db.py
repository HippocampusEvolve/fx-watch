"""Подключение к БД и фабрика сессий."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from fxwatch.config import get_settings

_settings = get_settings()

engine = create_engine(
    _settings.db_dsn,
    pool_pre_ping=True,  # переживаем разрыв соединения после простоя
    pool_size=5,
    max_overflow=5,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


@contextmanager
def session_scope() -> Iterator[Session]:
    """Транзакция целиком: либо всё записалось, либо ничего."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def wait_for_db(timeout_sec: int = 60) -> None:
    """Ждём БД на старте: в compose контейнеры поднимаются не мгновенно."""
    deadline = time.monotonic() + timeout_sec
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(1.5)
    raise RuntimeError(f"БД недоступна за {timeout_sec} с: {last_error}")
