"""Конфигурация сервиса. Все значения переопределяются переменными окружения FXWATCH_*."""

from __future__ import annotations

from functools import lru_cache
from zoneinfo import ZoneInfo

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FXWATCH_", env_file=".env", extra="ignore")

    db_dsn: str = "postgresql+psycopg://fxwatch:fxwatch@localhost:5432/fxwatch"
    tz: str = "Europe/Moscow"
    code_version: str = "dev"

    # --- Сбор -------------------------------------------------------------
    # Окно перезапроса. Одна конструкция закрывает три требования ТЗ сразу:
    # догон дыр после простоя (п.3), детекция ревизий (п.4) и граница
    # "период закрыт" (п.4): ревизия старше sweep_days - уже инцидент, а не норма.
    sweep_days: int = 14
    bootstrap_days: int = 90  # первичная заливка истории при пустой БД

    http_connect_timeout: float = 5.0
    http_read_timeout: float = 15.0
    http_total_timeout: float = 30.0
    http_max_attempts: int = 5

    # Circuit breaker: после N подряд неудач источник считаем недоступным
    # и cooldown_sec не трогаем - мёртвый источник долбить бессмысленно.
    breaker_failure_threshold: int = 4
    breaker_cooldown_sec: int = 900

    # --- Качество данных --------------------------------------------------
    # Ожидаемое число рядов задаётся у самого источника (Source.min_expected_series),
    # а не здесь: у ЦБ это ~44 валюты, у источника для сверки - ровно три.
    jump_warn_pct: float = 10.0
    jump_error_pct: float = 30.0
    stale_business_days: int = 3  # столько одинаковых значений подряд считаем застоем
    freshness_hours: int = 36
    cross_source_warn_pct: float = 5.0
    # Порог значимости ревизии: расхождение меньше - шум округления источника,
    # пишем в историю, но не алертим.
    revision_epsilon_pct: float = 0.05

    # --- Прочее -----------------------------------------------------------
    raw_retention_days: int = 30
    alert_webhook: str = ""
    heartbeat_url: str = ""  # внешний watchdog (dead man's switch)

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.tz)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
