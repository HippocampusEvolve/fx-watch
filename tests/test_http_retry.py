"""Поведение при недоступном, медленном и ошибающемся источнике (требование 3 ТЗ)."""

from __future__ import annotations

import httpx
import pytest
import respx

from fxwatch.http import RetryingClient
from fxwatch.sources.base import SourceError

URL = "https://example.test/data"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Убираем реальные паузы: проверяем логику повторов, а не время."""
    monkeypatch.setattr(RetryingClient, "_sleep", staticmethod(lambda _seconds: None))


@respx.mock
def test_recovers_after_temporary_5xx():
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(502),
            httpx.Response(200, content=b"x" * 200),
        ]
    )

    result = RetryingClient().get(URL)

    assert result.status_code == 200
    assert result.attempts == 3
    assert route.call_count == 3


@respx.mock
def test_recovers_after_timeout():
    route = respx.get(URL).mock(
        side_effect=[httpx.ReadTimeout("слишком долго"), httpx.Response(200, content=b"x" * 200)]
    )

    result = RetryingClient().get(URL)

    assert result.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_gives_up_after_max_attempts_without_crashing():
    """Исчерпав попытки, клиент возвращает управляемую ошибку, а не падает.

    Вызывающий код на её основе помечает прогон неуспешным и открывает
    предохранитель - сервис продолжает работать.
    """
    respx.get(URL).mock(return_value=httpx.Response(503))

    with pytest.raises(SourceError, match="после 5 попыток"):
        RetryingClient().get(URL)


@respx.mock
def test_client_errors_are_not_retried():
    """4xx повторять бессмысленно: это не сбой источника, а неверный запрос.

    Повторы здесь только жгли бы лимиты и задерживали появление ошибки в журнале.
    """
    route = respx.get(URL).mock(return_value=httpx.Response(404))

    with pytest.raises(SourceError, match="не повторяем"):
        RetryingClient().get(URL)

    assert route.call_count == 1


@respx.mock
def test_rate_limit_respects_retry_after(monkeypatch):
    """При 429 пауза берётся из заголовка источника, а не из нашей экспоненты."""
    slept: list[float] = []
    monkeypatch.setattr(RetryingClient, "_sleep", staticmethod(slept.append))

    respx.get(URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200, content=b"x" * 200),
        ]
    )

    result = RetryingClient().get(URL)

    assert result.status_code == 200
    assert slept == [7.0]


def test_backoff_grows_and_has_jitter():
    delays = [RetryingClient._backoff(attempt) for attempt in range(1, 5)]

    assert delays[0] < delays[-1]
    assert all(base <= d <= base * 1.4 for d, base in zip(delays, [1, 2, 4, 8], strict=True))
