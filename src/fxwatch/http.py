"""HTTP-клиент с политикой повторов.

Требование 3 ТЗ («не падает, если источник медленный или отвечает ошибкой»)
закрывается здесь. Правила:

* таймауты выставлены явно на каждую фазу - без них медленный источник вешает
  весь прогон, а не отдаёт управление обратно;
* повторяем только то, что имеет шанс починиться само: сетевые ошибки, таймауты,
  5xx и 429. На 4xx повторять бессмысленно - это наша ошибка, а не сбой источника;
* 429 обрабатывается отдельно: уважаем ``Retry-After``, иначе повторы только
  усугубят ограничение;
* задержка растёт экспоненциально и с джиттером - чтобы несколько источников,
  упавших одновременно, не ходили в сеть синхронно.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass

import httpx

from fxwatch.config import get_settings
from fxwatch.sources.base import SourceError

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504, 522, 524}


@dataclass(slots=True)
class HttpResponse:
    status_code: int
    content: bytes
    content_type: str | None
    attempts: int


class RetryingClient:
    def __init__(self, user_agent: str = "fx-watch/1.0 (+https://antonov-ai.ru)") -> None:
        s = get_settings()
        self._max_attempts = s.http_max_attempts
        self._timeout = httpx.Timeout(
            timeout=s.http_total_timeout,
            connect=s.http_connect_timeout,
            read=s.http_read_timeout,
        )
        self._headers = {"User-Agent": user_agent, "Accept-Encoding": "gzip"}

    def get(self, url: str, params: dict[str, str] | None = None) -> HttpResponse:
        last_error: str = "неизвестная ошибка"
        with httpx.Client(timeout=self._timeout, headers=self._headers, follow_redirects=True) as client:
            for attempt in range(1, self._max_attempts + 1):
                try:
                    response = client.get(url, params=params)
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    log.warning("попытка %s/%s не удалась: %s", attempt, self._max_attempts, last_error)
                else:
                    if response.status_code < 400:
                        return HttpResponse(
                            status_code=response.status_code,
                            content=response.content,
                            content_type=response.headers.get("content-type"),
                            attempts=attempt,
                        )
                    if response.status_code not in RETRYABLE_STATUS:
                        # 4xx - проблема на нашей стороне (неверный запрос, блокировка).
                        # Повторять её бессмысленно, отдаём ошибку сразу.
                        raise SourceError(f"HTTP {response.status_code} (не повторяем): {url}")
                    last_error = f"HTTP {response.status_code}"
                    if response.status_code == 429:
                        self._sleep(self._retry_after(response, attempt))
                        continue

                if attempt < self._max_attempts:
                    self._sleep(self._backoff(attempt))

        raise SourceError(f"источник недоступен после {self._max_attempts} попыток: {last_error}")

    @staticmethod
    def _backoff(attempt: int) -> float:
        """Экспонента 1, 2, 4, 8 с плюс джиттер до 40%."""
        base = min(2 ** (attempt - 1), 16)
        return base * (1 + random.random() * 0.4)

    @staticmethod
    def _retry_after(response: httpx.Response, attempt: int) -> float:
        header = response.headers.get("retry-after")
        if header:
            try:
                return min(float(header), 60.0)
            except ValueError:
                pass
        return RetryingClient._backoff(attempt)

    @staticmethod
    def _sleep(seconds: float) -> None:
        log.info("пауза перед повтором: %.1f с", seconds)
        time.sleep(seconds)
