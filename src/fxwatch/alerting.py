"""Уведомления.

Главное решение здесь - дедупликация по ключу. Сервис, который шлёт сообщение
на каждое срабатывание, за три месяца пришлёт несколько сотен одинаковых писем,
и их перестанут открывать. Формально мониторинг есть, фактически его нет.

Поэтому: повторное срабатывание того же условия обновляет счётчик у уже
существующей записи, а наружу уходит только первое. Всё остальное собирается
в периодический дайджест.

Второе решение - доставка вне транзакции. Запись алерта и поход в вебхук
разведены: :func:`raise_alert` только пишет в базу и складывает событие
в :class:`AlertOutbox`, а :func:`AlertOutbox.flush` отправляет его после
коммита. Иначе недоступный вебхук держал бы открытую транзакцию сбора
до десяти секунд на каждый алерт, а в прогоне с массовым карантином - минуты.

Канал доставки - необязательный вебхук. Если он не настроен, алерты всё равно
пишутся в базу и видны в отчёте и на дашборде: доставка может быть не настроена,
но факт срабатывания теряться не должен.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from fxwatch.config import get_settings
from fxwatch.db import session_scope
from fxwatch.models import Alert

log = logging.getLogger(__name__)


@dataclass(slots=True)
class _Pending:
    key: str
    severity: str
    title: str
    body: str | None


@dataclass(slots=True)
class AlertOutbox:
    """Очередь алертов, ожидающих отправки после коммита транзакции."""

    items: list[_Pending] = field(default_factory=list)

    def add(self, key: str, severity: str, title: str, body: str | None) -> None:
        self.items.append(_Pending(key, severity, title, body))

    def flush(self) -> int:
        """Отправить накопленное и отметить доставку. Вызывается вне транзакции сбора."""
        if not self.items:
            return 0
        delivered_keys = []
        for item in self.items:
            if _deliver(item.title, item.body, item.severity):
                delivered_keys.append(item.key)
        sent = len(delivered_keys)
        self.items.clear()
        if delivered_keys:
            # Короткая отдельная транзакция: отметка доставки не должна
            # удерживать транзакцию сбора данных.
            with session_scope() as session:
                for key in delivered_keys:
                    alert = session.execute(select(Alert).where(Alert.alert_key == key)).scalar_one_or_none()
                    if alert is not None:
                        alert.delivered = True
        return sent


def raise_alert(
    session: Session,
    key: str,
    severity: str,
    title: str,
    body: str | None = None,
    outbox: AlertOutbox | None = None,
) -> Alert:
    """Зафиксировать событие. Наружу уходит только первое срабатывание ключа.

    ``outbox`` обязателен везде, где вызов идёт внутри открытой транзакции:
    сетевой поход тогда откладывается до её коммита. Без ``outbox`` алерт
    только записывается - доставку в этом случае инициирует вызывающий код.
    """
    now = datetime.now(UTC)
    alert = session.execute(select(Alert).where(Alert.alert_key == key)).scalar_one_or_none()

    if alert is None:
        alert = Alert(
            alert_key=key, severity=severity, title=title, body=body,
            first_fired_at=now, last_fired_at=now, fire_count=1, delivered=False,
        )
        session.add(alert)
        session.flush()
        if outbox is not None:
            outbox.add(key, severity, title, body)
        return alert

    alert.last_fired_at = now
    alert.fire_count += 1
    alert.severity = severity
    alert.title = title
    alert.body = body
    if alert.resolved_at is not None:
        # Условие вернулось после восстановления - это новое событие, шлём снова.
        alert.resolved_at = None
        alert.delivered = False
        if outbox is not None:
            outbox.add(key, severity, title, body)
    return alert


def resolve_alert(session: Session, key: str) -> None:
    alert = session.execute(select(Alert).where(Alert.alert_key == key)).scalar_one_or_none()
    if alert is not None and alert.resolved_at is None:
        alert.resolved_at = datetime.now(UTC)


def ping_watchdog(source: str) -> bool:
    """Отметиться у внешнего наблюдателя (dead man's switch).

    Собственный журнал не доказывает, что сервис жив: если контейнер умрёт
    целиком, отметить это будет некому, и молчание будет неотличимо от порядка.
    Поэтому после каждого успешного цикла сервис дёргает внешний URL,
    который сам поднимет тревогу, если пинги прекратятся. Требование 6 ТЗ
    закрывается только так: наблюдатель должен быть снаружи наблюдаемого.

    Если URL не настроен, функция молча ничего не делает: внешний сервис -
    это выбор эксплуатации, а не обязательная зависимость сборки.
    """
    url = get_settings().heartbeat_url
    if not url:
        return False
    target = f"{url}{'&' if '?' in url else '?'}source={source}"
    try:
        with urllib.request.urlopen(target, timeout=5) as response:
            ok = 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        log.warning("внешний watchdog недоступен: %s", exc)
        return False
    if not ok:
        log.warning("внешний watchdog ответил не 2xx")
    return ok


def _deliver(title: str, body: str | None, severity: str) -> bool:
    webhook = get_settings().alert_webhook
    if not webhook:
        log.warning("[alert:%s] %s | %s", severity, title, body or "")
        return False
    payload = json.dumps({"severity": severity, "title": title, "body": body or ""}).encode("utf-8")
    request = urllib.request.Request(
        webhook, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError) as exc:
        # Недоставленный алерт не должен ронять сбор данных: он остаётся
        # в базе с delivered=false и виден в отчёте.
        log.error("не удалось доставить алерт: %s", exc)
        return False
