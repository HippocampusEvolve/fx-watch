"""Уведомления.

Главное решение здесь - дедупликация по ключу. Сервис, который шлёт сообщение
на каждое срабатывание, за три месяца пришлёт несколько сотен одинаковых писем,
и их перестанут открывать. Формально мониторинг есть, фактически его нет.

Поэтому: повторное срабатывание того же условия обновляет счётчик у уже
существующей записи, а наружу уходит только первое. Всё остальное собирается
в периодический дайджест.

Канал доставки - необязательный вебхук. Если он не настроен, алерты всё равно
пишутся в базу и видны в отчёте и на дашборде: доставка может быть не настроена,
но факт срабатывания теряться не должен.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from fxwatch.config import get_settings
from fxwatch.models import Alert

log = logging.getLogger(__name__)


def raise_alert(session: Session, key: str, severity: str, title: str, body: str | None = None) -> Alert:
    """Зафиксировать событие. Наружу уходит только первое срабатывание ключа."""
    now = datetime.now(UTC)
    alert = session.execute(select(Alert).where(Alert.alert_key == key)).scalar_one_or_none()

    if alert is None:
        alert = Alert(
            alert_key=key, severity=severity, title=title, body=body,
            first_fired_at=now, last_fired_at=now, fire_count=1, delivered=False,
        )
        session.add(alert)
        session.flush()
        alert.delivered = _deliver(title, body, severity)
        return alert

    alert.last_fired_at = now
    alert.fire_count += 1
    alert.severity = severity
    alert.title = title
    alert.body = body
    if alert.resolved_at is not None:
        # Условие вернулось после восстановления - это новое событие, шлём снова.
        alert.resolved_at = None
        alert.delivered = _deliver(title, body, severity)
    return alert


def resolve_alert(session: Session, key: str) -> None:
    alert = session.execute(select(Alert).where(Alert.alert_key == key)).scalar_one_or_none()
    if alert is not None and alert.resolved_at is None:
        alert.resolved_at = datetime.now(UTC)


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
