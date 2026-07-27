"""Планировщик.

Почему задачи разнесены по разным расписаниям - см. README, требование 1.
Кратко: ЦБ обновляет курсы один раз в сутки, обычно между 11:30 и 15:30 МСК,
поэтому опрос идёт частыми короткими попытками внутри окна публикации
и прекращается, как только данные за сегодня получены. Всё остальное время
частый опрос не даёт новой информации.

Два параметра APScheduler здесь принципиальны:

* ``coalesce`` - после простоя выполняется одна задача, а не очередь накопившихся;
* ``misfire_grace_time`` - задача, чьё время давно прошло, просто пропускается;
  догоном пропущенного занимается ``sweep``, а не планировщик.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from fxwatch import jobs
from fxwatch.config import get_settings
from fxwatch.db import session_scope, wait_for_db
from fxwatch.ingest import ingest_date, reap_stale_runs
from fxwatch.sources import PRIMARY_SOURCE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("fxwatch.scheduler")


def _poll_primary() -> None:
    """Опрос в окне публикации с ранним выходом.

    Если курс за сегодня уже получен, повторный запрос ничего не добавит:
    прогон всё равно пишется в журнал, но со статусом пропуска, чтобы в истории
    было видно, что сервис отработал по расписанию.
    """
    settings = get_settings()
    today = datetime.now(settings.zone).date()
    with session_scope() as session:
        already = session.execute(
            text("SELECT count(*) FROM observations WHERE source_code = :src AND value_date = :day"),
            {"src": PRIMARY_SOURCE, "day": today},
        ).scalar()
    if already:
        log.info("курс за %s уже получен, опрос пропущен", today)
        return
    jobs.job_poll(PRIMARY_SOURCE)


def _poll_secondary() -> None:
    settings = get_settings()
    try:
        ingest_date("erapi", datetime.now(settings.zone).date(), job="poll")
    except Exception as exc:  # noqa: BLE001
        # Второй источник нужен только для сверки: его падение не должно
        # влиять на основной поток данных.
        log.warning("вторичный источник недоступен: %s", exc)


def build_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(
        timezone=str(get_settings().zone),
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600},
    )

    # Окно публикации ЦБ: частые короткие попытки, ранний выход после успеха.
    scheduler.add_job(
        _poll_primary, CronTrigger(day_of_week="mon-fri", hour="11-15", minute="0,30"),
        id="poll_cbr_window", name="Опрос ЦБ в окне публикации",
    )
    # Подтверждающий вечерний проход: ловит публикацию, сдвинутую за окно.
    scheduler.add_job(
        _poll_primary, CronTrigger(hour=23, minute=0),
        id="poll_cbr_evening", name="Вечерний контрольный опрос ЦБ",
    )
    scheduler.add_job(
        _poll_secondary, CronTrigger(hour=12, minute=15),
        id="poll_erapi", name="Опрос второго источника для сверки",
    )
    # Ночное окно перезапроса: ревизии и догон дыр.
    scheduler.add_job(
        jobs.job_sweep, CronTrigger(hour=3, minute=0),
        id="sweep", name="Перезапрос окна и добор пропущенного",
    )
    scheduler.add_job(
        jobs.job_state_checks, CronTrigger(hour="*/6", minute=20),
        id="state_checks", name="Проверки накопленного состояния",
    )
    scheduler.add_job(
        jobs.job_retention, CronTrigger(hour=4, minute=30),
        id="retention", name="Очистка сырых ответов",
    )
    scheduler.add_job(
        lambda: reap_stale_runs(60), CronTrigger(minute=7),
        id="reaper", name="Закрытие зависших прогонов",
    )
    return scheduler


def main() -> None:
    wait_for_db()
    reaped = reap_stale_runs(0)
    if reaped:
        log.warning("закрыто зависших прогонов после перезапуска: %s", reaped)

    # Первичную заливку делаем в фоне: она занимает несколько минут и не должна
    # блокировать старт планировщика.
    threading.Thread(target=_bootstrap, name="bootstrap", daemon=True).start()

    scheduler = build_scheduler()
    scheduler.start()
    log.info("планировщик запущен, задач: %s", len(scheduler.get_jobs()))
    for job in scheduler.get_jobs():
        log.info("  %-22s %s", job.id, job.trigger)

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    stop.wait()

    log.info("останавливаюсь")
    scheduler.shutdown(wait=True)


def _bootstrap() -> None:
    try:
        inserted = jobs.bootstrap_if_empty()
        if inserted:
            log.info("первичная заливка завершена, записано наблюдений: %s", inserted)
        jobs.job_state_checks()
    except Exception as exc:  # noqa: BLE001
        log.exception("первичная заливка не удалась: %s", exc)


if __name__ == "__main__":
    main()
