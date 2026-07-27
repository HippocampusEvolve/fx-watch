"""Планировщик.

Почему задачи разнесены по разным расписаниям - см. README, требование 1.
Кратко: ЦБ публикует курсы на следующий день накануне, около 13:30 МСК,
поэтому к ночному перезапросу окна (03:00) курс «на сегодня» уже опубликован -
основной сбор делает именно ночной проход. Дневное окно опроса - страховка:
оно молчит, пока сегодняшняя дата разобрана, и начинает ходить в источник,
только если ночью лежал источник или сам сервис, либо публикация сдвинулась.

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

from fxwatch import jobs
from fxwatch.calendar import DayKind, classify_days, weekday_has_history
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
    """Дневной страховочный опрос.

    В обычный день он не делает ни одного запроса: курс за сегодня уже забрал
    ночной перезапрос окна. Запрос уходит в источник только если сегодняшняя
    дата всё ещё не разобрана:

    * за сегодня нет ни данных, ни успешной попытки - ночью лежал источник
      или сам сервис, надо добирать;
    * успешная попытка была, но данных нет, хотя по истории источника в этот
      день недели данные бывают - публикация сдвинулась, продолжаем спрашивать.

    На воскресенье и понедельник ЦБ курс не устанавливает - такие дни сервис
    распознаёт по собственному календарю источника и не дёргает его впустую.
    Пропуск в журнал не пишется: десяток строк «сходил зря» в день засорял бы
    журнал, ради чистоты которого всё и построено (в лог строка попадает).

    Отметка живости ставится в любом исходе, включая ранний выход. Живость
    здесь - это «логика окна отработала», а не «был сетевой запрос»: в штатном
    дне запроса не будет никогда, и отметка по факту запроса протухала бы ровно
    тогда, когда всё в порядке.
    """
    settings = get_settings()
    today = datetime.now(settings.zone).date()
    need_poll = True

    with session_scope() as session:
        kind = classify_days(session, today, today, PRIMARY_SOURCE)[today]
        if kind == DayKind.BUSINESS:
            log.info("курс за %s уже получен, страховочный опрос не нужен", today)
            need_poll = False
        elif kind == DayKind.NON_BUSINESS and not weekday_has_history(session, today, PRIMARY_SOURCE):
            log.info("на %s источник курс не публикует, опрос пропущен", today)
            need_poll = False

    if need_poll:
        jobs.job_poll(PRIMARY_SOURCE)

    # Интервал с запасом на выходные: окно 11-15:30 работает по будням,
    # но вечерний контрольный проход - ежедневный, поэтому 30 часов.
    jobs.touch_heartbeat(f"poll_window:{PRIMARY_SOURCE}", 30 * 3600)
    jobs.ping_watchdog(f"poll_window:{PRIMARY_SOURCE}")


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

    # Дневное страховочное окно: молчит, пока сегодняшняя дата разобрана,
    # и ходит в источник только после ночного сбоя или сдвига публикации.
    scheduler.add_job(
        _poll_primary, CronTrigger(day_of_week="mon-fri", hour="11-15", minute="0,30"),
        id="poll_cbr_window", name="Страховочный опрос ЦБ",
    )
    # Вечерний контрольный проход: последний рубеж того же условия.
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
