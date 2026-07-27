"""Командный интерфейс: ручной запуск тех же задач, что крутит планировщик.

Все операции идемпотентны: повторный запуск любой команды не портит историю
и не создаёт дублей, поэтому их безопасно выполнять руками при разборе проблемы.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from fxwatch import jobs, reporting
from fxwatch.config import get_settings
from fxwatch.db import session_scope, wait_for_db
from fxwatch.ingest import ingest_date, reap_stale_runs
from fxwatch.sources import PRIMARY_SOURCE


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        stream=sys.stdout,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fxwatch", description="Управление сервисом сбора курсов")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p_backfill = sub.add_parser("backfill", help="залить историю за N последних дней")
    p_backfill.add_argument("--days", type=int, default=90)
    p_backfill.add_argument("--source", default=PRIMARY_SOURCE)

    p_poll = sub.add_parser("poll", help="разовый опрос источника")
    p_poll.add_argument("--source", default=PRIMARY_SOURCE)
    p_poll.add_argument("--date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(), default=None)

    sub.add_parser("sweep", help="перезапросить окно и добрать пропущенное")
    sub.add_parser("checks", help="прогнать проверки накопленного состояния")
    sub.add_parser("retention", help="удалить устаревшие сырые ответы")
    sub.add_parser("reap", help="закрыть зависшие прогоны")

    p_report = sub.add_parser("report", help="отчёт за период")
    p_report.add_argument("--days", type=int, default=90)
    p_report.add_argument("--from", dest="date_from", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    p_report.add_argument("--to", dest="date_to", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    p_report.add_argument("--format", choices=["md", "json"], default="md")
    p_report.add_argument("--out", type=Path, default=None)

    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    wait_for_db()

    if args.command == "backfill":
        outcomes = jobs.job_backfill(args.days, args.source)
        inserted = sum(o.rows_inserted for o in outcomes)
        failed = sum(1 for o in outcomes if o.status == "failed")
        print(f"обработано дат: {len(outcomes)}, записано значений: {inserted}, неудачных прогонов: {failed}")
        return 0

    if args.command == "poll":
        target = args.date or datetime.now(get_settings().zone).date()
        outcome = ingest_date(args.source, target, job="manual")
        print(
            f"{outcome.source_code} за {outcome.target_date}: {outcome.status}, "
            f"получено {outcome.rows_fetched}, записано {outcome.rows_inserted}, "
            f"ревизий {outcome.rows_revised}, в карантине {outcome.rows_quarantined}"
        )
        return 0 if outcome.status in {"ok", "partial"} else 1

    if args.command == "sweep":
        outcomes = jobs.job_sweep()
        print(f"перезапрошено дат: {len(outcomes)}, ревизий: {sum(o.rows_revised for o in outcomes)}")
        return 0

    if args.command == "checks":
        results = jobs.job_state_checks()
        for check in results:
            print(f"  [{check.status}] {check.check_name} {check.series_key or ''} {check.message or ''}")
        return 0

    if args.command == "retention":
        print(f"удалено сырых ответов: {jobs.job_retention()}")
        return 0

    if args.command == "reap":
        print(f"закрыто зависших прогонов: {reap_stale_runs(0)}")
        return 0

    if args.command == "report":
        end = args.date_to or date.today()
        start = args.date_from or (end - timedelta(days=args.days))
        with session_scope() as session:
            report = reporting.build_report(session, start, end)
            body = (
                reporting.render_markdown(report)
                if args.format == "md"
                else json.dumps(report.as_dict(), ensure_ascii=False, indent=2)
            )
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(body, encoding="utf-8")
            print(f"отчёт сохранён: {args.out}")
        else:
            print(body)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
