"""Отчётность.

Это ответ на требование 6 ТЗ. Вопрос там поставлен так: сервис проработал три
месяца без присмотра, что нужно, чтобы быстро понять, что всё это время он
работал правильно.

Отдельно стоит зафиксировать: «сервис жив» и «данные корректны» - разные
утверждения, и первое не доказывает второе. Процесс может исправно подниматься,
ходить в сеть и писать в базу, при этом полгода складывая туда мусор.
Поэтому отчёт отвечает не на вопрос «был ли аптайм», а на четыре других:

1. **покрытие** - за сколько ожидаемых рабочих дней данные реально есть;
2. **инциденты** - когда сбор ломался, насколько надолго и восстановился ли сам;
3. **ревизии** - где источник менял ранее отданные значения и насколько сильно;
4. **качество** - какие проверки не проходили и что попало в карантин.

Каждый пункт считается из журнала, а не из памяти о том, «как оно работало».
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from fxwatch.calendar import DayKind, classify_days, expected_business_days
from fxwatch.config import get_settings
from fxwatch.sources import PRIMARY_SOURCE


@dataclass(slots=True)
class Incident:
    started_at: datetime
    ended_at: datetime | None
    duration_min: int
    failed_runs: int
    kind: str
    detail: str
    recovered: bool
    data_lost: bool = False  # осталась ли после инцидента дыра в истории

    def as_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_min": self.duration_min,
            "failed_runs": self.failed_runs,
            "kind": self.kind,
            "detail": self.detail,
            "recovered": self.recovered,
            "data_lost": self.data_lost,
        }


@dataclass(slots=True)
class PeriodReport:
    start: date
    end: date
    generated_at: datetime
    coverage: dict[str, Any] = field(default_factory=dict)
    runs: dict[str, Any] = field(default_factory=dict)
    incidents: list[Incident] = field(default_factory=list)
    revisions: dict[str, Any] = field(default_factory=dict)
    quality: dict[str, Any] = field(default_factory=dict)
    versions: list[dict[str, Any]] = field(default_factory=list)
    verdict: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "period": {"from": self.start.isoformat(), "to": self.end.isoformat()},
            "generated_at": self.generated_at.isoformat(),
            "verdict": self.verdict,
            "coverage": self.coverage,
            "runs": self.runs,
            "incidents": [i.as_dict() for i in self.incidents],
            "revisions": self.revisions,
            "quality": self.quality,
            "code_versions": self.versions,
        }


# --------------------------------------------------------------------------
# Составные части
# --------------------------------------------------------------------------

def compute_coverage(session: Session, start: date, end: date, source_code: str = PRIMARY_SOURCE) -> dict[str, Any]:
    """Доля ожидаемых рабочих дней, за которые данные действительно есть.

    Главная метрика отчёта. Аптайм процесса ничего не говорит о полноте истории,
    а покрытие говорит: если оно ниже ста процентов, в истории есть дни,
    за которые данных нет, и отчёт называет их поимённо.
    """
    kinds = classify_days(session, start, end, source_code)
    business = [d for d, k in kinds.items() if k == DayKind.BUSINESS]
    non_business = [d for d, k in kinds.items() if k == DayKind.NON_BUSINESS]
    unknown = sorted(d for d, k in kinds.items() if k == DayKind.UNKNOWN)
    expected = expected_business_days(session, start, end)

    return {
        "expected_business_days": expected,
        "covered_days": len(business),
        "non_business_days": len(non_business),
        "gap_days": [d.isoformat() for d in unknown],
        "coverage_pct": round(len(business) / expected * 100, 2) if expected else 100.0,
    }


def summarize_runs(session: Session, start: date, end: date) -> dict[str, Any]:
    rows = session.execute(
        text(
            """
            SELECT source_code, status, count(*) AS cnt,
                   sum(duration_ms) AS sum_ms,
                   count(duration_ms) AS timed,
                   max(duration_ms) AS max_ms,
                   sum(attempt_count) AS attempts
            FROM ingest_runs
            WHERE started_at >= :start AND started_at < :end
            GROUP BY source_code, status
            ORDER BY source_code, status
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).all()

    by_source: dict[str, dict[str, Any]] = {}
    totals = {"ok": 0, "partial": 0, "failed": 0, "skipped": 0, "running": 0}
    for source_code, status, cnt, sum_ms, timed, max_ms, attempts in rows:
        entry = by_source.setdefault(
            source_code,
            {
                "ok": 0, "partial": 0, "failed": 0, "skipped": 0, "running": 0,
                "avg_ms": 0, "max_ms": 0, "_sum_ms": 0, "_timed": 0,
            },
        )
        entry[status] = cnt
        # Среднее считается по всем прогонам источника разом. Брать максимум
        # из средних по статусам, как было раньше, значило показывать не среднее,
        # а длительность самой медленной группы.
        entry["_sum_ms"] += int(sum_ms or 0)
        entry["_timed"] += int(timed or 0)
        entry["avg_ms"] = round(entry["_sum_ms"] / entry["_timed"]) if entry["_timed"] else 0
        entry["max_ms"] = max(entry["max_ms"], max_ms or 0)
        entry["retries"] = entry.get("retries", 0) + int(attempts or 0)
        totals[status] = totals.get(status, 0) + cnt

    for entry in by_source.values():
        entry.pop("_sum_ms", None)
        entry.pop("_timed", None)

    total = sum(totals.values())
    success = totals["ok"] + totals["partial"]
    return {
        "total": total,
        "by_status": totals,
        "by_source": by_source,
        "success_rate_pct": round(success / total * 100, 2) if total else 0.0,
    }


def detect_incidents(session: Session, start: date, end: date) -> list[Incident]:
    """Сгруппировать неудачные прогоны в события.

    Смысл группировки в том, что «28 упавших прогонов» не говорит ничего,
    а «инцидент на двое суток, восстановился сам, данные добраны» - говорит.
    Читатель отчёта должен видеть события, а не строки лога.

    Границей события служит успешный прогон, а не пауза между неудачами.
    Во время долгого сбоя попытки идут с разной частотой - ночной перезапрос,
    дневное окно, вечерний контроль, - и разрез по паузе дробил бы один
    двухдневный сбой на несколько коротких, занижая его масштаб в отчёте.
    """
    rows = session.execute(
        text(
            """
            SELECT source_code, started_at, status, error_class, error_message
            FROM ingest_runs
            WHERE started_at >= :start AND started_at < :end
              AND status IN ('failed', 'skipped', 'ok', 'partial')
            ORDER BY source_code, started_at
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).all()

    incidents: list[Incident] = []
    current: dict[str, Any] | None = None

    for source_code, started_at, status, error_class, error_message in rows:
        if current is not None and current["source"] != source_code:
            incidents.append(_finalize_incident(session, current))
            current = None

        if status in ("ok", "partial"):
            if current is not None:
                incidents.append(_finalize_incident(session, current, recovered_at=started_at))
                current = None
            continue

        if current is None:
            current = {
                "source": source_code, "first": started_at, "last": started_at, "count": 1,
                "kind": error_class or status, "detail": (error_message or status)[:300],
            }
        else:
            current["last"] = started_at
            current["count"] += 1

    if current is not None:
        incidents.append(_finalize_incident(session, current))

    incidents.sort(key=lambda i: i.started_at, reverse=True)
    return incidents


def _finalize_incident(
    session: Session, raw: dict[str, Any], recovered_at: datetime | None = None
) -> Incident:
    if recovered_at is None:
        # Инцидент не закрылся внутри периода - ищем первый успех за его пределами.
        recovered_at = session.execute(
            text(
                """
                SELECT min(started_at) FROM ingest_runs
                WHERE source_code = :src AND started_at > :last AND status IN ('ok', 'partial')
                """
            ),
            {"src": raw["source"], "last": raw["last"]},
        ).scalar()

    # Длительность - до восстановления, а не до последней неудачи: всё это время
    # данных не поступало, и читателю важна именно эта цифра.
    end_stamp = recovered_at or raw["last"]
    duration = (end_stamp - raw["first"]).total_seconds() / 60
    return Incident(
        started_at=raw["first"],
        ended_at=recovered_at,
        duration_min=int(duration),
        failed_runs=raw["count"],
        kind=f"{raw['source']}: {raw['kind']}",
        detail=raw["detail"],
        recovered=recovered_at is not None,
    )


def summarize_revisions(session: Session, start: date, end: date) -> dict[str, Any]:
    row = session.execute(
        text(
            """
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE is_significant) AS significant,
                   count(*) FILTER (WHERE is_late) AS late,
                   count(*) FILTER (WHERE is_rollback) AS rollbacks,
                   coalesce(max(delta_pct), 0) AS max_delta
            FROM revisions
            WHERE revised_at >= :start AND revised_at < :end
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).one()

    top = session.execute(
        text(
            """
            SELECT series_key, value_date, old_value, new_value, delta_pct, revised_at, is_late
            FROM revisions
            WHERE revised_at >= :start AND revised_at < :end AND is_significant
            ORDER BY delta_pct DESC
            LIMIT 10
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).all()

    return {
        "total": row.total,
        "significant": row.significant,
        "late": row.late,
        "rollbacks": row.rollbacks,
        "max_delta_pct": float(row.max_delta or Decimal(0)),
        "top": [
            {
                "series_key": r[0], "value_date": r[1].isoformat(),
                "old_value": float(r[2]), "new_value": float(r[3]),
                "delta_pct": float(r[4]), "revised_at": r[5].isoformat(), "is_late": r[6],
            }
            for r in top
        ],
    }


def summarize_quality(session: Session, start: date, end: date) -> dict[str, Any]:
    checks = session.execute(
        text(
            """
            SELECT check_name, status, severity, count(*) FROM quality_checks
            WHERE checked_at >= :start AND checked_at < :end
            GROUP BY check_name, status, severity ORDER BY check_name
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).all()

    by_check: dict[str, dict[str, int]] = {}
    # Провалы разной серьёзности требуют разного: скачок курса на 12% принимается
    # с предупреждением, а неположительное значение отправляется в карантин.
    # Вердикт отчёта смотрит именно на эту разницу, поэтому она считается отдельно.
    failed_blocking = 0
    failed_warning = 0
    for check_name, status, severity, count in checks:
        slot = by_check.setdefault(check_name, {})
        slot[status] = slot.get(status, 0) + count
        if status == "fail":
            if severity == "error":
                failed_blocking += count
            else:
                failed_warning += count

    quarantine = session.execute(
        text(
            """
            SELECT count(*) AS total, count(*) FILTER (WHERE resolved_at IS NULL) AS open
            FROM quarantine WHERE quarantined_at >= :start AND quarantined_at < :end
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).one()

    alerts = session.execute(
        text(
            """
            SELECT severity,
                   count(*) AS unique_keys,
                   sum(fire_count) AS fired,
                   count(*) FILTER (WHERE resolved_at IS NULL) AS still_open
            FROM alerts
            WHERE last_fired_at >= :start AND last_fired_at < :end
            GROUP BY severity
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).all()

    open_alerts = session.execute(
        text(
            """
            SELECT alert_key, severity, title, last_fired_at, fire_count
            FROM alerts
            WHERE resolved_at IS NULL AND last_fired_at >= :start AND last_fired_at < :end
            ORDER BY severity, last_fired_at DESC
            LIMIT 20
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).all()

    return {
        "by_check": by_check,
        "failed_total": sum(v.get("fail", 0) for v in by_check.values()),
        "failed_blocking": failed_blocking,
        "failed_warning": failed_warning,
        "quarantine": {"total": quarantine.total, "open": quarantine.open},
        "alerts": [
            {"severity": a[0], "unique": a[1], "fired": int(a[2] or 0), "open": a[3]} for a in alerts
        ],
        "alerts_open": sum(a[3] for a in alerts),
        "open_alerts": [
            {
                "key": a[0], "severity": a[1], "title": a[2],
                "last_fired_at": a[3].isoformat(), "fire_count": a[4],
            }
            for a in open_alerts
        ],
    }


def code_versions(session: Session, start: date, end: date) -> list[dict[str, Any]]:
    """Какие версии кода работали в периоде.

    Без этого через три месяца невозможно ответить на вопрос, изменилось ли
    поведение из-за источника или из-за нашего же деплоя.
    """
    rows = session.execute(
        text(
            """
            SELECT code_version, min(started_at), max(started_at), count(*)
            FROM ingest_runs WHERE started_at >= :start AND started_at < :end
            GROUP BY code_version ORDER BY min(started_at)
            """
        ),
        {"start": start, "end": end + timedelta(days=1)},
    ).all()
    return [
        {"version": r[0], "from": r[1].isoformat(), "to": r[2].isoformat(), "runs": r[3]}
        for r in rows
    ]


def stale_heartbeats(session: Session) -> list[dict[str, Any]]:
    """Задачи, которые перестали отмечаться.

    Обратная логика по сравнению с обычным алертом: тревогу вызывает не событие,
    а его отсутствие. Задача, которая тихо перестала запускаться, иначе была бы
    неотличима от задачи, у которой всё хорошо.
    """
    now = datetime.now(UTC)
    rows = session.execute(text("SELECT name, last_ok_at, expected_interval_sec FROM heartbeats")).all()
    stale = []
    for name, last_ok_at, interval in rows:
        if last_ok_at is None:
            stale.append({"name": name, "last_ok_at": None, "overdue_sec": None})
            continue
        overdue = (now - last_ok_at).total_seconds() - interval
        if overdue > 0:
            stale.append({"name": name, "last_ok_at": last_ok_at.isoformat(), "overdue_sec": int(overdue)})
    return stale


# --------------------------------------------------------------------------
# Сводки
# --------------------------------------------------------------------------

def build_report(session: Session, start: date, end: date) -> PeriodReport:
    report = PeriodReport(start=start, end=end, generated_at=datetime.now(UTC))
    report.coverage = compute_coverage(session, start, end)
    report.runs = summarize_runs(session, start, end)
    report.incidents = detect_incidents(session, start, end)
    report.revisions = summarize_revisions(session, start, end)
    report.quality = summarize_quality(session, start, end)
    report.versions = code_versions(session, start, end)
    report.verdict = _verdict(report)
    return report


def _verdict(report: PeriodReport) -> str:
    """Одна строка, ради которой отчёт и открывают.

    Она обязана смотреть на весь отчёт, а не на его часть. Вердикт
    «вмешательство не требуется» при непройденных проверках и открытых алертах
    в том же документе - худший вид мониторинга: он не просто бесполезен,
    он отговаривает читать остальное.

    Поэтому градаций три:

    * что-то сломано и не починилось само - «требует внимания»;
    * данные полные, но есть замечания уровня предупреждения (например, скачок
      курса на 12% - это рынок, а не сбой) - говорим и о том, и о другом;
    * чисто везде - только тогда «вмешательство не требуется».
    """
    coverage = report.coverage.get("coverage_pct", 0)
    gaps = len(report.coverage.get("gap_days", []))
    unresolved = [i for i in report.incidents if not i.recovered]
    quality = report.quality
    open_quarantine = quality.get("quarantine", {}).get("open", 0)
    open_alerts = quality.get("alerts_open", 0)
    failed_blocking = quality.get("failed_blocking", 0)
    failed_warning = quality.get("failed_warning", 0)

    problems = []
    if gaps:
        problems.append(f"дней без данных: {gaps}")
    if coverage < 100:
        problems.append(f"покрытие {coverage}%")
    if unresolved:
        problems.append(f"незавершённых инцидентов: {len(unresolved)}")
    if open_quarantine:
        problems.append(f"записей в карантине не разобрано: {open_quarantine}")
    if open_alerts:
        problems.append(f"открытых алертов: {open_alerts}")
    if failed_blocking and not open_quarantine:
        # Блокирующие провалы уже разобраны, но умолчать о них нельзя.
        problems.append(f"блокирующих провалов проверок: {failed_blocking}")
    if problems:
        return "Требует внимания: " + ", ".join(problems)

    if failed_warning:
        return (
            "Данные полные, все сбои восстановились автоматически; "
            f"замечаний уровня предупреждения: {failed_warning} - данные приняты, разбор не требуется"
        )
    return "Данные полные, все сбои восстановились автоматически, вмешательство не требуется"


def overview(session: Session) -> dict[str, Any]:
    """Компактная сводка текущего состояния для дашборда и /health."""
    settings = get_settings()
    today = datetime.now(settings.zone).date()
    start = today - timedelta(days=90)

    last_runs = session.execute(
        text(
            """
            SELECT DISTINCT ON (source_code) source_code, started_at, status, rows_inserted, duration_ms
            FROM ingest_runs ORDER BY source_code, started_at DESC
            """
        )
    ).all()

    last_values = session.execute(
        text(
            """
            SELECT series_key, value_date, value_num, observed_at
            FROM rates_current
            WHERE source_code = :src AND series_key IN ('USD/RUB', 'EUR/RUB', 'CNY/RUB')
              AND value_date = (SELECT max(value_date) FROM rates_current WHERE source_code = :src)
            ORDER BY series_key
            """
        ),
        {"src": PRIMARY_SOURCE},
    ).all()

    totals = session.execute(
        text(
            """
            SELECT (SELECT count(*) FROM observations) AS observations,
                   (SELECT count(*) FROM ingest_runs) AS runs,
                   (SELECT count(*) FROM revisions) AS revisions,
                   (SELECT count(*) FROM quarantine WHERE resolved_at IS NULL) AS quarantine_open,
                   (SELECT count(*) FROM alerts WHERE resolved_at IS NULL) AS alerts_open,
                   (SELECT count(DISTINCT series_key) FROM observations) AS series
            """
        )
    ).one()

    return {
        "today": today.isoformat(),
        "coverage_90d": compute_coverage(session, start, today),
        "totals": {
            "observations": totals.observations,
            "runs": totals.runs,
            "revisions": totals.revisions,
            "quarantine_open": totals.quarantine_open,
            "alerts_open": totals.alerts_open,
            "series": totals.series,
        },
        "sources": [
            {
                "source_code": r[0], "last_run_at": r[1].isoformat(), "status": r[2],
                "rows_inserted": r[3], "duration_ms": r[4],
            }
            for r in last_runs
        ],
        "latest": [
            {
                "series_key": r[0], "value_date": r[1].isoformat(),
                "value": float(r[2]), "observed_at": r[3].isoformat(),
            }
            for r in last_values
        ],
        "stale_heartbeats": stale_heartbeats(session),
    }


# --------------------------------------------------------------------------
# Markdown
# --------------------------------------------------------------------------

def _fmt_duration(minutes: int) -> str:
    if minutes >= 180:
        return f"{minutes / 60:.0f} ч"
    return f"{minutes} мин"


def render_markdown(report: PeriodReport) -> str:
    """Отчёт в виде файла, который можно закоммитить и прочитать без сервиса."""
    c, r, q, rev = report.coverage, report.runs, report.quality, report.revisions
    lines: list[str] = [
        f"# Отчёт о работе сервиса: {report.start.isoformat()} - {report.end.isoformat()}",
        "",
        f"Сформирован: {report.generated_at.strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"**Вывод: {report.verdict}**",
        "",
    ]

    demo_runs = sum(v["runs"] for v in report.versions if v["version"] == "demo-seed")
    if demo_runs:
        lines += [
            f"> Внимание: {demo_runs} прогонов в периоде помечены версией кода `demo-seed` - "
            "это синтетические инциденты, залитые для демонстрации отчёта на молодом стенде. "
            "Удаляются целиком командой `make seed-clean`.",
            "",
        ]

    lines += [
        "## 1. Покрытие данными",
        "",
        "| Показатель | Значение |",
        "|---|---|",
        f"| Ожидалось рабочих дней | {c['expected_business_days']} |",
        f"| Есть данные за | {c['covered_days']} |",
        f"| Нерабочих дней в периоде | {c['non_business_days']} |",
        f"| Покрытие | {c['coverage_pct']}% |",
        f"| Дней без данных | {len(c['gap_days'])} |",
        "",
    ]
    if c["gap_days"]:
        lines += ["Даты без данных: " + ", ".join(c["gap_days"]), ""]
    else:
        lines += ["Пропусков нет: за каждый рабочий день периода данные получены.", ""]

    lines += [
        "## 2. Прогоны",
        "",
        "| Показатель | Значение |",
        "|---|---|",
        f"| Всего обращений к источникам | {r['total']} |",
        f"| Успешных | {r['by_status'].get('ok', 0)} |",
        f"| Частично успешных | {r['by_status'].get('partial', 0)} |",
        f"| Неудачных | {r['by_status'].get('failed', 0)} |",
        f"| Пропущено предохранителем | {r['by_status'].get('skipped', 0)} |",
        f"| Доля успешных | {r['success_rate_pct']}% |",
        "",
        "## 3. Инциденты",
        "",
    ]
    if not report.incidents:
        lines += ["За период сбоев не было.", ""]
    else:
        lines += ["| Начало | Длительность | Событие | Восстановление |", "|---|---|---|---|"]
        for incident in report.incidents:
            recovery = (
                f"автоматически, {incident.ended_at.strftime('%d.%m %H:%M')}"
                if incident.recovered and incident.ended_at
                else "не восстановился"
            )
            lines.append(
                f"| {incident.started_at.strftime('%d.%m.%Y %H:%M')} | {_fmt_duration(incident.duration_min)} "
                f"({incident.failed_runs} попыток) | {incident.kind} | {recovery} |"
            )
        lines.append("")

    lines += [
        "## 4. Ревизии источника",
        "",
        f"Всего изменений ранее полученных значений: **{rev['total']}**, "
        f"из них значимых: **{rev['significant']}**, "
        f"вне окна перезапроса: **{rev['late']}**, "
        f"возвратов к прежнему значению: **{rev.get('rollbacks', 0)}**. "
        f"Максимальное расхождение: {rev['max_delta_pct']:.4f}%.",
        "",
    ]
    if rev["top"]:
        lines += ["| Ряд | Дата | Было | Стало | Расхождение | Замечено |", "|---|---|---|---|---|---|"]
        for item in rev["top"]:
            lines.append(
                f"| {item['series_key']} | {item['value_date']} | {item['old_value']} | "
                f"{item['new_value']} | {item['delta_pct']:.4f}% | {item['revised_at'][:16].replace('T', ' ')} |"
            )
        lines.append("")

    lines += ["## 5. Качество данных", "", "| Проверка | Пройдено | Не пройдено | Пропущено |", "|---|---|---|---|"]
    from fxwatch.quality.rules import CHECK_TITLES

    for name, stats in sorted(q["by_check"].items()):
        title = CHECK_TITLES.get(name, name)
        lines.append(f"| {title} | {stats.get('pass', 0)} | {stats.get('fail', 0)} | {stats.get('skip', 0)} |")
    lines += [
        "",
        f"В карантине за период: {q['quarantine']['total']}, из них не разобрано: {q['quarantine']['open']}. "
        f"Провалов уровня «карантин»: {q.get('failed_blocking', 0)}, "
        f"уровня «предупреждение»: {q.get('failed_warning', 0)}.",
        "",
    ]

    open_alerts = q.get("open_alerts", [])
    if open_alerts:
        lines += [
            f"Открытых алертов: {q.get('alerts_open', len(open_alerts))} - условие ещё не вернулось в норму.",
            "",
            "| Серьёзность | Событие | Срабатываний | Последнее |",
            "|---|---|---|---|",
        ]
        for alert in open_alerts:
            lines.append(
                f"| {alert['severity']} | {alert['title']} | {alert['fire_count']} | "
                f"{alert['last_fired_at'][:16].replace('T', ' ')} |"
            )
        lines.append("")
    else:
        lines += ["Открытых алертов нет: все сработавшие условия вернулись в норму.", ""]

    lines += [
        "## 6. Версии кода в периоде",
        "",
        "| Версия | С | По | Прогонов |",
        "|---|---|---|---|",
    ]
    for version in report.versions:
        lines.append(
            f"| `{version['version']}` | {version['from'][:16].replace('T', ' ')} | "
            f"{version['to'][:16].replace('T', ' ')} | {version['runs']} |"
        )
    lines += [
        "",
        "---",
        "",
        "Отчёт собран из журнала прогонов, а не из внешнего мониторинга: "
        "все цифры воспроизводимы запросом к базе за тот же период.",
        "",
    ]
    return "\n".join(lines)
