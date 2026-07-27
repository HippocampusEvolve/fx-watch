"""HTTP-интерфейс: состояние сервиса, данные для дашборда и отчёты.

Отдельно стоит отметить эндпоинт ``/series`` с параметром ``as_of``.
Это не украшение: возможность получить данные в том виде, в каком они были
известны на прошлую дату, - прямое следствие того, что история хранится
как последовательность наблюдений, а не как перезаписываемая таблица.
Именно она позволяет воспроизвести любой прошлый отчёт и проверить,
на каких числах он был построен.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from fxwatch import __version__, reporting
from fxwatch.breaker import all_states
from fxwatch.config import get_settings
from fxwatch.db import SessionLocal
from fxwatch.quality.rules import CHECK_TITLES
from fxwatch.sources import PRIMARY_SOURCE, REGISTRY

log = logging.getLogger(__name__)
PREFIX = "/api/fxwatch"

app = FastAPI(
    title="fx-watch",
    version=__version__,
    description="Сбор курсов валют с историей ревизий и контролем качества данных",
    docs_url=f"{PREFIX}/docs",
    openapi_url=f"{PREFIX}/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_session() -> Session:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _today() -> date:
    """Дата «сегодня» в таймзоне сервиса.

    Контейнер живёт в UTC, планировщик - по МСК. Если считать «сегодня» по UTC,
    то с полуночи до трёх ночи по Москве границы периодов в API разъезжались бы
    с границами, по которым работает сбор.
    """
    return datetime.now(get_settings().zone).date()


# --------------------------------------------------------------------------
# Здоровье
# --------------------------------------------------------------------------

@app.get("/health/live", include_in_schema=False)
def liveness() -> dict[str, str]:
    """Живость процесса. Намеренно не трогает базу: это проверка контейнера."""
    return {"status": "ok", "version": __version__}


@app.get(f"{PREFIX}/health")
def health(session: Session = Depends(get_session)) -> dict[str, Any]:
    """Состояние сервиса и данных.

    Отдаёт не только «процесс жив», но и признаки, по которым видно, что
    поток данных не остановился: время последнего успеха по каждому источнику,
    состояние предохранителей и просроченные регулярные задачи.
    """
    settings = get_settings()
    now = datetime.now(UTC)
    states = all_states(session)

    sources = []
    degraded = False
    for state in states:
        age_hours = (now - state.last_success_at).total_seconds() / 3600 if state.last_success_at else None
        breaker_open = bool(state.breaker_open_until and state.breaker_open_until > now)
        source_ok = (
            age_hours is not None and age_hours <= settings.freshness_hours and not breaker_open
        )
        if state.source_code == PRIMARY_SOURCE and not source_ok:
            degraded = True
        sources.append(
            {
                "source_code": state.source_code,
                "title": REGISTRY[state.source_code].title if state.source_code in REGISTRY else state.source_code,
                "last_success_at": state.last_success_at.isoformat() if state.last_success_at else None,
                "hours_since_success": round(age_hours, 2) if age_hours is not None else None,
                "consecutive_failures": state.consecutive_failures,
                "breaker_open": breaker_open,
                "breaker_open_until": state.breaker_open_until.isoformat() if state.breaker_open_until else None,
                "last_error": state.last_error,
                "healthy": source_ok,
            }
        )

    stale = reporting.stale_heartbeats(session)
    return {
        "status": "degraded" if degraded or stale else "ok",
        "version": __version__,
        "code_version": settings.code_version,
        "checked_at": now.isoformat(),
        "sources": sources,
        "stale_heartbeats": stale,
    }


# --------------------------------------------------------------------------
# Данные
# --------------------------------------------------------------------------

@app.get(f"{PREFIX}/overview")
def get_overview(session: Session = Depends(get_session)) -> dict[str, Any]:
    return reporting.overview(session)


@app.get(f"{PREFIX}/currencies")
def get_currencies(session: Session = Depends(get_session)) -> dict[str, Any]:
    rows = session.execute(
        text(
            """
            SELECT series_key, count(*) AS observations,
                   min(value_date) AS first_date, max(value_date) AS last_date
            FROM observations WHERE source_code = :src
            GROUP BY series_key ORDER BY series_key
            """
        ),
        {"src": PRIMARY_SOURCE},
    ).all()
    return {
        "items": [
            {
                "series_key": r[0], "code": r[0].split("/")[0], "observations": r[1],
                "first_date": r[2].isoformat(), "last_date": r[3].isoformat(),
            }
            for r in rows
        ]
    }


@app.get(f"{PREFIX}/series")
def get_series(
    code: str = Query("USD/RUB", description="Ключ ряда, например USD/RUB"),
    days: int = Query(90, ge=1, le=3650),
    as_of: datetime | None = Query(
        None, description="Показать данные в том виде, в каком они были известны на этот момент"
    ),
    source: str = Query(PRIMARY_SOURCE),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    """Ряд значений за период.

    Без ``as_of`` отдаётся текущее знание: версия, которую источник подтверждал
    последней. С ``as_of`` - состояние на указанный момент, то есть наблюдения,
    полученные позже, игнорируются. Разница между этими двумя выборками и есть
    ревизии.

    Ранжирование в двух режимах разное, и это намеренно. Текущее состояние
    считается по ``last_seen_at``: если источник вернулся к значению, которое
    уже отдавал, текущим должно стать оно, а не то, что мы увидели позже.
    Реконструкция прошлого считается по ``observed_at``: на тот момент сервис
    знал ровно то, что успел получить, а более поздние подтверждения к его
    тогдашнему знанию отношения не имеют.
    """
    end = _today()
    start = end - timedelta(days=days)

    rows = session.execute(
        text(
            """
            SELECT DISTINCT ON (value_date) value_date, value_num, observed_at
            FROM observations
            WHERE source_code = :src AND series_key = :series
              AND value_date BETWEEN :start AND :end
              AND (CAST(:as_of AS timestamptz) IS NULL OR observed_at <= CAST(:as_of AS timestamptz))
            ORDER BY value_date,
                     CASE WHEN CAST(:as_of AS timestamptz) IS NULL THEN last_seen_at ELSE observed_at END DESC,
                     id DESC
            """
        ),
        {"src": source, "series": code, "start": start, "end": end, "as_of": as_of},
    ).all()

    revisions = session.execute(
        text(
            """
            SELECT value_date, old_value, new_value, delta_pct, revised_at, is_late
            FROM revisions
            WHERE source_code = :src AND series_key = :series AND value_date BETWEEN :start AND :end
            ORDER BY value_date
            """
        ),
        {"src": source, "series": code, "start": start, "end": end},
    ).all()

    return {
        "series_key": code,
        "source": source,
        "as_of": as_of.isoformat() if as_of else None,
        "points": [
            {"date": r[0].isoformat(), "value": float(r[1]), "observed_at": r[2].isoformat()} for r in rows
        ],
        "revisions": [
            {
                "date": r[0].isoformat(), "old_value": float(r[1]), "new_value": float(r[2]),
                "delta_pct": float(r[3]), "revised_at": r[4].isoformat(), "is_late": r[5],
            }
            for r in revisions
        ],
    }


@app.get(f"{PREFIX}/history")
def get_point_history(
    code: str = Query(..., description="Ключ ряда"),
    day: date = Query(..., description="Дата значения"),
    source: str = Query(PRIMARY_SOURCE),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    """Все версии одного значения: что и когда сервис знал об этой дате.

    ``last_seen_at`` показывает, когда источник подтверждал версию в последний
    раз. По нему видно и возврат к прежнему значению: у старой версии время
    подтверждения окажется свежее, чем у той, что появилась после неё.
    """
    rows = session.execute(
        text(
            """
            SELECT value_num, observed_at, last_seen_at, run_id, payload_hash
            FROM observations
            WHERE source_code = :src AND series_key = :series AND value_date = :day
            ORDER BY observed_at
            """
        ),
        {"src": source, "series": code, "day": day},
    ).all()
    current_hash = max(rows, key=lambda r: r[2])[4] if rows else None
    return {
        "series_key": code,
        "value_date": day.isoformat(),
        "versions": [
            {
                "value": float(r[0]), "observed_at": r[1].isoformat(),
                "last_seen_at": r[2].isoformat(), "run_id": r[3],
                "payload_hash": r[4][:12], "is_current": r[4] == current_hash,
            }
            for r in rows
        ],
    }


# --------------------------------------------------------------------------
# Наблюдаемость
# --------------------------------------------------------------------------

@app.get(f"{PREFIX}/runs")
def get_runs(
    limit: int = Query(50, ge=1, le=500),
    status: str | None = Query(None),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    query = """
        SELECT id, source_code, job, started_at, finished_at, status, target_date,
               http_status, attempt_count, duration_ms, rows_fetched, rows_inserted,
               rows_revised, rows_quarantined, error_class, error_message, code_version
        FROM ingest_runs
        {where}
        ORDER BY started_at DESC LIMIT :limit
    """.format(where="WHERE status = :status" if status else "")
    params: dict[str, Any] = {"limit": limit}
    if status:
        params["status"] = status

    rows = session.execute(text(query), params).all()
    return {
        "items": [
            {
                "id": r[0], "source_code": r[1], "job": r[2],
                "started_at": r[3].isoformat(), "finished_at": r[4].isoformat() if r[4] else None,
                "status": r[5], "target_date": r[6].isoformat() if r[6] else None,
                "http_status": r[7], "attempt_count": r[8], "duration_ms": r[9],
                "rows_fetched": r[10], "rows_inserted": r[11], "rows_revised": r[12],
                "rows_quarantined": r[13], "error_class": r[14], "error_message": r[15],
                "code_version": r[16],
            }
            for r in rows
        ]
    }


@app.get(f"{PREFIX}/incidents")
def get_incidents(days: int = Query(90, ge=1, le=3650), session: Session = Depends(get_session)) -> dict[str, Any]:
    end = _today()
    start = end - timedelta(days=days)
    incidents = reporting.detect_incidents(session, start, end)
    return {"items": [i.as_dict() for i in incidents]}


@app.get(f"{PREFIX}/revisions")
def get_revisions(
    days: int = Query(90, ge=1, le=3650),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    start = _today() - timedelta(days=days)
    rows = session.execute(
        text(
            """
            SELECT source_code, series_key, value_date, old_value, new_value,
                   delta_pct, first_observed_at, revised_at, is_significant, is_late
            FROM revisions WHERE revised_at >= :start
            ORDER BY revised_at DESC LIMIT :limit
            """
        ),
        {"start": start, "limit": limit},
    ).all()
    return {
        "items": [
            {
                "source_code": r[0], "series_key": r[1], "value_date": r[2].isoformat(),
                "old_value": float(r[3]), "new_value": float(r[4]), "delta_pct": float(r[5]),
                "first_observed_at": r[6].isoformat(), "revised_at": r[7].isoformat(),
                "is_significant": r[8], "is_late": r[9],
            }
            for r in rows
        ]
    }


@app.get(f"{PREFIX}/checks")
def get_checks(
    days: int = Query(7, ge=1, le=365),
    only_failed: bool = Query(False),
    limit: int = Query(200, ge=1, le=2000),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    start = datetime.now(UTC) - timedelta(days=days)
    condition = "AND status = 'fail'" if only_failed else ""
    rows = session.execute(
        text(
            f"""
            SELECT check_name, severity, status, series_key, value_date,
                   observed, expected, message, checked_at, run_id
            FROM quality_checks WHERE checked_at >= :start {condition}
            ORDER BY checked_at DESC LIMIT :limit
            """
        ),
        {"start": start, "limit": limit},
    ).all()

    summary = session.execute(
        text(
            """
            SELECT check_name, status, count(*) FROM quality_checks
            WHERE checked_at >= :start GROUP BY check_name, status
            """
        ),
        {"start": start},
    ).all()

    by_check: dict[str, dict[str, Any]] = {}
    for name, status, count in summary:
        entry = by_check.setdefault(name, {"title": CHECK_TITLES.get(name, name), "pass": 0, "fail": 0, "skip": 0})
        entry[status] = count

    return {
        "summary": by_check,
        "items": [
            {
                "check_name": r[0], "title": CHECK_TITLES.get(r[0], r[0]), "severity": r[1],
                "status": r[2], "series_key": r[3], "value_date": r[4].isoformat() if r[4] else None,
                "observed": r[5], "expected": r[6], "message": r[7],
                "checked_at": r[8].isoformat(), "run_id": r[9],
            }
            for r in rows
        ],
    }


@app.get(f"{PREFIX}/quarantine")
def get_quarantine(
    limit: int = Query(100, ge=1, le=1000), session: Session = Depends(get_session)
) -> dict[str, Any]:
    rows = session.execute(
        text(
            """
            SELECT id, run_id, source_code, series_key, value_date, raw_value,
                   reason, check_name, quarantined_at, resolved_at, last_seen_at, seen_count
            FROM quarantine ORDER BY last_seen_at DESC LIMIT :limit
            """
        ),
        {"limit": limit},
    ).all()
    return {
        "items": [
            {
                "id": r[0], "run_id": r[1], "source_code": r[2], "series_key": r[3],
                "value_date": r[4].isoformat() if r[4] else None, "raw_value": r[5],
                "reason": r[6], "check_name": r[7], "quarantined_at": r[8].isoformat(),
                "resolved_at": r[9].isoformat() if r[9] else None,
                "last_seen_at": r[10].isoformat(), "seen_count": r[11],
            }
            for r in rows
        ]
    }


@app.get(f"{PREFIX}/report")
def get_report(
    days: int = Query(90, ge=1, le=3650),
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    end = date_to or _today()
    start = date_from or (end - timedelta(days=days))
    if start > end:
        raise HTTPException(status_code=400, detail="начало периода позже конца")
    return reporting.build_report(session, start, end).as_dict()


@app.get(f"{PREFIX}/report.md", response_class=PlainTextResponse)
def get_report_markdown(
    days: int = Query(90, ge=1, le=3650),
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
    session: Session = Depends(get_session),
) -> str:
    end = date_to or _today()
    start = date_from or (end - timedelta(days=days))
    if start > end:
        raise HTTPException(status_code=400, detail="начало периода позже конца")
    return reporting.render_markdown(reporting.build_report(session, start, end))
