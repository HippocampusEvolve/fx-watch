"""Начальная схема: битемпоральные наблюдения, журнал прогонов, качество, карантин.

Revision ID: 0001
Revises:

Таблицы описаны явно, а не через ``Base.metadata.create_all``. Разница
принципиальная для сервиса, который живёт месяцами и правится: create_all -
это слепок текущих моделей, поэтому изменение ``models.py`` без новой ревизии
молча даёт разные схемы на новой и на старой базе, и прогон миграций на чистой
базе этого не покажет никогда. Явные ``create_table`` фиксируют схему на момент
ревизии, а расхождение с моделями ловится в CI командой ``alembic check``.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


# Витрина текущего состояния. DISTINCT ON берёт самое свежее наблюдение
# по каждой тройке (источник, ряд, дата) - то есть последнюю известную версию
# значения. Историю при этом никто не трогает, она лежит в observations целиком.
# Ревизия 0002 меняет порядок ранжирования на last_seen_at.
CURRENT_VIEW = """
CREATE OR REPLACE VIEW rates_current AS
SELECT DISTINCT ON (source_code, series_key, value_date)
       source_code,
       series_key,
       value_date,
       value_num,
       nominal,
       observed_at,
       run_id
FROM observations
ORDER BY source_code, series_key, value_date, observed_at DESC, id DESC;
"""

# Сколько раз источник менял мнение по каждой дате. Ровно из этой вьюхи видно,
# что хранится история наших знаний, а не только последнее состояние.
VERSIONS_VIEW = """
CREATE OR REPLACE VIEW rates_versions AS
SELECT source_code,
       series_key,
       value_date,
       count(*)             AS versions,
       min(observed_at)     AS first_seen_at,
       max(observed_at)     AS last_seen_at,
       min(value_num)       AS min_value,
       max(value_num)       AS max_value
FROM observations
GROUP BY source_code, series_key, value_date;
"""


def upgrade() -> None:
    op.create_table(
        "observations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_code", sa.String(length=32), nullable=False),
        sa.Column("series_key", sa.String(length=32), nullable=False),
        sa.Column("value_date", sa.Date(), nullable=False),
        sa.Column("value_num", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("nominal", sa.Integer(), nullable=False),
        sa.Column(
            "observed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.UniqueConstraint(
            "source_code", "series_key", "value_date", "payload_hash", name="uq_observation_value"
        ),
    )
    op.create_index("ix_obs_series_date", "observations", ["source_code", "series_key", "value_date"])
    op.create_index("ix_obs_observed_at", "observations", ["observed_at"])

    op.create_table(
        "ingest_runs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_code", sa.String(length=32), nullable=False),
        sa.Column("job", sa.String(length=32), nullable=False),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("target_date", sa.Date(), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("rows_fetched", sa.Integer(), nullable=False),
        sa.Column("rows_inserted", sa.Integer(), nullable=False),
        sa.Column("rows_revised", sa.Integer(), nullable=False),
        sa.Column("rows_quarantined", sa.Integer(), nullable=False),
        sa.Column("error_class", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("raw_sha256", sa.String(length=64), nullable=True),
        sa.Column("code_version", sa.String(length=64), nullable=False),
    )
    op.create_index("ix_runs_source_started", "ingest_runs", ["source_code", "started_at"])
    op.create_index("ix_runs_status", "ingest_runs", ["status"])

    op.create_table(
        "revisions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_code", sa.String(length=32), nullable=False),
        sa.Column("series_key", sa.String(length=32), nullable=False),
        sa.Column("value_date", sa.Date(), nullable=False),
        sa.Column("old_value", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("new_value", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("delta_pct", sa.Numeric(precision=12, scale=6), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "revised_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("is_significant", sa.Boolean(), nullable=False),
        sa.Column("is_late", sa.Boolean(), nullable=False),
    )
    op.create_index("ix_rev_date", "revisions", ["value_date"])
    op.create_index("ix_rev_revised_at", "revisions", ["revised_at"])

    op.create_table(
        "raw_payloads",
        sa.Column("run_id", sa.BigInteger(), primary_key=True, autoincrement=False),
        sa.Column("source_code", sa.String(length=32), nullable=False),
        sa.Column("content_gzip", sa.LargeBinary(), nullable=False),
        sa.Column("content_type", sa.String(length=128), nullable=True),
        sa.Column("byte_size", sa.Integer(), nullable=False),
        sa.Column(
            "fetched_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
    )
    op.create_index("ix_raw_fetched_at", "raw_payloads", ["fetched_at"])

    op.create_table(
        "quality_checks",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("source_code", sa.String(length=32), nullable=False),
        sa.Column("check_name", sa.String(length=48), nullable=False),
        sa.Column("severity", sa.String(length=8), nullable=False),
        sa.Column("status", sa.String(length=8), nullable=False),
        sa.Column("series_key", sa.String(length=32), nullable=True),
        sa.Column("value_date", sa.Date(), nullable=True),
        sa.Column("observed", sa.String(length=64), nullable=True),
        sa.Column("expected", sa.String(length=64), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column(
            "checked_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
    )
    op.create_index("ix_qc_run", "quality_checks", ["run_id"])
    op.create_index("ix_qc_status_time", "quality_checks", ["status", "checked_at"])

    op.create_table(
        "quarantine",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("source_code", sa.String(length=32), nullable=False),
        sa.Column("series_key", sa.String(length=32), nullable=True),
        sa.Column("value_date", sa.Date(), nullable=True),
        sa.Column("raw_value", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("check_name", sa.String(length=48), nullable=True),
        sa.Column(
            "quarantined_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution", sa.Text(), nullable=True),
    )
    op.create_index("ix_quarantine_time", "quarantine", ["quarantined_at"])

    op.create_table(
        "alerts",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("alert_key", sa.String(length=160), nullable=False),
        sa.Column("severity", sa.String(length=8), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column(
            "first_fired_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "last_fired_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("fire_count", sa.Integer(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered", sa.Boolean(), nullable=False),
        sa.UniqueConstraint("alert_key"),
    )
    op.create_index("ix_alerts_fired", "alerts", ["last_fired_at"])

    op.create_table(
        "source_state",
        sa.Column("source_code", sa.String(length=32), primary_key=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("breaker_open_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
    )

    op.create_table(
        "heartbeats",
        sa.Column("name", sa.String(length=48), primary_key=True),
        sa.Column("last_ok_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expected_interval_sec", sa.Integer(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
    )

    op.execute(CURRENT_VIEW)
    op.execute(VERSIONS_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS rates_versions")
    op.execute("DROP VIEW IF EXISTS rates_current")
    op.drop_table("heartbeats")
    op.drop_table("source_state")
    op.drop_table("alerts")
    op.drop_table("quarantine")
    op.drop_table("quality_checks")
    op.drop_table("raw_payloads")
    op.drop_table("revisions")
    op.drop_table("ingest_runs")
    op.drop_table("observations")
