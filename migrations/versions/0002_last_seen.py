"""Отметка последнего подтверждения значения, откаты источника, склейка карантина.

Revision ID: 0002
Revises: 0001

Зачем это понадобилось. Уникальный ключ по хэшу значения делал сбор
идемпотентным, но у него была слепая зона: последовательность A -> B -> A.
Третья попытка натыкалась на уже существующую строку A, и витрина,
ранжировавшая версии по ``observed_at``, навсегда оставалась на B, хотя
источник отдаёт A. То есть ровно в требовании про «источник отдал другое
значение за тот же период» терялось одно из возможных изменений.

Отсюда ``last_seen_at``: ``observed_at`` остаётся временем первого появления
версии, а текущей считается та версия, которую источник подтверждал последней.
Возврат к прежнему значению при этом записывается как ревизия с флагом
``is_rollback`` - новой строки наблюдения не появляется, но событие есть.

Заодно карантин перестаёт дублироваться: окно перезапроса трогает те же даты
каждую ночь, и одно битое значение давало по строке в сутки.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


CURRENT_VIEW_NEW = """
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
ORDER BY source_code, series_key, value_date, last_seen_at DESC, observed_at DESC, id DESC;
"""

CURRENT_VIEW_OLD = """
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

VERSIONS_VIEW_NEW = """
CREATE OR REPLACE VIEW rates_versions AS
SELECT source_code,
       series_key,
       value_date,
       count(*)             AS versions,
       min(observed_at)     AS first_seen_at,
       max(last_seen_at)    AS last_seen_at,
       min(value_num)       AS min_value,
       max(value_num)       AS max_value
FROM observations
GROUP BY source_code, series_key, value_date;
"""

VERSIONS_VIEW_OLD = """
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
    op.add_column(
        "observations",
        sa.Column(
            "last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
    )
    # У уже накопленной истории момент подтверждения неизвестен, и единственное
    # честное значение - момент первого наблюдения: до этой ревизии повторные
    # подтверждения нигде не фиксировались.
    op.execute("UPDATE observations SET last_seen_at = observed_at")
    op.create_index("ix_obs_last_seen_at", "observations", ["last_seen_at"])

    op.add_column(
        "revisions",
        sa.Column("is_rollback", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.alter_column("revisions", "is_rollback", server_default=None)

    op.add_column(
        "quarantine",
        sa.Column(
            "last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
    )
    op.add_column(
        "quarantine",
        sa.Column("seen_count", sa.Integer(), server_default=sa.text("1"), nullable=False),
    )
    op.alter_column("quarantine", "seen_count", server_default=None)
    op.execute("UPDATE quarantine SET last_seen_at = quarantined_at")
    op.create_index(
        "ix_quarantine_slot", "quarantine", ["source_code", "series_key", "value_date", "check_name"]
    )

    # Отметка живости опроса переехала: раньше её ставил сам сбор, то есть
    # в штатном дне, когда страховочный опрос намеренно молчит, она протухала
    # и /health вечно показывал degraded. Теперь её ставит окно опроса
    # под именем poll_window:<источник>, а старые записи только мешают.
    op.execute("DELETE FROM heartbeats WHERE name LIKE 'poll:%'")

    op.execute(CURRENT_VIEW_NEW)
    op.execute(VERSIONS_VIEW_NEW)


def downgrade() -> None:
    op.execute(VERSIONS_VIEW_OLD)
    op.execute(CURRENT_VIEW_OLD)
    op.drop_index("ix_quarantine_slot", table_name="quarantine")
    op.drop_column("quarantine", "seen_count")
    op.drop_column("quarantine", "last_seen_at")
    op.drop_column("revisions", "is_rollback")
    op.drop_index("ix_obs_last_seen_at", table_name="observations")
    op.drop_column("observations", "last_seen_at")
