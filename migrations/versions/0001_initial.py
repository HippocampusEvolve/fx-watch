"""Начальная схема: битемпоральные наблюдения, журнал прогонов, качество, карантин.

Revision ID: 0001
Revises:
"""

from __future__ import annotations

from alembic import op

from fxwatch.models import Base

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


# Витрина текущего состояния. DISTINCT ON берёт самое свежее наблюдение
# по каждой тройке (источник, ряд, дата) - то есть последнюю известную версию
# значения. Историю при этом никто не трогает, она лежит в observations целиком.
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
    Base.metadata.create_all(bind=op.get_bind())
    op.execute(CURRENT_VIEW)
    op.execute(VERSIONS_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS rates_versions")
    op.execute("DROP VIEW IF EXISTS rates_current")
    Base.metadata.drop_all(bind=op.get_bind())
