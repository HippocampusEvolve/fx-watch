FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Код и зависимости ставятся одним слоем сознательно: editable-установка требует
# src рядом с pyproject, а проект собирается за минуту. Если зависимостей станет
# заметно больше, их стоит вынести в отдельный слой до COPY src.
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev]"

COPY alembic.ini ./
COPY migrations ./migrations
COPY scripts ./scripts
COPY tests ./tests
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# CODE_VERSION прокидывается при сборке (git sha) и пишется в каждый прогон,
# чтобы через три месяца было видно, с какой версии кода поменялось поведение.
ARG CODE_VERSION=dev
ENV FXWATCH_CODE_VERSION=${CODE_VERSION}

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["api"]
