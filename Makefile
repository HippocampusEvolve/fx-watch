.PHONY: up down logs test lint backfill sweep checks report seed reset ps

# Полный стенд. Ключей не требуется: оба источника публичные.
up:
	docker compose up -d --build
	@echo "API:       http://localhost:8790/api/fxwatch/health"
	@echo "Документация: http://localhost:8790/api/fxwatch/docs"

down:
	docker compose down

reset:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f worker api

test:
	docker compose run --rm --entrypoint pytest api -q

lint:
	docker compose run --rm --entrypoint ruff api check src tests

# Заливка истории. Архив ЦБ доступен по датам, поэтому три месяца истории
# появляются сразу, а не через три месяца.
backfill:
	docker compose run --rm --entrypoint fxwatch worker backfill --days 90

sweep:
	docker compose run --rm --entrypoint fxwatch worker sweep

checks:
	docker compose run --rm --entrypoint fxwatch worker checks

# Отчёт за период в файл reports/.
report:
	docker compose run --rm --entrypoint fxwatch worker report --days 90 --out /app/reports/latest.md

# Демонстрационные инциденты (синтетические, помечены code_version='demo-seed').
seed:
	docker compose run --rm --entrypoint python worker /app/scripts/seed_incidents.py
