.PHONY: up down logs test lint backfill sweep checks report seed seed-clean reset ps

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

# --no-deps: юнит-тестам и линтеру не нужны ни база, ни миграции - только образ.
test:
	docker compose run --rm --no-deps --entrypoint pytest api -q

# Полный набор вместе с интеграционными: они работают с настоящим Postgres
# в отдельной базе fxwatch_test, рабочие данные не трогаются.
test-db:
	docker compose exec -T db psql -U fxwatch -d fxwatch -c "CREATE DATABASE fxwatch_test" || true
	docker compose run --rm --entrypoint pytest \
		-e FXWATCH_TEST_DSN=postgresql+psycopg://fxwatch:fxwatch@db:5432/fxwatch_test api -q

lint:
	docker compose run --rm --no-deps --entrypoint ruff api check src tests

# Заливка истории. Архив ЦБ доступен по датам, поэтому три месяца истории
# появляются сразу, а не через три месяца.
backfill:
	docker compose run --rm --entrypoint fxwatch worker backfill --days 90

sweep:
	docker compose run --rm --entrypoint fxwatch worker sweep

checks:
	docker compose run --rm --entrypoint fxwatch worker checks

# Отчёт за период в файл reports/ на хосте: без volume файл остался бы
# внутри одноразового контейнера и исчез вместе с ним.
report:
	docker compose run --rm -v "$(CURDIR)/reports:/app/reports" --entrypoint fxwatch worker report --days 90 --out /app/reports/latest.md

# Демонстрационные инциденты (синтетические, привязаны к прогонам demo-seed).
seed:
	docker compose run --rm --entrypoint python worker /app/scripts/seed_incidents.py

# Полное удаление синтетики: прогоны, наблюдения, ревизии, проверки, карантин, алерты.
seed-clean:
	docker compose run --rm --entrypoint python worker /app/scripts/seed_incidents.py --clean
