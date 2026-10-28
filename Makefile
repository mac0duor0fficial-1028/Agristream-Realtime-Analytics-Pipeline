.PHONY: setup up up-all down clean logs test lint smoke psql

setup:            ## create .env from the template
	@test -f .env || cp .env.example .env && echo ".env ready - edit the passwords!"

up: setup         ## core pipeline
	docker compose up -d --build

up-all: setup     ## + Grafana, Prometheus, Kafka UI
	docker compose --profile monitoring up -d --build

down:
	docker compose --profile monitoring down

clean:            ## also deletes all data volumes
	docker compose --profile monitoring down -v

logs:
	docker compose logs -f --tail=100 flink-job producer

test:
	pip install -q -r requirements-dev.txt && pytest -q

lint:
	ruff check .

smoke:
	tests/smoke_test.sh

psql:
	docker compose exec postgres sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB'
