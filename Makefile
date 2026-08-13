.PHONY: sync up down logs ps health migrate revision seed load fmt lint test clean

# Host-side connection. In-network the services use postgres:5432; 5433 is
# published to stay clear of a locally installed Postgres.
DATABASE_URL_HOST ?= postgresql+asyncpg://argus:argus@localhost:5433/argus

sync:          ## resolve + install the uv workspace
	uv sync --all-packages

up:            ## start the full stack
	docker compose up -d --build

down:          ## stop the stack
	docker compose down

logs:          ## tail all service logs
	docker compose logs -f

ps:            ## show container status
	docker compose ps

health:        ## hit every service health endpoint
	@for p in 8000 8001 8002; do printf "  :%s  " $$p; curl -s -m 5 http://localhost:$$p/health || echo unreachable; echo; done

migrate:       ## apply migrations to the running database
	DATABASE_URL=$(DATABASE_URL_HOST) uv run alembic upgrade head

revision:      ## create a migration — make revision m="add conversations"
	DATABASE_URL=$(DATABASE_URL_HOST) uv run alembic revision -m "$(m)"

seed:          ## fill the dashboard with 30 minutes of synthetic traffic
	uv run python tools/loadgen.py --events 800 --concurrency 20 --spread-minutes 30
	uv run python tools/loadgen.py --events 200 --concurrency 20

load:          ## sustained load against the pipeline (not a provider)
	uv run python tools/loadgen.py --events 5000 --concurrency 50

fmt:           ## format
	uv run ruff format .

lint:          ## lint + import sort check
	uv run ruff check .

test:          ## run the test suite
	uv run pytest -q

clean:
	docker compose down -v
	rm -rf .ruff_cache .pytest_cache
