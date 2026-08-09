.PHONY: sync up down logs ps health migrate revision fmt lint test clean

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

fmt:           ## format
	uv run ruff format .

lint:          ## lint + import sort check
	uv run ruff check .

test:          ## run the test suite
	uv run pytest -q

clean:
	docker compose down -v
	rm -rf .ruff_cache .pytest_cache
