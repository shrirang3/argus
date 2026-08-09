.PHONY: sync up down logs ps fmt lint test clean

sync:          ## resolve + install the uv workspace
	uv sync

up:            ## start the full stack
	docker compose up -d --build

down:          ## stop the stack
	docker compose down

logs:          ## tail all service logs
	docker compose logs -f

ps:            ## show container status
	docker compose ps

fmt:           ## format
	uv run ruff format .

lint:          ## lint + import sort check
	uv run ruff check .

test:          ## run the test suite
	uv run pytest -q

clean:
	docker compose down -v
	rm -rf .ruff_cache .pytest_cache
