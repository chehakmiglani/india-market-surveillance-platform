.PHONY: up up-lite db-only demo-native down logs demo test lint dbt-docs clean free-space

up:            ## build and start Postgres + Airflow
	docker compose up -d --build

up-lite:       ## same stack without Java/Spark (smaller image; pandas Silver engine)
	WITH_SPARK=false docker compose up -d --build

db-only:       ## smallest option: only Postgres in Docker, run the pipeline natively
	docker compose up -d postgres

demo-native:   ## run the demo pipeline with local Python against the Docker Postgres
	SILVER_ENGINE=pandas PG_DSN=postgresql://imp:imp@localhost:5432/market LAKE_ROOT=./lake \
	  python -m imp.cli run --demo --start 2024-06-03 --end 2025-07-31

free-space:    ## remove unused Docker images, build cache and stopped containers
	docker system prune -af
	docker builder prune -af

down:
	docker compose down

logs:
	docker compose logs -f airflow

demo:          ## run the whole pipeline on synthetic data inside the container (no internet needed)
	docker compose exec airflow python -m imp.cli run --demo --start 2024-06-03 --end 2025-07-31

test:          ## unit + integration tests (needs local Postgres at PG_DSN, Java for Spark)
	PYTHONPATH=pipeline pytest -q

lint:
	ruff check pipeline tests dags

dbt-docs:      ## generate and serve dbt docs (lineage graph) on :8081
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8081

clean:
	docker compose down -v
