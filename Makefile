.PHONY: up down logs demo test lint dbt-docs clean

up:            ## build and start Postgres + Airflow
	docker compose up -d --build

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
