.PHONY: install data pipeline test db clean

install:
	pip install -r requirements.txt

data:
	python -m data.generate_data

pipeline:
	python -m pipelines.run_pipeline

pipeline-strict:
	python -m pipelines.run_pipeline --strict

test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=pipelines --cov-report=term-missing

db:
	docker compose up -d postgres
	@echo "PostgreSQL running on localhost:5432 (db: marketing_platform, user: mktadmin)"

db-ui:
	docker compose up -d
	@echo "pgAdmin at http://localhost:5050"

db-down:
	docker compose down

clean:
	rm -rf data/bronze data/silver data/gold
	find . -type d -name __pycache__ | xargs rm -rf
	find . -type d -name .pytest_cache | xargs rm -rf
