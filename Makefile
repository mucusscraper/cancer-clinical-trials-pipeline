.PHONY: infra build init up destroy

infra:
	cd infra && ./apply.sh

build:
	docker compose build --no-cache

init:
	docker compose up airflow-postgres -d
	sleep 60
	docker compose run --rm airflow-init

up:
	docker compose up -d airflow-scheduler metabase
	sleep 100
	docker compose up -d airflow-webserver
	sleep 200
	echo "Open Airflow at localhost:8080 and Metabase at localhost:3000"


destroy:
	docker compose down -v
	cd infra && ./destroy.sh