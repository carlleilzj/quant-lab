.PHONY: help up up-obs up-all down ps logs initdb

help:
	@echo "okx-quant-lab"
	@echo ""
	@echo "make up        -> core services"
	@echo "make up-obs    -> core + (prometheus+grafana)"
	@echo "make up-all    -> core + obs + ops(backup) + ai"
	@echo "make down      -> stop & remove volumes"
	@echo "make logs      -> follow logs"
	@echo "make initdb    -> create tables"
	@echo "make ps        -> show containers"

up:
	docker compose up -d --build

up-obs:
	docker compose --profile observability up -d --build

up-all:
	docker compose --profile observability --profile ops --profile ai up -d --build

down:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=200

initdb:
	docker compose run --rm api python scripts/init_db.py
