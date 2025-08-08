full:
	docker compose -f docker-compose.yml -f docker-compose.override.yml up -d

lite:
	docker compose -f docker-compose.yml up -d

down:
	docker compose -f docker-compose.yml -f docker-compose.override.yml down

