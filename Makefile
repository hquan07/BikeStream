.PHONY: up down logs status clean

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

status:
	docker compose ps

# Watch Kafka topic
kafka-watch:
	docker exec bs-kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic station_status --from-beginning --max-messages 10

# Check DB row counts
db-status:
	docker exec bs-timescaledb psql -U bikestream -d bikestream \
		-c "SELECT city, count(*), max(time) as latest FROM station_snapshots GROUP BY city ORDER BY city;"

clean:
	docker compose down -v --remove-orphans
