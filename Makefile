include .env
export

ingester:
	cargo run --bin eth-archive-ingester -- \
		-e http://195.201.56.33:8545 \
		--block-batch-size 50 \
		--http-req-concurrency 100 \
		--block-window-size 300000 \
		--request-timeout-secs 300 \
		--connect-timeout-ms 1000 \
		--db-user $(DB_USER) \
		--db-password $(DB_PASS) \
		--db-name $(DB_NAME) \
		--db-host $(DB_HOST) \
		--db-port $(DB_PORT)

writer:
	cargo run --bin eth-archive-parquet-writer -- \
		--data-path ./data \
		-e http://195.201.56.33:8545 \
		--block-batch-size 50 \
		--http-req-concurrency 100 \
		--request-timeout-secs 300 \
		--connect-timeout-ms 1000 \
		--db-user $(DB_USER) \
		--db-password $(DB_PASS) \
		--db-name $(DB_NAME) \
		--db-host $(DB_HOST) \
		--db-port $(DB_PORT)

gateway:
	cargo run --bin eth-archive-gateway -- \
		--data-path ./data \
		--max-block-range 500000 \
		--default-block-range 1000000 \
		--response-log-limit 10000 \
		--query-chunk-size 256 \
		--query-time-limit-ms 6000 \
		--db-user $(DB_USER) \
		--db-password $(DB_PASS) \
		--db-name $(DB_NAME) \
		--db-host $(DB_HOST) \
		--db-port $(DB_PORT)

up:
	docker-compose up -d

down:
	docker-compose down

.PHONY: ingester gateway
