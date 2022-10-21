export RUST_LOG=info

ingester:
	cargo run --bin eth-archive-ingester -- \
		-e http://195.201.56.33:8545 \
		--block-batch-size 50 \
		--http-req-concurrency 100 \
		--block-window-size 300000 \
		--db-user postgres \
		--db-password postgres \
		--db-name eth_archive_db \
		--db-host localhost \
		--db-port 29598

writer:
	cargo run --bin eth-archive-parquet-writer -- \
		--data-path ./data \
		-e http://195.201.56.33:8545 \
		--block-batch-size 50 \
		--http-req-concurrency 100 \
		--db-user postgres \
		--db-password postgres \
		--db-name eth_archive_db \
		--db-host localhost \
		--db-port 29598

gateway:
	cargo run --bin eth-archive-gateway -- \
		--data-path ./data \
		--query-chunk-size 256 \
		--query-time-limit-ms 6000 \
		--db-user postgres \
		--db-password postgres \
		--db-name eth_archive_db \
		--db-host localhost \
		--db-port 29598

up:
	docker-compose up -d

down:
	docker-compose down

.PHONY: ingester gateway
