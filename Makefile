.PHONY: install test test-quick benchmark charts clean server client help

help:                  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install:               ## Install dependencies (rich for TUI)
	pip install rich

install-dev:           ## Install dev + test dependencies
	pip install rich pytest matplotlib

test:                  ## Run all 32 tests
	python -m pytest tests/test_transfer.py -v

test-quick:            ## Run core tests only (fast)
	python -m pytest tests/test_transfer.py -v -k "TestSingleClient or TestCache"

benchmark:             ## Run full benchmark suite
	python benchmark_suite.py

benchmark-quick:       ## Run quick benchmark
	python benchmark_suite.py --quick

charts:                ## Generate benchmark charts
	python generate_chart.py

server:                ## Start hybrid server standalone
	python server_hybrid.py

server-threaded:       ## Start threaded server standalone
	python server.py

launch:                ## Start server + TUI (one command)
	python launcher.py

docker-build:          ## Build Docker images
	docker build -f docker/Dockerfile --target server -t fts-server ..
	docker build -f docker/Dockerfile --target client -t fts-client ..

docker-up:             ## Start server via docker-compose
	docker compose -f docker/docker-compose.yml up -d server

docker-down:           ## Stop docker containers
	docker compose -f docker/docker-compose.yml down

clean:                 ## Remove generated files
	rm -rf received_files server_storage CacheFolder
	rm -rf __pycache__ tests/__pycache__ .pytest_cache
	rm -f benchmark_results.json benchmark_suite_results.json
