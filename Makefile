.PHONY: start stop test benchmark clean

PYTHONPATH := $(shell pwd)
export PYTHONPATH

start:
	@echo "Starting Flowgrid cluster..."
	@bash start_cluster.sh

stop:
	@echo "Stopping Flowgrid cluster..."
	@bash stop_cluster.sh

test:
	@echo "Running test suite..."
	@python3 -m unittest discover tests

benchmark:
	@echo "Running industry benchmarks..."
	@python3 -m benchmarks.industry_benchmark

clean:
	@echo "Cleaning up logs and temporary files..."
	@rm -rf logs/
	@rm -f master.log worker*.log cluster_start.log
	@rm -f .master.pid .worker.pids
	@find . -type d -name "__pycache__" -exec rm -rf {} +
