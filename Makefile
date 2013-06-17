BENCH_FILE=benchmark/results.txt

test:
	PYTHONPATH=$(CURDIR) nosetests test

bench:
	echo "" > $(BENCH_FILE)
	echo "queue_push_1_run_1" >> $(BENCH_FILE)
	python -m timeit -n 1000 -s 'import benchmark' 'benchmark.queue_push_1_run_1()' >> $(BENCH_FILE)
	echo "docket_push_1_run_1" >> $(BENCH_FILE)
	python -m timeit -n 1000 -s 'import benchmark' 'benchmark.docket_push_1_run_1()' >> $(BENCH_FILE)
	echo "queue_push_10_run_10" >> $(BENCH_FILE)
	python -m timeit -n 1000 -s 'import benchmark' 'benchmark.queue_push_10_run_10()' >> $(BENCH_FILE)
	echo "docket_push_10_run_10" >> $(BENCH_FILE)
	python -m timeit -n 1000 -s 'import benchmark' 'benchmark.docket_push_10_run_10()' >> $(BENCH_FILE)

.PHONY: test
