"""
benchmarks should be run like so:

   python -m timeit -s 'import benchmark' 'benchmark.<benchmark()'

If redis lives at a location other than db4 on localhost:6739, set REDIS_HOST, REDIS_PORT, or REDIS_DB:

   REDIS_HOST=host python -m timeit -s 'import benchmark' 'benchmark.some_benchmark()'
"""
import os
from redis import Redis
from dockets.queue import Queue
from dockets.docket import Docket

redis = Redis(os.getenv('REDIS_HOST', 'localhost'), int(os.getenv('REDIS_PORT', 6379)),
              db=int(os.getenv('REDIS_DB', 4)))

redis.flushdb()

def test_item(key=1):
    return {'key': key, 'nonkey': 2}

class BenchQueue(Queue):
    def __init__(self):
        super(BenchQueue, self).__init__(redis, 'bench_queue', key=['key'])

    def process_item(self):
        pass

queue = BenchQueue()

class BenchDocket(Docket):
    def __init__(self):
        super(BenchDocket, self).__init__(redis, 'bench_docket', key=['key'])

    def process_item(self):
        pass

docket = BenchDocket()

def push_1_run_1(q):
    q.push(test_item())
    q.run_once('bench_worker')

def queue_push_1_run_1():
    push_1_run_1(queue)

def docket_push_1_run_1():
    push_1_run_1(docket)

def push_10_run_10(q):
    for i in range(10):
        q.push(test_item(i))
    for i in range(10):
        q.run_once('bench_worker')

def queue_push_10_run_10():
    push_10_run_10(queue)

def docket_push_10_run_10():
    push_10_run_10(docket)
