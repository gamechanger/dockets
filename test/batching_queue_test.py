import simplejson

from util import *

from dockets.batching_queue import BatchingQueue

class TestSize10BatchingQueue(BatchingQueue):
    def __init__(self, *args, **kwargs):
        kwargs['batch_size'] = 10
        super(TestSize10BatchingQueue, self).__init__(*args, **kwargs)
        self.item_sets_processed = []

    def process_items(self, items):
        self.item_sets_processed.append(items)

def make_queue():
    return TestSize10BatchingQueue(redis, 'test')

def setup():
    redis.flushdb()

def test_push_twice_run_once():
    queue = make_queue()
    queue.push({'a': 1})
    queue.push({'a': 2})
    queue.run_once(worker_id='test_worker')
    assert queue.item_sets_processed == [[{'a': 1}, {'a': 2}]]
    assert queue.queued() == 0

def test_push_10x_run_once():
    queue = make_queue()
    for i in range(10):
        queue.push({'a': i})
    queue.run_once(worker_id='test_worker')
    assert queue.item_sets_processed == [[{'a': i} for i in range(10)]]
    assert queue.queued() == 0

def test_push_12x_run_once():
    queue = make_queue()
    for i in range(12):
        queue.push({'a': i})
    queue.run_once(worker_id='test_worker')
    assert queue.item_sets_processed == [[{'a': i} for i in range(10)]]
    assert queue.queued() == 2
