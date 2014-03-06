import os
from multiprocessing import Process

import simplejson
from nose import with_setup
from mock import Mock, ANY, patch

from util import *
from dockets.queue import Queue
from dockets.isolation_queue import IsolationQueue
from dockets.batching_queue import BatchingQueue, BatchingIsolationQueue
from dockets.docket import Docket
from dockets.errors import ExpiredError

def default_process_item(obj, item):
    if (not isinstance(item, dict) or 'action' not in item
            or item['action'] == 'success'):
        obj.items_processed.append(item)
        return
    if item['action'] == 'retry':
        raise TestRetryError(item['message'])
    if item['action'] == 'expire':
        raise ExpiredError
    if item['action'] == 'error':
        raise Exception(item['message'])

class TestQueue(Queue):
    def __init__(self, *args, **kwargs):
        super(TestQueue, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

class TestIsolationQueue(IsolationQueue):
    def __init__(self, *args, **kwargs):
        super(TestIsolationQueue, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

class TestBatchingQueue(BatchingQueue):
    def __init__(self, *args, **kwargs):
        kwargs['batch_size'] = 1
        super(TestBatchingQueue, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

class TestBatchingIsolationQueue(BatchingIsolationQueue):
    def __init__(self, *args, **kwargs):
        kwargs['batch_size'] = 1
        super(TestBatchingIsolationQueue, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

class TestDocket(Docket):
    def __init__(self, *args, **kwargs):
        super(TestDocket, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)


def clear_redis():
    redis.flushdb()

def make_queue(cls):
    return cls(redis, 'test', use_error_queue=True,
               retry_error_classes=[TestRetryError],
               max_attempts=5)

all_queue_tests = []

def register(fn):
    all_queue_tests.append(with_setup(clear_redis)(fn))
    return fn

# queue tests
@register
def run_once(queue):
    queue.register_worker(worker_id='test_worker')
    queue.run_once(worker_id='test_worker')
    assert redis.exists('queue.test.test_worker.active')
    assert redis.sismember('queue.test.workers', 'test_worker')
    assert_error_queue_empty(queue)

@register
def push_once(queue):
    queue.push({'a': 1})
    assert queue.queued() == 1
    assert_queue_entry(queue.queued_items()[0], {'a': 1})
    assert_error_queue_empty(queue)

@register
def push_twice(queue):
    queue.push({'a': 1})
    queue.push({'b': 2})
    assert queue.queued() == 2
    assert_queue_entry(queue.queued_items()[1], {'a': 1})
    assert_queue_entry(queue.queued_items()[0], {'b': 2})
    assert_error_queue_empty(queue)

@register
def push_once_pop_once(queue):
    queue.push({'a': 1})
    queue.pop(worker_id='test_worker')
    assert queue.queued() == 0
    assert redis.llen('queue.test.test_worker.working') == 1
    assert_queue_entry(queue.redis.lindex('queue.test.test_worker.working', 0), {'a': 1})
    assert redis.exists('queue.test.test_worker.active')
    assert redis.sismember('queue.test.workers', 'test_worker')
    assert_error_queue_empty(queue)

@register
def push_once_pop_once_complete_once(queue):
    queue.push({'a': 1})
    item = queue.pop(worker_id='test_worker')
    queue.complete(item, worker_id='test_worker')
    assert queue.queued() == 0
    assert redis.llen('queue.test.test_worker.working') == 0
    assert_error_queue_empty(queue)

@register
def register_worker(queue):
    queue.register_worker('test_worker')
    metadata = redis.hgetall('queue.test.test_worker.metadata')
    assert metadata
    assert 'hostname' in metadata
    assert 'start_ts' in metadata

@register
def push_once_run_once(queue):
    queue.push({'a': 1})
    queue.register_worker(worker_id='test_worker')
    queue.run_once(worker_id='test_worker')
    assert queue.items_processed == [{'a': 1}]
    assert redis.exists('queue.test.test_worker.active')
    assert 'test_worker' in queue.active_worker_metadata()
    assert queue.queued() == 0
    metadata = queue.active_worker_metadata()['test_worker']
    assert metadata
    assert 'start_ts' in metadata
    print metadata
    assert int(metadata['success']) == 1
    assert_error_queue_empty(queue)

@register
def push_twice_run_once(queue):
    queue.push({'a': 1})
    queue.push({'b': 2})
    queue.run_once(worker_id='test_worker')
    assert queue.queued() == 1
    assert_error_queue_empty(queue)

@register
def push_twice_run_twice(queue):
    queue.push({'a': 1})
    queue.push({'b': 2})
    queue.run_once(worker_id='test_worker')
    queue.run_once(worker_id='test_worker')

    assert queue.items_processed == [{'a': 1}, {'b': 2}]
    assert redis.exists('queue.test.test_worker.active')
    assert 'test_worker' in queue.active_worker_metadata()
    assert queue.queued() == 0
    assert_error_queue_empty(queue)
    metadata = queue.active_worker_metadata()['test_worker']
    assert metadata
    assert 'last_success_ts' in metadata
    assert int(metadata['success']) == 2
    for key in ['error', 'last_error_ts', 'retry', 'last_retry_ts', 'expire', 'last_expire_ts']:
        assert key not in metadata

@register
def run_retry_item(queue):
    queue.push({'action': 'retry', 'message': 'Retry Error!'})
    queue.run_once(worker_id='test_worker')
    assert not queue.items_processed
    assert queue.queued() == 1
    assert_queue_entry(queue.queued_items()[0], {'action': 'retry', 'message': 'Retry Error!'})
    assert_error_queue_empty(queue)
    metadata = queue.active_worker_metadata()['test_worker']
    assert metadata
    assert 'last_retry_ts' in metadata
    assert int(metadata['retry']) == 1
    for key in ['error', 'last_error_ts', 'success', 'last_success_ts', 'expire', 'last_expire_ts']:
        assert key not in metadata

@register
def run_retry_item_3x_queue_default_retry(queue):
    if isinstance(queue, (TestIsolationQueue, TestBatchingIsolationQueue)):
        return
    queue.push({'action': 'retry', 'message': 'Retry Error!'})
    for _ in range(2):
        queue.run_once(worker_id='test_worker')
    assert not queue.items_processed
    assert queue.queued() == 1

    error_queue = queue.error_queue
    queue.run_once(worker_id='test_worker')
    assert queue.queued() == 1
    assert error_queue.length() == 0

@register
def run_retry_item_3x_per_item_retry(queue):
    if isinstance(queue, (TestIsolationQueue, TestBatchingIsolationQueue)):
        return
    queue.push({'action': 'retry', 'message': 'Retry Error!'}, max_attempts=3)
    for _ in range(2):
        queue.run_once(worker_id='test_worker')
    assert not queue.items_processed
    assert queue.queued() == 1

    queue.run_once(worker_id='test_worker')
    error_queue = queue.error_queue
    assert queue.queued() == 0
    assert error_queue.length() == 1

@register
def run_retry_item_5x(queue):
    queue.push({'action': 'retry', 'message': 'Retry Error!'})
    for _ in range(4):
        queue.run_once(worker_id='test_worker')

    assert not queue.items_processed
    assert queue.queued() == 1

    queue.run_once(worker_id='test_worker')

    assert queue.queued() == 0
    metadata = queue.active_worker_metadata()['test_worker']
    assert metadata
    assert 'last_retry_ts' in metadata
    assert int(metadata['retry']) == 4
    assert 'last_error_ts' in metadata
    assert int(metadata['error']) == 1
    for key in ['success', 'last_success_ts', 'expire', 'last_expire_ts']:
        assert key not in metadata

    assert queue.error_queue.length() == 1
    assert len(queue.error_queue.errors()) == 1

    errors_in_redis = redis.hgetall('queue.test.errors')
    assert len(errors_in_redis) == 1
    assert errors_in_redis.keys()[0] == simplejson.loads(errors_in_redis.values()[0])['id']

    error = queue.error_queue.errors()[0]
    assert error
    assert 'traceback' in error
    assert error['error_type'] == 'TestRetryError'
    assert error['error_text'] == 'Retry Error!'
    assert error['envelope']['item'] == {'action': 'retry', 'message': 'Retry Error!'}

@register
def run_expire_item(queue):
    queue.push({'action': 'expire'})
    queue.run_once(worker_id='test_worker')
    assert not queue.items_processed
    assert queue.queued() == 0
    assert_error_queue_empty(queue)
    metadata = queue.active_worker_metadata()['test_worker']
    assert metadata
    assert 'last_expire_ts' in metadata
    assert int(metadata['expire']) == 1
    for key in ['retry', 'last_retry_ts', 'success', 'last_success_ts', 'error', 'last_error_ts']:
        assert key not in metadata

@register
def run_error_item(queue):
    queue.push({'action': 'error', 'message': 'Error!'})
    queue.run_once(worker_id='test_worker')
    assert not queue.items_processed
    assert queue.queued() == 0
    metadata = queue.active_worker_metadata()['test_worker']
    assert metadata
    assert 'last_error_ts' in metadata
    assert int(metadata['error']) == 1
    for key in ['retry', 'last_retry_ts', 'success', 'last_success_ts', 'expire', 'last_expire_ts']:
        assert key not in metadata

    assert queue.error_queue.length() == 1
    assert len(queue.error_queue.errors()) == 1

    errors_in_redis = redis.hgetall('queue.test.errors')
    assert len(errors_in_redis) == 1
    assert errors_in_redis.keys()[0] == simplejson.loads(errors_in_redis.values()[0])['id']

    error = queue.error_queue.errors()[0]
    assert error
    assert 'traceback' in error
    assert error['error_type'] == 'Exception'
    assert error['error_text'] == 'Error!'
    assert error['envelope']['item'] == {'action': 'error', 'message': 'Error!'}

    error_by_id = queue.error_queue.error(queue.error_queue.error_ids()[0])
    assert error_by_id == error

@register
def run_error_item_and_requeue(queue):
    queue.push({'action': 'error', 'message': 'Error!'})
    queue.run_once(worker_id='test_worker')
    queue.error_queue.requeue_error(queue.error_queue.errors()[0]['id'])
    assert not queue.items_processed
    assert queue.queued() == 1
    assert_queue_entry(queue.queued_items()[0], {'action': 'error', 'message': 'Error!'})
    assert_error_queue_empty(queue)

@register
def run_multiple_error_items_and_requeue_all(queue):
    for i in range(5):
        queue.push({'action': 'error', 'message': 'Error{}'.format(i)})
        queue.run_once(worker_id='test_worker')
    queue.error_queue.requeue_all_errors()
    assert not queue.items_processed
    assert queue.queued() == 5
    assert_error_queue_empty(queue)

@register
def run_error_item_and_delete(queue):
    queue.push({'action': 'error', 'message': 'Error!'})
    queue.run_once(worker_id='test_worker')
    queue.error_queue.delete_error(queue.error_queue.errors()[0]['id'])
    assert not queue.items_processed
    assert queue.queued() == 0
    assert_error_queue_empty(queue)

@register
def push_once_pop_once_reclaim(queue):
    queue.push({'a': 1})
    queue.pop(worker_id='test_worker')
    queue._reclaim()
    assert queue.queued() == 0

@register
def push_once_reclaim_once_unset_worker_key_reclaim(queue):
    queue.push({'a': 1})
    queue.pop(worker_id='test_worker')
    redis.delete(queue._worker_activity_key('test_worker'))
    queue._reclaim()
    assert queue.queued() == 1
    assert_queue_entry(queue.queued_items()[0], {'a': 1})

def run_bad_worker(queue):
    queue.push({'a': 1})
    def bad_process_item(item):
        os._exit(0)
    queue.process_item = bad_process_item
    queue.run_once('test_worker')

@register
def push_once_run_bad_worker_unset_worker_key_reclaim(queue):
    p = Process(target=run_bad_worker, args=(queue,))
    p.start()
    p.join()
    assert queue.queued() == 0
    redis.delete(queue._worker_activity_key('test_worker'))
    queue._reclaim()
    assert queue.queued() == 1

@register
@patch('dockets.queue.time.sleep')
def run_with_constant_false_should_continue(queue, sleep):
    queue.run_once = Mock(return_value=True)
    queue.run(should_continue=(lambda: False))
    assert not queue.run_once.called
    assert not sleep.called

@register
@patch('dockets.queue.time.sleep')
def run_with_one_true_should_continue(queue, sleep):
    queue.run_once = Mock(return_value=True)
    queue.run(should_continue=Mock(side_effect=[True, False]))
    queue.run_once.assert_called_once_with(ANY)
    assert not sleep.called

@register
@patch('dockets.queue.time.sleep')
def run_with_one_true_should_continue_and_no_items(queue, sleep):
    queue.run_once = Mock(return_value=None)
    queue.run(should_continue=Mock(side_effect=[True, False]))
    queue.run_once.assert_called_once_with(ANY)
    sleep.assert_called_once_with(queue._wait_time)


@register
def deserialization_error(queue):
    old_serializer = queue._serializer
    class ErroringSerializer(object):
        def serialize(self, *args, **kwargs):
            return old_serializer.serialize(*args, **kwargs)
        def deserialize(self, *args, **kwargs):
            raise ValueError("I've made a huge mistake")

    queue._serializer = ErroringSerializer()
    queue.push({'a': 1})
    queue.pop(worker_id='test_worker')
    assert queue.queued() == 0
    assert redis.llen('queue.test.test_worker.working') == 0
    assert_error_queue_empty(queue)

def test_all_queues():
    for cls in (TestQueue, TestIsolationQueue, TestBatchingQueue, TestBatchingIsolationQueue, TestDocket):
        for test_case in all_queue_tests:
            yield test_case, make_queue(cls)
