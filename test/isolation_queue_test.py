import simplejson
from mock import Mock

from util import *
from dockets.isolation_queue import IsolationQueue

class TestIsolationQueueWithKey(IsolationQueue):
    def __init__(self, *args, **kwargs):
        kwargs['key'] = ['a']
        super(TestIsolationQueueWithKey, self).__init__(
            *args,
            retry_error_classes=[TestRetryError],
            use_error_queue=True,
            **kwargs)
        self.items_processed = []

    def process_item(self, item):
        if (not isinstance(item, dict) or 'action' not in item
            or item['action'] == 'success'):
            self.items_processed.append(item)
            return
        if item['action'] == 'retry':
            raise RetryError
        if item['action'] == 'error':
            raise Exception(item['message'])

def make_queue():
    return TestIsolationQueueWithKey(redis, 'test')

@clear
def test_isolated_transaction_on_exists_when_exists():
    queue = make_queue()
    queue.push({'a': 1})
    key_exists_cb = Mock()
    key_does_not_exist_cb = Mock()
    queue.isolated_transaction_on_exists('1', key_exists_cb, key_does_not_exist_cb)
    assert key_exists_cb.called
    assert not key_does_not_exist_cb.called

@clear
def test_isolated_transaction_on_exists_when_not_exists():
    queue = make_queue()
    key_exists_cb = Mock()
    key_does_not_exist_cb = Mock()
    queue.isolated_transaction_on_exists('1', key_exists_cb, key_does_not_exist_cb)
    assert not key_exists_cb.called
    assert key_does_not_exist_cb.called

@clear
def test_isolated_transaction_on_latest_when_latest():
    queue = make_queue()
    queue.push({'a': 1})
    queue.push({'a': 1})
    key_latest_cb = Mock()
    key_not_latest_cb = Mock()
    queue.isolated_transaction_on_latest('1', key_latest_cb, key_not_latest_cb)
    assert key_latest_cb.called
    assert not key_not_latest_cb.called

@clear
def test_isolated_transaction_on_latest_when_not_latest():
    queue = make_queue()
    queue.push({'a': 1})
    key_latest_cb = Mock()
    key_not_latest_cb = Mock()
    queue.isolated_transaction_on_latest('1', key_latest_cb, key_not_latest_cb)
    assert not key_latest_cb.called
    assert key_not_latest_cb.called

@clear
def test_push_once():
    queue = make_queue()
    queue.push({'a': 1})
    assert redis.get('queue.test.entry.1') == '1'
    assert not queue.redis.hgetall('queue.test.latest')

@clear
def test_push_twice_different_keys():
    queue = make_queue()
    queue.push({'a': 1})
    queue.push({'a': 2})
    assert redis.get('queue.test.entry.1') == '1'
    assert redis.get('queue.test.entry.2') == '1'
    assert not redis.hgetall('queue.test.latest')

@clear
def test_push_twice_same_key():
    queue = make_queue()
    queue.push({'a': 1, 'b': 1})
    queue.push({'a': 1, 'b': 2})
    assert queue.queued() == 1
    assert redis.get('queue.test.entry.1') == '1'
    assert queue.redis.hgetall('queue.test.latest') == {'1': simplejson.dumps({'a': 1, 'b': 2})}

@clear
def test_push_twice_same_key_run_once():
    queue = make_queue()
    queue.push({'a': 1, 'b': 1})
    queue.push({'a': 1, 'b': 2})
    queue.run_once()
    assert queue.queued() == 1
    assert redis.get('queue.test.entry.1') == '1'
    assert not queue.redis.hgetall('queue.test.latest')
    assert queue.items_processed == [{'a': 1, 'b': 1}]

@clear
def test_push_twice_same_key_run_twice():
    queue = make_queue()
    queue.push({'a': 1, 'b': 1})
    queue.push({'a': 1, 'b': 2})
    queue.run_once()
    queue.run_once()
    assert queue.queued() == 0
    assert not redis.get('queue.test.entry.1')
    assert not redis.hgetall('queue.test.latest')
    assert queue.items_processed == [{'a': 1, 'b': 1}, {'a': 1, 'b': 2}]

@clear
def test_delete_from_error_queue():
    queue = make_queue()
    queue.push({'a': 1, 'b': 1, 'action': 'error'})
    queue.run_once()
    assert queue.queued() == 0
    assert queue.error_queue.length() == 1
    queue.error_queue.delete_error(queue.error_queue.error_ids()[0])
    queue.push({'a': 1, 'b': 2})
    queue.run_once()
    assert queue.queued() == 0
    assert not redis.get('queue.test.entry.1')
    assert not redis.hgetall('queue.test.latest')
    assert queue.items_processed == [{'a': 1, 'b': 2}]
