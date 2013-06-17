import simplejson
from util import *

from dockets.docket import Docket

class TestDocketWithKey(Docket):
    def __init__(self, *args, **kwargs):
        kwargs['key'] = ['a']
        super(TestDocketWithKey, self).__init__(*args, retry_error_classes=[TestRetryError], **kwargs)
        self.items_processed = []

    def process_item(self, item):
        if (not isinstance(item, dict) or 'action' not in item
            or item['action'] == 'success'):
            self.items_processed.append(item)
            return
        if item['action'] == 'retry':
            raise TestRetryError
        if item['action'] == 'error':
            raise Exception(item['message'])

def make_queue():
    return TestDocketWithKey(redis, 'test')

@clear
def test_push_remove():
    queue = make_queue()
    queue.push({'a': 1})
    queue.remove({'a': 1})
    assert queue.queued() == 0
    assert queue.push({'a': 1})

@clear
def push_pop_before_time():
    queue = make_queue()
    queue.push({'a': 1}, when=2)
    queue.pop(worker_id='test_worker', current_time=1)
    assert queue.queued() == 1
    assert not queue.push({'a': 1})

@clear
def test_push():
    queue = make_queue()
    queue.push({'a': 7}, when=2)
    assert queue.get_existing_item_for_item({'a': 7}) == {'a': 7}
    print queue.get_existing_item_for_item({'a': 1})
    assert queue.get_existing_item_for_item({'a': 1}) == None
    assert queue.get_fire_time({'a': 7}) == 2
