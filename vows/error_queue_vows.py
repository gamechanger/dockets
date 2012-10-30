import simplejson
from pyvows import Vows, expect
from util import FakeRedisContext

from dockets.queue import Queue
from dockets.error_queue import ErrorQueue

def default_process_item(obj, item):
    if (not isinstance(item, dict) or 'action' not in item
        or item['action'] == 'success'):
        obj.items_processed.append(item)
        return
    if item['action'] == 'retry':
        raise RetryError
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

class ErrorQueueContext(FakeRedisContext):
    def __init__(self, *args, **kwargs):
        try:
            super(ErrorQueueContext, self).__init__(*args, **kwargs)
        except:
            print_exc()
        self.ignore('use_queue')

    def use_redis(self, redis):
        queue = TestQueue(redis, 'test')
        error_queue = ErrorQueue.make_error_queue(queue)
        self.use_queue(queue)
        return error_queue

    def use_queue(self, queue):
        raise NotImplementedError

@Vows.batch
class AnErrorQueue(Vows.Context):
    class WhenMainQueueIsRun(ErrorQueueContext):
        def use_queue(self, queue):
            queue.push({'a': 2})
            queue.run_once(worker_id='test_worker')

        def should_be_empty(self, error_queue):
            expect(error_queue.queued()).to_equal(0)

    class WhenMainQueueIsRunWithError(ErrorQueueContext):
        def use_queue(self, queue):
            queue.push({'action': 'error', 'message': 'test message'})
            queue.run_once(worker_id='test_worker')

        def should_have_an_item(self, error_queue):
            expect(error_queue.queued()).to_equal(1)

        class TheQueuedItem(Vows.Context):
            def topic(self, error_queue):
                return simplejson.loads(error_queue.queued_items()[0])['item']

            def should_have_item_and_error(self, topic):
                expect(topic).to_be_like({'item': {'action': 'error',
                                                   'message': 'test message'},
                                          'error_type': 'Exception',
                                          'error_text': 'test message'})
