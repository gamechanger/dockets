import simplejson
from traceback import print_exc

from pyvows import Vows, expect
from util import FakeRedisContext


from dockets.batching_queue import BatchingQueue
from dockets.errors import RetryError

class TestSize10BatchingQueue(BatchingQueue):
    def __init__(self, *args, **kwargs):
        kwargs['batch_size'] = 10
        super(TestSize10BatchingQueue, self).__init__(*args, **kwargs)
        self.item_sets_processed = []

    def process_items(self, items):
        self.item_sets_processed.append(items)


class Size10BatchingQueueContext(FakeRedisContext):
    def __init__(self, *args, **kwargs):
        try:
            super(Size10BatchingQueueContext, self).__init__(*args, **kwargs)
        except:
            print_exc()
        self.ignore('use_queue')

    def use_redis(self, redis):
        queue = TestSize10BatchingQueue(redis, 'test')
        self.use_queue(queue)
        return queue

    def use_queue(self, queue):
        raise NotImplementedError


@Vows.batch
class ABatchingQueue(Vows.Context):
    class WhenPushedToTwiceAndRunOnce(Size10BatchingQueueContext):
        def use_queue(self, queue):
            queue.push({'a': 1})
            queue.push({'a': 2})
            queue.run_once(worker_id='test_worker')

        def should_process_both_items(self, queue):
            expect(queue.item_sets_processed).to_be_like([[{'a': 1}, {'a': 2}]])

        def should_be_empty(self, queue):
            expect(queue.queued()).to_equal(0)

    class WhenPushedToTenTimesAndRunOnce(Size10BatchingQueueContext):
        def use_queue(self, queue):
            for i in range(10):
                queue.push({'a': i})
            queue.run_once(worker_id='test_worker')

        def should_process_all_items(self, queue):
            expect(queue.item_sets_processed).to_be_like([[{'a': i} for i in range(10)]])

        def should_be_empty(self, queue):
            expect(queue.queued()).to_equal(0)

    class WhenPushedToTwelveTimesAndRunOnce(Size10BatchingQueueContext):
        def use_queue(self, queue):
            for i in range(12):
                queue.push({'a': i})
            queue.run_once(worker_id='test_worker')

        def should_process_ten_items(self, queue):
            expect(queue.item_sets_processed).to_be_like([[{'a': i} for i in range(10)]])

        def should_have_two_items(self, queue):
            expect(queue.queued()).to_equal(2)
