import simplejson
from traceback import print_exc

from pyvows import Vows, expect
from util import FakeRedisContext


from dockets.isolation_queue import IsolationQueue

from basic_queue_vows import TestRetryError

@Vows.create_assertions
def to_match_as_set(topic, expected):
    return set(topic) == set(expected)


class TestIsolationQueueWithKey(IsolationQueue):
    def __init__(self, *args, **kwargs):
        kwargs['key'] = ['a']
        super(TestIsolationQueueWithKey, self).__init__(*args, retry_error_classes=[TestRetryError], **kwargs)
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


class IsolationQueueWithKeyContext(FakeRedisContext):
    def __init__(self, *args, **kwargs):
        try:
            super(IsolationQueueWithKeyContext, self).__init__(*args, **kwargs)
        except:
            print_exc()
        self.ignore('use_queue')

    def use_redis(self, redis):
        queue = TestIsolationQueueWithKey(redis, 'test')
        self.use_queue(queue)
        return queue

    def use_queue(self, queue):
        raise NotImplementedError


@Vows.batch
class AnIsolationQueue(Vows.Context):
    class WhenPushedToOnce(IsolationQueueWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 1})

        class TheEntryKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.get('queue.test.entry.1')

            def should_be_set(self, topic):
                expect(topic).to_equal('1')

        class TheLatestAddKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.hgetall('queue.test.latest')

            def should_be_empty(self, topic):
                expect(topic).to_be_empty()

    class WhenPushedToTwiceWithDifferentKeys(IsolationQueueWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 1})
            queue.push({'a': 2})

        class TheEntryKeys(Vows.Context):
            def topic(self, queue):
                return [queue.redis.get('queue.test.entry.1'),
                        queue.redis.get('queue.test.entry.2')]

            def should_be_set(self, topic):
                expect(topic).to_match_as_set(['1', '1'])

        class TheLatestAddKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.hgetall('queue.test.latest')

            def should_be_empty(self, topic):
                expect(topic).to_be_empty()

    class WhenPushedToTwiceWithSameKey(IsolationQueueWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 1, 'b': 1})
            queue.push({'a': 1, 'b': 2})

        def should_have_one_entry(self, queue):
            expect(queue.queued()).to_equal(1)

        class TheEntryKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.get('queue.test.entry.1')

            def should_be_set(self, topic):
                expect(topic).to_equal('1')

        class TheLatestAddKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.hgetall('queue.test.latest')

            def should_have_correct_value(self, topic):
                expect(topic).to_be_like({'1': simplejson.dumps({'a': 1, 'b': 2})})

    class WhenPushedToTwiceWithSameKeyAndRunOnce(IsolationQueueWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 1, 'b': 1})
            queue.push({'a': 1, 'b': 2})
            queue.run_once(worker_id='test_worker')

        def should_have_one_entry(self, queue):
            expect(queue.queued()).to_equal(1)

        class TheEntryKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.get('queue.test.entry.1')

            def should_be_set(self, topic):
                expect(topic).to_equal('1')

        class TheLatestAddKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.hgetall('queue.test.latest')

            def should_be_empty(self, topic):
                expect(topic).to_be_empty()

        class TheItemProcessed(Vows.Context):
            def topic(self, queue):
                return queue.items_processed

            def should_be_first_item(self, topic):
                expect(topic).to_be_like([{'a': 1, 'b': 1}])

    class WhenPushedToTwiceWithSameKeyAndRunTwice(IsolationQueueWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 1, 'b': 1})
            queue.push({'a': 1, 'b': 2})
            queue.run_once(worker_id='test_worker')
            queue.run_once(worker_id='test_worker')

        def should_be_empty(self, queue):
            expect(queue.queued()).to_equal(0)

        class TheEntryKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.get('queue.test.entry.1')

            def should_not_be_set(self, topic):
                expect(topic).to_be_null()

        class TheLatestAddKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.hgetall('queue.test.latest')

            def should_be_empty(self, topic):
                expect(topic).to_be_empty()

        class TheItemProcessed(Vows.Context):
            def topic(self, queue):
                return queue.items_processed

            def should_be_first_and_second_items(self, topic):
                expect(topic).to_be_like([{'a': 1, 'b': 1}, {'a': 1, 'b': 2}])
