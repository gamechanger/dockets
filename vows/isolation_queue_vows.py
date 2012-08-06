import simplejson
from traceback import print_exc

from pyvows import Vows, expect
from util import FakeRedisContext


from dockets.isolation_queue import IsolationQueue
from dockets.errors import RetryError

@Vows.create_assertions
def to_match_as_set(topic, expected):
    return set(topic) == set(expected)


class TestIsolationQueueWithKey(IsolationQueue):
    def __init__(self, *args, **kwargs):
        kwargs['key'] = ['a']
        super(TestIsolationQueueWithKey, self).__init__(*args, **kwargs)
        self.data_processed = []

    def process_data(self, data):
        if (not isinstance(data, dict) or 'action' not in data
            or data['action'] == 'success'):
            self.data_processed.append(data)
            return
        if data['action'] == 'retry':
            raise RetryError
        if data['action'] == 'error':
            raise Exception(data['message'])


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

        class TheEntrySet(Vows.Context):
            def topic(self, queue):
                return queue.redis.smembers('queue.test.entries')

            def should_have_correct_entry(self, topic):
                expect(topic).to_match_as_set(['1'])

        class TheLatestAddKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.hgetall('queue.test.latest')

            def should_be_empty(self, topic):
                expect(topic).to_be_empty()

    class WhenPushedToTwiceWithDifferentKeys(IsolationQueueWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 1})
            queue.push({'a': 2})

        class TheEntrySet(Vows.Context):
            def topic(self, queue):
                return queue.redis.smembers('queue.test.entries')

            def should_have_correct_entries(self, topic):
                expect(topic).to_match_as_set(['1', '2'])

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

        class TheEntrySet(Vows.Context):
            def topic(self, queue):
                return queue.redis.smembers('queue.test.entries')

            def should_have_correct_entry(self, topic):
                expect(topic).to_match_as_set(['1'])

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

        class TheEntrySet(Vows.Context):
            def topic(self, queue):
                return queue.redis.smembers('queue.test.entries')

            def should_have_correct_entry(self, topic):
                expect(topic).to_match_as_set(['1'])

        class TheLatestAddKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.hgetall('queue.test.latest')

            def should_be_empty(self, topic):
                expect(topic).to_be_empty()

        class TheDataProcessed(Vows.Context):
            def topic(self, queue):
                return queue.data_processed

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

        class TheEntrySet(Vows.Context):
            def topic(self, queue):
                return queue.redis.smembers('queue.test.entries')

            def should_be_empty(self, topic):
                expect(topic).to_be_empty()

        class TheLatestAddKey(Vows.Context):
            def topic(self, queue):
                return queue.redis.hgetall('queue.test.latest')

            def should_be_empty(self, topic):
                expect(topic).to_be_empty()

        class TheDataProcessed(Vows.Context):
            def topic(self, queue):
                return queue.data_processed

            def should_be_first_and_second_items(self, topic):
                expect(topic).to_be_like([{'a': 1, 'b': 1}, {'a': 1, 'b': 2}])
