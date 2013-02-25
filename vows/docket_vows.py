import simplejson
from traceback import print_exc

from pyvows import Vows, expect
from util import FakeRedisContext


from dockets.docket import Docket
from dockets.errors import RetryError

class TestDocketWithKey(Docket):
    def __init__(self, *args, **kwargs):
        kwargs['key'] = ['a']
        super(TestDocketWithKey, self).__init__(*args, **kwargs)
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


class DocketWithKeyContext(FakeRedisContext):
    def __init__(self, *args, **kwargs):
        try:
            super(DocketWithKeyContext, self).__init__(*args, **kwargs)
        except:
            print_exc()
        self.ignore('use_queue')

    def use_redis(self, redis):
        queue = TestDocketWithKey(redis, 'test')
        self.use_queue(queue)
        return queue

    def use_queue(self, queue):
        raise NotImplementedError

@Vows.batch
class ADocket(Vows.Context):
    class WhenPushedToOnceAndRemovedFromOnce(DocketWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 1})
            queue.remove({'a': 1})

        def should_be_empty(self, queue):
            expect(queue.queued()).to_equal(0)

        class WhenPushedToAgain(Vows.Context):
            def topic(self, queue):
                return queue.push({'a': 1})

            def push_should_return_true(self, topic):
                expect(topic).to_be_true()

    class WhenPushedToOnceAndPoppedFromBeforeTime(DocketWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 1}, when=2)
            queue.pop(worker_id='test_worker', current_time=1)

        def queued_should_be_one(self, queue):
            expect(queue.queued()).to_equal(1)

        class WhenPushedToAgain(Vows.Context):
            def topic(self, queue):
                return queue.push({'a': 1})

            def push_should_return_false(self, topic):
                expect(topic).to_be_false()

    class WhenAnItemIsPushed(DocketWithKeyContext):
        def use_queue(self, queue):
            queue.push({'a': 7}, when=2)

        def item_should_match_pushed(self, queue):
            expect(queue.get_existing_item_for_item({'a': 7})).to_equal({'a': 7})

        def item_should_only_match_self(self, queue):
            expect(queue.get_existing_item_for_item({'a': 1})).to_equal(None)

        def item_should_have_same_fire_time(self, queue):
            expect(queue.get_fire_time({'a': 7})).to_equal(2)
