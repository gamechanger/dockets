from pyvows import Vows, expect
from dockets.stat_gatherer import StatGatherer
from fakeredis import FakeRedis
from mock import Mock
from dockets.queue import Queue
import time

redis = FakeRedis()


@Vows.create_assertions
def to_be_within_a_second_of(topic, expected):
    return topic > expected - 1 and topic < expected + 1


@Vows.batch
class AQueue(Vows.Context):
    def topic(self):
        queue = Queue(redis, 'test')
        return queue

    def should_provide_stats(self, queue):
        expect(queue.stats).Not.to_be_null()
        expect(queue.stats.successes_per_s).Not.to_be_null()


@Vows.batch
class AQueueWithStatsDisabled(Vows.Context):
    def topic(self):
        queue = Queue(redis, 'test', stat_gatherer_cls=None)
        return queue.stats

    def should_refuse_to_provide_stats(self, exception):
        expect(exception).to_be_an_error_like(AttributeError)


class FakeGatherer(object):
    pass


@Vows.batch
class AQueueWithACustomStatGatherer(Vows.Context):

    def topic(self):
        queue = Queue(redis, 'test', stat_gatherer_cls=FakeGatherer)
        return queue

    def should_provide_stats(self, queue):
        expect(queue.stats).to_be_instance_of(FakeGatherer)


@Vows.batch
class AStatGatherer(Vows.Context):
    def topic(self):
        queue = Mock()
        queue.redis = redis
        queue.name = 'fake'
        gatherer = StatGatherer()
        gatherer.on_register(queue)
        return gatherer

    class OnPush(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_push(pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_push())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_pop(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_pop(123, pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_pop())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_complete(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_complete(123, 456, pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_complete())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_success(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_success(pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_success())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_error(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_error(pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_error())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_retry(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_retry(pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_retry())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_remove(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_remove(pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_remove())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_expire(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_expire(pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_expire())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_operation_error(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_operation_error(pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_operation_error())
            expect(ts).to_be_within_a_second_of(time.time())

    class on_reclaim(Vows.Context):
        def topic(self, gatherer):
            pipeline = redis.pipeline()
            gatherer.on_reclaim(pipeline=pipeline)
            pipeline.execute()
            return gatherer

        def should_be_just_now(self, gatherer):
            ts = float(gatherer.last_reclaim())
            expect(ts).to_be_within_a_second_of(time.time())
