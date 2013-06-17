import time
from dockets.stat_gatherer import StatGatherer
from mock import Mock
from util import *
from dockets.queue import Queue

def assert_within_a_second_of(topic, expected):
    assert topic > expected - 1 and topic < expected + 1

def test_queue_provides_stats():
    queue = Queue(redis, 'test')
    assert queue.stats
    assert queue.stats.successes_per_s is not None

def test_queue_with_stats_disabled_does_not_provide_stats():
    queue = Queue(redis, 'test', stat_gatherer_cls=None)
    assert not hasattr(queue, 'stats')

class FakeGatherer(object):
    pass

def test_queue_with_custom_stat_gatherer():
    queue = Queue(redis, 'test', stat_gatherer_cls=FakeGatherer)
    assert isinstance(queue.stats, FakeGatherer)

queue = Mock()
queue.redis = redis
gatherer = StatGatherer()
gatherer.on_register(queue)

@clear
def test_push():
    pipeline = redis.pipeline()
    gatherer.on_push(pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_push()), time.time())

@clear
def test_pop():
    pipeline = redis.pipeline()
    gatherer.on_pop(123, pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_pop()), time.time())

@clear
def test_complete():
    pipeline = redis.pipeline()
    gatherer.on_complete(123, 456, pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_complete()), time.time())

@clear
def test_success():
    pipeline = redis.pipeline()
    gatherer.on_success(pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_success()), time.time())

@clear
def test_error():
    pipeline = redis.pipeline()
    gatherer.on_error(pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_error()), time.time())

@clear
def test_retry():
    pipeline = redis.pipeline()
    gatherer.on_retry(pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_retry()), time.time())

@clear
def test_remove():
    pipeline = redis.pipeline()
    gatherer.on_remove(pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_remove()), time.time())

@clear
def test_expire():
    pipeline = redis.pipeline()
    gatherer.on_expire(pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_expire()), time.time())

@clear
def test_operation_error():
    pipeline = redis.pipeline()
    gatherer.on_operation_error(pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_operation_error()), time.time())

@clear
def test_reclaim():
    pipeline = redis.pipeline()
    gatherer.on_reclaim(pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_reclaim()), time.time())

@clear
def test_pop():
    pipeline = redis.pipeline()
    gatherer.on_pop(123, pipeline=pipeline)
    pipeline.execute()
    assert_within_a_second_of(float(gatherer.last_pop()), time.time())
