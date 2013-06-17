import os
import simplejson
from nose import with_setup
from redis import Redis

redis = Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=int(os.getenv('REDIS_PORT', 6379)),
              db=int(os.getenv('REDIS_DB', 0)))

def assert_queue_entry(json, item):
    assert isinstance(json, basestring)
    entry = simplejson.loads(json)
    assert isinstance(entry, dict)
    assert 'ts' in entry
    assert 'first_ts' in entry
    assert 'v' in entry
    assert 'item' in entry
    assert entry['item'] == item

def assert_error_queue_empty(queue):
    assert not queue.error_queue.errors()

class TestRetryError(Exception):
    pass

clear = with_setup(redis.flushdb)
