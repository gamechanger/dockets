import sys
import logging
import time
from datetime import datetime
from numbers import Number
import uuid

import dateutil.parser
from redis import WatchError

from dockets import errors
from dockets.pipeline import PipelineObject
from dockets.queue import Queue
from dockets.json_serializer import JsonSerializer
from dockets.metadata import TimeTracker

class Docket(Queue):

    def __init__(self, *args, **kwargs):
        super(Docket, self).__init__(*args, **kwargs)
        self._wait_time = kwargs.get('wait_time') or 60

    def _payload_key(self):
        return '{0}.payload'.format(self._queue_key())

    @PipelineObject.with_pipeline
    def push(self, item, pipeline, when=None, envelope=None):
        when = when or (envelope and envelope['when']) or time.time()
        timestamp = self._get_timestamp(when)
        envelope = {'when': timestamp,
                    'ts': time.time(),
                    'first_ts': time.time(),
                    'item': item,
                    'ttl': None,
                    'v': 1}
        key = self.item_key(item)
        pipeline.hset(self._payload_key(), key,
                      self._serializer.serialize(envelope))
        pipeline.zadd(self._queue_key(),
                      key,
                      timestamp)
        self._event_registrar.on_push(item=item, item_key=key, pipeline=pipeline)

    @PipelineObject.with_pipeline
    def pop(self, worker_id, pipeline, current_time=None):
        next_envelope = None
        with self.redis.pipeline() as pop_pipeline:
            while True:
                try:
                    pop_pipeline.watch(self._queue_key())
                    try:
                        next_envelope_key = pop_pipeline.zrange(self._queue_key(), 0, 1)[0]
                    except IndexError:
                        return None
                    next_envelope_json = pop_pipeline.hget(self._payload_key(),
                                                           next_envelope_key)
                    deserialization_failure = False
                    try:
                        next_envelope = self._serializer.deserialize(next_envelope_json)
                    except:
                        # Operation error. Bail out.
                        pop_pipeline.multi()
                        pop_pipeline.zrem(self._queue_key(), next_envelope_key)
                        pop_pipeline.hdel(self._payload_key(), next_envelope_key)
                        pop_pipeline.execute()
                        self._event_registrar.on_operation_error(exc_info=sys.exc_info(),
                                                                 pipeline=pipeline)
                        return None
                    if next_envelope['when'] > (current_time or time.time()):
                        return None
                    pop_pipeline.multi()
                    pop_pipeline.zrem(self._queue_key(), next_envelope_key)
                    pop_pipeline.hdel(self._payload_key(), next_envelope_key)
                    pop_pipeline.lpush(self._working_queue_key(worker_id),
                                       next_envelope_json)
                    pop_pipeline.execute()
                    break
                except WatchError:
                    continue
        self._record_worker_activity(worker_id, pipeline=pipeline)
        return next_envelope

    def remove(self, item):
        # can't use with_pipeline here because we need the return value
        key = self.item_key(item)
        with self.redis.pipeline() as pipeline:
            pipeline.zrem(self._queue_key(), key)
            pipeline.hdel(self._payload_key(), key)
            self._event_registrar.on_remove(item=item, item_key=key,
                pipeline=pipeline)
            return pipeline.execute()[0]

    @PipelineObject.with_pipeline
    def complete(self, envelope, worker_id, pipeline):
        super(Docket, self).complete(envelope, worker_id, pipeline=pipeline)

    def _reclaim_worker_queue(self, worker_id):
        working_contents = self.redis.lrange(self._working_queue_key(worker_id),
                                             0, sys.maxint)
        for envelope_json in working_contents:
            envelope = self._serializer.deserialize(envelope_json)
            self.push(envelope['item'], envelope=envelope)
        self.redis.delete(self._working_queue_key(worker_id))


    def run(self, worker_id=None, extra_metadata={}):
        worker_id = self.register_worker(worker_id, extra_metadata)
        while True:
            if not self.run_once(worker_id):
                logging.info('No scheduled items. Sleeping for %s seconds' % self._wait_time)
                time.sleep(self._wait_time)

    def queued(self):
        return self.redis.zcard(self._queue_key())

    def queued_items(self):
        return [self.redis.hget(self._payload_key(), key)
                for key
                in self.redis.zrevrange(self._queue_key(), 0, sys.maxint)]

    @staticmethod
    def _get_timestamp(when):
        try:
            if isinstance(when, datetime):
                return time.mktime(when.timetuple())
            elif isinstance(when, basestring):
                return dateutil.parser.parse(when)
            else:
                return float(when)
        except:
            raise TypeError('Invalid time specification', when)

class TestDocket(Queue):
    def process_item(self, item):
        time.sleep(2)
        logging.info('in process_item, processing {0}'.format(item))
