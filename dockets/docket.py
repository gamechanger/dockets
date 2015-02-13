import sys
import time
from datetime import datetime
import pickle

import dateutil.parser
from redis import WatchError
from time import sleep

from dockets.pipeline import PipelineObject
from dockets.queue import Queue


class Docket(Queue):

    def push(self, item, pipeline=None, when=None, envelope=None,
             max_attempts=None, attempts=None, error_classes=None):
        passed_pipeline = True
        if not pipeline:
            passed_pipeline = False
            pipeline = self.redis.pipeline()
        when = when or (envelope and envelope['when']) or time.time()
        attempts = attempts or (envelope and envelope['attempts']) or 0
        timestamp = self._get_timestamp(when)
        envelope = {'when': timestamp,
                    'ts': time.time(),
                    'first_ts': time.time(),
                    'item': item,
                    'ttl': None,
                    'v': 1,
                    'attempts': attempts,
                    'max_attempts': max_attempts or self._max_attempts,
                    'error_classes': pickle.dumps(error_classes)}
        key = self.item_key(item)
        pipeline.hset(self._payload_key(), key,
                      self._serializer.serialize(envelope))
        pipeline.zadd(self._queue_key(), key, timestamp)
        self._event_registrar.on_push(
            item=item,
            item_key=key,
            pipeline=pipeline,
            pretty_printed_item=self.pretty_printer(item))
        if passed_pipeline:
            return None
        return_values = pipeline.execute()
        was_new_item = return_values[0] #this is the hset
        return was_new_item


    def get_existing_item_for_item(self, item):
        key = self.item_key(item)
        raw_payload = self.redis.hget(self._payload_key(), key)
        return raw_payload and self._serializer.deserialize(raw_payload)['item']


    def get_fire_time(self, item):
        key = self.item_key(item)
        raw_payload = self.redis.hget(self._payload_key(), key)
        return raw_payload and self._serializer.deserialize(raw_payload)['when']


    @PipelineObject.with_pipeline
    def pop(self, pipeline, current_time=None):
        next_envelope = None
        with self.redis.pipeline() as pop_pipeline:
            while True:
                try:
                    pop_pipeline.watch(self._queue_key())
                    try:
                        next_envelope_key = pop_pipeline.zrange(self._queue_key(), 0, 1)[0]
                    except IndexError:
                        # Simulate a blocking ZRANGE by sleeping if there is nothing returned.
                        # This ensures we aren't hammering Redis.
                        sleep(self._wait_time)
                        return

                    next_envelope_json = pop_pipeline.hget(self._payload_key(),
                                                           next_envelope_key)
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
                        # Simulate a blocking call by sleeping if there is nothing returned.
                        # This ensures we aren't hammering Redis.
                        sleep(self._wait_time)
                        return None

                    pop_pipeline.multi()
                    pop_pipeline.zrem(self._queue_key(), next_envelope_key)
                    pop_pipeline.hdel(self._payload_key(), next_envelope_key)
                    pop_pipeline.lpush(self._working_queue_key(),
                                       next_envelope_json)
                    pop_pipeline.execute()
                    break
                except WatchError:
                    continue
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
    def complete(self, envelope, pipeline):
        super(Docket, self).complete(envelope, pipeline=pipeline)

    def _reclaim_worker_queue(self, worker_id):
        working_contents = self.redis.lrange(self._working_queue_key(worker_id),
                                             0, sys.maxint)
        for envelope_json in working_contents:
            envelope = self._serializer.deserialize(envelope_json)
            envelope['attempts'] += 1
            self.push(envelope['item'], envelope=envelope)
        self.redis.delete(self._working_queue_key(worker_id))

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


