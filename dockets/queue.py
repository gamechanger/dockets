import sys
import logging
import uuid
import time
import pickle
import collections
import math

from threading import Thread
from time import sleep
from dockets import errors, _global_event_handler_classes, _global_retry_error_classes
from dockets.pipeline import PipelineObject
from dockets.metadata import WorkerMetadataRecorder
from dockets.json_serializer import JsonSerializer
from dockets.queue_event_registrar import QueueEventRegistrar
from dockets.stat_gatherer import StatGatherer
from dockets.error_queue import ErrorQueue, DummyErrorQueue

logger = logging.getLogger(__name__)

class Queue(PipelineObject):
    """
    Basic queue that does no entry tracking
    """

    FIFO = 'FIFO'
    LIFO = 'LIFO'

    # Queue events
    SUCCESS = 'success'
    EXPIRE = 'expire'
    ERROR = 'error'
    OPERATION_ERROR = 'operation-error'
    RETRY = 'retry'
    PUSH = 'push'

    # Operation errors
    SERIALIZATION = 'serialization'
    DESERIALIZATION = 'deserialization'

    def __init__(self, redis, name, stat_gatherer_cls=StatGatherer, use_error_queue=False, **kwargs):
        super(Queue, self).__init__(redis)

        self.name = name
        self.mode = kwargs.get('mode', self.FIFO)
        assert self.mode in [self.FIFO, self.LIFO], 'Invalid mode'
        self.key = kwargs.get('key')
        self.version = kwargs.get('version', 1)
        self.worker_id = uuid.uuid1()

        self._wait_time = kwargs.get('wait_time') or 10
        self._activity_timeout = kwargs.get('timeout', 60)
        self._serializer = kwargs.get('serializer', JsonSerializer())
        self._max_attempts = kwargs.get('max_attempts', 3)
        self._heartbeat_interval = kwargs.get('heartbeat_interval', 5)

        # register event handlers
        self._event_registrar = QueueEventRegistrar(self)
        for handler_class in _global_event_handler_classes:
            self.add_event_handler(handler_class())

        if stat_gatherer_cls is not None:
            self.stat_gatherer = stat_gatherer_cls()
            self.add_event_handler(self.stat_gatherer)

        self._retry_error_classes = list(_global_retry_error_classes) + kwargs.get('retry_error_classes', [])

        self.error_queue = ErrorQueue(self) if use_error_queue else DummyErrorQueue(self)



    @property
    def stats(self):
        if not hasattr(self, "stat_gatherer"):
            raise AttributeError("Stats are not enabled on this queue.")
        return self.stat_gatherer

    ## abstract methods
    def process_item(self, item):
        """
        Override this in subclasses.
        """
        raise NotImplementedError

    def pretty_printer(self, item):
        return self.item_key(item)

    def item_key(self, item):
        if self.key:
            return '_'.join(str(item.get(key_component, ''))
                            for key_component in self.key)
        return str(item)


    ## public methods

    @PipelineObject.with_pipeline
    def pop(self, pipeline):
        pop_pipeline = self.redis.pipeline()
        args = [self._queue_key(), self._working_queue_key(), self._wait_time]
        pop_pipeline.execute()
        serialized_envelope = self.redis.brpoplpush(*args)
        if not serialized_envelope:
            return None
        try:
            envelope = self._serializer.deserialize(serialized_envelope)
        except Exception:
            self.raw_complete(serialized_envelope)
            self._event_registrar.on_operation_error(exc_info=sys.exc_info(),
                                                     pipeline=pipeline)
            return None
        return envelope


    @PipelineObject.with_pipeline
    def push(self, item, pipeline, first_ts=None, ttl=None, envelope=None,
             max_attempts=None, attempts=0, error_classes=None):
        envelope = {'first_ts': first_ts or (envelope and envelope['first_ts'])
                    or time.time(),
                    'ts': time.time(),
                    'item': item,
                    'v': self.version,
                    'ttl': ttl,
                    'attempts': attempts,
                    'max_attempts': max_attempts or self._max_attempts,
                    'error_classes': pickle.dumps(error_classes)}
        serialized_envelope = self._serializer.serialize(envelope)
        if self.mode == self.FIFO:
            pipeline.lpush(self._queue_key(), serialized_envelope)
        else:
            pipeline.rpush(self._queue_key(), serialized_envelope)
        self._event_registrar.on_push(
            item=item,
            item_key=self.item_key(item),
            pipeline=pipeline,
            pretty_printed_item=self.pretty_printer(item))


    @PipelineObject.with_pipeline
    def complete(self, envelope, pipeline):
        """
        Basic queue complete doesn't use the envelope--we always just lpop
        """
        pipeline.lpop(self._working_queue_key())


    def delete(self, envelope, *args, **kwargs):
        """
        Callback invoked when an item in the queue's associated error queue
        is deleted. Allows subclasses to perform custom cleanup.
        """
        pass


    @PipelineObject.with_pipeline
    def raw_complete(self, serialized_envelope, pipeline):
        """
        Just removes the envelope from working. Used in error-recovery
        cases where we just want to bail out
        """
        pipeline.lrem(self._working_queue_key(), serialized_envelope)

    @PipelineObject.with_pipeline
    def _heartbeat(self, pipeline):
        """
        Pushes out this worker's timeout.
        """
        pipeline.sadd(self._workers_set_key(), self.worker_id)
        pipeline.setex(self._worker_activity_key(), 1,
                       int(math.ceil(self._heartbeat_interval * 5)))


    def _heartbeat_thread(self, should_stop):
        """
        Performs a "heartbeat" every interval. In practice this just pushes out
        the expiry on this worker's activity key to ensure its items are not
        reclaimed.
        """
        def run_heartbeat():
            while True:
                if should_stop():
                    break
                self._heartbeat()
                sleep(self._heartbeat_interval)

        return Thread(target=run_heartbeat)

    def run(self, should_continue=None):
        self.register_worker()
        should_continue = should_continue or (lambda: True)

        stopping = False

        heartbeat_thread = self._heartbeat_thread(lambda: stopping)
        heartbeat_thread.start()

        while True:
            if not should_continue():
                stopping = True

            if stopping:
                heartbeat_thread.join(self._heartbeat_interval * 2)
                break

            self.run_once()


    def register_worker(self):
        self._reclaim()

        worker_recorder = WorkerMetadataRecorder(self.redis, self._queue_key(),
                                                 self.worker_id)
        worker_recorder.record_initial_metadata()


    def run_once(self):
        """
        Run the queue for one step.
        """
        worker_recorder = WorkerMetadataRecorder(self.redis, self._queue_key(),
                                                 self.worker_id)
        # The Big Pipeline
        pipeline = self.redis.pipeline()
        envelope = self.pop(pipeline=pipeline)

        if not envelope:
            self._event_registrar.on_empty(pipeline=pipeline)
            pipeline.execute()
            return None

        item = envelope['item']
        pop_time = time.time()
        response_time = pop_time - float(envelope['first_ts'])
        item_error_classes = self.error_classes_for_envelope(envelope)
        self._event_registrar.on_pop(item=item, item_key=self.item_key(item),
                                     response_time=response_time,
                                     pipeline=pipeline)

        def handle_error():
            self._event_registrar.on_error(
                item=item,
                item_key=self.item_key(item),
                pipeline=pipeline, exc_info=sys.exc_info(),
                pretty_printed_item=self.pretty_printer(item))
            worker_recorder.record_error(pipeline=pipeline)
            self.error_queue.queue_error(envelope, pipeline=pipeline)

        try:
            if envelope['ttl'] and (envelope['first_ts'] + envelope['ttl'] < time.time()):
                raise errors.ExpiredError
            self.process_item(envelope['item'])
        except errors.ExpiredError:
            self._event_registrar.on_expire(
                item=item,
                item_key=self.item_key(item),
                pipeline=pipeline,
                pretty_printed_item=self.pretty_printer(item))
            worker_recorder.record_expire(pipeline=pipeline)
        except tuple(item_error_classes):
            # If we've tried this item three times already, cut our losses and
            # treat it like other errors.
            max_attempts = envelope.get('max_attempts', self._max_attempts)
            if envelope['attempts'] >= max_attempts - 1:
                handle_error()
            else:
                self._event_registrar.on_retry(
                    item=item,
                    item_key=self.item_key(item),
                    pipeline=pipeline,
                    pretty_printed_item=self.pretty_printer(item))
                worker_recorder.record_retry(pipeline=pipeline)
                # When we retry, first_ts stays the same
                try:
                    self.push(envelope['item'], pipeline=pipeline, envelope=envelope,
                              max_attempts=max_attempts,
                              attempts=envelope['attempts'] + 1,
                              error_classes=item_error_classes)
                except Exception as e:
                    print e
        except Exception:
            handle_error()
        else:
            self._event_registrar.on_success(
                item=item,
                item_key=self.item_key(item),
                pipeline=pipeline,
                pretty_printed_item=self.pretty_printer(item))
            worker_recorder.record_success(pipeline=pipeline)
        finally:
            self.complete(envelope, pipeline=pipeline)
            complete_time = time.time()
            turnaround_time = complete_time - float(envelope['first_ts'])
            processing_time = complete_time - pop_time
            self._event_registrar.on_complete(item=item, item_key=self.item_key(item),
                                              turnaround_time=turnaround_time,
                                              processing_time=processing_time,
                                              pipeline=pipeline)
            pipeline.execute()
            return envelope

    def error_classes_for_envelope(self, envelope):
        def tuplify(thing):
            return thing if isinstance(thing, collections.Iterable) else tuple([thing])

        if 'error_classes' in envelope:
            return tuplify(pickle.loads(envelope['error_classes']) or self._retry_error_classes)
        else:
            return tuplify(self._retry_error_classes)

    def add_event_handler(self, handler):
        self._event_registrar.register(handler)

    ## reporting

    def working(self):
        return sum(self.redis.llen(self._working_queue_key())
                   for worker_id
                   in self.redis.smembers(self._workers_set_key()))

    def queued(self):
        return self.redis.llen(self._queue_key())

    def queued_items(self):
        return self.redis.lrange(self._queue_key(), 0, sys.maxint)

    ## names of keys in redis

    def _queue_key(self):
        return 'queue.{0}'.format(self.name)

    def _workers_set_key(self):
        return 'queue.{0}.workers'.format(self.name)

    def _working_queue_key(self, worker_id=None):
        return 'queue.{0}.{1}.working'.format(self.name, worker_id or self.worker_id)

    def _worker_activity_key(self):
        return 'queue.{0}.{1}.active'.format(self.name, self.worker_id)

    ## worker handling

    # TODO rewrite using lua scripting
    def _reclaim(self):
        workers = self.redis.smembers(self._workers_set_key())
        for worker_id in workers:
            if not self.redis.exists(self._worker_activity_key()):
                self._reclaim_worker_queue(worker_id)
                self.redis.srem(self._workers_set_key(), worker_id)

    def _reclaim_worker_queue(self, worker_id):
        while self.redis.rpoplpush(self._working_queue_key(worker_id),
                                   self._queue_key()):
            continue

# For backwards compatibility, put constants at module-level as well.
FIFO = Queue.FIFO
LIFO = Queue.LIFO

# Queue events
SUCCESS = Queue.SUCCESS
EXPIRE = Queue.EXPIRE
ERROR = Queue.ERROR
OPERATION_ERROR = Queue.OPERATION_ERROR
RETRY = Queue.RETRY
PUSH = Queue.PUSH

# Operation errors
SERIALIZATION = Queue.SERIALIZATION
DESERIALIZATION = Queue.DESERIALIZATION


