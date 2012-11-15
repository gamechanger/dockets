import sys
import logging
import uuid
import time

from dockets import errors, _global_event_handler_classes
from dockets.pipeline import PipelineObject
from dockets.metadata import WorkerMetadataRecorder
from dockets.json_serializer import JsonSerializer
from dockets.queue_event_registrar import QueueEventRegistrar

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

    def __init__(self, redis, name, **kwargs):
        super(Queue, self).__init__(redis)

        self.name = name
        self.mode = kwargs.get('mode', self.FIFO)
        assert self.mode in [self.FIFO, self.LIFO], 'Invalid mode'
        self.key = kwargs.get('key')
        self.version = kwargs.get('version', 1)

        self._activity_timeout = kwargs.get('timeout', 60)
        self._serializer = kwargs.get('serializer', JsonSerializer())

        self._event_registrar = QueueEventRegistrar(self)

        for handler_class in _global_event_handler_classes:
            self.add_event_handler(handler_class())

    ## abstract methods
    def process_item(self, item):
        """
        Override this in subclasses.
        """
        raise NotImplementedError

    def item_key(self, item):
        if self.key:
            return '_'.join(str(item.get(key_component, ''))
                            for key_component in self.key)
        return str(item)


    ## public methods

    @PipelineObject.with_pipeline
    def pop(self, worker_id, pipeline, blocking=True, timeout=None):
        args = [self._queue_key(), self._working_queue_key(worker_id)]
        if timeout:
            args.append(timeout)
        if blocking:
            serialized_envelope = self.redis.brpoplpush(*args)
        else:
            serialized_envelope = self.redis.rpoplpush(*args)
        if not serialized_envelope:
            return None
        try:
            envelope = self._serializer.deserialize(serialized_envelope)
        except Exception as e:
            self.raw_complete(serialized_envelope, worker_id)
            self._event_registrar.on_operation_error(exc_info=sys.exc_info(),
                                                     pipeline=pipeline)
            return None
        self._record_worker_activity(worker_id, pipeline=pipeline)
        return envelope


    @PipelineObject.with_pipeline
    def push(self, item, pipeline, first_ts=None, ttl=None, envelope=None):
        envelope = {'first_ts': first_ts or (envelope and envelope['first_ts'])
                    or time.time(),
                    'ts': time.time(),
                    'item': item,
                    'v': self.version,
                    'ttl': ttl}
        serialized_envelope = self._serializer.serialize(envelope)
        if self.mode == self.FIFO:
            pipeline.lpush(self._queue_key(), serialized_envelope)
        else:
            pipeline.rpush(self._queue_key(), serialized_envelope)
        self._event_registrar.on_push(item=item, item_key=self.item_key(item),
                                      pipeline=pipeline)


    @PipelineObject.with_pipeline
    def complete(self, envelope, worker_id, pipeline):
        """
        Basic queue complete doesn't use the envelope--we always just lpop
        """
        pipeline.lpop(self._working_queue_key(worker_id))


    @PipelineObject.with_pipeline
    def raw_complete(self, serialized_envelope, worker_id, pipeline):
        """
        Just removes the envelope from working. Used in error-recovery
        cases where we just want to bail out
        """
        pipeline.lrem(self._working_queue_key(worker_id), serialized_envelope)

    def run(self, worker_id=None, extra_metadata={}):
        worker_id = self.register_worker(worker_id, extra_metadata)
        while True:
            self.run_once(worker_id)

    def register_worker(self, worker_id=None, extra_metadata={}):
        self._reclaim()

        # set up this worker
        worker_id = worker_id or uuid.uuid1()
        worker_recorder = WorkerMetadataRecorder(self.redis, self._queue_key(),
                                                 worker_id)
        worker_recorder.record_initial_metadata(extra_metadata)
        return worker_id

    def run_once(self, worker_id):
        """
        Run the queue for one step. Use blocking mode unless you can't
        (e.g. unit tests)
        """
        worker_recorder = WorkerMetadataRecorder(self.redis, self._queue_key(),
                                                 worker_id)
        # The Big Pipeline
        pipeline = self.redis.pipeline()
        envelope = self.pop(worker_id, pipeline=pipeline)

        # if we time out
        if not envelope:
            return None

        item = envelope['item']
        pop_time = time.time()
        response_time = pop_time - float(envelope['first_ts'])
        self._event_registrar.on_pop(item=item, item_key=self.item_key(item),
                                     response_time=response_time,
                                     pipeline=pipeline)

        try:
            if envelope['ttl'] and (envelope['first_ts'] + envelope['ttl'] < time.time()):
                raise errors.ExpiredError
            return_value = self.process_item(envelope['item'])
        except errors.ExpiredError:
            self._event_registrar.on_expire(item=item, item_key=self.item_key(item),
                                            pipeline=pipeline)
            worker_recorder.record_expire(pipeline=pipeline)
        except errors.RetryError:
            self._event_registrar.on_retry(item=item, item_key=self.item_key(item),
                                           pipeline=pipeline)
            worker_recorder.record_retry(pipeline=pipeline)
            # When we retry, first_ts stsys the same
            self.push(envelope['item'], pipeline=pipeline, envelope=envelope)
        except Exception as e:
            self._event_registrar.on_error(item=item, item_key=self.item_key(item),
                                           pipeline=pipeline, exc_info=sys.exc_info())
            worker_recorder.record_error(pipeline=pipeline)
        else:
            self._event_registrar.on_success(item=item, item_key=self.item_key(item),
                                             pipeline=pipeline)
            worker_recorder.record_success(pipeline=pipeline)
        finally:
            self.complete(envelope, worker_id, pipeline=pipeline)
            complete_time = time.time()
            turnaround_time = complete_time - float(envelope['first_ts'])
            processing_time = complete_time - pop_time
            self._event_registrar.on_complete(item=item, item_key=self.item_key(item),
                                              turnaround_time=turnaround_time,
                                              processing_time=processing_time,
                                              pipeline=pipeline)
            pipeline.set(self._queue_last_ran_key(), time.time())
            pipeline.execute()
        return envelope

    def add_event_handler(self, handler):
        self._event_registrar.register(handler)

    ## reporting

    def active_worker_metadata(self):
        data = {}
        for worker_id in self.redis.smembers(self._workers_set_key()):
            worker_metadata = WorkerMetadataRecorder(self.redis, self._queue_key(), worker_id).all_data()
            worker_metadata['working'] = self.redis.llen(self._working_queue_key(worker_id))
            worker_metadata['active'] = self.redis.exists(self._worker_activity_key(worker_id))
            data[worker_id] = worker_metadata
        return data

    def working(self):
        return sum(self.redis.llen(self._working_queue_key(worker_id))
                   for worker_id
                   in self.redis.smembers(self._workers_set_key()))

    def queued(self):
        return self.redis.llen(self._queue_key())

    def queued_items(self):
        return self.redis.lrange(self._queue_key(), 0, sys.maxint)

    def last_ran(self):
        """Returns the epoch time indicating when the queue processor last 
        ran to process an item."""
        return self.redis.get(self._queue_last_ran_key())

    ## names of keys in redis

    def _queue_key(self):
        return 'queue.{0}'.format(self.name)

    def _queue_last_ran_key(self):
        return 'queue.{0}.last_ran'.format(self.name)

    def _workers_set_key(self):
        return 'queue.{0}.workers'.format(self.name)

    def _working_queue_key(self, worker_id):
        return 'queue.{0}.{1}.working'.format(self.name, worker_id)

    def _worker_activity_key(self, worker_id):
        return 'queue.{0}.{1}.active'.format(self.name, worker_id)


    ## worker handling

    def _worker_metadata(self, extra_metadata):
        metadata = {'hostname': os.uname()[1],
                    'start_ts': time.time()}
        metadata.update(extra_metadata)
        return metadata

    @PipelineObject.with_pipeline
    def _record_worker_activity(self, worker_id, pipeline):
        pipeline.sadd(self._workers_set_key(), worker_id)
        pipeline.setex(self._worker_activity_key(worker_id), 1,
                       self._activity_timeout)

    # TODO rewrite using lua scripting
    def _reclaim(self):
        workers = self.redis.smembers(self._workers_set_key())
        for worker_id in workers:
            if not self.redis.exists(self._worker_activity_key(worker_id)):
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



class TestQueue(Queue):
    def process_item(self, item):
        time.sleep(2)
        logging.info('in process_item, processing {0}'.format(item))
