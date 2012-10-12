import sys
import logging
import uuid
from collections import defaultdict
import simplejson as json
import time

from dockets import errors
from dockets.pipeline import PipelineObject
from dockets.metadata import WorkerMetadataRecorder, RateTracker, TimeTracker
from dockets.json_serializer import JsonSerializer

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

        _error_tracker_size = kwargs.get('error_tracker_size', 50)
        _retry_tracker_size = kwargs.get('retry_tracker_size',50)
        _expire_tracker_size = kwargs.get('expire_tracker_size',50)
        _success_tracker_size = kwargs.get('success_tracker_size', 100)
        _operation_error_tracker_size = kwargs.get('operation_error_tracker_size', 50)

        _response_time_tracker_size = kwargs.get('response_time_tracker_size', 100)
        _turnaround_time_tracker_size = kwargs.get('turnaround_time_tracker_size', 100)

        self._error_tracker = RateTracker(self.redis, self._queue_key(), self.ERROR, _error_tracker_size)
        self._retry_tracker = RateTracker(self.redis, self._queue_key(), self.RETRY, _error_tracker_size)
        self._expire_tracker = RateTracker(self.redis, self._queue_key(), self.EXPIRE, _expire_tracker_size)
        self._success_tracker = RateTracker(self.redis, self._queue_key(), self.SUCCESS, _success_tracker_size)
        self._operation_error_tracker = RateTracker(self.redis, self._queue_key(),
                                                    self.OPERATION_ERROR, _operation_error_tracker_size)


        self._response_time_tracker = TimeTracker(self.redis, self._queue_key(), 'response', _response_time_tracker_size)
        self._turnaround_time_tracker = TimeTracker(self.redis, self._queue_key(), 'turnaround', _turnaround_time_tracker_size)

        self._handlers = defaultdict(list)

    ## abstract methods
    def process_item(self, item):
        """
        Override this in subclasses.
        """
        raise NotImplementedError

    def log(self, event, envelope, error=False):
        if error:
            logger.error('{0} {1} {2}'.format(self.name,
                                              event.capitalize(),
                                              self.item_key(envelope['item'])),
                         exc_info=True)
        logger.info('{0} {1} {2}'.format(self.name, event.capitalize(),
                                         self.item_key(envelope['item'])))

    def handle_operation_error(self, error, envelope=None):
        """
        This provides a hook for queues to deal with queue operation
        errors such as deserialization failures. When this function is
        called with an envelope, that envelope is guaranteed to be
        entirely out of the queue.

        The default is to log errors and drop the envelope on the
        floor.
        """
        self._operation_error_tracker.count()
        logger.error('Operation Error ({0}): {1}'.format(error.capitalize(), envelope),
                     exc_info=True)

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
            self.handle_operation_error(self.DESERIALIZATION, serialized_envelope)
            return None
        self._record_worker_activity(worker_id, pipeline=pipeline)
        self._response_time_tracker.add_time(time.time()-float(envelope['ts']),
                                             pipeline=pipeline)
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
        self.log(self.PUSH, envelope)

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

        try:
            if envelope['ttl'] and (envelope['first_ts'] + envelope['ttl'] < time.time()):
                raise errors.ExpiredError
            return_value = self.process_item(envelope['item'])
        except errors.ExpiredError:
            self._expire_tracker.count(pipeline=pipeline)
            self.log(self.EXPIRE, envelope)
            worker_recorder.record_expire(pipeline=pipeline)
        except errors.RetryError:
            self._retry_tracker.count(pipeline=pipeline)
            self.log(self.RETRY, envelope)
            worker_recorder.record_retry(pipeline=pipeline)
            # When we retry, first_ts stsys the same
            self.push(envelope['item'], pipeline=pipeline, envelope=envelope)
        except Exception as e:
            self.log(self.ERROR, envelope, error=True)
            self._handle_return_value(envelope, self.ERROR, pipeline)
            self._error_tracker.count(pipeline=pipeline)
            worker_recorder.record_error(pipeline=pipeline)
        else:
            self.log(self.SUCCESS, envelope)
            self._handle_return_value(envelope, return_value, pipeline)
            self._success_tracker.count(pipeline=pipeline)
            worker_recorder.record_success(pipeline=pipeline)
        finally:
            self.complete(envelope, worker_id, pipeline=pipeline)
            self._turnaround_time_tracker.add_time(time.time()-float(envelope['first_ts']),
                                                   pipeline=pipeline)
            pipeline.execute()
        return envelope



    def add_handler(self, return_value, handler):
        self._handlers[return_value].append(handler)

    ## reporting

    def error_rate(self):
        return self._error_tracker.rate()

    def success_rate(self):
        return self._success_tracker.rate()

    def retry_rate(self):
        return self._retry_tracker.rate()

    def operation_error_rate(self):
        return self._operation_error_tracker.rate()

    def response_time(self):
        return self._response_time_tracker.average_time()

    def turnaround_time(self):
        return self._turnaround_time_tracker.average_time()

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

    ## queueing fundamentals

    def _handle_return_value(self, envelope, value, pipeline):
        item = envelope['item']
        key = self.item_key(item)
        first_ts = envelope['first_ts']
        for handler in self._handlers.get(value, []):
            handler(item, first_ts=first_ts, pipeline=pipeline, redis=self.redis,
                    value=value, key=key)

    ## names of keys in redis

    def _queue_key(self):
        return 'queue.{0}'.format(self.name)

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
