import logging
import uuid
from collections import defaultdict
import simplejson as json
import time

from dockets import errors
from dockets.pipeline import PipelineObject
from dockets.metadata import WorkerMetadataRecorder, RateTracker, TimeTracker
FIFO = 'FIFO'
LIFO = 'LIFO'

logger = logging.getLogger(__name__)

# Queue events
SUCCESS = 'success'
EXPIRE = 'expire'
ERROR = 'error'
RETRY = 'retry'
PUSH = 'push'

class Queue(PipelineObject):
    """
    Basic queue that does no entry tracking
    """

    def __init__(self, redis, name, **kwargs):
        super(Queue, self).__init__(redis)
        self.name = name
        self.mode = kwargs.get('mode', FIFO)
        assert self.mode in [FIFO, LIFO], 'Invalid mode'
        self.key = kwargs.get('key')
        self.version = kwargs.get('version', 1)

        self._activity_timeout = kwargs.get('timeout', 60)
        self._loads_kwargs = kwargs.get('loads_kwargs', {})
        self._dumps_kwargs = kwargs.get('dumps_kwargs', {})

        _error_tracker_size = kwargs.get('error_tracker_size', 50)
        _retry_tracker_size = kwargs.get('retry_tracker_size',50)
        _expire_tracker_size = kwargs.get('expire_tracker_size',50)
        _success_tracker_size = kwargs.get('success_tracker_size', 100)

        _response_time_tracker_size = kwargs.get('response_time_tracker_size', 100)
        _turnaround_time_tracker_size = kwargs.get('turnaround_time_tracker_size', 100)

        self._error_tracker = RateTracker(self.redis, self.name, ERROR, _error_tracker_size)
        self._retry_tracker = RateTracker(self.redis, self.name, RETRY, _error_tracker_size)
        self._expire_tracker = RateTracker(self.redis, self.name, EXPIRE, _expire_tracker_size)
        self._success_tracker = RateTracker(self.redis, self.name, SUCCESS, _success_tracker_size)


        self._response_time_tracker = TimeTracker(self.redis, self.name, 'response', _response_time_tracker_size)
        self._turnaround_time_tracker = TimeTracker(self.redis, self.name, 'turnaround', _turnaround_time_tracker_size)

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

    def item_key(self, item):
        if self.key:
            return '_'.join(str(item.get(key_component, ''))
                            for key_component in self.key)
        return str(item)


    ## public methods

    @PipelineObject.with_pipeline
    def pop(self, worker_id, pipeline, blocking=True):
        args = [self._queue_key(), self._working_queue_key(worker_id)]
        if blocking:
            serialized_envelope = self.redis.brpoplpush(*args)
        else:
            serialized_envelope = self.redis.rpoplpush(*args)
        if not serialized_envelope:
            return None
        envelope = self._deserialize(serialized_envelope)
        self._record_worker_activity(worker_id, pipeline=pipeline)
        self._response_time_tracker.add_time(time.time()-float(envelope['ts']),
                                             pipeline=pipeline)
        return envelope


    @PipelineObject.with_pipeline
    def push(self, item, pipeline, first_ts=None, ttl=None):
        envelope = {'first_ts': first_ts or time.time(),
                'ts': time.time(),
                'item': item,
                'v': self.version,
                'ttl': ttl}
        serialized_envelope = self._serialize(envelope)
        if self.mode == FIFO:
            pipeline.lpush(self._queue_key(), serialized_envelope)
        else:
            pipeline.rpush(self._queue_key(), serialized_envelope)
        self.log(PUSH, envelope)


    @PipelineObject.with_pipeline
    def complete(self, envelope, worker_id, pipeline):
        pipeline.lrem(self._working_queue_key(worker_id), self._serialize(envelope))

    def run(self, worker_id=None, extra_metadata={}):
        self.register_worker(worker_id, extra_metadata)
        while True:
            self.run_once(worker_id)

    def register_worker(self, worker_id=None, extra_metadata={}):
        self._reclaim()

        # set up this worker
        worker_id = worker_id or uuid.uuid1()
        worker_recorder = WorkerMetadataRecorder(self.redis, self.name,
                                                 worker_id)
        worker_recorder.record_initial_metadata(extra_metadata)
        return worker_id

    def run_once(self, worker_id):
        """
        Run the queue for one step. Use blocking mode unless you can't
        (e.g. unit tests)
        """
        worker_recorder = WorkerMetadataRecorder(self.redis, self.name,
                                                 worker_id)
        # The Big Pipeline
        pipeline = self.redis.pipeline()
        envelope = self.pop(worker_id, pipeline=pipeline)

        # if we time out
        if not envelope:
            return

        try:
            if envelope['ttl'] and (envelope['first_ts'] + envelope['ttl'] < time.time()):
                raise errors.ExpiredError
            return_value = self.process_item(envelope['item'])
        except errors.ExpiredError:
            self._expire_tracker.count(pipeline=pipeline)
            self.log(EXPIRE, envelope)
            worker_recorder.record_expire(pipeline=pipeline)
        except errors.RetryError:
            self._retry_tracker.count(pipeline=pipeline)
            self.log(RETRY, envelope)
            worker_recorder.record_retry(pipeline=pipeline)
            # When we retry, first_ts stsys the same
            self.push(envelope['item'], first_ts=envelope['first_ts'], pipeline=pipeline)
        except Exception as e:
            self.log(ERROR, envelope, error=True)
            self._error_tracker.count(pipeline=pipeline)
            worker_recorder.record_error(pipeline=pipeline)
        else:
            self.log(SUCCESS, envelope)
            self._handle_return_value(envelope, return_value, pipeline)
            self._success_tracker.count(pipeline=pipeline)
            worker_recorder.record_success(pipeline=pipeline)
        finally:
            self.complete(envelope, worker_id, pipeline=pipeline)
            self._turnaround_time_tracker.add_time(time.time()-float(envelope['first_ts']),
                                                   pipeline=pipeline)
            pipeline.execute()



    def add_handler(self, return_value, handler):
        self._handlers[return_value].append(handler)

    ## reporting

    def error_rate(self):
        return self._error_tracker.rate()

    def success_rate(self):
        return self._success_tracker.rate()

    def retry_rate(self):
        return self._retry_tracker.rate()

    def response_time(self):
        return self._response_time_tracker.average_time()

    def turnaround_time(self):
        return self._turnaround_time_tracker.average_time()

    def active_worker_metadata(self):
        data = {}
        for worker_id in self.redis.smembers(self._workers_set_key()):
            worker_metadata = WorkerMetadataRecorder(self.redis, self.name, worker_id).all_data()
            worker_metadata['working'] = self.redis.llen(self._working_queue_key(worker_id))
            worker_metadata['active'] = self.redis.exists(self._worker_activity_key(worker_id))
            data[worker_id] = worker_metadata
        return data

    def queued(self):
        return self.redis.llen(self._queue_key())

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

    ## serialization

    def _serialize(self, obj):
        return json.dumps(obj, sort_keys=True, **self._dumps_kwargs)

    def _deserialize(self, json_obj):
        try:
            obj = json.loads(json_obj, **self._loads_kwargs)
        except:
            raise SerializationError
        return obj

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

class TestQueue(Queue):
    def process_item(self, item):
        time.sleep(2)
        logging.info('in process_item, processing {0}'.format(item))
