import logging
import time

from redis import WatchError

from dockets import errors
from dockets.pipeline import PipelineObject
from dockets.queue import Queue
from dockets.isolation_queue import IsolationQueue
from dockets.metadata import WorkerMetadataRecorder

def create_batching_queue(superclass):
    class BatchingQueue(superclass):
        """
        This queue processes its items in batches.
        """

        def __init__(self, *args, **kwargs):
            self._batch_timeout = kwargs.get('batch_timeout') or 60
            self._batch_size = kwargs.get('batch_size') or 10
            super(BatchingQueue, self).__init__(*args, **kwargs)

        def run_once(self, worker_id):
            """
            Run the queue for one step. Use blocking mode unless you can't
            (e.g. unit tests)
            """
            envelopes = []
            worker_recorder = WorkerMetadataRecorder(self.redis, self._queue_key(),
                                                     worker_id)
            # The Big Pipeline
            pipeline = self.redis.pipeline()
            while len(envelopes) < self._batch_size:
                envelope = self.pop(worker_id, pipeline=pipeline, timeout=self._batch_timeout)
                if not envelope:
                    break
                envelopes.append(envelope)
            if not envelopes:
                return None
            envelopes_to_process = list(envelopes)
            try:
                for envelope in envelopes:
                    if envelope['ttl'] and (envelope['first_ts'] + envelope['ttl'] < time.time()):
                        envelopes_to_process.remove(envelope)
                if not envelopes_to_process:
                    raise errors.ExpiredError
                return_value = self.process_items([envelope['item'] for envelope in envelopes_to_process])
            except errors.ExpiredError:
                for envelope in envelopes:
                    self._expire_tracker.count(pipeline=pipeline)
                    self.log(self.EXPIRE, envelope)
                    worker_recorder.record_expire(pipeline=pipeline)
            except errors.RetryError:
                for envelope in envelopes_to_process:
                    self._retry_tracker.count(pipeline=pipeline)
                    self.log(self.RETRY, envelope)
                    worker_recorder.record_retry(pipeline=pipeline)
                    # When we retry, first_ts stsys the same
                    self.push(envelope['item'], pipeline=pipeline, envelope=envelope)
            except Exception as e:
                for envelope in envelopes_to_process:
                    self.log(self.ERROR, envelope, error=True)
                    self._handle_return_value(envelope, self.ERROR, pipeline)
                    self._error_tracker.count(pipeline=pipeline)
                    worker_recorder.record_error(pipeline=pipeline)
            else:
                for envelope in envelopes_to_process:
                    self.log(self.SUCCESS, envelope)
                    self._handle_return_value(envelope, return_value, pipeline)
                    self._success_tracker.count(pipeline=pipeline)
                    worker_recorder.record_success(pipeline=pipeline)
            finally:
                for envelope in envelopes:
                    self.complete(envelope, worker_id, pipeline=pipeline)
                    self._turnaround_time_tracker.add_time(time.time()-float(envelope['first_ts']),
                                                           pipeline=pipeline)
                    pipeline.execute()
            return envelopes

        def process_items(self, items):
            """
            The default behavior is to call process_item repeatedly.
            """
            for item in items:
                self.process_item(item)
    return BatchingQueue

class BatchingQueue(create_batching_queue(Queue)):
    pass

class BatchingIsolationQueue(create_batching_queue(IsolationQueue)):
    pass
