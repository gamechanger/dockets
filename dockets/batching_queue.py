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
            self._batch_size = kwargs.get('batch_size') or 10
            super(BatchingQueue, self).__init__(*args, **kwargs)

        def run_once(self, worker_id):
            """
            Run the queue for one step.
            """
            envelopes = []
            worker_recorder = WorkerMetadataRecorder(self.redis, self._queue_key(),
                                                     worker_id)
            # The Big Pipeline
            pipeline = self.redis.pipeline()
            while len(envelopes) < self._batch_size:
                envelope = self.pop(worker_id, pipeline=pipeline)
                if not envelope:
                    break
                envelope['pop_time'] = time.time()
                response_time = envelope['pop_time'] - float(envelope['first_ts'])
                self._event_registrar.on_pop(item=envelope['item'],
                                             item_key=self.item_key(envelope['item']),
                                             response_time=response_time,
                                             pipeline=pipeline)
                envelopes.append(envelope)

            if not envelopes:
                pipeline.execute()
                return None

            # clear expired envelopes
            envelopes_to_process = list(envelopes)
            for envelope in envelopes:
                if envelope['ttl'] and (envelope['first_ts'] + envelope['ttl'] < time.time()):
                    envelopes_to_process.remove(envelope)
                    self._event_registrar.on_expire(item=envelope['item'],
                                                    item_key=self.item_key(envelope['item']),
                                                    pipeline=pipeline)
                    worker_recorder.record_expire(pipeline=pipeline)
            try:
                return_value = self.process_items([envelope['item'] for envelope in envelopes_to_process])
            except errors.ExpiredError:
                for envelope in envelopes:
                    self._event_registrar.on_expire(item=envelope['item'],
                                                    item_key=self.item_key(envelope['item']),
                                                    pipeline=pipeline)
                    worker_recorder.record_expire(pipeline=pipeline)
            except tuple(self.retry_error_classes):
                for envelope in envelopes_to_process:
                    self._event_registrar.on_retry(item=envelope['item'],
                                                   item_key=self.item_key(envelope['item']),
                                                   pipeline=pipeline)
                    worker_recorder.record_retry(pipeline=pipeline)
                    # When we retry, first_ts stsys the same
                    self.push(envelope['item'], pipeline=pipeline, envelope=envelope)
            except Exception as e:
                for envelope in envelopes_to_process:
                    self._event_registrar.on_error(item=envelope['item'],
                                                   item_key=self.item_key(envelope['item']),
                                                   pipeline=pipeline)
                    worker_recorder.record_error(pipeline=pipeline)
            else:
                for envelope in envelopes_to_process:
                    self._event_registrar.on_success(item=envelope['item'],
                                                     item_key=self.item_key(envelope['item']),
                                                     pipeline=pipeline)
                    worker_recorder.record_success(pipeline=pipeline)
            finally:
                for envelope in envelopes:
                    self.complete(envelope, worker_id, pipeline=pipeline)
                    complete_time = time.time()
                    turnaround_time = complete_time - float(envelope['first_ts'])
                    processing_time = complete_time - envelope['pop_time']
                    self._event_registrar.on_complete(item=envelope['item'],
                                                      item_key=self.item_key(envelope['item']),
                                                      turnaround_time=turnaround_time,
                                                      processing_time=processing_time,
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
