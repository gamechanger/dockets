import logging
import time
import pickle

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

        def gather_envelopes(self, pipeline, worker_id):
            envelopes = []
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
            return envelopes

        def clear_expired_envelopes(self, worker_recorder, pipeline, envelopes):
            envelopes_to_process = list(envelopes)
            for envelope in envelopes:
                if envelope['ttl'] and (envelope['first_ts'] + envelope['ttl'] < time.time()):
                    envelopes_to_process.remove(envelope)
                    self._event_registrar.on_expire(item=envelope['item'],
                                                    item_key=self.item_key(envelope['item']),
                                                    pipeline=pipeline,
                                                    pretty_printed_item=self.pretty_printer(envelope['item']))
                    worker_recorder.record_expire(pipeline=pipeline)
            return envelopes_to_process

        def run_once(self, worker_id):
            """
            Run the queue for one step.
            """
            worker_recorder = WorkerMetadataRecorder(self.redis, self._queue_key(),
                                                     worker_id)
            pipeline = self.redis.pipeline()
            envelopes = self.gather_envelopes(pipeline, worker_id)
            if not envelopes:
                pipeline.execute()
                return None
            envelopes = self.clear_expired_envelopes(worker_recorder, pipeline, envelopes)

            def handle_error(envelope):
                self._event_registrar.on_error(item=envelope['item'],
                                               item_key=self.item_key(envelope['item']),
                                               pipeline=pipeline,
                                               pretty_printed_item=self.pretty_printer(envelope['item']))
                worker_recorder.record_error(pipeline=pipeline)
                self.error_queue.queue_error(envelope)

            for envelope in envelopes:
                item_error_classes = self.error_classes_for_envelope(envelope)
                try:
                    self.process_item(envelope['item'])
                except errors.ExpiredError:
                    self._event_registrar.on_expire(item=envelope['item'],
                                                    item_key=self.item_key(envelope['item']),
                                                    pipeline=pipeline,
                                                    pretty_printed_item=self.pretty_printer(envelope['item']))
                    worker_recorder.record_expire(pipeline=pipeline)
                except tuple(item_error_classes):
                    max_attempts = envelope.get('max_attempts', self._max_attempts)
                    if envelope['attempts'] >= max_attempts - 1:
                        handle_error(envelope)
                    else:
                        self._event_registrar.on_retry(item=envelope['item'],
                                                       item_key=self.item_key(envelope['item']),
                                                       pipeline=pipeline,
                                                       pretty_printed_item=self.pretty_printer(envelope['item']))
                        worker_recorder.record_retry(pipeline=pipeline)
                        # When we retry, first_ts stays the same
                        self.push(envelope['item'], pipeline=pipeline, envelope=envelope,
                                  max_attempts=max_attempts,
                                  attempts=envelope['attempts'] + 1,
                                  error_classes=item_error_classes)
                except Exception as e:
                    handle_error(envelope)
                else:
                    self._event_registrar.on_success(item=envelope['item'],
                                                     item_key=self.item_key(envelope['item']),
                                                     pipeline=pipeline,
                                                     pretty_printed_item=self.pretty_printer(envelope['item']))
                    worker_recorder.record_success(pipeline=pipeline)
                finally:
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
