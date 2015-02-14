from dockets.metadata import RateTracker, TimeTracker
import time

class StatGatherer(object):
    """Implementation of a queue event handler which gathers a bunch of stats associated
    with the queue in response to events, and then exposes these to consumers."""

    def __init__(self, **kwargs):
        self._success_tracker_size = kwargs.get('success_tracker_size', 100)
        self._error_tracker_size = kwargs.get('error_tracker_size', 50)
        self._retry_tracker_size = kwargs.get('retry_tracker_size',50)
        self._expire_tracker_size = kwargs.get('expire_tracker_size',50)
        self._operation_error_tracker_size = kwargs.get('operation_error_tracker_size', 50)

        self._response_time_tracker_size = kwargs.get('response_time_tracker_size', 100)
        self._turnaround_time_tracker_size = kwargs.get('turnaround_time_tracker_size', 100)
        self._processing_time_tracker_size = kwargs.get('processing_time_tracker_size', 100)

    def _stat_key(self, stat):
        return "{0}.{1}".format(self.queue.name, stat)

    def _update_timestamp(self, stat, pipeline):
        pipeline.set(self._stat_key(stat), time.time())

    def _get_timestamp(self, stat):
        return self.redis.get(self._stat_key(stat))

    # QUEUE EVENT HANDLERS USED TO GATHER STATS - SHOULD NOT BE CALLED DIRECTLY.

    def on_register(self, queue):
        self.queue = queue
        self.redis = queue.redis
        prefix = '{0}.{1}'.format('metrics.queue', queue.name)
        self._success_tracker = RateTracker(queue.redis, prefix, queue.SUCCESS, self._success_tracker_size)
        self._error_tracker = RateTracker(queue.redis, prefix, queue.ERROR, self._error_tracker_size)
        self._retry_tracker = RateTracker(queue.redis, prefix, queue.RETRY, self._error_tracker_size)
        self._expire_tracker = RateTracker(queue.redis, prefix, queue.EXPIRE, self._expire_tracker_size)
        self._operation_error_tracker = RateTracker(queue.redis, prefix,
                                                    queue.OPERATION_ERROR, self._operation_error_tracker_size)


        self._response_time_tracker = TimeTracker(queue.redis, prefix, 'response', self._response_time_tracker_size)
        self._turnaround_time_tracker = TimeTracker(queue.redis, prefix, 'turnaround', self._turnaround_time_tracker_size)
        self._processing_time_tracker = TimeTracker(queue.redis, prefix, 'processing', self._processing_time_tracker_size)

    def on_pop(self, response_time, pipeline, **kwargs):
        self._update_timestamp('last_pop', pipeline)
        self._response_time_tracker.add_time(response_time, pipeline=pipeline)

    def on_complete(self, turnaround_time, processing_time, pipeline, **kwargs):
        self._update_timestamp('last_complete', pipeline)
        self._turnaround_time_tracker.add_time(turnaround_time, pipeline=pipeline)
        self._processing_time_tracker.add_time(processing_time, pipeline=pipeline)

    def on_success(self, pipeline, **kwargs):
        self._update_timestamp('last_success', pipeline)
        self._success_tracker.count(pipeline=pipeline)

    def on_error(self, pipeline, **kwargs):
        self._update_timestamp('last_error', pipeline)
        self._error_tracker.count(pipeline=pipeline)

    def on_retry(self, pipeline, **kwargs):
        self._update_timestamp('last_retry', pipeline)
        self._retry_tracker.count(pipeline=pipeline)

    def on_remove(self, pipeline, **kwargs):
        self._update_timestamp('last_remove', pipeline)

    def on_expire(self, pipeline, **kwargs):
        self._update_timestamp('last_expire', pipeline)
        self._expire_tracker.count(pipeline=pipeline)

    def on_operation_error(self, pipeline, **kwargs):
        self._update_timestamp('last_operation_error', pipeline)
        self._operation_error_tracker.count(pipeline=pipeline)

    def on_reclaim(self, pipeline, **kwargs):
        self._update_timestamp('last_reclaim', pipeline)

    def on_push(self, pipeline, **kwargs):
        self._update_timestamp('last_push', pipeline)

    def on_delay_pop(self, pipeline, **kwargs):
        self._update_timestamp('last_delay_pop', pipeline)



    # ACCESSOR METHODS FOR STATS

    def last_pop(self):
        return self._get_timestamp('last_pop')

    def last_delay_pop(self):
        return self._get_timestamp('last_delay_pop')

    def last_complete(self):
        return self._get_timestamp('last_complete')

    def last_success(self):
        return self._get_timestamp('last_success')

    def last_error(self):
        return self._get_timestamp('last_error')

    def last_retry(self):
        return self._get_timestamp('last_retry')

    def last_remove(self):
        return self._get_timestamp('last_remove')

    def last_expire(self):
        return self._get_timestamp('last_expire')

    def last_operation_error(self):
        return self._get_timestamp('last_operation_error')

    def last_reclaim(self):
        return self._get_timestamp('last_reclaim')

    def last_push(self):
        return self._get_timestamp('last_push')

    def successes_per_s(self):
        return self._success_tracker.rate()

    def errors_per_s(self):
        return self._error_tracker.rate()

    def retries_per_s(self):
        return self._retry_tracker.rate()

    def expirations_per_s(self):
        return self._expire_tracker.rate()

    def operation_errors_per_s(self):
        return self._operation_error_tracker.rate()

    def average_response_time(self):
        return self._response_time_tracker.average_time()

    def average_turnaround_time(self):
        return self._turnaround_time_tracker.average_time()

    def average_processing_time(self):
        return self._processing_time_tracker.average_time()
