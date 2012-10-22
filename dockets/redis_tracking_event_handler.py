from dockets.metadata import RateTracker, TimeTracker

class RedisTrackingEventHandler(object):

    def __init__(self, **kwargs):
        self._success_tracker_size = kwargs.get('success_tracker_size', 100)
        self._error_tracker_size = kwargs.get('error_tracker_size', 50)
        self._retry_tracker_size = kwargs.get('retry_tracker_size',50)
        self._expire_tracker_size = kwargs.get('expire_tracker_size',50)
        self._operation_error_tracker_size = kwargs.get('operation_error_tracker_size', 50)

        self._response_time_tracker_size = kwargs.get('response_time_tracker_size', 100)
        self._turnaround_time_tracker_size = kwargs.get('turnaround_time_tracker_size', 100)
        self._processing_time_tracker_size = kwargs.get('processing_time_tracker_size', 100)


    def on_register(self, queue):
        self.queue = queue
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
        self._response_time_tracker.add_time(response_time, pipeline=pipeline)

    def on_complete(self, turnaround_time, processing_time, pipeline, **kwargs):
        self._turnaround_time_tracker.add_time(turnaround_time, pipeline=pipeline)
        self._processing_time_tracker.add_time(processing_time, pipeline=pipeline)

    def on_success(self, pipeline, **kwargs):
        self._success_tracker.count(pipeline=pipeline)

    def on_error(self, pipeline, **kwargs):
        self._error_tracker.count(pipeline=pipeline)

    def on_retry(self, pipeline, **kwargs):
        self._retry_tracker.count(pipeline=pipeline)

    def on_expire(self, pipeline, **kwargs):
        self._expire_tracker.count(pipeline=pipeline)

    def on_operation_error(self, pipeline, **kwargs):
        self._operation_error_tracker.count(pipeline=pipeline)
