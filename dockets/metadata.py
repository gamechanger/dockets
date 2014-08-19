import os
import time

from dockets.pipeline import PipelineObject
METADATA_TIMEOUT = 24*60*60

class CappedList(PipelineObject):

    def __init__(self, redis, key_name, size):
        super(CappedList, self).__init__(redis)
        self.key_name = key_name
        self.size = size

    @PipelineObject.with_pipeline
    def add(self, item, pipeline):
        pipeline.lpush(self.key_name, item)
        pipeline.ltrim(self.key_name, 0, self.size-1)

    def __getitem__(self, index):
        return self.redis.lindex(self.key_name, index)

    def __len__(self):
        return self.redis.llen(self.key_name)

    def __iter__(self):
        return iter(self.redis.lrange(self.key_name, 0, self.size-1))

class RateTracker(object):

    def __init__(self, redis, name, type, size):
        self.list = CappedList(redis,
                               '{0}.rate.{1}'.format(name, type),
                               size)

    def count(self, pipeline=None, current_time=None):
        self.list.add(current_time or time.time(), pipeline=pipeline)

    def rate(self):
        divisor = float(self.list[0] or 0) - float(self.list[-1] or 0)
        if divisor == 0:
            return 0
        return (len(self.list) - 1) / divisor


class TimeTracker(object):

    def __init__(self, redis, name, type, size):
        self.list = CappedList(redis,
                               '{0}.time.{1}'.format(name, type),
                               size)

    def add_time(self, time, pipeline=None):
        self.list.add(time, pipeline=pipeline)

    def average_time(self):
        divisor = len(self.list)
        if divisor == 0:
            return 0
        return sum(float(t) for t in self.list)/len(self.list)

class WorkerMetadataRecorder(PipelineObject):

    def __init__(self, redis, name, worker_id):
        super(WorkerMetadataRecorder, self).__init__(redis)
        self.worker_id = worker_id
        self.name = name

    def _key_name(self):
        return '{0}.{1}.metadata'.format(self.name, self.worker_id)

    @PipelineObject.with_pipeline
    def record_initial_metadata(self, pipeline):
        metadata = {'hostname': os.uname()[1],
                    'start_ts': time.time()}
        pipeline.hmset(self._key_name(), metadata)
        pipeline.expire(self._key_name(), METADATA_TIMEOUT)

    @PipelineObject.with_pipeline
    def record_success(self, pipeline):
        pipeline.hincrby(self._key_name(), 'success', 1)
        pipeline.hset(self._key_name(), 'last_success_ts', time.time())
        pipeline.expire(self._key_name(), METADATA_TIMEOUT)

    @PipelineObject.with_pipeline
    def record_error(self, pipeline):
        pipeline.hincrby(self._key_name(), 'error', 1)
        pipeline.hset(self._key_name(), 'last_error_ts', time.time())
        pipeline.expire(self._key_name(), METADATA_TIMEOUT)

    @PipelineObject.with_pipeline
    def record_retry(self, pipeline):
        pipeline.hincrby(self._key_name(), 'retry', 1)
        pipeline.hset(self._key_name(), 'last_retry_ts', time.time())
        pipeline.expire(self._key_name(), METADATA_TIMEOUT)

    @PipelineObject.with_pipeline
    def record_expire(self, pipeline):
        pipeline.hincrby(self._key_name(), 'expire', 1)
        pipeline.hset(self._key_name(), 'last_expire_ts', time.time())
        pipeline.expire(self._key_name(), METADATA_TIMEOUT)
