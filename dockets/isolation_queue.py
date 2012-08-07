import logging

from redis import WatchError

from dockets.pipeline import PipelineObject
from dockets.queue import Queue


class IsolationQueue(Queue):
    """
    This queue provides the "I" part of ACID transactions.

    It provides the following guarantees:

    - At any given time, it will only process one item per key.
    - The *last* item queued for a given key will always be processed.

    These guarantees hold regardless of how many queue processors are
    in operation.
    """


    # TODO this should use Lua scripting
    def push(self, item, **kwargs):
        """
        IsolationQueue.push needs to allocate its own pipeline, since
        it needs to do a watch. This has some implications on retries
        and incoming queue pipes: they will not be atomic.
        """
        if 'pipeline' in kwargs:
            # If we're being piped to or retrying
            return super(IsolationQueue, self).push(item, **kwargs)

        key = self.item_key(item)
        with self.redis.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self._entry_set_key())
                    key_in_queue = pipeline.sismember(self._entry_set_key(), key)
                    pipeline.multi()
                    if key_in_queue:
                        pipeline.hset(self._latest_add_key(),
                                      key, self._serialize(item))
                    else:
                        super(IsolationQueue, self).push(item, pipeline=pipeline)
                        pipeline.sadd(self._entry_set_key(), key)
                    pipeline.execute()
                    break
                except WatchError:
                    continue

    def complete(self, envelope, *args, **kwargs):
        """
        Complete is also not quite atomic, since we need to do a
        watch.
        """
        key = self.item_key(envelope['item'])
        with self.redis.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self._entry_set_key())
                    latest_version = pipeline.hget(self._latest_add_key(), key)
                    pipeline.multi()
                    if latest_version:
                        latest_version = self._deserialize(latest_version)
                        # we just call Queue.push since we know it's already in the queue
                        super(IsolationQueue, self).push(latest_version, pipeline=pipeline)
                        pipeline.hdel(self._latest_add_key(), key)
                    else:
                        pipeline.srem(self._entry_set_key(), key)
                    pipeline.execute()
                    break
                except WatchError:
                    continue
        super(IsolationQueue, self).complete(envelope, *args, **kwargs)


    # key names

    def _entry_set_key(self):
        return 'queue.{0}.entries'.format(self.name)

    def _latest_add_key(self):
        return 'queue.{0}.latest'.format(self.name)


class TestIsolationQueue(IsolationQueue):

    def process_item(self, item):
        logging.info('in process_item, processing {}'.format(item))
