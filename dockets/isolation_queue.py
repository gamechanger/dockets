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

    def isolated_transaction_on_exists(self, key, key_exists_cb, key_does_not_exist_cb):
        with self.redis.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self._entry_key(key))
                    key_in_queue = pipeline.get(self._entry_key(key))
                    pipeline.multi()
                    if key_in_queue:
                        key_exists_cb(self, pipeline)
                    else:
                        key_does_not_exist_cb(self, pipeline)
                    pipeline.execute()
                    break
                except WatchError:
                    continue

    def isolated_transaction_on_latest(self, key, key_latest_cb, key_not_latest_cb):
        with self.redis.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self._entry_key(key))
                    latest_version = pipeline.hget(self._latest_add_key(), key)
                    pipeline.multi()
                    if latest_version:
                        key_latest_cb(self, pipeline, latest_version)
                    else:
                        key_not_latest_cb(self, pipeline, latest_version)
                    pipeline.execute()
                    break
                except WatchError:
                    continue

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

        def update_latest(queue, pipeline):
            pipeline.hset(queue._latest_add_key(), key, queue._serializer.serialize(item))

        def push_new(queue, pipeline):
            super(IsolationQueue, queue).push(item, pipeline=pipeline, **kwargs)
            pipeline.set(queue._entry_key(key), 1)

        self.isolated_transaction_on_exists(key, update_latest, push_new)

    def complete(self, envelope, *args, **kwargs):
        """
        Complete is also not quite atomic, since we need to do a
        watch.
        """
        key = self.item_key(envelope['item'])

        def complete_latest(queue, pipeline, latest_version):
            latest_version = queue._serializer.deserialize(latest_version)
            super(IsolationQueue, queue).push(latest_version, pipeline=pipeline)
            pipeline.hdel(queue._latest_add_key(), key)

        def complete_key(queue, pipeline, latest_version):
            pipeline.delete(queue._entry_key(key))

        self.isolated_transaction_on_latest(key, complete_latest, complete_key)
        super(IsolationQueue, self).complete(envelope, *args, **kwargs)

    def delete(self, envelope, *args, **kwargs):
        """
        A callback mechanism invoked when an item is deleted from the
        queue's error queue. Ensures the item's entry and latest add
        keys get cleared up.
        """
        key = self.item_key(envelope['item'])
        with self.redis.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self._entry_key(key))
                    pipeline.hdel(self._latest_add_key(), key)
                    pipeline.delete(self._entry_key(key))
                    pipeline.execute()
                    break
                except WatchError:
                    continue
        super(IsolationQueue, self).delete(envelope, *args, **kwargs)

    # key names

    def _entry_key(self, key):
        return 'queue.{0}.entry.{1}'.format(self.name, key)

    def _latest_add_key(self):
        return 'queue.{0}.latest'.format(self.name)


class TestIsolationQueue(IsolationQueue):

    def process_item(self, item):
        logging.info('in process_item, processing {}'.format(item))
