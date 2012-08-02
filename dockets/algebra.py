import logging
from redis import WatchError

def queue_pipe(queue_in, queue_out, return_value=None):
    def handler(item, pipeline, **kwargs):
        queue_out.push(item, pipeline=pipeline)
    queue_in.add_handler(return_value, handler)


def queue_and(queues, queue_out, key_function=None, timeout=60*15):
    """
    ANDS several queues together.
    """
    def _key_name(key):
        return '{0}={1}.{2}'.format(queue_out.name,
                                    '+'.join(queue['queue'].name for queue in queues),
                                    key)

    def make_set_handler(index):
        def set_handler(item, key, redis, pipeline, **kwargs):
            if key_function:
                key = key_function(item)
            with redis.pipeline() as set_pipeline:
                set_pipeline.setbit(_key_name(key), index, 1)
                set_pipeline.expire(_key_name(key), timeout)
                set_pipeline.execute()
            bitstring = redis.get(_key_name(key))
            total = 0
            for byte in bitstring:
                # population count
                n = ord(byte)
                while n:
                    total += 1
                    n &= n - 1
            logging.info('total is %s' % total)
            if total == len(queues):
                queue_out.push(key, pipeline)
        return set_handler

    def make_unset_handler(index):
        def unset_handler(item, key, redis, pipeline, **kwargs):
            if key_function:
                key = key_function(item)
            pipeline.setbit(_key_name(key), index, 0)
            pipeline.expire(_key_name(key), timeout)
        return unset_handler

    for i, queue_spec in enumerate(queues):
        index = i
        queue = queue_spec['queue']
        set_value = queue_spec.get('set')
        unset_value = queue_spec.get('unset')

        queue.add_handler(set_value, make_set_handler(i))
        if unset_value:
            queue.add_handler(unset_value, make_unset_handler(i))
