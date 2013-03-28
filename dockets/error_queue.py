import sys
import time
from uuid import uuid1
from traceback import format_exc

from dockets.pipeline import PipelineObject

class ErrorQueue(PipelineObject):
    """
    This is a queue that can act as a queue error handler. Items which
    error out in the main queue will end up in this queue, with error
    information.
    """

    def __init__(self, main_queue):
        self.name = main_queue.name
        self._serializer = main_queue._serializer
        self.queue = main_queue
        super(ErrorQueue, self).__init__(main_queue.redis)

    @PipelineObject.with_pipeline
    def queue_error(self, envelope, pipeline):
        """
        Record an error in processing the current item. This accesses
        sys.exc_info, so it should only be called from an exception
        handler.
        """
        error_id = str(uuid1())
        exc_info = sys.exc_info()
        assert exc_info[0], "queue_error must be called from inside an exception handler"
        error_item = {'envelope': envelope,
                      'error_type': str(exc_info[0].__name__),
                      'error_text': str(exc_info[1]),
                      'traceback': format_exc(),
                      'ts': time.time(),
                      'id': error_id}

        pipeline.hset(self._hash_key(),
                      error_id,
                      self._serializer.serialize(error_item))

    def requeue_error(self, error_id):
        error = self._serializer.deserialize(self.redis.hget(self._hash_key(), error_id))
        with self.redis.pipeline() as pipe:
            pipe.hdel(self._hash_key(), error_id)
            self.queue.push(error['envelope']['item'], pipeline=pipe, envelope=envelope)

    def errors(self):
        return map(self._serializer.deserialize, self.redis.hvals(self._hash_key()))

    def length(self):
        return self.redis.hlen(self._hash_key())

    def _hash_key(self):
        return 'queue.{}.errors'.format(self.name)
