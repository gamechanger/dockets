from dockets.queue import Queue

class ErrorQueue(Queue):
    """
    This is a queue that can act as a queue error handler. Items which
    error out in the main queue will end up in this queue, with error
    information.
    """
    @classmethod
    def make_error_queue(cls, queue):
        error_queue = cls(queue)
        queue.add_event_handler(error_queue)
        return error_queue

    def __init__(self, main_queue):
        redis = main_queue.redis
        name = '{0}_error'.format(main_queue.name)
        serializer = main_queue._serializer
        super(ErrorQueue, self).__init__(redis, name, serializer=serializer)

    def on_register(self, *args, **kwargs):
        pass

    def on_error(self, item, pipeline, exc_info, *args, **kwargs):
        error_item = {'item': item,
                      'error_type': str(exc_info[0].__name__),
                      'error_text': str(exc_info[1])}
        self.push(error_item)
