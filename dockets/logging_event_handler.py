import logging

class LoggingEventHandler(object):
    def __init__(self, **kwargs):
        self.make_event_logger('push')
        self.make_event_logger('expire')
        self.make_event_logger('retry')
        self.make_event_logger('error')
        self.make_event_logger('success')

    def on_register(self, queue):
        self.logger = logging.getLogger('dockets.queue.{0}'.format(queue.name))
        self.queue = queue


    def _log(self, event, item_key, exc_info):
        event_description = '{0} {1} {2}'.format(self.queue.name,
                                         event.capitalize(),
                                         item_key)
        if exc_info:
            self.logger.error(event_description, exc_info=exc_info)
        else:
            self.logger.info(event_description)

    def make_event_logger(self, name):
        def handler(item_key, exc_info=None, **kwargs):
            self._log(name, item_key, exc_info)
        setattr(self, 'on_{0}'.format(name), handler)

    def on_operation_error(self, exc_info=None, **kwargs):
        self.logger.error('{0}: Operation error'.format(self.queue.name), exc_info=exc_info)

    def on_empty(self, **kwargs):
        self.logger.info("{0} Empty. Sleeping.".format(self.queue.name))
