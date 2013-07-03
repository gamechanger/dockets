import logging

class LoggingEventHandler(object):
    def __init__(self, **kwargs):
        self.make_event_logger('push', logging.DEBUG)
        self.make_event_logger('expire')
        self.make_event_logger('retry')
        self.make_event_logger('error', logging.ERROR)
        self.make_event_logger('success')

    def on_register(self, queue):
        self.logger = logging.getLogger('dockets.queue.{0}'.format(queue.name))
        self.queue = queue

    def make_event_logger(self, name, level=logging.INFO):
        def handler(pretty_printed_item, exc_info=None, **kwargs):
            event_description = '{0} {1} {2}'.format(self.queue.name,
                                                     name.capitalize(),
                                                     pretty_printed_item)
            self.logger.log(level, event_description, exc_info=exc_info)
        setattr(self, 'on_{0}'.format(name), handler)

    def on_operation_error(self, exc_info=None, **kwargs):
        self.logger.error('{0}: Operation error'.format(self.queue.name), exc_info=exc_info)

    def on_empty(self, **kwargs):
        self.logger.debug("{0} Empty. Sleeping.".format(self.queue.name))
