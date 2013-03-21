import logging
from dockets.logging_event_handler import LoggingEventHandler

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logging.getLogger('dockets').addHandler(NullHandler())


_global_event_handler_classes = set()

def add_global_event_handler(handler_class):
    _global_event_handler_classes.add(handler_class)

def clear_global_event_handlers():
    _global_event_handler_classes.clear()

add_global_event_handler(LoggingEventHandler)
