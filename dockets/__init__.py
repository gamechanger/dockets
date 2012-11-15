import logging
from dockets.logging_event_handler import LoggingEventHandler

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logging.getLogger('dockets').addHandler(NullHandler())


_global_event_handler_classes = []

def add_global_event_handler(handler_class):
    _global_event_handler_classes.append(handler_class)

def clear_global_event_handlers():
    del _global_event_handler_classes[:]

add_global_event_handler(LoggingEventHandler)
