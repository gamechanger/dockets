import logging
from dockets.logging_event_handler import LoggingEventHandler
from dockets.redis_tracking_event_handler import RedisTrackingEventHandler

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logging.getLogger('dockets').addHandler(NullHandler())


_global_event_handlers = []

def add_global_event_handler(handler):
    _global_event_handlers.append(handler)

def clear_global_event_handlers():
    del _global_event_handlers[:]

add_global_event_handler(LoggingEventHandler())
add_global_event_handler(RedisTrackingEventHandler())
