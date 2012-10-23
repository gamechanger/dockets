import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logging.getLogger('dockets').addHandler(NullHandler())


_global_event_handlers = []

def add_global_event_handler(handler):
    _global_event_handlers.append(handler)
