from pyvows import Vows, expect
from mock import Mock

from dockets.queue_event_registrar import QueueEventRegistrar
from dockets.queue import Queue
import dockets

class QueueEventRegistrarContext(Vows.Context):
    def __init__(self, *args, **kwargs):
        super(QueueEventRegistrarContext, self).__init__(*args, **kwargs)
        self.ignore('use_registrar')

    def topic(self):
        registrar = QueueEventRegistrar(Mock())
        self.use_registrar(registrar)
        return registrar

    def use_registrar(self, registrar):
        pass

@Vows.batch
class AQueueEventRegistar(Vows.Context):

    class OneHandlerRegistered(QueueEventRegistrarContext):
        def use_registrar(self, registrar):
            registrar.register(Mock())

        def should_call_handler_on_register(self, topic):
            topic._handlers[0].on_register.assert_called_once_with(topic._queue)

        def should_not_call_other_methods(self, topic):
            expect(topic._handlers[0].mock_calls).to_length(1)

    class TwoEventHandlersRegistered(QueueEventRegistrarContext):
        def use_registrar(self, registrar):
            registrar.register(Mock())
            registrar.register(Mock())

        def should_call_first_handler_on_register(self, topic):
            topic._handlers[0].on_register.assert_called_once_with(topic._queue)

        def should_call_second_handler_on_register(self, topic):
            topic._handlers[1].on_register.assert_called_once_with(topic._queue)

        def should_not_call_other_methods_on_first_handler(self, topic):
            expect(topic._handlers[0].mock_calls).to_length(1)

        def should_not_call_other_methods_on_second_handler(self, topic):
            expect(topic._handlers[1].mock_calls).to_length(1)

    class OneHandlerRegisteredAndOnPopCalled(QueueEventRegistrarContext):
        def use_registrar(self, registrar):
            registrar.register(Mock())
            registrar.on_pop('test')

        def should_call_handler_on_pop(self, topic):
            topic._handlers[0].on_pop.assert_called_once_with('test')

    class TwoHandlersRegisteredAndOnPopCalled(QueueEventRegistrarContext):
        def use_registrar(self, registrar):
            registrar.register(Mock())
            registrar.register(Mock())
            registrar.on_pop('test')

        def should_call_first_handler_on_pop(self, topic):
            topic._handlers[0].on_pop.assert_called_once_with('test')

        def should_call_second_handler_on_pop(self, topic):
            topic._handlers[1].on_pop.assert_called_once_with('test')

    class HandlerRegisteredWithoutPopAndOnPopCalled(QueueEventRegistrarContext):
        def use_registrar(self, registrar):
            handler = Mock()
            del handler.on_pop
            registrar.register(handler)
            registrar.on_pop('test')

        def should_not_call_handler_methods(self, topic):
            expect(topic._handlers[0].mock_calls).to_length(1)


@Vows.batch
class TestGlobalHandlerRegistered(Vows.Context):
    def topic(self):
        handler = Mock()
        return handler

    def should_register_globals(self, handler):
        dockets.add_global_event_handler(handler)
        queue = Queue(Mock(), 'test')
        dockets.clear_global_event_handlers()
        handler.on_register.assert_called_once_with(queue)
