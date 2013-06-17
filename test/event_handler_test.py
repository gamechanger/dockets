from mock import Mock

from dockets.queue_event_registrar import QueueEventRegistrar
from dockets.queue import Queue
import dockets

def make_registrar():
    return QueueEventRegistrar(Mock)

def test_one_handler_registered():
    registrar = make_registrar()
    registrar.register(Mock())
    registrar._handlers[0].on_register.assert_called_once_with(registrar._queue)

def test_two_handlers_registered():
    registrar = make_registrar()
    registrar.register(Mock())
    registrar.register(Mock())
    registrar._handlers[0].on_register.assert_called_once_with(registrar._queue)
    assert len(registrar._handlers[0].mock_calls) == 1
    registrar._handlers[1].on_register.assert_called_once_with(registrar._queue)
    assert len(registrar._handlers[1].mock_calls) == 1

def test_one_handler_registered_on_pop_called():
    registrar = make_registrar()
    registrar.register(Mock())
    registrar.on_pop('test')
    registrar._handlers[0].on_pop.assert_called_once_with('test')
    assert len(registrar._handlers[0].mock_calls) == 2

def test_two_handlers_registered_on_pop_called():
    registrar = make_registrar()
    registrar.register(Mock())
    registrar.register(Mock())
    registrar.on_pop('test')
    registrar._handlers[0].on_pop.assert_called_once_with('test')
    assert len(registrar._handlers[0].mock_calls) == 2
    registrar._handlers[1].on_pop.assert_called_once_with('test')
    assert len(registrar._handlers[1].mock_calls) == 2

def test_popless_handler_registered_on_pop_called():
    registrar = make_registrar()
    handler = Mock()
    del handler.on_pop
    registrar.register(handler)
    registrar.on_pop('test')
    assert len(registrar._handlers[0].mock_calls) == 1

def test_global_handler_registered():
    handler = Mock()
    dockets.add_global_event_handler(handler)
    dockets.add_global_event_handler(handler)
    queue = Queue(Mock(), 'test')
    dockets.clear_global_event_handlers()
    handler().on_register.assert_called_once_with(queue)
