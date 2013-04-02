from pyvows import Vows, expect
from mock import Mock

import dockets
from dockets.queue import Queue

@Vows.batch
class TestGlobalHandlerRegistered(Vows.Context):
    def topic(self):
        err = Mock()
        return err

    def should_only_register_each_class_once(self, err):
        dockets.add_global_retry_error_class(err)
        dockets.add_global_retry_error_class(err)
        queue = Queue(Mock(), 'test')
        dockets.clear_global_retry_error_classes()
        expect(queue._retry_error_classes).to_equal([err])
