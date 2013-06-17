from mock import Mock

import dockets
from dockets.queue import Queue

def test_register_error_class():
    err = Mock()
    dockets.add_global_retry_error_class(err)
    dockets.add_global_retry_error_class(err)
    queue = Queue(Mock(), 'test')
    dockets.clear_global_retry_error_classes()
    assert queue._retry_error_classes == [err]
