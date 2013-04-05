import unittest

class QueueTestCase(unittest.TestCase):
    """A test case for a dockets Queue.

    Subclasses must override get_queue(), which returns the queue to be tested.
    """

    def setUp(self):
        super(QueueTestCase, self).setUp()
        self.queue = self.get_queue()
        self.queue.redis.flushall()

    def assert_queue_length(self, length):
        self.assertEqual(self.queue.queued(), length)

    def assert_queue_empty(self):
        self.assert_queue_length(0)

    def assert_error_queue_length(self, length):
        self.assertEqual(self.queue.error_queue.length(), length)

    def assert_error_queue_empty(self):
        self.assert_error_queue_length(0)

    def run_item(self, item):
        self.queue.push(item)
        worker_id = self.queue.register_worker()
        self.queue.run_once(worker_id)
