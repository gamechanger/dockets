import simplejson

from pyvows import Vows, expect
from util import FakeRedisContext

from dockets.queue import Queue
from dockets.isolation_queue import IsolationQueue
from dockets.batching_queue import BatchingQueue, BatchingIsolationQueue
from dockets.docket import Docket
from dockets.errors import ExpiredError


class TestRetryError(Exception):
    pass

def default_process_item(obj, item):
    if (not isinstance(item, dict) or 'action' not in item
        or item['action'] == 'success'):
        obj.items_processed.append(item)
        return
    if item['action'] == 'retry':
        raise TestRetryError
    if item['action'] == 'expire':
        raise ExpiredError
    if item['action'] == 'error':
        raise Exception(item['message'])

class TestQueue(Queue):
    def __init__(self, *args, **kwargs):
        super(TestQueue, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

class TestIsolationQueue(IsolationQueue):
    def __init__(self, *args, **kwargs):
        super(TestIsolationQueue, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

class TestBatchingQueue(BatchingQueue):
    def __init__(self, *args, **kwargs):
        kwargs['batch_size'] = 1
        super(TestBatchingQueue, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

class TestBatchingIsolationQueue(BatchingIsolationQueue):
    def __init__(self, *args, **kwargs):
        kwargs['batch_size'] = 1
        super(TestBatchingIsolationQueue, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

class TestDocket(Docket):
    def __init__(self, *args, **kwargs):
        super(TestDocket, self).__init__(*args, **kwargs)
        self.items_processed = []

    def process_item(self, item):
        default_process_item(self, item)

def single_queue_context(queue_class):
    class SingleQueueContext(FakeRedisContext):
        def __init__(self, *args, **kwargs):
            super(SingleQueueContext, self).__init__(*args, **kwargs)
            self.ignore('use_queue')

        def use_redis(self, redis):
            queue = queue_class(redis, 'test', use_error_queue=True, retry_error_classes=[TestRetryError])
            self.use_queue(queue)
            return queue

        def use_queue(self, queue):
            raise NotImplementedError

    return SingleQueueContext


def queue_entry_checker(entry_value):
    class TheEntry(Vows.Context):

        def should_be_string(self, topic):
            expect(topic).to_be_instance_of(basestring)

        class WhenDeserialized(Vows.Context):
            def topic(self, json):
                return simplejson.loads(json)

            def should_be_dict(self, topic):
                expect(topic).to_be_instance_of(dict)

            def should_have_ts_key(self, topic):
                expect(topic).to_include('ts')

            def should_have_first_ts_key(self, topic):
                expect(topic).to_include('first_ts')

            def should_have_version_key(self, topic):
                expect(topic).to_include('v')

            def should_have_item_key(self, topic):
                expect(topic).to_include('item')

            class TheItemKey(Vows.Context):
                def topic(self, envelope):
                    return envelope['item']

                def should_be_input_item(self, topic):
                    expect(topic).to_equal(entry_value)
    return TheEntry

class ErrorQueueContext(Vows.Context):
    def topic(self, queue):
        return queue.error_queue

class EmptyErrorQueueContext(ErrorQueueContext):
    def should_be_empty(self, topic):
        expect(topic.errors()).to_be_empty()

def basic_queue_tests(context_class):
    class QueueTests(Vows.Context):
        class WhenRunOnceWithoutPushing(context_class):

            def use_queue(self, queue):
                queue.register_worker(worker_id='test_worker')
                queue.run_once(worker_id='test_worker')

            class TheWorkersActivityKey(Vows.Context):

                def should_exist(self, queue):
                    expect(queue.redis.exists('queue.test.test_worker.active')).to_be_true()

            class TheWorkerSet(Vows.Context):

                def should_contain_the_worker_id(self, queue):
                    expect(queue.redis.sismember('queue.test.workers', 'test_worker'))

            class TheErrorQueue(EmptyErrorQueueContext):
                pass


        class WhenPushedToOnce(context_class):

            def use_queue(self, queue):
                queue.push({'a': 1})

            def should_have_one_entry(self, queue):
                expect(queue.queued()).to_equal(1)

            def should_have_one_redis_entry(self, queue):
                expect(queue.queued()).to_equal(1)

            class ThePushedEntry(queue_entry_checker({'a': 1})):
                def topic(self, queue):
                    return queue.queued_items()[0]

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenPushedToTwice(context_class):

            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.push({'b': 2})

            def should_have_two_entries(self, queue):
                expect(queue.queued()).to_equal(2)

            def should_have_two_redis_entries(self, queue):
                expect(queue.queued()).to_equal(2)

            class TheFirstPushedEntry(queue_entry_checker({'a': 1})):
                def topic(self, queue):
                    return queue.queued_items()[1]

            class TheSecondPushedEntry(queue_entry_checker({'b': 2})):
                def topic(self, queue):
                    return queue.queued_items()[0]

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenPushedToOnceAndPoppedFromOnce(context_class):

            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.pop(worker_id='test_worker')

            def should_have_no_entries(self, queue):
                expect(queue.queued()).to_equal(0)

            def should_have_no_redis_entries(self, queue):
                expect(queue.queued()).to_equal(0)

            class TheWorkerQueue(Vows.Context):

                def should_have_one_entry(self, queue):
                    expect(queue.redis.llen('queue.test.test_worker.working')).to_equal(1)

                class TheWorkingEntry(queue_entry_checker({'a': 1})):
                    def topic(self, queue):
                        return queue.redis.lindex('queue.test.test_worker.working', 0)

            class TheWorkersActivityKey(Vows.Context):

                def should_exist(self, queue):
                    expect(queue.redis.exists('queue.test.test_worker.active')).to_be_true()

            class TheWorkerSet(Vows.Context):

                def should_contain_the_worker_id(self, queue):
                    expect(queue.redis.sismember('queue.test.workers', 'test_worker'))

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenPushedToOnceAndPoppedFromOnceAndCompleteCalledOnce(context_class):

            def use_queue(self, queue):
                queue.push({'a': 1})
                item = queue.pop(worker_id='test_worker')
                queue.complete(item, worker_id='test_worker')

            def should_have_no_entries(self, queue):
                expect(queue.redis.llen('queue_test')).to_equal(0)

            class TheWorkerQueue(Vows.Context):

                def should_be_empty(self, queue):
                    expect(queue.redis.llen('queue.test.test_worker.working')).to_equal(0)

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenWorkerRegistered(context_class):

            def use_queue(self, queue):
                queue.register_worker('test_worker')

            class TheWorkerMetadata(Vows.Context):

                def topic(self, queue):
                    return queue.redis.hgetall('queue.test.test_worker.metadata')

                def should_exist(self, topic):
                    expect(topic).Not.to_be_null()

                def should_contain_hostname(self, topic):
                    expect(topic).to_include('hostname')

                def should_contain_start_ts(self, topic):
                    expect(topic).to_include('start_ts')

        class WhenPushedToOnceAndRunOnce(context_class):

            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.register_worker(worker_id='test_worker')
                queue.run_once(worker_id='test_worker')

            def item_should_be_in_items_processed(self, queue):
                expect(queue.items_processed).to_include({'a': 1})

            def worker_should_be_active(self, queue):
                expect(queue.redis.exists('queue.test.test_worker.active')).to_be_true()

            def active_metadata_list_should_have_entry(self, queue):
                expect(queue.active_worker_metadata()).to_include('test_worker')

            def should_be_empty(self, queue):
                expect(queue.queued()).to_equal(0)

            class TheWorkerMetadata(Vows.Context):
                def topic(self, queue):
                    return queue.active_worker_metadata()['test_worker']

                def should_exist(self, topic):
                    expect(topic).Not.to_be_null()

                def should_contain_last_success_ts(self, topic):
                    expect(topic).to_include('last_success_ts')

                def should_contain_success(self, topic):
                    expect(topic).to_include('success')

                def success_should_be_one(self, topic):
                    expect(topic['success']).to_equal(1)

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenPushedToTwiceAndRunOnce(WhenPushedToOnceAndRunOnce):
            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.push({'b': 2})
                queue.run_once(worker_id='test_worker')

            def should_be_empty(self, queue):
                pass

            def should_have_one_entry(self, queue):
                expect(queue.queued()).to_equal(1)

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenPushedToTwiceAndRunTwice(context_class):
            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.push({'b': 2})
                queue.run_once(worker_id='test_worker')
                queue.run_once(worker_id='test_worker')

            def first_item_should_be_in_items_processed(self, queue):
                expect(queue.items_processed).to_include({'a': 1})

            def second_item_should_be_in_items_processed(self, queue):
                expect(queue.items_processed).to_include({'b': 2})

            def worker_should_be_active(self, queue):
                expect(queue.redis.exists('queue.test.test_worker.active')).to_be_true()

            def active_metadata_list_should_have_entry(self, queue):
                expect(queue.active_worker_metadata()).to_include('test_worker')

            def should_be_empty(self, queue):
                expect(queue.queued()).to_equal(0)

            class TheWorkerMetadata(Vows.Context):
                def topic(self, queue):
                    return queue.active_worker_metadata()['test_worker']

                def should_exist(self, topic):
                    expect(topic).Not.to_be_null()

                def should_contain_last_success_ts(self, topic):
                    expect(topic).to_include('last_success_ts')

                def should_contain_success(self, topic):
                    expect(topic).to_include('success')

                def success_should_be_two(self, topic):
                    expect(topic['success']).to_equal(2)

                def should_not_contain_error(self, topic):
                    expect(topic).Not.to_include('error')

                def should_not_contain_last_error_ts(self, topic):
                    expect(topic).Not.to_include('last_error_ts')

                def should_not_contain_retry(self, topic):
                    expect(topic).Not.to_include('retry')

                def should_not_contain_last_retry_ts(self, topic):
                    expect(topic).Not.to_include('last_retry_ts')

                def should_not_contain_expire(self, topic):
                    expect(topic).Not.to_include('expire')

                def should_not_contain_expire_ts(self, topic):
                    expect(topic).Not.to_include('last_expire_ts')

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenRetryItemIsRunOnce(context_class):
            def use_queue(self, queue):
                queue.push({'action': 'retry'})
                queue.run_once(worker_id='test_worker')

            def items_processed_should_be_empty(self, queue):
                expect(queue.items_processed).to_be_empty()

            def should_have_one_entry(self, queue):
                expect(queue.queued()).to_equal(1)

            class ThePushedEntry(queue_entry_checker({'action': 'retry'})):
                def topic(self, queue):
                    return queue.queued_items()[0]

            class TheWorkerMetadata(Vows.Context):
                def topic(self, queue):
                    return queue.active_worker_metadata()['test_worker']

                def should_exist(self, topic):
                    expect(topic).Not.to_be_null()

                def should_contain_last_retry_ts(self, topic):
                    expect(topic).to_include('last_retry_ts')

                def should_contain_retry(self, topic):
                    expect(topic).to_include('retry')

                def retry_should_be_one(self, topic):
                    expect(topic['retry']).to_equal(1)

                def should_not_contain_error(self, topic):
                    expect(topic).Not.to_include('error')

                def should_not_contain_last_error_ts(self, topic):
                    expect(topic).Not.to_include('last_error_ts')

                def should_not_contain_expire(self, topic):
                    expect(topic).Not.to_include('expire')

                def should_not_contain_expire_ts(self, topic):
                    expect(topic).Not.to_include('last_expire_ts')

                def should_not_contain_success(self, topic):
                    expect(topic).Not.to_include('success')

                def should_not_contain_last_success_ts(self, topic):
                    expect(topic).Not.to_include('last_success_ts')

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenExpireItemIsRunOnce(context_class):
            def use_queue(self, queue):
                queue.push({'action': 'expire'})
                queue.run_once(worker_id='test_worker')

            def items_processed_should_be_empty(self, queue):
                expect(queue.items_processed).to_be_empty()

            def should_have_no_entries(self, queue):
                expect(queue.queued()).to_equal(0)

            class TheWorkerMetadata(Vows.Context):
                def topic(self, queue):
                    return queue.active_worker_metadata()['test_worker']

                def should_exist(self, topic):
                    expect(topic).Not.to_be_null()

                def should_contain_last_expire_ts(self, topic):
                    expect(topic).to_include('last_expire_ts')

                def should_contain_expire(self, topic):
                    expect(topic).to_include('expire')

                def expire_should_be_one(self, topic):
                    expect(topic['expire']).to_equal(1)

                def should_not_contain_error(self, topic):
                    expect(topic).Not.to_include('error')

                def should_not_contain_last_error_ts(self, topic):
                    expect(topic).Not.to_include('last_error_ts')

                def should_not_contain_retry(self, topic):
                    expect(topic).Not.to_include('retry')

                def should_not_contain_last_retry_ts(self, topic):
                    expect(topic).Not.to_include('last_retry_ts')

                def should_not_contain_success(self, topic):
                    expect(topic).Not.to_include('success')

                def should_not_contain_last_success_ts(self, topic):
                    expect(topic).Not.to_include('last_success_ts')

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenErrorItemIsRunOnce(context_class):
            def use_queue(self, queue):
                queue.push({'action': 'error', 'message': 'Error!'})
                queue.run_once(worker_id='test_worker')

            def items_processed_should_be_empty(self, queue):
                expect(queue.items_processed).to_be_empty()

            def should_be_empty(self, queue):
                expect(queue.queued()).to_equal(0)

            class TheWorkerMetadata(Vows.Context):
                def topic(self, queue):
                    return queue.active_worker_metadata()['test_worker']

                def should_exist(self, topic):
                    expect(topic).Not.to_be_null()

                def should_contain_last_error_ts(self, topic):
                    expect(topic).to_include('last_error_ts')

                def should_contain_error(self, topic):
                    expect(topic).to_include('error')

                def error_should_be_one(self, topic):
                    expect(topic['error']).to_equal(1)

                def should_not_contain_retry(self, topic):
                    expect(topic).Not.to_include('retry')

                def should_not_contain_last_retry_ts(self, topic):
                    expect(topic).Not.to_include('last_retry_ts')

                def should_not_contain_expire(self, topic):
                    expect(topic).Not.to_include('expire')

                def should_not_contain_expire_ts(self, topic):
                    expect(topic).Not.to_include('last_expire_ts')

                def should_not_contain_success(self, topic):
                    expect(topic).Not.to_include('retry')

                def should_not_contain_last_success_ts(self, topic):
                    expect(topic).Not.to_include('last_retry_ts')

            class TheErrorQueue(ErrorQueueContext):

                def should_have_length_one(self, topic):
                    expect(topic.length()).to_equal(1)

                def should_have_one_error(self, topic):
                    expect(topic.errors()).to_length(1)

                def should_have_one_error_id(self, topic):
                    expect(topic.error_ids()).to_length(1)                    

                class TheRedisHash(Vows.Context):
                    def topic(self, error_queue):
                        return error_queue.redis.hgetall('queue.test.errors')

                    def should_have_length_one(self, topic):
                        expect(topic).to_length(1)

                    def should_have_good_id(self, topic):
                        expect(topic.keys()[0]).to_equal(simplejson.loads(topic.values()[0])['id'])

                class TheError(Vows.Context):
                    def topic(self, error_queue):
                        return error_queue.errors()[0]

                    def should_exist(self, topic):
                        expect(topic).Not.to_be_null()

                    def should_contain_traceback(self, topic):
                        expect(topic).to_include('traceback')

                    def should_have_correct_error_type(self, topic):
                        expect(topic['error_type']).to_equal('Exception')

                    def should_have_correct_error_text(self, topic):
                        expect(topic['error_text']).to_equal('Error!')

                    def should_have_correct_envelope(self, topic):
                        expect(topic['envelope']['item']).to_equal({'action': 'error', 'message': 'Error!'})

                class TheErrorRetrievedById(Vows.Context):
                    def topic(self, error_queue):
                        return error_queue.error(error_queue.error_ids()[0])

                    def should_exist(self, topic):
                        expect(topic).Not.to_be_null()

                    def should_contain_traceback(self, topic):
                        expect(topic).to_include('traceback')

                    def should_have_correct_error_type(self, topic):
                        expect(topic['error_type']).to_equal('Exception')

                    def should_have_correct_error_text(self, topic):
                        expect(topic['error_text']).to_equal('Error!')

                    def should_have_correct_envelope(self, topic):
                        expect(topic['envelope']['item']).to_equal({'action': 'error', 'message': 'Error!'})

        class WhenErrorItemIsRunOnceAndRequeued(context_class):
            def use_queue(self, queue):
                queue.push({'action': 'error', 'message': 'Error!'})
                queue.run_once(worker_id='test_worker')
                queue.error_queue.requeue_error(queue.error_queue.errors()[0]['id'])

            def items_processed_should_be_empty(self, queue):
                expect(queue.items_processed).to_be_empty()

            def should_have_length_one(self, queue):
                expect(queue.queued()).to_equal(1)

            class ThePushedEntry(queue_entry_checker({'action': 'error', 'message': 'Error!'})):
                def topic(self, queue):
                    return queue.queued_items()[0]

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenMultipleErrorItemsAreRunOnceAndRequeued(context_class):
            def use_queue(self, queue):
                for i in range(5):
                    queue.push({'action': 'error', 'message': 'Error{}'.format(i)})
                    queue.run_once(worker_id='test_worker')
                queue.error_queue.requeue_all_errors()

            def items_processed_should_be_empty(self, queue):
                expect(queue.items_processed).to_be_empty()

            def should_have_length_five(self, queue):
                expect(queue.queued()).to_equal(5)

            class TheErrorQueue(EmptyErrorQueueContext):
                pass


        class WhenErrorItemIsRunOnceAndDeleted(context_class):
            def use_queue(self, queue):
                queue.push({'action': 'error', 'message': 'Error!'})
                queue.run_once(worker_id='test_worker')
                queue.error_queue.delete_error(queue.error_queue.errors()[0]['id'])

            def items_processed_should_be_empty(self, queue):
                expect(queue.items_processed).to_be_empty()

            def should_be_empty(self, queue):
                expect(queue.queued()).to_equal(0)

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

        class WhenPushedToOnceAndPoppedFromOnceAndReclaimed(context_class):
            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.pop(worker_id='test_worker')
                queue._reclaim()

            def should_be_empty(self, queue):
                expect(queue.queued()).to_equal(0)

        class WhenPushedToOnceAndPoppedFromOnceAndWorkerKeyUnsetAndReclaimed(context_class):
            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.pop(worker_id='test_worker')
                queue.redis.delete(queue._worker_activity_key('test_worker'))
                queue._reclaim()

            def queued_should_be_one(self, queue):
                expect(queue.queued()).to_equal(1)

            class TheReclaimedEntry(queue_entry_checker({'a': 1})):
                def topic(self, queue):
                    return queue.queued_items()[0]

        class WhenDeserializationErrorOccurs(context_class):
            def use_queue(self, queue):
                old_serializer = queue._serializer

                class ErroringSerializer(object):
                    def serialize(self, *args, **kwargs):
                        return old_serializer.serialize(*args, **kwargs)
                    def deserialize(self, *args, **kwargs):
                        raise ValueError("I've made a huge mistake")

                queue._serializer = ErroringSerializer()
                queue.push({'a': 1})
                queue.pop(worker_id='test_worker')

            def should_be_empty(self, queue):
                expect(queue.queued()).to_equal(0)

            class TheWorkerQueue(Vows.Context):
                def should_be_empty(self, queue):
                    expect(queue.redis.llen('queue.test.test_worker.working')).to_equal(0)

            class TheErrorQueue(EmptyErrorQueueContext):
                pass

    return QueueTests

@Vows.batch
class BasicQueueVows(Vows.Context):
    class ABaseQueue(basic_queue_tests(single_queue_context(TestQueue))):
        pass

    class AnIsolationQueue(basic_queue_tests(single_queue_context(TestIsolationQueue))):
        pass

    class ABatchingQueue(basic_queue_tests(single_queue_context(TestBatchingQueue))):
        pass

    class ABatchingIsolationQueue(basic_queue_tests(single_queue_context(TestBatchingIsolationQueue))):
        pass

    class ADocket(basic_queue_tests(single_queue_context(TestDocket))):
        pass
