import simplejson

from pyvows import Vows, expect
from util import FakeRedisContext

from dockets.queue import Queue
from dockets.isolation_queue import IsolationQueue
from dockets.errors import RetryError, ExpiredError

class TestQueue(Queue):
    def __init__(self, *args, **kwargs):
        super(TestQueue, self).__init__(*args, **kwargs)
        self.data_processed = []

    def process_data(self, data):
        if (not isinstance(data, dict) or 'action' not in data
            or data['action'] == 'success'):
            self.data_processed.append(data)
            return
        if data['action'] == 'retry':
            raise RetryError
        if data['action'] == 'expire':
            raise ExpiredError
        if data['action'] == 'error':
            raise Exception(data['message'])

class TestIsolationQueue(IsolationQueue):
    def __init__(self, *args, **kwargs):
        super(TestIsolationQueue, self).__init__(*args, **kwargs)
        self.data_processed = []

    def process_data(self, data):
        if (not isinstance(data, dict) or 'action' not in data
            or data['action'] == 'success'):
            self.data_processed.append(data)
            return
        if data['action'] == 'retry':
            raise RetryError
        if data['action'] == 'expire':
            raise ExpiredError
        if data['action'] == 'error':
            raise Exception(data['message'])

class SingleQueueContext(FakeRedisContext):
    def __init__(self, *args, **kwargs):
        super(SingleQueueContext, self).__init__(*args, **kwargs)
        self.ignore('use_queue')

    def use_redis(self, redis):
        queue = TestQueue(redis, 'test')
        self.use_queue(queue)
        return queue

    def use_queue(self, queue):
        raise NotImplementedError

class SingleIsolationQueueContext(FakeRedisContext):
    def __init__(self, *args, **kwargs):
        super(SingleIsolationQueueContext, self).__init__(*args, **kwargs)
        self.ignore('use_queue')

    def use_redis(self, redis):
        queue = TestIsolationQueue(redis, 'test')
        self.use_queue(queue)
        return queue

    def use_queue(self, queue):
        raise NotImplementedError

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

            def should_have_data_key(self, topic):
                expect(topic).to_include('data')

            class TheDataKey(Vows.Context):
                def topic(self, item):
                    return item['data']

                def should_be_input_data(self, topic):
                    expect(topic).to_equal(entry_value)
    return TheEntry


def basic_queue_tests(context_class):
    class QueueTests(Vows.Context):
        class WhenPushedToOnce(context_class):

            def use_queue(self, queue):
                queue.push({'a': 1})

            def should_have_one_entry(self, queue):
                expect(queue.queued()).to_equal(1)

            def should_have_one_redis_entry(self, queue):
                expect(queue.redis.llen('queue.test')).to_equal(1)

            class ThePushedEntry(queue_entry_checker({'a': 1})):
                def topic(self, queue):
                    return queue.redis.lindex('queue.test', 0)

        class WhenPushedToTwice(context_class):

            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.push({'b': 2})

            def should_have_two_entries(self, queue):
                expect(queue.queued()).to_equal(2)

            def should_have_two_redis_entries(self, queue):
                expect(queue.redis.llen('queue.test')).to_equal(2)

            class TheFirstPushedEntry(queue_entry_checker({'a': 1})):
                def topic(self, queue):
                    return queue.redis.lindex('queue.test', 1)

            class TheSecondPushedEntry(queue_entry_checker({'b': 2})):
                def topic(self, queue):
                    return queue.redis.lindex('queue.test', 0)

        class WhenPushedToOnceAndPoppedFromOnce(context_class):

            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.pop(worker_id='test_worker')

            def should_have_no_entries(self, queue):
                expect(queue.queued()).to_equal(0)

            def should_have_no_redis_entries(self, queue):
                expect(queue.redis.llen('queue.test')).to_equal(0)

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

            def item_should_be_in_data_processed(self, queue):
                expect(queue.data_processed).to_include({'a': 1})

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

        class WhenPushedToTwiceAndRunOnce(WhenPushedToOnceAndRunOnce):
            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.push({'b': 2})
                queue.run_once(worker_id='test_worker')

            def should_be_empty(self, queue):
                pass

            def should_have_one_entry(self, queue):
                expect(queue.queued()).to_equal(1)

        class WhenPushedToTwiceAndRunTwice(context_class):
            def use_queue(self, queue):
                queue.push({'a': 1})
                queue.push({'b': 2})
                queue.run_once(worker_id='test_worker')
                queue.run_once(worker_id='test_worker')

            def first_item_should_be_in_data_processed(self, queue):
                expect(queue.data_processed).to_include({'a': 1})

            def second_item_should_be_in_data_processed(self, queue):
                expect(queue.data_processed).to_include({'b': 2})

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

        class WhenRetryItemIsRunOnce(context_class):
            def use_queue(self, queue):
                queue.push({'action': 'retry'})
                queue.run_once(worker_id='test_worker')

            def data_processed_should_be_empty(self, queue):
                expect(queue.data_processed).to_be_empty()

            def should_have_one_entry(self, queue):
                expect(queue.queued()).to_equal(1)

            class ThePushedEntry(queue_entry_checker({'action': 'retry'})):
                def topic(self, queue):
                    return queue.redis.lindex('queue.test', 0)

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

        class WhenExpireItemIsRunOnce(context_class):
            def use_queue(self, queue):
                queue.push({'action': 'expire'})
                queue.run_once(worker_id='test_worker')

            def data_processed_should_be_empty(self, queue):
                expect(queue.data_processed).to_be_empty()

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


        class WhenErrorItemIsRunOnce(context_class):
            def use_queue(self, queue):
                queue.push({'action': 'error', 'message': 'Error!'})
                queue.run_once(worker_id='test_worker')

            def data_processed_should_be_empty(self, queue):
                expect(queue.data_processed).to_be_empty()

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
    return QueueTests

@Vows.batch
class BasicQueueVows(Vows.Context):
    class ABaseQueue(basic_queue_tests(SingleQueueContext)):
        pass

    class AnIsolationQueue(basic_queue_tests(SingleIsolationQueueContext)):
        pass
