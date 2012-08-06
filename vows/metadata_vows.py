from pyvows import Vows, expect

from util import FakeRedisContext

from dockets.metadata import CappedList, RateTracker, TimeTracker

class CappedListContext(FakeRedisContext):

    def __init__(self, *args, **kwargs):
        super(CappedListContext, self).__init__(*args, **kwargs)
        self.ignore('use_list')

    def use_redis(self, redis):
        lst = CappedList(redis, 'test_capped_list', 2)
        self.use_list(lst)
        return lst

    def use_list(self, lst):
        raise NotImplementedError

@Vows.batch
class ACappedList(Vows.Context):

    class WhenAddedToOnce(CappedListContext):
        def use_list(self, lst):
            lst.add(1)

        def should_have_one_entry(self, topic):
            expect(topic).to_length(1)

        def should_contain_entry(self, topic):
            expect(topic).to_include(1)

        def should_be_in_redis(self, topic):
            expect(topic.redis.exists('test_capped_list')).to_be_true()

    class WhenAddedToTwice(CappedListContext):
        def use_list(self, lst):
            lst.add(1)
            lst.add(2)

        def should_have_two_entries(self, topic):
            expect(topic).to_length(2)

        def should_contain_first_entry(self, topic):
            expect(topic).to_include(1)

        def should_contain_second_entry(self, topic):
            expect(topic).to_include(2)

        def should_be_in_redis(self, topic):
            expect(topic.redis.exists('test_capped_list')).to_be_true()

    class WhenAddedToThrice(CappedListContext):
        def use_list(self, lst):
            lst.add(1)
            lst.add(2)
            lst.add(3)

        def should_have_two_entries(self, topic):
            expect(topic).to_length(2)

        def should_contain_second_entry(self, topic):
            expect(topic).to_include(2)

        def should_contain_third_entry(self, topic):
            expect(topic).to_include(3)

        def should_be_in_redis(self, topic):
            expect(topic.redis.exists('test_capped_list')).to_be_true()


class RateTrackerContext(FakeRedisContext):

    def __init__(self, *args, **kwargs):
        super(RateTrackerContext, self).__init__(*args, **kwargs)
        self.ignore('use_tracker')

    def use_redis(self, redis):
        tracker = RateTracker(redis, 'test', 'test', 2)
        self.use_tracker(tracker)
        return tracker

    def use_tracker(self, tracker):
        raise NotImplementedError

@Vows.batch
class ARateTracker(Vows.Context):

    class WhenEmpty(RateTrackerContext):
        def use_tracker(self, tracker):
            pass

        def rate_should_be_zero(self, topic):
            expect(topic.rate()).to_equal(0)

    class WhenOneTimeIsAdded(RateTrackerContext):
        def use_tracker(self, tracker):
            tracker.count(current_time=1)

        def rate_should_be_zero(self, tracker):
            expect(tracker.rate()).to_equal(0)

    class WhenTwoTimesAreAddedSeparatedByOne(RateTrackerContext):
        def use_tracker(self, tracker):
            tracker.count(current_time=1)
            tracker.count(current_time=2)

        def rate_should_be_one(self, tracker):
            expect(tracker.rate()).to_equal(1)

    class WhenTwoTimesAreAddedSeparatedByTwo(RateTrackerContext):
        def use_tracker(self, tracker):
            tracker.count(current_time=1)
            tracker.count(current_time=3)

        def rate_should_be_one_half(self, tracker):
            expect(tracker.rate()).to_equal(.5)

    class WhenTwoTimesAreAddedSeparatedByOneHalf(RateTrackerContext):
        def use_tracker(self, tracker):
            tracker.count(current_time=1)
            tracker.count(current_time=1.5)

        def rate_should_be_two(self, tracker):
            expect(tracker.rate()).to_equal(2)

    class WhenThreeTimesAreAddedWithLastTwoSeparatedByOne(RateTrackerContext):
        def use_tracker(self, tracker):
            tracker.count(current_time=1)
            tracker.count(current_time=5)
            tracker.count(current_time=6)

        def rate_should_be_one(self, tracker):
            expect(tracker.rate()).to_equal(1)

class TimeTrackerContext(FakeRedisContext):

    def __init__(self, *args, **kwargs):
        super(TimeTrackerContext, self).__init__(*args, **kwargs)
        self.ignore('use_tracker')

    def use_redis(self, redis):
        tracker = TimeTracker(redis, 'test', 'test', 2)
        self.use_tracker(tracker)
        return tracker

    def use_tracker(self, tracker):
        raise NotImplementedError

@Vows.batch
class ATimeTracker(Vows.Context):

    class WhenEmpty(TimeTrackerContext):
        def use_tracker(self, tracker):
            pass

        def average_time_should_be_zero(self, tracker):
            expect(tracker.average_time()).to_equal(0)

    class WhenOneTimeIsAdded(TimeTrackerContext):
        def use_tracker(self, tracker):
            tracker.add_time(1.5)

        def average_time_should_be_the_added_time(self, tracker):
            expect(tracker.average_time()).to_equal(1.5)

    class WhenTwoTimesAreAdded(TimeTrackerContext):
        def use_tracker(self, tracker):
            tracker.add_time(1.5)
            tracker.add_time(2)

        def average_time_should_be_the_average(self, tracker):
            expect(tracker.average_time()).to_equal(1.75)

    class WhenThreeTimesAreAdded(TimeTrackerContext):
        def use_tracker(self, tracker):
            tracker.add_time(1)
            tracker.add_time(1.5)
            tracker.add_time(2)

        def average_time_should_be_the_average_of_the_last_two_times(self, tracker):
            expect(tracker.average_time()).to_equal(1.75)
