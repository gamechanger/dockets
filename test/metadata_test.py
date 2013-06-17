from util import *
from dockets.metadata import CappedList, RateTracker, TimeTracker

capped_list = CappedList(redis, 'test_capped_list', 2)
rate_tracker = RateTracker(redis, 'test', 'test', 2)
time_tracker = TimeTracker(redis, 'test', 'test', 2)

# capped list tests
@clear
def test_add_to_capped_list():
    capped_list.add(1)
    assert len(capped_list) == 1
    assert '1' in capped_list
    assert redis.exists('test_capped_list')

@clear
def test_add_to_capped_list_twice():
    capped_list.add(1)
    capped_list.add(2)
    assert len(capped_list) == 2
    assert '1' in capped_list
    assert '2' in capped_list
    assert redis.exists('test_capped_list')

@clear
def test_add_to_capped_list_thrice():
    capped_list.add(1)
    capped_list.add(2)
    capped_list.add(3)
    assert len(capped_list) == 2
    assert '2' in capped_list
    assert '3' in capped_list
    assert redis.exists('test_capped_list')

# rate tracker tests

@clear
def test_empty_rate_tracker():
    assert rate_tracker.rate() == 0

@clear
def test_add_one_time_to_rate_tracker():
    rate_tracker.count(current_time=1)
    assert rate_tracker.rate() == 0

@clear
def test_add_two_times_to_rate_tracker_separated_by_one():
    rate_tracker.count(current_time=1)
    rate_tracker.count(current_time=2)
    assert rate_tracker.rate() == 1

@clear
def test_add_two_times_to_rate_tracker_separated_by_two():
    rate_tracker.count(current_time=1)
    rate_tracker.count(current_time=3)
    assert rate_tracker.rate() == .5

@clear
def test_add_two_times_to_rate_tracker_separated_by_one_half():
    rate_tracker.count(current_time=1)
    rate_tracker.count(current_time=1.5)
    assert rate_tracker.rate() == 2

@clear
def test_add_three_times_to_rate_tracker_last_two_separated_by_one():
    rate_tracker.count(current_time=1)
    rate_tracker.count(current_time=5)
    rate_tracker.count(current_time=6)
    assert rate_tracker.rate() == 1

# time tracker tests

@clear
def test_empty_time_tracker():
    assert time_tracker.average_time() == 0

def test_add_one_time_to_time_tracker():
    time_tracker.add_time(1.5)
    assert time_tracker.average_time() == 1.5

def test_add_two_times_to_time_tracker():
    time_tracker.add_time(1.5)
    time_tracker.add_time(2)
    assert time_tracker.average_time() == 1.75

def test_add_three_times_to_time_tracker():
    time_tracker.add_time(1)
    time_tracker.add_time(1.5)
    time_tracker.add_time(2)
    assert time_tracker.average_time() == 1.75
