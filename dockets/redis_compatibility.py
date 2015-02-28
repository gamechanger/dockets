"""Helpers to support using Redis or StrictRedis clients interchangeably."""

from redis.client import Pipeline

def _is_legacy_pipeline(pipeline):
    return isinstance(pipeline, Pipeline)

def compatible_zadd(pipeline, key, score, member):
    if _is_legacy_pipeline(pipeline):
        return pipeline.zadd(key, member, score)
    else:
        return pipeline.zadd(key, score, member)

def compatible_setex(pipeline, key, seconds, value):
    if _is_legacy_pipeline(pipeline):
        return pipeline.setex(key, value, seconds)
    else:
        return pipeline.setex(key, seconds, value)

def compatible_lrem(pipeline, key, count, value):
    if _is_legacy_pipeline(pipeline):
        return pipeline.lrem(key, value, count)
    else:
        return pipeline.lrem(key, count, value)
