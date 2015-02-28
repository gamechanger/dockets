"""Helpers to support using Redis or StrictRedis clients interchangeably."""

from redis.client import Pipeline

def compatible_zadd(pipeline, key, score, member):
    if isinstance(pipeline, Pipeline): # only true for legacy Pipeline and Redis client
        return pipeline.zadd(key, member, score)
    else:
        return pipeline.zadd(key, score, member)
