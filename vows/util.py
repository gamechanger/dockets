import fakeredis
from pyvows import Vows, expect


class FakeRedisContext(Vows.Context):
    def __init__(self, *args, **kwargs):
        super(FakeRedisContext, self).__init__(*args, **kwargs)
        self.ignore('use_redis')

    def topic(self):
        redis = fakeredis.FakeRedis()
        return self.use_redis(redis) or redis


    def use_redis(self, redis):
        raise NotImplementedError
