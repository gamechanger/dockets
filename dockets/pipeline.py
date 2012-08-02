class PipelineObject(object):

    def __init__(self, redis):
        self.redis = redis

    @staticmethod
    def with_pipeline(function):
        def wrapper(self, *args, **kwargs):
            if not kwargs.get('pipeline'):
                kwargs['pipeline'] = pipeline = self.redis.pipeline()
                return_value = function(self, *args, **kwargs)
                pipeline.execute()
                return return_value
            return function(self, *args, **kwargs)
        return wrapper
