import simplejson as json

class JsonSerializer(object):
    def __init__(self, dumps_kwargs=None, loads_kwargs=None):
        self.dumps_kwargs = dumps_kwargs or {}
        self.loads_kwargs = loads_kwargs or {}

    def serialize(self, obj):
        return json.dumps(obj, sort_keys=True, **self.dumps_kwargs)

    def deserialize(self, json_obj):
        return json.loads(json_obj, **self.loads_kwargs)
