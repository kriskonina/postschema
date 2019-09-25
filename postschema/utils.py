from functools import partial

import ujson
from aiohttp import web

dumps = partial(ujson.dumps, ensure_ascii=False, escape_forward_slashes=False)


def json_response(data, **kwargs):
    kwargs.setdefault("dumps", dumps)
    return web.json_response(data, **kwargs)


def retype_schema(cls, new_methods):
    methods = dict(cls.__dict__)
    for k, v in methods.pop('_declared_fields', {}).items():
        methods[k] = v
    methods.update(new_methods)
    return type(cls.__name__, cls.__bases__, methods)