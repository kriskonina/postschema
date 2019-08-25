from functools import partial

import ujson
from aiohttp import web

dumps = partial(ujson.dumps, ensure_ascii=False, escape_forward_slashes=False)


def json_response(data, **kwargs):
    kwargs.setdefault("dumps", dumps)
    return web.json_response(data, **kwargs)
