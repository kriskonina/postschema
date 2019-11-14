import string
import re
import random
import secrets
from functools import partial

import ujson
from aiohttp import web

dumps = partial(ujson.dumps, ensure_ascii=False, escape_forward_slashes=False)
NUMSET = list(string.digits)
random.shuffle(NUMSET)
PG_ERR_PAT = re.compile(
    r'(?P<prefix>([\s\w]|_)+)\((?P<name>.*?)\)\=\((?P<val>.*?)\)(?P<reason>.*)'
)


def generate_random_word(ln=10):
    return secrets.token_urlsafe(ln)


def generate_num_sequence(ln=4):
    return ''.join(random.sample(NUMSET, ln))


def json_response(data, **kwargs):
    kwargs.setdefault("dumps", dumps)
    return web.json_response(data, **kwargs)


def parse_postgres_err(perr):
    res = PG_ERR_PAT.search(perr.diag.message_detail)
    errs = {}
    if res:
        parsed = res.groupdict()
        prefix = parsed['prefix']
        names = parsed['name'].split(', ')
        vals = parsed['val'].split(', ')
        reason = parsed['reason'].strip()
        for key, val in zip(names, vals):
            errs[key] = [f'{prefix}({val}) ' + reason]
    return errs or perr.diag.message_detail


def retype_schema(cls, new_methods):
    methods = dict(cls.__dict__)
    for k, v in methods.pop('_declared_fields', {}).items():
        methods[k] = v
    methods.update(new_methods)
    return type(cls.__name__, cls.__bases__, methods)
