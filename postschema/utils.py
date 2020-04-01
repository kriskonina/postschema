import json
import string
import re
import random
import secrets

import orjson
from aiohttp import web
from psycopg2.extras import Json as PsycopJson


NUMSET = list(string.digits)
random.shuffle(NUMSET)
PG_ERR_PAT = re.compile(
    r'(?P<prefix>([\s\w]|_)+)\((?P<name>.*?)\)\=\((?P<val>.*?)\)(?P<reason>.*)'
)
PG_CONSTR_PAT = re.compile(
    r'constraint \"(?P<constraint>\w+)\"'
)
ORJSON_FLAGS = orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_UUID | orjson.OPT_UTC_Z


def def_dump(val):
    if isinstance(val, (set, frozenset)):
        return list(val)
    return json.dumps(str(val))


def dumps(val):
    try:
        return orjson.dumps(val, option=ORJSON_FLAGS).decode()
    except orjson.JSONEncodeError:
        return json.dumps(val, default=def_dump)


def Json(val):
    return PsycopJson(val, dumps=dumps)


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
    return errs or {'error': perr.diag.message_detail}


def parse_postgres_constraint_err(perr):
    res = PG_CONSTR_PAT.search(str(perr))
    if res:
        return res.groupdict()['constraint']


def retype_schema(cls, new_methods):
    '''Extend schema class `cls` with `new_methods` dict,
    containing new attributes/methods.'''

    methods = dict(cls.__dict__)
    for k, v in methods.pop('_declared_fields', {}).items():
        methods[k] = v
    methods.update(new_methods)
    return type(cls.__name__, cls.__bases__, methods)


def seconds_to_human(ttl_seconds):
    ttl_minutes = round(ttl_seconds / 60)
    if ttl_minutes >= 1440:  # a day
        return f'{round(ttl_minutes / 1440)} day(s)'
    elif ttl_minutes >= 60:
        return f'{round(ttl_minutes / 60)} hour(s)'
    else:
        return f'{ttl_minutes} minute(s)'
