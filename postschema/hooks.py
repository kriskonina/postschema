from contextlib import suppress
from psycopg2.extras import DateTimeRange, DateTimeTZRange
from .utils import Json


def translate_naive_nested(schema_cls, extraction_field):
    def wrapped(self, payload, **kwargs):
        try:
            nested_map = schema_cls._nested_select_stmts
        except AttributeError:
            # for non-inheriting resources nothing is done here
            return payload
        for payload_field in payload.get(extraction_field, []):
            if payload_field in nested_map:
                payload[extraction_field].remove(payload_field)
                payload[extraction_field].append(nested_map[payload_field])
        return payload
    return wrapped


def translate_naive_nested_to_dict(nested_map, extraction_field):
    def wrapped(self, payload, **kwargs):
        return {
            payload_field: nested_map.get(payload_field, payload_field)
            for payload_field in payload.get(extraction_field, [])
        }
    return wrapped


def escape_iterable(fieldnames):
    def wrapped(payload, view_instance, **kwargs):
        for fieldname in fieldnames:
            with suppress(KeyError):
                payload[fieldname] = Json(payload[fieldname])
        return payload
    return wrapped


def escape_rangeable(fieldnames):
    def wrapped(payload, view_instance, **kwargs):
        for fieldname in fieldnames:
            with suppress(KeyError):
                is_aware = view_instance.schema.declared_fields[fieldname].metadata.get('is_aware', False)
                range_cls = DateTimeTZRange if is_aware else DateTimeRange
                payload[fieldname] = range_cls(*payload[fieldname])
        return payload
    return wrapped
