from collections import deque
from dataclasses import dataclass
from importlib import import_module
from weakref import proxy

import ujson
from aiohttp import web
from async_property import async_cached_property
from marshmallow import Schema, ValidationError, fields, validate, post_load
from sqlalchemy.sql.schema import Sequence

from postschema import exceptions as post_exceptions
from postschema.contrib import Pagination
# from postschema.fields import OneToMany
from postschema.hooks import translate_naive_fieldnames, translate_naive_fieldnames_to_dict
from postschema.utils import json_response

__all__ = ['ViewsBase']

NESTABLE_FIELDS = (fields.Dict, fields.Tuple, fields.List, fields.Nested)


class WrongType(Exception):
    pass


@dataclass(frozen=True)
class MANDATORY_PAGINATION_FIELDS:
    page: fields.Integer
    limit: fields.Integer
    order_by: fields.List
    order_dir: fields.String

    def __iter__(self):
        for i in self.__dict__.items():
            yield i

    def __post_init__(self):
        annotations = self.__annotations__
        for k, v in self:
            expected_type = annotations[k]
            if not isinstance(v, expected_type):
                raise WrongType(f"Pagination class {self._cls_name}'s `{k}` is not of {expected_type} type")


class FormatDict(dict):
    def __missing__(self, key):
        return f'{{{key}}}'


class FallbackString(str):
    def format(self, **kwargs):
        kk = FormatDict(kwargs)
        return self.format_map(kk)


def adjust_pagination_schema(pagination_schema, schema_cls, list_by_fields, pk):
    declared_fields = pagination_schema._declared_fields
    cls_name = pagination_schema.__name__
    MANDATORY_PAGINATION_FIELDS._cls_name = cls_name

    try:
        MANDATORY_PAGINATION_FIELDS(**declared_fields)
    except TypeError as datacls_err:
        raise TypeError(f'Custom pagination class {pagination_schema.__module__}.{cls_name}.'
                        + datacls_err.args[0])
    except WrongType as wrong_type:
        raise TypeError(wrong_type)

    # Construct a brand new pagination class based on `pagination_schema`
    # to include `list_by_fields` as an argument to `OneOf` validator of `order_by` field.
    pagination_methods = declared_fields.copy()
    # en
    # Ensure that `order_by` doesn't include any nestable fields.
    for fname, fval in schema_cls._declared_fields.items():
        if isinstance(fval, NESTABLE_FIELDS):
            try:
                list_by_fields.remove(fname)
            except ValueError:
                pass

    # upon completing the loading, translate the nested fields (if present) to their composite form
    pagination_methods['adjust_nested_fields'] = post_load(
        translate_naive_fieldnames(schema_cls, 'order_by'))

    orig_validators = pagination_methods['order_by'].validate or []
    orig_validators.append(validate.OneOf(list_by_fields))

    missing_val = pagination_methods['order_by'].missing or [pk]
    pagination_methods['order_by'] = fields.List(
        fields.String(validate=orig_validators),
        missing=missing_val)

    return type(cls_name, (Schema, ), pagination_methods)


def make_select_fields_schema(schema_cls):
    def _get_all_selectable_fields():
        is_kid = schema_cls.is_kid
        declared_fields = dict(schema_cls._declared_fields)

        if is_kid:
            extends_on = schema_cls.Meta.extends_on
            declared_fields.pop(extends_on, None)

        for fieldname, fieldval in declared_fields.items():
            if fieldval.dump_only:
                continue
            yield fieldname

    allowed_fields = set(_get_all_selectable_fields())
    nested_map = schema_cls._nested_select_stmts if schema_cls.is_kid else {}

    return type(f'{schema_cls.__name__}Selects', (Schema, ), {
        'select': fields.List(
            fields.String(
                validate=[validate.OneOf(list(allowed_fields))]
            )
        ),
        'clean_payload': post_load(
            translate_naive_fieldnames_to_dict(nested_map, 'select')
        )
    })


class ViewsClassBase(web.View):

    def __init__(self, request):
        self._request = request
        self._orig_cleaned_payload = {}

    @classmethod
    def post_init(cls):
        cls.schemas = import_module('postschema.schema')._schemas
        table = cls.model.__table__

        cls.pk_col = table.primary_key.columns_autoinc_first[0]
        cls.pk_column_name = cls.pk_col.name
        cls.pk_autoicr = isinstance(cls.pk_col.default, Sequence)

        schema_metacls = getattr(cls.schema_cls, 'Meta', object)

        if cls.schema_cls.is_kid:
            cls._naive_fields_to_composite_stmts()
        cls._join_stmts = cls._prepare_join_statements()

        try:
            selects_nested_map = cls.schema_cls._nested_select_stmts
        except AttributeError:
            selects_nested_map = {}

        get_by = getattr(schema_metacls, 'get_by', [cls.pk_column_name])
        get_by_select = {field: selects_nested_map.get(field, field) for field in get_by}
        list_by = getattr(schema_metacls, 'list_by', get_by) or get_by
        list_by_select = {field: selects_nested_map.get(field, field) for field in list_by}
        delete_by = getattr(schema_metacls, 'delete_by', [cls.pk_column_name])

        pagination_schema_raw = getattr(schema_metacls, 'pagination_schema', Pagination)

        cls.pagination_schema = adjust_pagination_schema(pagination_schema_raw,
                                                         cls.schema_cls, list_by.copy(),
                                                         cls.pk_column_name)()
        cls.select_schema = make_select_fields_schema(cls.schema_cls)()

        excluded = getattr(schema_metacls, 'exclude_from_updates', [])
        pk_excluded = [col.name for col in table.primary_key.columns.values()]

        update_excluded = [*excluded, *pk_excluded]

        cls.insert_query_stmt = cls._prepare_insert_query()
        cls.get_query_stmt = cls._prepare_get_query(get_by_select)
        cls.list_query_stmt = cls._prepare_list_query(list_by_select)
        cls.update_query_stmt = FallbackString(f"""
            WITH rows AS (
                UPDATE {cls.schema_cls.__tablename__}
                SET {{updates}}
                WHERE {{where}}
                RETURNING 1
            )
            SELECT count(*) FROM rows""")
        cls.delete_query_stmt = FallbackString(f"""
            WITH rows AS (
                DELETE FROM {cls.schema_cls.__tablename__}
                WHERE {{where}}
                RETURNING 1
            SELECT count(*) FROM rows""")

        cls.post_schema = cls.schema_cls()
        cls.get_schema = cls.schema_cls(only=get_by, partial=True)
        cls.list_schema = cls.schema_cls(only=list_by, partial=True)
        cls.patch_schema = cls.put_schema = cls.schema_cls(partial=True, exclude=update_excluded)
        cls.delete_schema = cls.schema_cls(partial=True, only=delete_by)

    @classmethod
    def _naive_fields_to_composite_stmts(cls):
        schema = cls.schema_cls
        parent = schema.__base__
        tablename = parent.__tablename__
        extends_on = getattr(schema.Meta, 'extends_on', None)

        nested_fields_to_json_query = {}
        nested_fields_to_select = {}
        nested_inst = parent._declared_fields[extends_on].nested

        for aname, aval in nested_inst._declared_fields.items():
            if aname not in schema.child_field_names:
                continue
            attr_name = aval.attribute or aname
            if isinstance(aval, fields.Number):
                frmt = f'=%({attr_name})s::jsonb'
            elif isinstance(aval, fields.List):
                frmt = f' @> to_jsonb(%({attr_name})s)'
            else:
                frmt = f' ? %({attr_name})s'
            nested_fields_to_json_query[aname] = f"{tablename}.{extends_on}->'{aname}'{frmt}"
            nested_fields_to_select[aname] = f"{tablename}.{extends_on}->'{aname}'"
        schema._nested_where_stmts = nested_fields_to_json_query
        schema._nested_select_stmts = nested_fields_to_select

    @classmethod
    def _prepare_join_statements(cls, joins=None):
        schema = cls.schema_cls
        joins = joins or schema._joins
        join_stmts = []
        for join_to, join_opts in joins.items():
            joinee_name = join_opts['joinee']
            joinee_schema = getattr(cls.schemas, joinee_name, None)
            joinee_tablename = joinee_schema.__tablename__
            join_stmts.append(
                f'LEFT JOIN {joinee_tablename} ON {schema.__tablename__}.{join_to}_id={joinee_tablename}.id')
        return ' '.join(join_stmts)

    @classmethod
    def _prepare_selects(cls, include_dict):
        schema = cls.schema_cls
        tablename = schema.__tablename__
        for field_naive, get_by_phrase in include_dict.items():
            if '.' not in get_by_phrase:
                include_dict[field_naive] = f'{tablename}.{field_naive}'
        for joined_tablename, obj in schema._joins.items():
            for field in obj['only']:
                target = f"{joined_tablename}.{field}"
                include_dict[target] = target
        return include_dict

    @classmethod
    def _prepare_list_query(cls, list_by, compile_selects=False):
        tablename = cls.schema_cls.__tablename__
        tablename_cte = f'{tablename}_cte'
        selects = cls._prepare_selects(list_by) if compile_selects else list_by
        select = ','.join(f"'{k}',{v}" for k, v in selects.items())
        return FallbackString(f'''WITH {tablename_cte} AS (
                SELECT json_build_object({select}) AS js,
                       count(*) OVER() AS full_count
                       FROM {tablename} {cls._join_stmts}
                       WHERE {{where}}
                       ORDER BY {{orderby}} {{orderhow}}
            )
            SELECT json_build_object('data', json_agg(js), 'total_count', t.ct) FROM (
                SELECT js, {tablename_cte}.full_count as ct FROM {tablename_cte}
                LIMIT {{limit}}
                OFFSET {{offset}}
            ) t
            GROUP BY t.ct
        ''')

    @classmethod
    def _prepare_get_query(cls, get_by, compile_selects=False):
        tablename = cls.schema_cls.__tablename__
        selects = cls._prepare_selects(get_by) if compile_selects else get_by
        select = ','.join(f"'{k}',{v}" for k, v in selects.items())
        return f'''SELECT json_build_object({select}) AS inner
               FROM {tablename} {cls._join_stmts} WHERE {{where}}'''

    @classmethod
    def _prepare_insert_query(cls):
        insert_cols = [
            key for key, val in cls.model.__table__.columns.items()
            if key != cls.pk_column_name
        ]

        # ensure the pk ends up as a last col
        # insert_cols.append(cls.pk_column_name)
        colnames = ','.join(insert_cols)

        values = [f"%({colname})s" for colname in insert_cols]
        if cls.pk_autoicr:
            values.append(f"NEXTVAL('{cls.pk_col.default.name}')")
        else:
            values.append(f'%({cls.pk_column_name})')
        valnames = ','.join(values)

        return f"""INSERT INTO {cls.tablename} ({colnames},id)
            VALUES ( {valnames} )
            RETURNING {cls.pk_column_name}"""


class ViewsBase(ViewsClassBase):

    @async_cached_property
    async def payload(self):
        try:
            return await self.request.json(loads=ujson.loads)
        except Exception:
            raise web.HTTPBadRequest(reason='cannot read payload')

    @property
    def schema(self):
        return getattr(self, f'{self.method}_schema')

    @property
    def request(self):
        return self._request

    @property
    def method(self):
        return getattr(self, '_method', self.request.method).lower()

    @property
    def translated_payload(self):
        nested_map = self.schema._nested_select_stmts
        for k, v in self._orig_cleaned_payload.items():
            self._orig_cleaned_payload[k] = nested_map.get(k, k)
        return self._orig_cleaned_payload

    async def _clean_update_payload(self):
        ''''Common method for validating the payload of `PUT` and `PATCH` methods'''
        payload_raw = await self.payload
        if not isinstance(payload_raw, dict):
            raise post_exceptions.ValidationError({
                "_schema": [
                    "Invalid input type."
                ]
            })

        # ensure `payload` contains `select` and `payload` keys
        REQ = ['This field is required']
        EMPTY = ['This field cannot be empty']
        errs = {}
        try:
            select = payload_raw.get('select', {})
            if not select:
                errs['select'] = EMPTY
        except KeyError:
            errs['select'] = REQ
        try:
            payload = payload_raw['payload']
            if not payload:
                errs['payload'] = EMPTY
        except KeyError:
            errs['payload'] = REQ
        if errs:
            raise post_exceptions.ValidationError(errs)

        # clear both sets
        cleaned_select = await self._validate_singular_payload(
            payload=select, schema=self.get_schema, envelope_key='select')
        cleaned_payload = await self._validate_singular_payload(
            payload=payload, envelope_key='payload')

        return cleaned_select, cleaned_payload

    def _clean_write_payload(self, payload):
        '''Post-validation payload cleaning abstract methods
        used with POST, PUT and PATCH. Primarily to handle the relationships.'''
        for cleaner in self.schema._post_validation_write_cleaners:
            payload = cleaner(payload, self) or payload
        return payload

    async def _fetch(self, cleaned_payload, query):
        '''Common logic for `get()` and `list()`'''

        query, values = self._whereize_query(cleaned_payload, query)

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, values)
                except Exception:
                    print("!!", cur.query.decode())
                    raise
                try:
                    data = (await cur.fetchone())[0]
                except TypeError:
                    data = {}
                return json_response(data)

    async def _parse_select_fields(self, get_query, query_maker=None):
        get_query_raw = self.request.query
        if 'select' in get_query:
            unified_select_fields = get_query_raw.getall('select')
            if ',' in unified_select_fields[0]:
                unified_select_fields = unified_select_fields[0].split(',')
            select_with = await self._validate_singular_payload(
                {'select': unified_select_fields},
                self.select_schema, 'query'
            )
            del get_query['select']
            return query_maker(select_with, compile_selects=False)

    async def _validate_singular_payload(self, payload=None, schema=None, envelope_key=None,
                                         raise_orig=False):
        ref_schema = schema if schema is not None else self.schema
        payload_used = payload if payload is not None else await self.payload
        ref_schema.app = proxy(self.request.app)
        err_msg = None
        try:
            loaded = ref_schema.load(payload_used)
            # TODO: perform async validation, joining errors with
            # ones already aggregated in the previous step
        except ValidationError as merr:
            if raise_orig:
                raise merr
            err_msg = merr.messages
            try:
                err_msg = await ref_schema.run_async_validators(payload_used) or err_msg
            except AttributeError:
                # ignore validating \w Schemas not inheriting from PostSchema
                pass
            if envelope_key:
                err_msg = {
                    envelope_key: err_msg
                }
            raise post_exceptions.ValidationError(err_msg)

        return loaded

    def _whereize_query(self, cleaned_payload, query):
        json_where_stmts = self.schema._nested_where_stmts
        wheres = deque()
        values = {}

        for nested_field in self.schema.child_field_names:
            nested_in_payload = cleaned_payload.pop(nested_field, None)
            if nested_in_payload:
                values.update({nested_field: nested_in_payload})
                sql_part = json_where_stmts[nested_field]
                wheres.append(sql_part)

        for key in cleaned_payload:
            wheres.appendleft(f"{key}=%({key})s")
        values.update(cleaned_payload)

        wheres_q = ' AND '.join(wheres) or ' 1=1 '

        return query.format(where=wheres_q), values
