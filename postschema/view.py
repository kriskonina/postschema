import functools
import re
import warnings

from collections import deque, defaultdict as dd
from dataclasses import dataclass
from importlib import import_module
from weakref import proxy

import ujson
from aiohttp import web
from aiojobs.aiohttp import spawn
from async_property import async_cached_property
from marshmallow import Schema, ValidationError, fields, validate, post_load
from psycopg2 import errors as postgres_errors
from sqlalchemy.sql.schema import Sequence

from postschema import exceptions as post_exceptions
from postschema.contrib import Pagination
from postschema.fields import Set, Relationship
from postschema.hooks import translate_naive_nested, translate_naive_nested_to_dict
from postschema.utils import json_response, retype_schema
from postschema.validators import must_not_be_empty, adjust_children_field

__all__ = ['ViewsBase']

NESTABLE_FIELDS = (fields.Dict, fields.Nested, Set)
ITERABLE_FIELDS = (Set, fields.List)
PG_ERR_PAT = re.compile(
    r'(?P<prefix>([\s\w]|_)+)\((?P<name>.*?)\)\=\((?P<val>.*?)\)(?P<reason>.*)'
)


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
        translate_naive_nested(schema_cls, 'order_by'))

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
            translate_naive_nested_to_dict(nested_map, 'select')
        )
    })


class CommonViewMixin:
    async def _validate_singular_payload(self, payload=None, schema=None, envelope_key=None,
                                         raise_orig=False):
        ref_schema = schema if schema is not None else self.schema
        payload_used = payload if payload is not None else await self.payload
        if ref_schema == self.schema and self.schema is None:
            warnings.warn("Can't validate payload without body schema")
            return {}
        ref_schema.app = proxy(self.request.app)
        err_msg = None
        try:
            loaded = ref_schema.load(payload_used)
        except ValidationError as merr:
            if raise_orig:
                raise merr
            err_msg = merr.messages
            raise post_exceptions.ValidationError(err_msg if not envelope_key else {envelope_key: err_msg})

        try:
            err_msg = await ref_schema.run_async_validators(payload_used) or err_msg
        except AttributeError:
            # ignore validating \w Schemas not inheriting from PostSchema
            pass
        if err_msg:
            raise post_exceptions.ValidationError(err_msg if not envelope_key else {envelope_key: err_msg})

        return loaded


class AuxViewMeta(type):
    def __new__(cls, name, bases, methods): # noqa
        if not bases:
            return super(AuxViewMeta, cls).__new__(cls, name, bases, methods)
        schemas = dd(dict)
        iterable_fields = []
        allowed_locations = ['path', 'query', 'header', 'body']
        invmap = {}
        for k, v in methods.copy().items():
            if isinstance(v, fields.Field):
                meta = v.metadata
                try:
                    location = meta['location']
                except KeyError:
                    raise KeyError(f'Field `{k}` need to define `location` attribute')
                if location not in allowed_locations:
                    raise AttributeError(f'Location {location} (defined on `{k}`) is invalid')
                if location == 'path':
                    v.required = True
                elif location in ['query', 'header'] and isinstance(v, ITERABLE_FIELDS):
                    iterable_fields.append(k)
                schemas[location][k] = methods.pop(k)
                invmap[k] = location

        for k, v in methods.copy().items():
            if callable(v) and '__marshmallow_hook__' in v.__dict__:
                try:
                    fieldname = v.__marshmallow_hook__['validates']['field_name']
                except KeyError:
                    fieldname = None
                if fieldname in invmap:
                    location = invmap[fieldname]
                    schemas[location][k] = methods.get(k)

        schemas = {
            f'{k}_schema': type('PathSchema', (Schema,), v)()
            for k, v in schemas.items()
        }
        try:
            methods['header_schema'] = type(schemas.pop('header_schema'))(unknown='INCLUDE')
        except KeyError:
            pass
        methods.update({
            '_iterable_fields': iterable_fields,
            **schemas
        })
        kls = super(AuxViewMeta, cls).__new__(cls, name, bases, methods)
        return kls


class AuxView(metaclass=AuxViewMeta):
    pass


class AuxViewBase(web.View, CommonViewMixin):
    def __init__(self, request):
        self._request = request
        self.path_payload = {}

    async def _iter(self):
        if hasattr(self, 'path_schema'):
            payload = self.request.match_info
            cleaned = await self._validate_singular_payload(
                payload, schema=self.path_schema, envelope_key='path')
            self.path_payload = cleaned
        return await super()._iter()

    async def validate_header(self):
        try:
            return await self._validate_singular_payload(
                self.header_payload, schema=self.header_schema, envelope_key='header')
        except ValidationError as vexc:
            raise post_exceptions.ValidationError(vexc.messages)

    async def validate_payload(self):
        try:
            return await self._validate_singular_payload()
        except ValidationError as vexc:
            raise post_exceptions.ValidationError(vexc.messages)

    async def validate_query(self):
        try:
            return await self._validate_singular_payload(
                self.query_payload, schema=self.query_schema, envelope_key='query')
        except ValidationError as vexc:
            raise post_exceptions.ValidationError(vexc.messages)

    @async_cached_property
    async def payload(self):
        '''Refers to JSON payload transmitted in body'''
        try:
            return await self.request.json(loads=ujson.loads)
        except Exception:
            raise web.HTTPBadRequest(reason='cannot read payload')

    @property
    @functools.lru_cache()
    def header_payload(self):
        headers_raw = self.request.headers
        headers = dict(headers_raw)
        for fieldname in self._iterable_fields:
            if fieldname in headers:
                unified_order_field = headers_raw.getall(fieldname)
                if ',' in unified_order_field[0]:
                    unified_order_field = unified_order_field[0].split(',')
                headers[fieldname] = unified_order_field
        return headers

    @property
    @functools.lru_cache()
    def query_payload(self):
        get_query_raw = self.request.query
        get_query = dict(get_query_raw)
        for fieldname in self._iterable_fields:
            if fieldname in get_query:
                unified_order_field = get_query_raw.getall(fieldname)
                if ',' in unified_order_field[0]:
                    unified_order_field = unified_order_field[0].split(',')
                get_query[fieldname] = unified_order_field
        return get_query

    @property
    def method(self):
        return getattr(self, '_method', self.request.method).lower()

    @property
    def schema(self):
        try:
            return self.body_schema
        except AttributeError:
            return None


class ViewsClassBase(web.View):

    def __init__(self, request):
        self._request = request
        self._orig_cleaned_payload = {}
        self._tables_to_join = None

    @classmethod
    def relationize_schema(cls, joins):
        schema = cls.schema_cls

        new_methods = {
            fieldname: fields.Nested(
                linked_schema, validate=must_not_be_empty)
            for fieldname, (linked_schema, _) in joins.items()
        }

        for fieldname in joins:
            _, postload = adjust_children_field(fieldname)
            new_methods[f'post_load_{fieldname}'] = post_load(postload)

        if new_methods:
            return retype_schema(schema, new_methods)

    @classmethod
    def post_init(cls, joins):
        cls.schemas = import_module('postschema.schema')._schemas
        table = cls.model.__table__
        declared_fields = cls.schema_cls._declared_fields.items()

        cls.iterable_fields = [field for field, fieldval in declared_fields
                               if isinstance(fieldval, ITERABLE_FIELDS)
                               and not isinstance(fieldval, Relationship)]

        extends_on = getattr(cls.schema_cls.Meta, 'extends_on', None)
        mergeable_fields = cls.iterable_fields[:]
        if extends_on:
            mergeable_fields.append(extends_on)
        cls.mergeable_fields = mergeable_fields

        cls.pk_col = table.primary_key.columns_autoinc_first[0]
        cls.pk_column_name = cls.pk_col.name
        cls.pk_autoicr = isinstance(cls.pk_col.default, Sequence)

        schema_metacls = getattr(cls.schema_cls, 'Meta', object)

        if cls.schema_cls.is_kid:
            cls._naive_fields_to_composite_stmts()
        cls.schema_cls._join_to_schema_where_stmt = joins

        try:
            selects_nested_map = cls.schema_cls._nested_select_stmts
        except AttributeError:
            selects_nested_map = {}

        get_by = getattr(schema_metacls, 'get_by', None) or [cls.pk_column_name]
        get_by_select = {field: selects_nested_map.get(field, field) for field in get_by}
        list_by = getattr(schema_metacls, 'list_by', get_by) or get_by
        list_by_select = {field: selects_nested_map.get(field, field) for field in list_by}
        delete_by = getattr(schema_metacls, 'delete_by', None) or [cls.pk_column_name]
        read_only_fields = [field for field, fieldval in declared_fields
                            if fieldval.metadata.get('read_only', False)]

        pagination_schema_raw = getattr(schema_metacls, 'pagination_schema', Pagination)

        cls.pagination_schema = adjust_pagination_schema(pagination_schema_raw,
                                                         cls.schema_cls, list_by.copy(),
                                                         cls.pk_column_name)()
        cls.select_schema = make_select_fields_schema(cls.schema_cls)()

        excluded = getattr(schema_metacls, 'exclude_from_updates', [])
        update_excluded = [*excluded, *read_only_fields]

        cls.insert_query_stmt = cls._prepare_insert_query()
        cls.get_query_stmt = cls._prepare_get_query(get_by_select)
        cls.list_query_stmt = cls._prepare_list_query(list_by_select)
        cls.update_query_stmt = FallbackString(f"""
            WITH rows AS (
                UPDATE "{cls.schema_cls.__tablename__}"
                SET {{updates}}
                WHERE {{where}}
                RETURNING 1
            )
            SELECT count(*) FROM rows""")
        cls.delete_query_stmt = FallbackString(f"""
            WITH rows AS (
                DELETE FROM "{cls.schema_cls.__tablename__}"
                WHERE {{where}}
                RETURNING 1
            )
            SELECT count(*) FROM rows""")

        # render delete statements for linked tables, in case of deep delete request
        cls.delete_deep_query_stmt = FallbackString(f"""
            WITH rows AS (
                DELETE FROM "{cls.schema_cls.__tablename__}"
                WHERE {{where}}
                RETURNING {cls.pk_column_name}::text
            )
            SELECT json_agg(rows.{cls.pk_column_name}) FROM rows;
        """)
        # cls.deep_delete_assoc_stmt = cls._render_associated_delete_stmts()
        cls.cherrypick_m2m_stmts = cls._render_cherrypick_m2m_stmts()

        get_joins, list_joins = cls._prepare_join_statements(joins, get_by, list_by)

        cls.post_schema = cls.schema_cls(use='write', exclude=read_only_fields)
        cls.patch_schema = cls.put_schema = cls.schema_cls(use='write', partial=True, exclude=update_excluded)

        read_schema = cls.relationize_schema(joins) or cls.schema_cls
        cls.get_schema = read_schema(use='read', joins=get_joins, only=get_by, partial=True)
        cls.list_schema = read_schema(use='read', joins=list_joins, only=list_by, partial=True)
        cls.delete_schema = read_schema(use='read', joins=get_joins, partial=True, only=delete_by)

    @classmethod
    def _naive_fields_to_composite_stmts(cls):
        schema = cls.schema_cls
        parent = schema.__base__
        # tablename = parent.__tablename__
        extends_on = getattr(schema.Meta, 'extends_on', None)

        nested_fields_to_json_query = {}
        nested_fields_to_select = {}
        nested_inst = parent._declared_fields[extends_on].nested

        for aname, aval in nested_inst._declared_fields.items():
            if aname not in schema.child_fieldnames:
                continue
            attr_name = aval.attribute or aname
            if isinstance(aval, fields.Number):
                frmt = f'=%({attr_name})s::jsonb'
            elif isinstance(aval, fields.List):
                frmt = f' @> to_jsonb(%({attr_name})s)'
            else:
                frmt = f' ? %({attr_name})s'
            nested_fields_to_json_query[aname] = f"{extends_on}->'{aname}'{frmt}"
            nested_fields_to_select[aname] = f"{extends_on}->'{aname}'"
        schema._nested_where_stmts = nested_fields_to_json_query
        schema._nested_select_stmts = nested_fields_to_select

    @classmethod
    def _prepare_join_statements(cls, joins, get_by, list_by):
        schema = cls.schema_cls
        tablename = schema.__tablename__
        get_joins = {}
        list_joins = {}
        for fieldname, (linked_schema, _) in joins.items():
            linked_schema_tablename = linked_schema.__tablename__
            linked_schema_pk = linked_schema._model.__table__.primary_key.columns_autoinc_first[0].name
            linked = f'"{linked_schema_tablename}".{linked_schema_pk}'
            join_stmt = f'LEFT JOIN "{linked_schema_tablename}" ON "{tablename}".{fieldname}={linked}'
            if fieldname in get_by:
                get_joins[fieldname] = join_stmt
            if fieldname in list_by:
                list_joins[fieldname] = join_stmt
        return get_joins, list_joins

    @classmethod
    def _prepare_selects(cls, include_dict, schema=None):
        schema = schema or cls.schema_cls
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
        field_to_table = {
            field: li[0]._model.__table__.name
            for field, li in cls.schema_cls._join_to_schema_where_stmt.items()
        }

        def _join_selects(select_dict, tablename):
            return ','.join(
                f"'{k}', \"{tablename}\".{v}"
                if not isinstance(v, dict)
                else f"'{k}', json_build_object({_join_selects(v, field_to_table[k])})"
                for k, v in select_dict.items()
            )
        tablename = cls.schema_cls.__tablename__
        tablename_cte = f'{tablename}_cte'

        joined_fields = dd(dict)
        for getter_field in list_by.copy():
            joins_to_schemas = cls.schema_cls._join_to_schema_where_stmt
            if getter_field in joins_to_schemas:
                linked_schema = joins_to_schemas[getter_field][0]
                popped_field = list_by.pop(getter_field, None)
                if not popped_field:
                    continue

                table = linked_schema._model.__table__
                pk_column_name = table.primary_key.columns_autoinc_first[0].name
                schema_metacls = getattr(linked_schema, 'Meta', object)

                try:
                    selects_nested_map = linked_schema._nested_select_stmts
                except AttributeError:
                    selects_nested_map = {}

                linked_list_by = getattr(schema_metacls, 'list_by', None) or [pk_column_name]
                linked_list_by_select = {
                    field: selects_nested_map.get(field, field)
                    for field in linked_list_by
                }
                linked_selects = cls._prepare_selects(linked_list_by_select, linked_schema) \
                    if compile_selects else linked_list_by_select
                joined_fields[getter_field].update(linked_selects)

        main_selects = cls._prepare_selects(list_by) if compile_selects else list_by
        main_selects.update(joined_fields)

        select_stmt = _join_selects(main_selects, tablename)

        # selects = cls._prepare_selects(list_by) if compile_selects else list_by
        # select = ','.join(f"'{k}',{tablename}.{v}" for k, v in selects.items())
        return FallbackString(f'''WITH "{tablename_cte}" AS (
                SELECT json_build_object({select_stmt}) AS js,
                       count(*) OVER() AS full_count
                       FROM "{tablename}" {{joins}}
                       WHERE {{where}}
                       ORDER BY {{orderby}} {{orderhow}}
            )
            SELECT json_build_object('data', json_agg(js), 'total_count', t.ct) FROM (
                SELECT js, {tablename_cte}.full_count as ct FROM "{tablename_cte}"
                LIMIT {{limit}}
                OFFSET {{offset}}
            ) t
            GROUP BY t.ct
        ''')

    @classmethod
    def _prepare_get_query(cls, get_by, compile_selects=False):
        field_to_table = {
            field: li[0]._model.__table__.name
            for field, li in cls.schema_cls._join_to_schema_where_stmt.items()
        }

        def _join_selects(select_dict, tablename):
            return ','.join(
                f"'{k}', \"{tablename}\".{v}"
                if not isinstance(v, dict)
                else f"'{k}', json_build_object({_join_selects(v, field_to_table[k])})"
                for k, v in select_dict.items()
            )

        joined_fields = dd(dict)
        for getter_field in get_by.copy():
            joins_to_schemas = cls.schema_cls._join_to_schema_where_stmt
            if getter_field in joins_to_schemas:
                linked_schema = joins_to_schemas[getter_field][0]
                popped_field = get_by.pop(getter_field, None)
                if not popped_field:
                    continue

                table = linked_schema._model.__table__
                pk_column_name = table.primary_key.columns_autoinc_first[0].name
                schema_metacls = getattr(linked_schema, 'Meta', object)

                try:
                    selects_nested_map = linked_schema._nested_select_stmts
                except AttributeError:
                    selects_nested_map = {}

                linked_get_by = getattr(schema_metacls, 'get_by', None) or [pk_column_name]
                linked_get_by_select = {
                    field: selects_nested_map.get(field, field)
                    for field in linked_get_by
                }
                linked_selects = cls._prepare_selects(linked_get_by_select, linked_schema) \
                    if compile_selects else linked_get_by_select
                joined_fields[getter_field].update(linked_selects)

        tablename = cls.schema_cls.__tablename__
        main_selects = cls._prepare_selects(get_by) if compile_selects else get_by
        main_selects.update(joined_fields)

        select_stmt = _join_selects(main_selects, tablename)
        return f'''SELECT json_build_object({select_stmt}) AS "inner"
               FROM "{tablename}" {{joins}} WHERE {{where}}'''

    @classmethod
    def _prepare_insert_query(cls):
        insert_cols = [
            key for key, val in cls.model.__table__.columns.items()
            if key != cls.pk_column_name and val.primary_key
        ]

        values = [f"%({colname})s" for colname in insert_cols]

        # ensure the pk ends up as a last col
        insert_cols.append(cls.pk_column_name)
        colnames = ','.join(insert_cols)
        if cls.pk_autoicr:
            values.append(f"NEXTVAL('{cls.pk_col.default.name}')")
        else:
            values.append(f'%({cls.pk_column_name})')
        valnames = ','.join(values)

        return f"""INSERT INTO "{cls.tablename}" ({colnames}{{cols}})
            VALUES ( {valnames}{{vals}} )
            RETURNING {cls.pk_column_name}"""

    @classmethod
    def _render_associated_delete_stmts(cls):
        this_tablename = cls.schema_cls.__tablename__
        this_table_pk = cls.pk_column_name
        stmt = '\n'.join(
            FallbackString(f"""WITH rows AS (
                    DELETE FROM "{foreign_table}"
                    USING "{this_tablename}"
                    WHERE "{foreign_table}"."{foreign_pk}"="{this_tablename}"."{linking_field}"
                    AND "{this_tablename}".{this_table_pk} = ANY({{deleted_ids}})
                )""")
            for foreign_pk, foreign_table, linking_field in cls.schema_cls._deletion_cascade
        ) or ''
        return stmt

    @classmethod
    def _render_cherrypick_m2m_stmts(cls):
        query = ',\n'.join(FallbackString(f"""{foreign_table}_cte AS (
            UPDATE "{foreign_table}"
                SET "{foreign_field}" = (SELECT (SELECT jsonb_agg(t.e) FROM (
                    SELECT jsonb_array_elements_text("{foreign_field}") AS e
                ) t))-{{deleted_pks}}
                FROM (
                    SELECT DISTINCT("inner".id) AS id FROM (
                        SELECT {foreign_pk}, jsonb_array_elements_text("{foreign_field}") AS j
                        FROM "{foreign_table}"
                    ) "inner"
                    WHERE "inner".j = ANY({{deleted_pks}})
                ) "outer"
                WHERE "{foreign_table}".{foreign_pk} = "outer".id
                RETURNING 1
            ),
            {foreign_table}_cte_summed AS (
                SELECT count(*) AS out FROM {foreign_table}_cte
            )""") for foreign_table, foreign_field, foreign_pk in cls.schema_cls._m2m_cherrypicks) or ''
        if query:
            summary_core = ','.join(f"""'{fktable}', "{fktable}_cte_summed".out """
                                    for fktable, *_ in cls.schema_cls._m2m_cherrypicks)
            summary = f'json_build_object({summary_core})'
            froms = ','.join(f'"{fktable}_cte_summed"' for fktable, *_ in cls.schema_cls._m2m_cherrypicks)
            query = 'WITH ' + query + f'\nSELECT {summary} FROM {froms}'
        return query


class ViewsBase(ViewsClassBase, CommonViewMixin):

    @async_cached_property
    async def payload(self):
        try:
            return await self.request.json(loads=ujson.loads)
        except Exception:
            raise web.HTTPBadRequest(reason='cannot read payload')

    @property
    def method(self):
        return getattr(self, '_method', self.request.method).lower()

    @property
    def request(self):
        return self._request

    @property
    def schema(self):
        return getattr(self, f'{self.method}_schema')

    @property
    def tables_to_join(self):
        if self._tables_to_join is None:
            return self.schema._default_joinable_tables or []
        return self._tables_to_join

    @property
    def translated_payload(self):
        nested_map = self.schema._nested_select_stmts
        for k, v in self._orig_cleaned_payload.items():
            self._orig_cleaned_payload[k] = nested_map.get(k, k)
        return self._orig_cleaned_payload

    async def _clean_update_payload(self):
        ''''
        Common method for validating the payload for `PUT` and `PATCH` methods
        '''
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
                    print("!!", cur.query.decode())
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
            # TODO: allow for dot-separated fields to indicate linked tables' fields to be included
            self._tables_to_join = set(list(select_with) + self.cleaned_payload_keys) & self.schema._joinable_fields # noqa
            del get_query['select']
            return query_maker(select_with, compile_selects=False)

    def _render_insert_query(self, payload):
        vals = ','.join(f"%({colname})s" for colname in payload)
        cols = ','.join(payload)
        if vals:
            vals = ',' + vals
            cols = ',' + cols
        return self.insert_query_stmt.format(cols=cols, vals=vals)

    def _whereize_query(self, cleaned_payload, query): # noqa
        try:
            json_where_stmts = self.schema._nested_where_stmts
            child_fieldnames = self.schema.child_fieldnames
        except AttributeError:
            # only inherited resources will have it
            json_where_stmts = []
            child_fieldnames = []

        tablename = self.schema.__tablename__
        joins = []
        wheres = deque()
        values = {}

        for nested_field in child_fieldnames:
            nested_in_payload = cleaned_payload.pop(nested_field, None)
            if nested_in_payload:
                values.update({nested_field: nested_in_payload})
                wheres.append(json_where_stmts[nested_field])
        for m2m_field, m2m_field_translated in self.schema._m2m_where_stmts.items():
            relation_in_payload = cleaned_payload.pop(m2m_field, None)
            if relation_in_payload:
                values.update({m2m_field: relation_in_payload})
                wheres.append(m2m_field_translated)

        for fk_field, (linked_schema, where_stmt) in self.schema._join_to_schema_where_stmt.items():
            if fk_field in self.tables_to_join:
                joins.append(self.schema._joins[fk_field])
            fk_in_payload = cleaned_payload.pop(fk_field, None)
            if fk_in_payload:
                for key, val in fk_in_payload.items():
                    trans_key = f'{fk_field}_{key}'
                    values.update({trans_key: val})
                    wheres.append(where_stmt.format(subkey=key, fill=trans_key))

        for key in cleaned_payload.copy():
            wheres.appendleft(f'"{tablename}".{key}=%(w_{key})s')

        values.update({f'w_{k}': v for k, v in cleaned_payload.items()})

        wheres_q = ' AND '.join(wheres) or ' 1=1 '
        joins = ' '.join(joins)

        return query.format(where=wheres_q, joins=joins), values


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


class ViewsTemplate:
    async def get(self):
        # validate the query payload
        cleaned_payload = await self._validate_singular_payload()
        self.cleaned_payload_keys = list(cleaned_payload) or []

        get_query = dict(self.request.query)
        base_stmt = await self._parse_select_fields(
            get_query, self._prepare_get_query) or self.get_query_stmt

        if hasattr(self.schema, 'get'):
            return await self.schema.get(self.request, cleaned_payload)

        if hasattr(self.schema, 'before_get'):
            cleaned_payload = await self.schema.before_get(self.request, cleaned_payload) or cleaned_payload

        return await self._fetch(cleaned_payload, base_stmt)

    async def list(self):
        # validate the query payload
        raise_orig = self.schema.is_kid
        try:
            cleaned_payload = await self._validate_singular_payload(raise_orig=raise_orig)
        except ValidationError as vexc:
            # Take out only those keys from the error bag that belong to the nested schema
            # (aka the kid) and rerun the validation only on them using that nested schema
            payload = vexc.valid_data
            nested_fieldnames = self.schema.child_fieldnames
            for fieldname in vexc.messages:
                if fieldname in nested_fieldnames:
                    payload[fieldname] = vexc.data[fieldname]
            if not payload:
                raise post_exceptions.ValidationError(vexc.messages)
            parent_nested_schema = self.schema.parent._declared_fields[self.schema.extends_on].nested
            cleaned_payload = await self._validate_singular_payload(
                payload=payload, schema=parent_nested_schema)

        self.cleaned_payload_keys = list(cleaned_payload) or []

        # validate the GET payload, if present
        get_query_raw = self.request.query
        get_query = dict(get_query_raw)

        if 'order_by' in get_query:
            unified_order_field = get_query_raw.getall('order_by')
            if ',' in unified_order_field[0]:
                unified_order_field = unified_order_field[0].split(',')
            get_query['order_by'] = unified_order_field

        base_stmt = await self._parse_select_fields(
            get_query, self._prepare_list_query) or self.list_query_stmt

        pagination_data = await self._validate_singular_payload(
            get_query or {}, self.pagination_schema, 'query')
        limit = pagination_data['limit']
        page = pagination_data['page'] - 1
        offset = page * limit
        orderby = ','.join(f'{self.tablename}.{field}' for field in pagination_data['order_by'])
        orderhow = pagination_data['order_dir'].upper()

        if hasattr(self.schema, 'before_list'):
            cleaned_payload = await self.schema.before_list(self.request, cleaned_payload) or cleaned_payload
            print(cleaned_payload)

        query = base_stmt.format(
            limit=limit,
            offset=offset,
            orderby=orderby,
            orderhow=orderhow)

        return await self._fetch(cleaned_payload, query)

    async def post(self):
        # validate the payload
        payload = await self._validate_singular_payload()
        cleaned_payload = self._clean_write_payload(payload)

        if hasattr(self.schema, 'before_post'):
            cleaned_payload = await self.schema.before_post(self.request, cleaned_payload) or cleaned_payload

        insert_query = self._render_insert_query(cleaned_payload)

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(insert_query, cleaned_payload)
                except postgres_errors.IntegrityError as ierr:
                    raise post_exceptions.ValidationError(parse_postgres_err(ierr))
                except Exception as exc:
                    print(f'!! Failed adding to {self.tablename} resource', flush=1)
                    print(cur.query.decode(), flush=1)
                    # TODO: sentry
                    raise exc
                res = await cur.fetchone()

        if hasattr(self.schema, 'after_post'):
            await spawn(self.request, self.schema.after_post(self.request, cleaned_payload, res[0]))

        return json_response({self.pk_column_name: res[0]})

    async def put(self):
        cleaned_select, payload = await self._clean_update_payload()
        cleaned_payload = self._clean_write_payload(payload)

        if hasattr(self.schema, 'before_update'):
            cleaned_payload = await self.schema.before_update(self.request, cleaned_payload) \
                or cleaned_payload

        query_raw = self.update_query_stmt
        query_with_where, query_values = self._whereize_query(cleaned_select, query_raw)
        updates = []

        for payload_k, payload_v in cleaned_payload.items():
            updates.append(f"{payload_k}=%({payload_k})s")
            query_values[payload_k] = payload_v

        query = query_with_where.format(updates=','.join(updates))

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, query_values)
                    print(cur.query.decode())
                except postgres_errors.IntegrityError as ierr:
                    raise post_exceptions.ValidationError({"payload": parse_postgres_err(ierr)})
                except Exception as exc:
                    print(f'!! Failed updating the {self.tablename} resource', flush=1)
                    print(cur.query.decode())
                    # TODO: sentry
                    raise exc
                res = await cur.fetchone()
                if not res or not res[0]:
                    raise post_exceptions.UpdateFailed()

        if hasattr(self.schema, 'after_put'):
            await spawn(self.request,
                        self.schema.after_put(self.request, cleaned_select, cleaned_payload, res))

        return json_response({'updated': res[0]})

    async def patch(self): # noqa
        cleaned_select, payload = await self._clean_update_payload()
        cleaned_payload = self._clean_write_payload(payload)

        if hasattr(self.schema, 'patch'):
            return await self.schema.patch(self.request, cleaned_select, cleaned_payload)

        if hasattr(self.schema, 'before_update'):
            cleaned_payload = await self.schema.before_update(self.request, cleaned_payload) \
                or cleaned_payload

        query_raw = self.update_query_stmt
        query_with_where, query_values = self._whereize_query(cleaned_select, query_raw)
        updates = []

        for payload_k, payload_v in cleaned_payload.items():
            if payload_k in self.mergeable_fields:
                updates.append(f"{payload_k}=jsonb_merge_deep({payload_k}, %({payload_k})s)")
            else:
                updates.append(f"{payload_k}=%({payload_k})s")
            query_values[payload_k] = payload_v

        query = query_with_where.format(updates=','.join(updates))

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, query_values)
                except postgres_errors.IntegrityError as ierr:
                    raise post_exceptions.ValidationError({"payload": parse_postgres_err(ierr)})
                except Exception as exc:
                    print(f'!! Failed updating the {self.tablename} resource', flush=1)
                    print(cur.query.decode())
                    # TODO: sentry
                    raise exc
                res = await cur.fetchone()
                if not res or not res[0]:
                    print(cur.query.decode())
                    raise post_exceptions.UpdateFailed()

        if hasattr(self.schema, 'after_patch'):
            await spawn(self.request,
                        self.schema.after_patch(self.request, cleaned_select, cleaned_payload, res))

        return json_response({'updated': res[0]})

    async def delete(self): # noqa
        cleaned_payload = await self._validate_singular_payload()

        # validate the GET payload, if present
        get_query_raw = self.request.query
        get_query = dict(get_query_raw)
        # for now, we only support 'deep' param, which denotes that only M2M references of the given model
        # are to be deleted together with the parent.
        # TODO: specify which chidren
        deep_delete = get_query.get('deep', False) or False

        if not cleaned_payload:
            raise post_exceptions.ValidationError({
                '_schema': [
                    "Payload cannot be empty"
                ]
            })

        if hasattr(self.schema, 'delete'):
            return await self.schema.delete(self.request, cleaned_payload)

        if hasattr(self.schema, 'before_delete'):
            cleaned_payload = await self.schema.before_delete(self.request, cleaned_payload) \
                or cleaned_payload

        delete_query = self.delete_query_stmt
        if deep_delete or self.schema._m2m_cherrypicks:
            delete_query = self.delete_deep_query_stmt

        query, query_values = self._whereize_query(cleaned_payload, delete_query)

        deleted_resource_instances = 0
        deleted_m2m_refs = 0

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                async with cur.begin():
                    try:
                        await cur.execute(query, query_values)
                        print(cur.query.decode())
                    except Exception as exc:
                        print(f'!! Failed to delete from the {self.tablename} resource', flush=1)
                        print(cur.query.decode())
                        # TODO: sentry
                        raise exc

                    # fetch the query result under the same transaction, before commiting
                    res = await cur.fetchone()
                    if not res or not res[0]:
                        raise post_exceptions.DeleteFailed()
                    deleted_ids = res[0]

                    try:
                        deleted_resource_instances = len(deleted_ids)
                    except TypeError:
                        deleted_resource_instances = deleted_ids

                    # post-delete hooks, only for the m2m relations
                    if self.schema._m2m_cherrypicks:
                        m2m_query = self.cherrypick_m2m_stmts.format(deleted_pks=f'array{deleted_ids}')
                        print(m2m_query)
                        try:
                            await cur.execute(m2m_query)
                        except Exception as exc:
                            print(f"!! Failed to delete the resource's M2M dependencies", exc, flush=1)
                            await cur.execute('rollback;')
                            raise post_exceptions.DeleteFailed()

                        res = await cur.fetchone()
                        if not res or not res[0]:
                            await cur.execute('rollback;')
                            print("FAIL!")
                            raise post_exceptions.DeleteFailed(body="Failed to delete the M2M dependencies")
                        deleted_m2m_refs = res[0]
                    # await cur.execute('rollback;')

        if hasattr(self.schema, 'after_delete'):
            await spawn(self.request, self.schema.after_delete(self.request, res))

        return json_response({
            'deleted_resource_records': deleted_resource_instances,
            'deleted_m2m_refs': deleted_m2m_refs
        })