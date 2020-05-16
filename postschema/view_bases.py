import warnings
import weakref

import orjson
from aiohttp import web
from cached_property import cached_property
from collections import deque, defaultdict as dd
from contextlib import suppress
from functools import lru_cache
from importlib import import_module

from marshmallow import Schema, ValidationError, fields, validate, post_load
from sqlalchemy.sql.schema import Sequence

from . import exceptions as post_exceptions
from .auth.clauses import SessionContext
from .commons import MANDATORY_PAGINATION_FIELDS
from .fields import (
    Set, Relationship, AutoImpliedForeignResource,
    AutoSessionField, AutoSessionForeignResource,
    RangeDTField, TimeRange
)
from .exceptions import WrongType
from .hooks import translate_naive_nested, translate_naive_nested_to_dict
from .schema import DefaultMetaBase
from .utils import json_response, retype_schema
from .validators import must_not_be_empty, adjust_children_field

NESTABLE_FIELDS = (fields.Dict, fields.Nested, Set)
ITERABLE_FIELDS = (Set, fields.List)
NON_ITERABLE_FIELDS = (Relationship, TimeRange, RangeDTField)


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

    # Ensure that `order_by` doesn't include any nestable fields.
    for fname, fval in schema_cls._declared_fields.items():
        if isinstance(fval, NESTABLE_FIELDS):
            with suppress(ValueError):
                list_by_fields.remove(fname)

    # Upon completing the loading, translate the nested fields (if present) to their composite form
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
        declared_fields = dict(schema_cls._declared_fields)
        for fieldname, fieldval in declared_fields.items():
            if fieldval.dump_only:
                continue
            yield fieldname

    allowed_fields = set(_get_all_selectable_fields())
    nested_map = {}

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


def make_write_schema(schema_cls):
    '''Get rid of all extended fields and their copies in get_by and list_by selectors'''
    new_schema_cls = retype_schema(schema_cls, {})
    for field in new_schema_cls._declared_fields.copy():
        if '__' in field:
            new_schema_cls._declared_fields.pop(field)
    pub_get_by = [get_by for get_by in getattr(new_schema_cls.Public, 'get_by', []) if '__' not in get_by]
    pub_list_by = [list_by for list_by in getattr(new_schema_cls.Public, 'list_by', []) if '__' not in list_by]
    if pub_get_by:
        new_schema_cls.Public.get_by = pub_get_by
    if pub_list_by:
        new_schema_cls.Public.list_by = pub_list_by

    if hasattr(new_schema_cls, 'Authed'):
        authed_get_by = [get_by for get_by in getattr(new_schema_cls.Authed, 'get_by', []) if '__' not in get_by]
        authed_list_by = [list_by for list_by in getattr(new_schema_cls.Authed, 'list_by', []) if '__' not in list_by]
        if authed_get_by:
            new_schema_cls.Authed.get_by = authed_get_by
        if authed_list_by:
            new_schema_cls.Authed.list_by = authed_list_by

    if hasattr(new_schema_cls, 'Private'):
        private_get_by = [get_by for get_by in getattr(new_schema_cls.Private, 'get_by', []) if '__' not in get_by]
        private_list_by = [list_by for list_by in getattr(new_schema_cls.Private, 'list_by', []) if '__' not in list_by]
        if private_get_by:
            new_schema_cls.Private.get_by = private_get_by
        if private_list_by:
            new_schema_cls.Private.list_by = private_list_by
    return new_schema_cls


class CommonViewMixin:

    @classmethod
    async def log_request(cls, req, resp):
        logging_cls = getattr(cls.schema_cls, 'AccessLogging', None)
        if not logging_cls:
            return

        is_authed = req.session.needs_session
        if not is_authed:
            pub_logging_conf = getattr(logging_cls, 'public', None)
            if not pub_logging_conf or req.operation not in pub_logging_conf:
                return
        else:
            auth_logging_conf = getattr(logging_cls, 'authed', None)
            if not auth_logging_conf or req.operation not in auth_logging_conf:
                return

        status = resp.status
        if status >= 400:
            method = 'error'
            txt_resp = str(resp.reason)
        else:
            txt_resp = str(resp.text)
            method = 'info'

        txt_resp = str(resp.reason)
        try:
            payload = await req.json()
        except Exception:
            payload = {}
        msg = dict(
            resp=txt_resp[:5000] + '...' if resp.body_length >= 5000 else txt_resp,
            payload=payload,
            **req.app.access_logger._context.pop('msg_context', {})
        )
        await getattr(req.app.access_logger, method)(
            msg,
            status=status,
            path=req.path,
            op=req.operation)

    async def _validate_singular_payload(self, payload=None, schema=None, envelope_key=None,
                                         raise_orig=False):
        ref_schema = schema if schema is not None else self.schema
        payload_used = payload if payload is not None else await self.payload

        if ref_schema == self.schema and self.schema is None:
            warnings.warn("Can't validate payload without body schema")
            return {}

        ref_schema.session = weakref.proxy(self.request.session)
        ref_schema.app = weakref.proxy(self.request.app)

        try:
            autosession_fields = ref_schema._autosession_fields
        except AttributeError:
            autosession_fields = {}

        self.extend_payload_with_session(payload_used, autosession_fields)

        err_msg = None
        try:
            loaded = ref_schema.load(payload_used)
        except ValidationError as merr:
            if raise_orig:
                raise merr
            err_msg = merr.messages
            raise post_exceptions.ValidationError(err_msg if not envelope_key else {envelope_key: err_msg})

        with suppress(AttributeError):
            # ignore validating \w schemas not inheriting from PostSchema
            err_msg = await ref_schema.run_async_validators(payload_used) or err_msg

        if err_msg:
            raise post_exceptions.ValidationError(err_msg if not envelope_key else {envelope_key: err_msg})

        return loaded


class AuxViewMeta(type):
    def __new__(cls, name, bases, methods): # noqa
        if not bases:
            return super(AuxViewMeta, cls).__new__(cls, name, bases, methods)

        # Aux views can't support Private permission class, as it doesn't make sense in this context
        if 'Private' in methods:
            raise AttributeError('Aux views don\'t support Private permission definitions')

        schemas = dd(dict)
        iterable_fields = []
        allowed_locations = ['path', 'query', 'header', 'body', 'form']
        invmap = {}
        for k, v in methods.copy().items():
            if isinstance(v, fields.Field):
                meta = v.metadata
                try:
                    location = meta['location']
                except KeyError:
                    raise KeyError(f'Field `{name}.{k}` need to define `location` attribute')
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

        with suppress(KeyError):
            methods['header_schema'] = type(schemas.pop('header_schema'))(unknown='INCLUDE')

        meta_cls = methods.get('Meta')
        try:
            meta_methods = dict(meta_cls.__dict__)
        except AttributeError:
            meta_methods = {}

        meta_methods.pop('route_base', None)
        new_meta = type('Meta', (DefaultMetaBase, ), meta_methods)
        methods['Meta'] = new_meta

        methods.update({
            '_iterable_fields': iterable_fields,
            **schemas
        })
        return super(AuxViewMeta, cls).__new__(cls, name, bases, methods)


class AuxViewBase(web.View, CommonViewMixin):
    def __init__(self, request):
        self._request = request
        self.path_payload = {}
        try:
            self.request_type = request.session.request_type  # one of: public, authed, private
        except AttributeError:
            # logout case
            self.request_type = 'public'

    async def _iter(self):
        if hasattr(self, 'path_schema'):
            payload = self.request.match_info
            cleaned = await self._validate_singular_payload(
                payload, schema=self.path_schema, envelope_key='path')
            self.path_payload = cleaned

        method = getattr(self, self.request.operation, None)
        if method is None:
            self._raise_allowed_methods()
        return await method()

    async def validate_form(self):
        form_payload = await self.form_payload
        try:
            return await self._validate_singular_payload(
                form_payload, schema=self.form_schema, envelope_key='form')
        except ValidationError as vexc:
            raise post_exceptions.ValidationError(vexc.messages)

    async def validate_header(self):
        try:
            return await self._validate_singular_payload(
                self.header_payload, schema=self.header_schema, envelope_key='header')
        except ValidationError as vexc:
            raise post_exceptions.ValidationError(vexc.messages)

    async def validate_payload(self):
        try:
            body_schema = self.body_schema
        except AttributeError:
            body_schema = None
        try:
            return await self._validate_singular_payload(schema=body_schema)
        except ValidationError as vexc:
            raise post_exceptions.ValidationError(vexc.messages)

    async def validate_query(self):
        try:
            return await self._validate_singular_payload(
                self.query_payload, schema=self.query_schema, envelope_key='query')
        except ValidationError as vexc:
            raise post_exceptions.ValidationError(vexc.messages)

    @cached_property
    async def payload(self):
        '''Refers to JSON payload transmitted in body'''
        try:
            return await self.request.json(loads=orjson.loads)
        except Exception:
            raise web.HTTPBadRequest(reason='cannot read payload')

    @cached_property
    async def form_payload(self):
        return await self.request.post()

    @property
    @lru_cache()
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
    @lru_cache()
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


class ViewsClassBase(web.View):
    def __init__(self, request):
        self._request = request
        self.operation = request.operation
        self._orig_cleaned_payload = {}
        self._tables_to_join = None
        try:
            self.request_type = request.session.request_type  # one of: public, authed, private
        except AttributeError:
            # logout case
            self.request_type = 'public'

    async def _iter(self):
        method = getattr(self, self.request.operation, None)
        if method is None:
            self._raise_allowed_methods()
        return await method()

    @classmethod
    def relationize_schema(cls, joins):
        schema = cls.schema_cls
        new_methods = {
            fieldname: fields.Nested(
                join_obj['linked_schema'], validate=must_not_be_empty)
            for fieldname, join_obj in joins.items()
        }

        for fieldname in joins:
            _, postload = adjust_children_field(fieldname)
            new_methods[f'post_load_{fieldname}'] = post_load(postload)

        if new_methods:
            return retype_schema(schema, new_methods)

    @classmethod
    def post_init(cls, joins):
        from .contrib import Pagination
        cls.has_autopk = False
        cls.schemas = import_module('postschema.schema')._schemas
        table = cls.model.__table__
        declared_fields = cls.schema_cls._declared_fields.items()

        cls.iterable_fields = [field for field, fieldval in declared_fields
                               if isinstance(fieldval, fields.Dict) or (isinstance(fieldval, ITERABLE_FIELDS)
                                                                        and not isinstance(fieldval,
                                                                                           NON_ITERABLE_FIELDS)
                                                                        )]

        autosession_fields = {field: fieldval.session_key for field, fieldval in declared_fields
                              if isinstance(fieldval, AutoSessionField)}

        cls.mergeable_fields = cls.iterable_fields[:]

        cls.pk_col = table.primary_key.columns_autoinc_first[0]
        cls.pk_column_name = pk_name = cls.pk_col.name
        cls.schema_cls.pk_column_name = pk_name
        cls.pk_autoicr = isinstance(cls.pk_col.default, Sequence)

        schema_metacls = getattr(cls.schema_cls, 'Meta', object)

        meta_cls = cls.schema_cls.Meta
        public_meta = cls.schema_cls.Public
        private_meta = getattr(cls.schema_cls, 'Private', None)
        authed_meta = getattr(cls.schema_cls, 'Authed', None)

        cls._naive_fields_to_composite_stmts()
        cls.schema_cls._special_output_processing = cls._find_special_output_fields()
        cls.schema_cls._join_to_schema_where_stmt = joins

        try:
            selects_nested_map = cls.schema_cls._nested_select_stmts
        except AttributeError:
            selects_nested_map = {}

        common_order_by = getattr(meta_cls, 'order_by', None) or [cls.pk_column_name]

        public_get_by = getattr(public_meta, 'get_by', None) or [cls.pk_column_name]
        public_get_by_select = {field: selects_nested_map.get(field, field) for field in public_get_by}
        public_list_by = getattr(public_meta, 'list_by', public_get_by) or public_get_by
        public_list_by_select = {field: selects_nested_map.get(field, field) for field in public_list_by}
        public_delete_by = getattr(public_meta, 'delete_by', None) or [cls.pk_column_name]
        read_only_fields = [field for field, fieldval in declared_fields
                            if fieldval.metadata.get('read_only', False)]

        auth_get_by = getattr(authed_meta, 'get_by', public_get_by)
        auth_get_by_select = {field: selects_nested_map.get(field, field) for field in auth_get_by}
        auth_list_by = getattr(authed_meta, 'list_by', auth_get_by) or auth_get_by
        auth_list_by_select = {field: selects_nested_map.get(field, field) for field in auth_list_by}
        auth_delete_by = getattr(authed_meta, 'delete_by', None) or [cls.pk_column_name]

        private_get_by = getattr(private_meta, 'get_by', auth_get_by)
        private_get_by_select = {field: selects_nested_map.get(field, field) for field in private_get_by}
        private_list_by = getattr(private_meta, 'list_by', auth_list_by)
        private_list_by_select = {field: selects_nested_map.get(field, field)
                                  for field in private_list_by}
        private_delete_by = getattr(private_meta, 'delete_by', auth_delete_by)

        pagination_schema_raw = getattr(schema_metacls, 'pagination_schema', Pagination)

        cls.pagination_schema = adjust_pagination_schema(pagination_schema_raw,
                                                         cls.schema_cls, common_order_by,
                                                         cls.pk_column_name)()
        cls.select_schema = make_select_fields_schema(cls.schema_cls)()

        excluded = getattr(schema_metacls, 'exclude_from_updates', [])
        update_excluded = [*excluded, *read_only_fields]

        cls.insert_query_stmt = insrt = cls._prepare_insert_query()
        cls.schema_cls.insert_query_stmt = insrt

        cls.allowed_selectors_variants = {
            'public': {
                'get_query_stmt': cls._prepare_get_query(public_get_by_select, request_type='public'),
                'list_query_stmt': cls._prepare_list_query(public_list_by_select, request_type='public')
            },
            'authed': {
                'get_query_stmt': cls._prepare_get_query(auth_get_by_select, request_type='authed'),
                'list_query_stmt': cls._prepare_list_query(auth_list_by_select, request_type='authed')
            },
            'private': {
                'get_query_stmt': cls._prepare_get_query(private_get_by_select, request_type='private'),
                'list_query_stmt': cls._prepare_list_query(private_list_by_select, request_type='private')
            }
        }

        cls.update_query_stmt = FallbackString(f"""
            WITH rows AS (
                UPDATE "{cls.schema_cls.__tablename__}"
                SET {{updates}}
                {{froms}}
                WHERE {{where}}
                RETURNING 1
            )
            SELECT count(*) FROM rows""")

        cls.delete_query_stmt = FallbackString(f"""
            WITH rows AS (
                DELETE FROM "{cls.schema_cls.__tablename__}"
                {{using}}
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
        cls.cherrypick_m2m_stmts = cls._render_cherrypick_m2m_stmts()

        public_get_joins, public_list_joins = cls._prepare_join_statements(
            joins, public_get_by, public_list_by)
        auth_get_joins, auth_list_joins = cls._prepare_join_statements(
            joins, auth_get_by, auth_list_by)
        private_get_joins, private_list_joins = cls._prepare_join_statements(
            joins, private_get_by, private_list_by)

        write_schema = make_write_schema(cls.schema_cls)
        cls.post_schema = write_schema(use='write', exclude=read_only_fields,
                                       autosession_fields=autosession_fields)
        cls.patch_schema = cls.put_schema = write_schema(use='write', partial=True, exclude=update_excluded)

        read_schema = cls.relationize_schema(joins) or cls.schema_cls
        cls.schema_variants = {
            'public': {
                'get_schema': read_schema(
                    use='read', joins=public_get_joins, only=public_get_by, partial=True),
                'list_schema': read_schema(
                    use='read', joins=public_list_joins, only=public_list_by, partial=True),
                'delete_schema': read_schema(
                    use='read', joins=public_get_joins, partial=True, only=public_delete_by)
            },
            'authed': {
                'get_schema': read_schema(use='read', joins=auth_get_joins, only=auth_get_by, partial=True),
                'list_schema': read_schema(use='read', joins=auth_list_joins, only=auth_list_by, partial=True),
                'delete_schema': read_schema(
                    use='read', joins=auth_get_joins, partial=True, only=auth_delete_by)
            },
            'private': {
                'get_schema': read_schema(
                    use='read', joins=private_get_joins, only=private_get_by, partial=True),
                'list_schema': read_schema(
                    use='read', joins=private_list_joins, only=private_list_by, partial=True),
                'delete_schema': read_schema(
                    use='read', joins=private_get_joins, partial=True, only=private_delete_by)
            }
        }

    @classmethod
    def _find_special_output_fields(cls):
        schema = cls.schema_cls
        tablename = schema.__tablename__
        out = {}
        for aname, aval in schema._declared_fields.items():
            attr_name = aval.attribute or aname
            if isinstance(aval, RangeDTField):
                out[aname] = f'json_build_array(lower("{tablename}".{attr_name}), upper("{tablename}".{attr_name}))'
        return out

    @classmethod
    def _naive_fields_to_composite_stmts(cls):
        schema = cls.schema_cls
        tablename = schema.__tablename__
        nested_fields_to_json_query = {}
        # nested_fields_to_select = {}

        for aname, aval in schema._declared_fields.items():
            attr_name = aval.attribute or aname
            if isinstance(aval, fields.List) and '__' not in aname:
                frmt = f' @> to_jsonb(%({attr_name})s)'
            # elif isinstance(aval, fields.Dict):
            #     frmt = f' ? %({attr_name})s'
                nested_fields_to_json_query[aname] = f'"{tablename}".{aname}{frmt}'

            # nested_fields_to_json_query[aname] = f"{extends_on}->'{aname}'{frmt}"
            # nested_fields_to_select[aname] = f"{extends_on}->'{aname}'"
        schema._nested_where_stmts = nested_fields_to_json_query
        # schema._nested_select_stmts = nested_fields_to_select

    @classmethod
    def _prepare_join_statements(cls, joins, get_by, list_by):
        schema = cls.schema_cls
        tablename = schema.__tablename__
        get_joins = {}
        list_joins = {}
        for fieldname, join_obj in joins.items():
            linked_schema = join_obj['linked_schema']
            target_table = join_obj['target_table']
            linked_schema_tablename = linked_schema.__tablename__
            target_col = target_table['target_col']
            linked = f'_{fieldname}_j.{target_col}'
            join_stmt = f'LEFT JOIN "{linked_schema_tablename}" _{fieldname}_j ON "{tablename}".{fieldname}={linked}'
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
    def _prepare_list_query(cls, list_by, compile_selects=False, request_type=None):

        metacls_name = request_type.title()

        def _join_selects(select_dict, tablename):
            return ','.join(
                f"'{k}', \"{tablename}\".{v}"
                if not isinstance(v, dict)
                else f"'{k}', json_build_object({_join_selects(v, '_' + k + '_j')})"
                for k, v in select_dict.items()
            )

        tablename = cls.schema_cls.__tablename__
        tablename_cte = f'{tablename}_cte'
        joined_fields = dd(dict)
        extra_fields = {}

        joins_to_schemas = cls.schema_cls._join_to_schema_where_stmt
        special_output_processing = cls.schema_cls._special_output_processing

        for getter_field in list_by.copy():
            if getter_field in joins_to_schemas:
                linked_schema = joins_to_schemas[getter_field]['linked_schema']
                popped_field = list_by.pop(getter_field, None)
                if not popped_field:
                    continue

                table = linked_schema._model.__table__
                pk_column_name = table.primary_key.columns_autoinc_first[0].name
                schema_metacls = getattr(linked_schema, metacls_name, object)

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

            elif getter_field in special_output_processing:
                extra_fields[getter_field] = special_output_processing[getter_field]

        main_selects_with_extended_fields = cls._prepare_selects(list_by) if compile_selects else list_by
        main_selects = {
            field: target for field, target in main_selects_with_extended_fields.items()
            if '__' not in field
        }
        main_selects.update(joined_fields)

        select_stmt = _join_selects(main_selects, tablename)
        if extra_fields:
            select_stmt += ',' + ','.join(f"'{k}',{v}" for k, v in extra_fields.items())

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
    def _prepare_get_query(cls, get_by, compile_selects=False, request_type=None):

        metacls_name = request_type.title()

        def _join_selects(select_dict, tablename):
            return ','.join(
                f"'{k}', \"{tablename}\".{v}"
                if not isinstance(v, dict)
                else f"'{k}', json_build_object({_join_selects(v, '_' + k + '_j')})"
                for k, v in select_dict.items()
            )

        joined_fields = dd(dict)
        joins_to_schemas = cls.schema_cls._join_to_schema_where_stmt
        special_output_processing = cls.schema_cls._special_output_processing
        extra_fields = {}

        for getter_field in get_by.copy():
            if getter_field in joins_to_schemas:
                linked_schema = joins_to_schemas[getter_field]['linked_schema']
                popped_field = get_by.pop(getter_field, None)
                if not popped_field:
                    continue

                table = linked_schema._model.__table__
                pk_column_name = table.primary_key.columns_autoinc_first[0].name
                schema_metacls = getattr(linked_schema, metacls_name, object)
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

            elif getter_field in special_output_processing:
                extra_fields[getter_field] = special_output_processing[getter_field]

        tablename = cls.schema_cls.__tablename__
        main_selects_with_extended_fields = cls._prepare_selects(get_by) if compile_selects else get_by
        main_selects = {
            field: target for field, target in main_selects_with_extended_fields.items()
            if '__' not in field
        }
        main_selects.update(joined_fields)
        select_stmt = _join_selects(main_selects, tablename)
        if extra_fields:
            select_stmt += ',' + ','.join(f"'{k}',{v}" for k, v in extra_fields.items())
        return f'''SELECT json_build_object({select_stmt}) AS "inner"
               FROM "{tablename}" {{joins}} WHERE {{where}}'''

    @classmethod
    def _prepare_insert_query(cls):
        '''Pre-render the INSERT query with fixed values,
        such as sequence-based primary keys and auto-inferred FKs
        '''

        insert_cols = deque(
            key for key, val in cls.model.__table__.columns.items()
            if key != cls.pk_column_name and val.primary_key
        )

        values = deque(f"%({colname})s" for colname in insert_cols)

        is_autopk = isinstance(cls.schema_cls._declared_fields[cls.pk_column_name], AutoSessionField)

        if not is_autopk:
            # ensure the pk ends up as a last col, but only if it's not of AutoSession type
            insert_cols.append(cls.pk_column_name)

        if cls.pk_autoicr:
            values.append(f"NEXTVAL('{cls.pk_col.default.name}')")
        else:
            # if PK isn't set to auto-increment, and it's of AutoSession type,
            # don't include it in a pre-rendered query
            if not is_autopk:
                values.append(f'%({cls.pk_column_name})s')
            else:
                cls.has_autopk = True

        valnames = ','.join(values)

        # handle the AutoImpliedFK columns by first grouping
        # them by the same destination table
        cte_members = {}
        extra_colnames = []
        extra_values = []
        from_ctes = set()
        declared_fields = cls.schema_cls._declared_fields
        cte_autoimplied = dd(list)

        for k, v in declared_fields.items():
            if isinstance(v, AutoSessionForeignResource):
                target_table = v.target_table['name']
                target_schema = cls.registered_schemas._cont(target_table)
                if v.target_column not in target_schema._declared_fields:
                    raise AttributeError(f'{target_schema} doesn\'t define `{v.target_column}` field as referenced by {cls.schema_cls}.{k}')
                if v.session_field not in SessionContext.__annotations__:
                    raise AttributeError(f'`{v.session_field}` field defined on {cls.schema_cls}.{k} is not a valid session context field')

            elif isinstance(v, AutoImpliedForeignResource):
                self_col_name = v.from_column
                if self_col_name not in declared_fields:
                    raise AttributeError(f'`{self_col_name}` is not present on {cls.schema_cls}')

                from_column = declared_fields[self_col_name]
                from_column.required = True
                from_column_target_table = from_column.target_table
                dest_tablename = from_column_target_table['name']
                extra_colnames.append(k)
                extra_values.append(f'"{self_col_name}_cte".{v.foreign_column}')
                from_ctes.add(f'"{self_col_name}_cte"')
                dest_key = f"{dest_tablename}:{from_column_target_table['target_col']}:{self_col_name}"
                cte_autoimplied[dest_key].append(v.foreign_column)

        for dest_key, fk_cols in cte_autoimplied.items():
            dest_tablename, dest_pk, self_col_name = dest_key.split(':')
            dest_cols = ','.join(fk_col for fk_col in fk_cols)
            cte_members[self_col_name] = f'SELECT {dest_cols} FROM "{dest_tablename}" WHERE "{dest_tablename}".{dest_pk}=%({self_col_name})s'

        if cte_members:
            insert_cols.extendleft(extra_colnames)
            values.extendleft(extra_values)
            cte = ',\n'.join(f'{k}_cte AS ({stmt})' for k, stmt in cte_members.items())
            cte = 'WITH ' + cte
            colnames = ','.join(insert_cols)
            valnames = ','.join(values)
            from_stmt = ','.join(from_ctes)

            return (f'{cte}\nINSERT INTO "{cls.tablename}" ({colnames}{{cols}}) '
                    f'SELECT {valnames}{{vals}} FROM {from_stmt} '
                    f'{{on_conflict}} '
                    f'RETURNING {cls.pk_column_name}')

        colnames = ','.join(insert_cols)
        return f"""INSERT INTO "{cls.tablename}" ({colnames}{{cols}})
            VALUES ( {valnames}{{vals}} )
            {{on_conflict}}
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

    @cached_property
    async def payload(self):
        try:
            return await self.request.json(loads=orjson.loads)
        except Exception:
            raise web.HTTPBadRequest(reason='cannot read payload')

    @property
    def get_schema(self):
        return self.schema_variants[self.request_type]['get_schema']

    @property
    def list_schema(self):
        return self.schema_variants[self.request_type]['list_schema']

    @property
    def delete_schema(self):
        return self.schema_variants[self.request_type]['delete_schema']

    @property
    def get_query_stmt(self):
        return self.allowed_selectors_variants[self.request_type]['get_query_stmt']

    @property
    def list_query_stmt(self):
        return self.allowed_selectors_variants[self.request_type]['list_query_stmt']

    @property
    def request(self):
        return self._request

    @property
    def schema(self):
        return getattr(self, f'{self.operation}_schema')

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

    def extend_payload_with_session(self, payload, autosession_fields):
        for declared_field, session_key in autosession_fields.items():
            try:
                payload[declared_field] = self.request.session[session_key]
            except (AttributeError, KeyError):
                # shouldn't happen. Something's wrong with the session
                self.request.app.error_logger.exception('Session not found or corrupted')
                raise web.HTTPUnauthorized(reason='Session not found or corrupted')

    async def _fetch(self, cleaned_payload, query):
        '''Common logic for `get()` and `list()`'''

        try:
            extended_fields = self.schema._extended_fields_values
        except AttributeError:
            extended_fields = {}

        query, values = self._whereize_query(cleaned_payload, query, extended_fields)
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, values)
                except Exception:
                    self.request.app.error_logger.exception('Failed to fetch results',
                                                            query=cur.query.decode())
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
            return query_maker(select_with, compile_selects=False, request_type=self.request_type)

    def _render_insert_query(self, payload, on_conflict=''):
        vals = ','.join(f"%({colname})s" for colname in payload
                        if (colname != self.pk_column_name) or self.has_autopk)
        cols = ','.join(colname for colname in payload
                        if (colname != self.pk_column_name) or self.has_autopk)

        if vals and not self.has_autopk:
            vals = ',' + vals
            cols = ',' + cols
        return self.insert_query_stmt.format(cols=cols, vals=vals, on_conflict=on_conflict,
                                             session=self.request.session)

    def _whereize_query(self, cleaned_payload, query, extended_fields, in_delete=False): # noqa
        in_update = 'UPDATE' in query
        try:
            nested_where_stmts = self.schema._nested_where_stmts
        except AttributeError:
            # only inherited resources will have it
            nested_where_stmts = []

        tablename = self.schema.__tablename__
        joins = []
        usings = []
        froms = []  # for updates only
        values = {}

        wheres = deque()

        # inject authorization condition
        with suppress(KeyError, TypeError):
            wheres.append(self.request.auth_conditions['stmt'])

        for nested_field, nested_trans in nested_where_stmts.items():
            nested_in_payload = cleaned_payload.pop(nested_field, None)
            if nested_in_payload:
                values.update({nested_field: nested_in_payload})
                wheres.append(nested_trans)

        for m2m_field, m2m_field_translated in self.schema._m2m_where_stmts.items():
            relation_in_payload = cleaned_payload.pop(m2m_field, None)
            if relation_in_payload:
                values.update({m2m_field: relation_in_payload})
                wheres.append(m2m_field_translated)

        for fk_field, join_obj in self.schema._join_to_schema_where_stmt.items():
            linked_schema = join_obj['linked_schema']
            if fk_field in self.tables_to_join:
                joins.append(self.schema._joins[fk_field])
                usings.append(fk_field)
            fk_in_payload = cleaned_payload.pop(fk_field, None)
            if fk_in_payload:
                where_stmt = join_obj['unaliased_comp_query'] if in_update or in_delete else join_obj['aliased_comp_query']
                with suppress(AttributeError):
                    # if <schema>.Meta defines a `default_get_critera` function
                    # which in turn returns an expected FK value, we can ignore this
                    for key, val in fk_in_payload.items():
                        trans_key = f'{fk_field}_{key}'
                        values.update({trans_key: val})
                        wheres.append(where_stmt.format(subkey=key, fill=trans_key))
                        if in_delete:
                            wheres.append(f'"{tablename}".{fk_field}={fk_field}.{key}')
                if in_update:
                    linked_tb_name = linked_schema.__tablename__
                    froms.append(linked_tb_name)
                    pk = linked_schema.pk_column_name
                    wheres.appendleft(f'"{linked_tb_name}".{pk}="{tablename}".{fk_field}')

        if not self.request.auth_conditions.get('has_open_clauses', False):
            for key in cleaned_payload.copy():
                if key in extended_fields:
                    ext_field = extended_fields[key]
                    colname = ext_field[0]
                    wheres.append(ext_field[1].format(fieldname=f'w_{colname}'))
                    value = cleaned_payload.pop(key)
                    if isinstance(value, list):
                        values[f'w_{colname}_lower'] = value[0]
                        values[f'w_{colname}_upper'] = value[1]
                    else:
                        values[f'w_{colname}'] = ext_field[2].format(val=value)
                else:
                    values[f'w_{key}'] = cleaned_payload[key]
                    wheres.append(f'"{tablename}".{key}=%(w_{key})s')
        else:
            # 
            values = cleaned_payload

        joins = ' '.join(joins)
        using = ','.join(usings)
        froms = ','.join(froms)
        if using:
            using = f'USING {using}'
        if froms:
            froms = f'FROM "{froms}"'

        wheres_q = ' AND '.join(wheres) or ' 1=1 '
        return query.format(where=wheres_q, joins=joins, using=using, froms=froms), values
