import re

import sqlalchemy as sql
import ujson
from aiohttp.hdrs import METH_ALL
from aiojobs.aiohttp import spawn
from psycopg2 import errors

from marshmallow import fields, ValidationError, missing
from sqlalchemy.ext.declarative import declarative_base

from . import exceptions
from postschema import hooks, exceptions as post_exceptions
from postschema.schema import PostSchema
from postschema.utils import json_response
from postschema.view import ViewsBase

Base = declarative_base()

PG_ERR_PAT = re.compile(r'\((?P<name>.*?)\)\=\((?P<val>.*?)\)(?P<reason>.*)')
METH_ALL = [meth.lower() for meth in METH_ALL]


class DefaultMetaBase:
    excluded_ops = []
    get_by = []
    list_by = []
    exclude_from_updates = []
    create_views = True
    extends_on = None


def retype(cls, new_methods):
    methods = dict(cls.__dict__)
    methods.update(new_methods)
    for k, v in methods.pop('_declared_fields', {}).items():
        methods[k] = v
    return type(cls.__name__, cls.__bases__, methods)


def popattr(cls, attr):
    try:
        delattr(cls, attr)
    except AttributeError:
        pass


def getattrs(cls):
    return {k: v for k, v in cls.__dict__.items() if not k.startswith('__')}


def parse_postgres_err(perr):
    res = PG_ERR_PAT.search(perr.diag.message_detail)
    errs = {}
    if res:
        name, val, reason = res.groups()
        names = name.split(', ')
        vals = val.split(', ')
        reason = reason.strip().capitalize()
        for key, val in zip(names, vals):
            errs[key] = [reason]
    return errs or perr.diag.message_detail


def create_model(schema_cls): # noqa
    if schema_cls.is_kid:
        return
    name = schema_cls.__name__
    methods = dict(schema_cls.__dict__)
    try:
        tablename = methods.get('__tablename__')
        model_methods = {
            '__tablename__': tablename
        }
    except KeyError:
        raise AttributeError(f'{name} needs to define `__tablename__`')

    meta = methods['Meta']
    declared_fields = methods['_declared_fields']

    if hasattr(meta, '__table_args__'):
        model_methods['__table_args__'] = meta.__table_args__

    for field_name, field_attrs in declared_fields.items():
        if isinstance(field_attrs, fields.Field):
            metadata = field_attrs.metadata
            try:
                field_instance = metadata.pop('sqlfield')
            except KeyError:
                # skip fields with no sql bindings
                continue
            except AttributeError:
                raise AttributeError(
                    f'Schema field `{field_name}` needs to define a SQLAlchemy field instance')

            translated = {
                'nullable': field_attrs.allow_none
            }

            default_value = field_attrs.default
            if default_value != missing:
                translated['server_default'] = default_value

            args = []
            if 'fk' in metadata:
                args.append(metadata.pop('fk'))
            if 'autoincrement' in metadata:
                args.append(metadata.pop('autoincrement'))
            model_methods[field_name] = sql.Column(field_instance, *args, **metadata, **translated)

    modelname = name + 'Model'
    new_model = type(modelname, (Base,), model_methods)
    return new_model


class ViewsTemplate:

    async def get(self):
        get_query = dict(self.request.query)
        base_stmt = await self._parse_select_fields(
            get_query, self._prepare_get_query) or self.get_query_stmt

        # validate the query payload
        cleaned_payload = await self._validate_singular_payload()

        if hasattr(self.schema, 'get'):
            return await self.schema.get(self.request, cleaned_payload)

        if hasattr(self.schema, 'before_get'):
            cleaned_payload = self.schema.before_post(cleaned_payload) or cleaned_payload

        return await self._fetch(cleaned_payload, base_stmt)

    async def list(self):
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
        orderby = ','.join(pagination_data['order_by'])
        orderhow = pagination_data['order_dir'].upper()

        # validate the query payload
        raise_orig = self.schema.is_kid
        try:
            cleaned_payload = await self._validate_singular_payload(raise_orig=raise_orig)
        except ValidationError as vexc:
            # Take out only those keys from the error bag that belong to the nested schema
            # (aka the kid) and rerun the validation only on them using that nested schema
            payload = vexc.valid_data
            nested_field_names = self.schema.child_field_names
            for fieldname in vexc.messages:
                if fieldname in nested_field_names:
                    payload[fieldname] = vexc.data[fieldname]
            if not payload:
                raise post_exceptions.ValidationError(vexc.messages)
            parent_nested_schema = self.schema.parent._declared_fields[self.schema.extends_on].nested
            cleaned_payload = await self._validate_singular_payload(
                payload=payload, schema=parent_nested_schema)

        if hasattr(self.schema, 'list'):
            return await self.schema.list(self.request, payload)

        if hasattr(self.schema, 'before_list'):
            cleaned_payload = self.schema.before_list(cleaned_payload) or cleaned_payload

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

        if hasattr(self.schema, 'post'):
            return await self.schema.post(self.request, cleaned_payload)

        if hasattr(self.schema, 'before_post'):
            cleaned_payload = self.schema.before_post(cleaned_payload) or cleaned_payload

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(self.insert_query_stmt, cleaned_payload)
                except errors.UniqueViolation as uerr:
                    raise exceptions.ValidationError(parse_postgres_err(uerr))
                except errors.ForeignKeyViolation as ferr:
                    raise exceptions.ValidationError({
                        "error": "Foreign Key Violation",
                        "details": ferr.diag.message_detail
                    })
                except Exception as exc:
                    print(f'!! Failed adding to {self.tablename} resource', flush=1)
                    print(cur.query.decode(), flush=1)
                    # TODO: sentry
                    raise exc
                res = await cur.fetchone()

        if hasattr(self.schema, 'after_post'):
            spawn(self.request, self.schema.after_post(self.request, res))

        return json_response({self.pk_column_name: res[0]})

    async def put(self):
        cleaned_select, payload = await self._clean_update_payload()
        cleaned_payload = self._clean_write_payload(payload)

        if hasattr(self.schema, 'put'):
            return await self.schema.put(self.request, cleaned_select, cleaned_payload)

        if hasattr(self.schema, 'before_update'):
            cleaned_payload = self.schema.before_update(cleaned_payload) or cleaned_payload

        query_raw = self.update_query_stmt
        query_with_where, query_values = self._whereize_query(cleaned_select, query_raw)
        updates = []

        for payload_k, payload_v in cleaned_payload.items():
            # if isinstance(payload_v, (dict, list)):
            #     updates.append(f"{payload_k}={payload_k} || %({payload_k})s")
            #     query_values[payload_k] = Json(payload_v)
            # else:
            updates.append(f"{payload_k}=%({payload_k})s")
            query_values[payload_k] = payload_v

        # returning = ','.join(f"'{k}',{v}" for k, v in self.translated_payload.items())
        query = query_with_where.format(updates=','.join(updates))

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, query_values)
                    # print(cur.query.decode())
                except errors.UniqueViolation as uerr:
                    raise exceptions.ValidationError({
                        "error": "Unique Record Violation",
                        "details": uerr.diag.message_detail
                    })
                except errors.ForeignKeyViolation as ferr:
                    raise exceptions.ValidationError({
                        "error": "Foreign Key Violation",
                        "details": ferr.diag.message_detail
                    })
                except Exception as exc:
                    print(f'!! Failed updating the {self.tablename} resource', flush=1)
                    print(cur.query.decode())
                    # TODO: sentry
                    raise exc
                res = await cur.fetchone()
                if not res or not res[0]:
                    raise exceptions.UpdateFailed()

        if hasattr(self.schema, 'after_put'):
            spawn(self.request, self.schema.after_put(self.request, res))

        return json_response({'updated': res[0]})

    async def patch(self): # noqa
        cleaned_select, payload = await self._clean_update_payload()
        cleaned_payload = self._clean_write_payload(payload)

        if hasattr(self.schema, 'patch'):
            return await self.schema.patch(self.request, cleaned_select, cleaned_payload)

        if hasattr(self.schema, 'before_update'):
            cleaned_payload = self.schema.before_update(cleaned_payload) or cleaned_payload

        query_raw = self.update_query_stmt
        query_with_where, query_values = self._whereize_query(cleaned_select, query_raw)
        updates = []

        extends_on = getattr(self.schema, 'extends_on', None)

        for payload_k, payload_v in cleaned_payload.items():
            if payload_k == extends_on:
                updates.append(f"{payload_k}=jsonb_merge_deep({payload_k}, %({payload_k})s)")
            else:
                updates.append(f"{payload_k}=%({payload_k})s")
            query_values[payload_k] = payload_v

        query = query_with_where.format(updates=','.join(updates))

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, query_values)
                except errors.UniqueViolation as uerr:
                    raise exceptions.ValidationError({
                        "error": "Unique Record Violation",
                        "details": uerr.diag.message_detail
                    })
                except errors.ForeignKeyViolation as ferr:
                    raise exceptions.ValidationError({
                        "error": "Foreign Key Violation",
                        "details": ferr.diag.message_detail
                    })
                except Exception as exc:
                    print(f'!! Failed updating the {self.tablename} resource', flush=1)
                    print(cur.query.decode())
                    # TODO: sentry
                    raise exc
                res = await cur.fetchone()
                if not res or not res[0]:
                    raise exceptions.UpdateFailed()

        if hasattr(self.schema, 'after_patch'):
            spawn(self.request, self.schema.after_patch(self.request, res))

        return json_response({'updated': res[0]})

    async def delete(self):
        cleaned_payload = await self._validate_singular_payload()

        if hasattr(self.schema, 'delete'):
            return await self.schema.delete(self.request, cleaned_payload)

        if hasattr(self.schema, 'before_delete'):
            cleaned_payload = self.schema.before_delete(cleaned_payload) or cleaned_payload

        query, query_values = self._whereize_query(cleaned_payload, self.delete_query_stmt)

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, query_values)
                except Exception as exc:
                    print(f'!! Failed delete from the {self.tablename} resource', flush=1)
                    print(cur.query.decode())
                    # TODO: sentry
                    raise exc
                res = await cur.fetchone()
                if not res or not res[0]:
                    raise exceptions.DeleteFailed()

        if hasattr(self.schema, 'after_delete'):
            spawn(self.request, self.schema.after_delete(self.request, res))

        return json_response({'deleted': res[0]})


class ViewMaker:
    def __init__(self, schema_cls, router):
        self.schema_cls = schema_cls
        self.router = router
        meta_cls = self.rebase_metacls()
        self.schema_cls.Meta = meta_cls
        self.meta_cls = meta_cls

    @property
    def excluded_ops(self):
        return self.meta_cls.excluded_ops

    def create_views(self):
        async def list_proxy(self):
            self._method = 'list'
            return await self.list()

        async def get_proxy(self):
            self._method = 'get'
            return await self.get()

        # common definitions
        schema_name = self.schema_cls.__name__.title()
        view_methods = {}
        base_methods = {}
        model = self.schema_cls._model

        base_methods['model'] = model
        base_methods['schema_cls'] = self.schema_cls
        base_methods['tablename'] = model.__tablename__
        view_methods.update(base_methods)

        # only use these methods off the ViewsTemplate that make sense for our use case
        for method_name, method in ViewsTemplate.__dict__.items():
            if not method_name.startswith('__'):
                if method_name in self.excluded_ops:
                    continue
                view_methods[method_name] = method

        # create the web.View-derived view
        cls_view = type(f'{schema_name}View', (ViewsBase,), view_methods)
        cls_view.post_init()

        route_base = self.meta_cls.route_base.replace('/', '').lower()
        base_resource_url = f'/{route_base}/'
        self.router.add_route("*", base_resource_url, cls_view)

        # some questionable entities may scrape body payload from attempting GET requests
        class cls_view_get_copy(cls_view):
            pass
        for method in METH_ALL:
            if method == 'get':
                continue
            popattr(cls_view_get_copy, method)
        setattr(cls_view_get_copy, 'post', get_proxy)
        self.router.add_post(base_resource_url + 'get/', cls_view_get_copy)

        if 'list' not in self.excluded_ops:
            class cls_view_list_copy(cls_view_get_copy):
                pass

            popattr(cls_view_list_copy, 'post')
            setattr(cls_view_list_copy, 'get', list_proxy)
            self.router.add_get(base_resource_url + 'list/', cls_view_list_copy)

    @property
    def omit_me(self):
        return not self.meta_cls.create_views

    def make_new_post_view(self, schema_cls):
        return self.__class__.__base__(schema_cls, self.router)

    def rebase_metacls(self):
        """Basically create the new meta type with `DefaultMetaBase` as its base
        to ensure the inclusion of all default methods/attribute    """
        meta_cls = self.schema_cls.Meta
        meta_methods = dict(meta_cls.__dict__)

        # force-set common meta attrs
        meta_methods.setdefault('route_base', self.schema_cls.__name__.lower())
        meta_methods['render_module'] = ujson

        new_meta = type('Meta', (DefaultMetaBase, ), meta_methods)
        self.schema_cls.Meta = new_meta
        return new_meta

    def process_relationships(self):
        joins = {}

        # REWRITE schema_cls so that it includes after_{post/put/patch},
        # shifting the kids keys from root # to `extends_on` key.
        # Then, Json-ning the `extends_on` nest. That should do the trick with the nested relationship.

        # print(self.schema_cls.is_kid, self.schema_cls)
        new_schema_methods = {}
        if self.schema_cls.is_kid:
            # parent = self.schema_cls.__base__
            # create a post-validation hook for all the writing methods
            # print(self.schema_cls.__dict__.keys())
            self.schema_cls._post_validation_write_cleaners.append(
                hooks.clean_before_nested_write(self.schema_cls)
            )

        # for fieldname, fieldval in self.schema_cls._declared_fields.items():
        #     if isinstance(fieldval, postschema_fields.Relationship):
        #         print(fieldname, fieldval.__class__)

        # for aname, aval in methods.copy().items():

        #     # any PostSchema can contain nested fields, so let's aggregate them
        #     # don't count Join field with `many` flag on, as these represent M2M relationships
        #     # if isinstance(aval, fields.Nested) and not aval.many:
        #     #     joins[aname] = {
        #     #         'only': aval.only,
        #     #         'joinee': aval.nested
        #     #     }
        #     if isinstance(aval, postschema_fields.OneToMany):
        #         print(_schemas)
        #         print(aval._deferred_schema_inst)
        #     elif isinstance(aval, postschema_fields.ManyToMany):
        #         # M2M relationship, where the holder of this field will store FKs to its children.
        #         # Create a custom validator specifically for this purpose
        #         children_validator, make_children_post_load = validators.adjust_children_field(aname)
        #         methods[f'validate_{aname}'] = validates(aname)(children_validator)
        #         # on top of that, ensure that ManyToMany's value is wrapped with Json adapter
        #         methods[f'post_load_{aname}'] = post_load(make_children_post_load)

        self.schema_cls = retype(self.schema_cls, new_schema_methods)
        self.schema_cls._joins = joins


class InheritedViewMaker(ViewMaker):
    def __init__(self, *args):
        super().__init__(*args)

    def rebase_metacls(self):
        """On top what its parental version does,
        also perform a deep merge with this schema's parent Meta
        """
        kids_meta = super().rebase_metacls()
        extends_on = getattr(kids_meta, 'extends_on', None)
        assert extends_on, AttributeError(
            f"`{self.schema_cls}`'s Meta class should define `extends_on` field name")

        # FIELD_LISTING_COLS = ['get_by', 'exclude_from_updates', 'list_by']
        kids_meta_methods = dict(kids_meta.__dict__)
        parent = self.schema_cls.__base__
        parent_meta_attrs = getattrs(parent.Meta)
        for meta_name, meta_val in kids_meta_methods.items():
            if isinstance(meta_val, list):
                base_cols = parent_meta_attrs.get(meta_name, [])
                # if meta_name in FIELD_LISTING_COLS:
                #     new_cols = [f'{extends_on}.{attr}' for attr in kids_meta_methods[meta_name]]
                # else:
                new_cols = meta_val
                base_cols.extend(new_cols)
                kids_meta_methods[meta_name] = base_cols
        for parent_metaname, parent_val in parent_meta_attrs.items():
            if parent_metaname not in kids_meta_methods:
                kids_meta_methods[parent_metaname] = parent_val
        kids_meta_methods['create_views'] = True
        self.extends_on = extends_on

        return type('Meta', (kids_meta,), kids_meta_methods)

    def rewrite_inherited(self):
        """Copy appropriate fields from the parent and create the new schema,
        replacing the defining on.
        """
        parent = self.schema_cls.__base__

        extends_on = self.extends_on
        methods = dict(self.schema_cls.__dict__)

        parent_field_names = set(parent._declared_fields.keys())
        # for child_field_name in child_field_names:
        #     del self.schema_cls._declared_fields[child_field_name]
        # child_fields = {k: v for k, v in self if k in child_field_names}
        # nested_methods = dict(self.schema_cls.__dict__)
        # nested_methods['_declared_fields'] = child_fields
        # nested_schema = type(f'{name.title()}{extends_on.title()}', self.schema_cls.__bases__,
        #                       nested_methods)
        # nested_field_opts = {
        #     'validate': [validators.must_not_be_empty]
        # }
        methods = dict(self.schema_cls.__dict__)
        # df = methods.pop('_declared_fields')
        # for kf, vf in df.items():
        #     setattr(self.schema_cls, kf, vf)

        methods.update(methods.pop('_declared_fields'))
        self.schema_cls.child_field_names = {
            k for k, v in self.schema_cls._declared_fields.items()
        } - parent_field_names
        self.schema_cls.__tablename__ = parent.__tablename__
        self.schema_cls._model = parent._model
        self.schema_cls.extends_on = extends_on
        parent._declared_fields[extends_on] = fields.Nested(self.schema_cls(partial=True))

        # return object()
        # parent_fields = parent._declared_fields

        # cls_name = f'{name}{self.extends_on.title()}'
        # nested_methods = dict(self.schema_cls.__dict__)
        # nested_methods['__qualname__'] = cls_name
        # nested_methods['__tablename__'] = parent._model.__tablename__
        # nested_schema_cls = type(cls_name, (Schema,), nested_methods)

        # new_parent_methods = nested_methods.copy()
        # new_parent_methods['__qualname__'] = name

        # # clear child's methods of any pre-existing fields
        # for m, m_val in new_parent_methods.copy().items():
        #     if isinstance(m_val, fields.Field):
        #         del new_parent_methods[m]

        # # merge parental fields
        # new_parent_methods.update({k: v for k, v in parent_fields.items()})
        # extends_on_attrs['validate'] = [validators.must_not_be_empty]
        # new_parent_methods[self.extends_on] = fields.Nested(nested_schema_cls, **extends_on_attrs)

        # # reference nested keys under `_nests` (on the parent)
        # parent._nests = getattr(parent, '_nests', [])
        # parent._nests.append(self.extends_on)
        # new_parent_methods['_model'] = parent._model
        # new_parent_methods['__tablename__'] = parent._model.__tablename__

        # schema_cls = type(name, (Schema,), new_parent_methods)

        # ret =  ViewMaker(schema_cls, self.router)
        # print(ret, ret.schema_cls)

        # return object()


def build_app(app, registered_schemas):
    print("* Building views...")
    router = app.router

    for schema_name, schema_cls in registered_schemas:
        tablename = getattr(schema_cls, '__tablename__', None)
        print(f'\t+ processing {tablename}')

        # mark a schema for model creation
        if tablename is not None:
            schema_cls._model = create_model(schema_cls)

        # dispatch the view creation handler depending on schema's dependency scheme
        if schema_cls.__base__ is not PostSchema:
            post_view = InheritedViewMaker(schema_cls, router)
            post_view.rewrite_inherited()
        else:
            post_view = ViewMaker(schema_cls, router)

        # invoke the relationship processing
        post_view.process_relationships()

        # skip the routes creation, should it be signaled
        if post_view.omit_me:
            continue

        post_view.create_views()
