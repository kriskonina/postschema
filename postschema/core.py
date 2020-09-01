import weakref
from collections import defaultdict as dd
from contextlib import suppress
from copy import deepcopy
from functools import lru_cache

import sqlalchemy as sql
import orjson
from aiohttp import web
from aiohttp.hdrs import METH_ALL

from marshmallow import (
    fields,
    missing,
    post_load,
    validate,
    validates
)
from sqlalchemy import event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import DDL

from . import (
    hooks,
    fields as postschema_fields,
    validators as postschema_validators
)
from .auth.perms import TopSchemaPermFactory, AuxSchemaPermFactory
from .schema import DefaultMetaBase
from .spec import APISpecBuilder
from .utils import retype_schema
from .view import ViewsTemplate
from .view_bases import AuxViewBase, ViewsBase

Base = declarative_base()

METH_ALL = [meth.lower() for meth in METH_ALL]
JSON_ESCAPABLE_FIELDS = (
    fields.List,
    fields.Mapping,
    postschema_fields.Set
)


def popattr(cls, attr):
    with suppress(AttributeError):
        delattr(cls, attr)


def getattrs(cls):
    return {k: v for k, v in cls.__dict__.items() if not k.startswith('__')}


create_id_const_query = '''DROP TRIGGER IF EXISTS id_constr_{tablename}_{self_col} ON "{tablename}";
CREATE TRIGGER id_constr_{tablename}_{self_col}
BEFORE INSERT OR UPDATE on "{tablename}"
FOR EACH ROW
EXECUTE PROCEDURE identity_constraint_fn (
    '{target_table_name}', '{target_table_pk}',
    '{self_col}', '{target_col}', '{target_table_local_ref}');'''


def add_index(metadata, index_name, tablename, col, index_type):
    event.listen(
        metadata,
        'after_create',
        DDL(f'CREATE INDEX IF NOT EXISTS {index_name} ON {tablename} USING {index_type.upper()}({col})')
    )


def add_identity_triggers(metadata, identity_constraint):
    query = create_id_const_query.format(**identity_constraint)
    event.listen(
        metadata,
        'after_create',
        DDL(query)
    )


def create_model(schema_cls, info_logger): # noqa
    ALLOWED_HOOKS = {'before_create', 'after_create'}
    name = schema_cls.__name__
    methods = dict(schema_cls.__dict__)
    try:
        tablename = methods.get('__tablename__', getattr(schema_cls, '__tablename__'))
        model_methods = {
            '__tablename__': tablename
        }
    except KeyError:
        raise AttributeError(f'{name} needs to define `__tablename__`')

    meta = methods.get('Meta')
    hooks = methods.get('Hooks')
    declared_fields = methods['_declared_fields']

    if hasattr(meta, '__table_args__'):
        model_methods['__table_args__'] = meta.__table_args__
    
    if hooks:
        hook_methods = {attr for attr in dir(hooks) if not attr.startswith('__') and callable(getattr(hooks, attr))}
        hooks = {attr: getattr(hooks, attr) for attr in hook_methods & ALLOWED_HOOKS}

    id_constraints = []
    indexes = {}

    for fieldname, field_attrs in declared_fields.items():
        if isinstance(field_attrs, fields.Field):
            if isinstance(field_attrs, postschema_fields.AutoSessionField):
                perms = getattr(methods.get('Public'), 'permissions', None)
                if perms and hasattr(perms, 'post') and 'primary_key' in field_attrs.metadata:
                    # auto-injected primary key is based on the session context,
                    # so we can't allow public posts.
                    raise AttributeError(f"{name} can't include 'post' as a public permission attribute")

            metadata = field_attrs.metadata
            try:
                field_instance = metadata.pop('sqlfield', None) or metadata['fk']
                if not field_instance:
                    continue
            except KeyError:
                # skip fields with no sql bindings
                continue
            except AttributeError:
                raise AttributeError(
                    f'Schema field `{fieldname}` needs to define a SQLAlchemy field instance')

            translated = {}
            default_value = field_attrs.default
            if default_value != missing:
                translated['server_default'] = default_value() if callable(default_value) else default_value

            args = []
            if 'fk' in metadata:
                args.append(metadata['fk'])
            if 'autoincrement' in metadata:
                args.append(metadata.pop('autoincrement'))
            metadict = metadata.copy()
            metadict.pop('fk', None)
            metadict.pop('read_only', None)
            metadict.pop('is_aware', None)
            if metadict.pop('gist_index', False):
                indexes[f'{fieldname}_gist_idx'] = [tablename, fieldname, 'gist']
            if metadict.pop('gin_index', False):
                indexes[f'{fieldname}_gin_idx'] = [tablename, fieldname, 'gin']
            identity_constraint = metadict.pop('identity_constraint', {})
            model_methods[fieldname] = sql.Column(field_instance, *args, **metadict, **translated)

            # parse identity_constraint
            if identity_constraint:
                identity_constraint['target_table_local_ref'] = fieldname
                identity_constraint['tablename'] = tablename
                id_constraints.append(identity_constraint)

    modelname = name + 'Model'
    new_model = type(modelname, (Base,), model_methods)
    if hooks:
        for method, method_fn in hooks.items():
            event.listens_for(Base.metadata.tables[tablename], method)(method_fn)

    for index_name, index_items in indexes.items():
        tablename, fieldname, index_type = index_items
        add_index(Base.metadata, index_name, tablename, fieldname, index_type)

    for id_constraint in id_constraints:
        add_identity_triggers(Base.metadata, id_constraint)

    info_logger.debug(f"- created model `{modelname}`")
    return new_model


class ViewMaker:
    def __init__(self, schema_cls, router, registered_schemas, url_prefix):
        self.url_prefix = url_prefix
        self.schema_cls = schema_cls
        self.router = router
        self.registered_schemas = registered_schemas
        meta_cls = self.rebase_metaclass()
        self.schema_cls.Meta = meta_cls
        self.meta_cls = meta_cls

    @property
    def excluded_ops(self):
        return self.meta_cls.excluded_ops

    @property
    @lru_cache()
    def base_resource_url(self):
        route_base = self.meta_cls.route_base.replace('/', '').lower()
        return f'{self.url_prefix}/{route_base}/'

    def create_views(self, joins):
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
        cls_view.registered_schemas = weakref.proxy(self.registered_schemas)
        cls_view.post_init(joins)

        self.router.add_route("*", self.base_resource_url, cls_view)

        return cls_view

    def create_aux_views(self, parent_cls_view, perm_builder):
        def gen():
            yield
        all_ops = ['post', 'get', 'patch', 'put', 'delete', 'list']

        class mod_parent_base(parent_cls_view):
            pass

        if hasattr(self.schema_cls, '__aux_routes__'):
            for routename, proto_viewcls in self.schema_cls.__aux_routes__.items():
                if routename.startswith('/'):
                    routename = routename[1:]
                if not routename.endswith('/'):
                    routename += '/'
                url = self.base_resource_url + routename
                view_methods = dict(proto_viewcls.__dict__)

                # ensure _iter is inherited from AuxViewBase, not ViewsClassBase
                view_methods['_iter'] = AuxViewBase._iter
                view_cls = type(
                    proto_viewcls.__qualname__,
                    (mod_parent_base, AuxViewBase),
                    view_methods)

                perms = perm_builder(proto_viewcls)
                view_cls._perm_options = {
                    'perms': perms,
                    **perm_builder.operation_constraints
                }
                # view_cls._disallow_authed = perm_builder.disallow_authed

                allowed = [op for op in all_ops if op in view_methods]
                for op in all_ops:
                    if hasattr(view_cls, op) and op not in view_methods:
                        setattr(view_cls, op, lambda self: gen().throw(
                            web.HTTPMethodNotAllowed(method=op, allowed_methods=allowed))
                        )
                self.router.add_route("*", url, view_cls)
                yield routename, view_cls

    @property
    def omit_me(self):
        return not self.meta_cls.create_views

    def make_new_post_view(self, schema_cls):
        return self.__class__.__base__(schema_cls, self.router)

    def rebase_metaclass(self):
        meta_cls = self.schema_cls.Meta
        meta_methods = dict(meta_cls.__dict__)

        # force-set common meta attrs
        meta_methods.setdefault('route_base', self.schema_cls.__name__.lower())
        meta_methods['render_module'] = orjson

        new_meta = type('Meta', (DefaultMetaBase, ), meta_methods)
        self.schema_cls.Meta = new_meta
        return new_meta

    def process_relationships(self):
        joins = {}

        new_schema_methods = {}
        self.schema_cls._m2m_where_stmts = relation_where_stmts = {}
        this_table, this_pk = str(
            self.schema_cls._model.__table__.primary_key.columns_autoinc_first[0]).split('.')

        deletion_cascade = getattr(self.schema_cls, '_deletion_cascade', [])
        m2m_cherrypicks = getattr(self.schema_cls, '_m2m_cherrypicks', [])

        for fieldname, fieldval in self.schema_cls._declared_fields.items():
            if isinstance(fieldval, postschema_fields.Relationship):
                if isinstance(fieldval, postschema_fields.AutoSessionField) and 'primary_key' in fieldval.metadata:
                    # omit session-based auto-injected pks
                    continue
                this_target = (fieldname, this_table, this_pk)
                foreign_target = fieldval.target_table
                linked_table = foreign_target['name']
                linked_table_pk = foreign_target['target_col']
                linked_target = (linked_table_pk, linked_table, fieldname)
                linked_schema = self.registered_schemas[linked_table]
                linked_schema.pk_column_name = linked_table_pk
                linked_schema._deletion_cascade = getattr(linked_schema, '_deletion_cascade', [])
                linked_schema._m2m_cherrypicks = getattr(linked_schema, '_m2m_cherrypicks', [])

                if isinstance(fieldval, postschema_fields.AutoSessionForeignResource):
                    validator_fn = postschema_validators.autosession_field_validator(fieldname)
                    new_schema_methods[f'validate_{fieldname}'] = validates(fieldname)(validator_fn)

                elif isinstance(fieldval, postschema_fields.ForeignResources):
                    # in the deletes departments, we need to faciliate the following scenario:
                    # - one of our parent's ForeignResources' fks gets deleted
                    # - its FK reference in our parent needs to be cleared too
                    linked_schema._m2m_cherrypicks.append((this_table, fieldname, this_pk))

                    # The holder of this field will store references to its 'relatives'
                    # Hook a custom validator to ensure that incoming FKs correspond to valid records
                    children_validator, make_children_post_load = \
                        postschema_validators.adjust_children_field(fieldname)

                    # add the validator only in case of this schema being used for writing
                    new_schema_methods[f'validate_{fieldname}'] = validates(fieldname)(children_validator)

                    # ensure that ForeignResources' value is formatted correctly
                    new_schema_methods[f'post_load_{fieldname}'] = post_load(make_children_post_load)
                    relation_where_stmts[fieldname] = f'{fieldname} ?& %({fieldname})s'

                elif isinstance(fieldval, postschema_fields.ForeignResource):
                    if not fieldval.metadata.get('unique', False):
                        linked_schema._deletion_cascade.append(this_target)
                    else:
                        # unique clause in, t's a O2O relationship,
                        # delete instruction should be present on both tables
                        deletion_cascade.append(linked_target)
                        linked_schema._deletion_cascade.append(this_target)  # this is debatable
                    joins[fieldname] = {
                        'linked_schema': linked_schema,
                        'aliased_comp_query': f'_{fieldname}_j.{{subkey}}=%({{fill}})s',
                        'unaliased_comp_query': f'"{linked_table}".{{subkey}}=%({{fill}})s',
                        'target_table': fieldval.target_table
                    }

        new_schema_methods['_deletion_cascade'] = deletion_cascade
        new_schema_methods['_m2m_cherrypicks'] = m2m_cherrypicks
        self.schema_cls = retype_schema(self.schema_cls, new_schema_methods)
        return joins


def adjust_fields(schema_cls, all_schemas):
    declared_fields = dict(schema_cls._declared_fields)
    iterables = []
    rangeables = []
    for coln, colv in declared_fields.items():
        meta = colv.metadata
        if not colv.required or meta.get('primary_key', False):
            colv.metadata['nullable'] = True
        else:
            colv.metadata['nullable'] = False
        # if the field is a String, then take the max len and use it
        # to create a marshmallow validator
        if isinstance(colv, fields.String):
            sqlfield = colv.metadata.get('sqlfield')
            if sqlfield is not None:
                with suppress(AttributeError):
                    # sqlalchemy field in use could be inheriting from `String` class
                    validator = validate.Length(max=sqlfield.length)
                    colv.validators.append(validator)
        elif isinstance(colv, postschema_fields.RangeDTField):
            rangeables.append(coln)
        elif isinstance(colv, JSON_ESCAPABLE_FIELDS):
            # ensure relation fields are not included
            if not isinstance(colv, postschema_fields.Relationship):
                iterables.append(coln)
        elif isinstance(colv, postschema_fields.ForeignResource):
            # establish the type of the linked table's pk and apply the correspoding
            # marshmallow field type as a base for this field.
            colv_clone = deepcopy(colv)
            while type(colv_clone) not in (fields.Integer, fields.String):
                target_tablename = colv_clone.target_table['name']
                target_pkname = colv_clone.target_table['target_col']
                target_schema = all_schemas[target_tablename]
                colv_clone = target_schema._declared_fields[target_pkname]
            colv.target_pk_type = colv_clone

    schema_meta = schema_cls.Meta
    omit_me = not getattr(schema_meta, 'create_views', True)
    if not omit_me:
        pv_hooks = []
        if rangeables:
            pv_hooks.append(hooks.escape_rangeable(rangeables))
        if iterables:
            pv_hooks.append(hooks.escape_iterable(iterables))
        schema_cls._post_validation_write_cleaners.extend(pv_hooks)
    return schema_cls


FIELD_EXTENSIONS = {
    fields.String: {
        'contains': [
            fields.String(), '{colname} LIKE %({{fieldname}})s', '%{val}%'
        ],
        'icontains': [
            fields.String(), '{colname} ILIKE %({{fieldname}})s', '%{val}%'
        ],
        'beginswith': [
            fields.String(), '{colname} LIKE %({{fieldname}})s', '{val}%'
        ],
        'endswith': [
            fields.String(), '{colname} LIKE %({{fieldname}})s', '%{val}'
        ],
        'ibeginswith': [
            fields.String(), '{colname} ILIKE %({{fieldname}})s', '{val}%'
        ],
        'iendswith': [
            fields.String(), '{colname} ILIKE %({{fieldname}})s', '%{val}'
        ]
    },
    fields.Integer: {
        'lt': [
            fields.Integer(), '{colname}<%({{fieldname}})s', '{val}'
        ],
        'lte': [
            fields.Integer(), '{colname}<=%({{fieldname}})s', '{val}'
        ],
        'gt': [
            fields.Integer(), '{colname}>%({{fieldname}})s', '{val}'
        ],
        'gte': [
            fields.Integer(), '{colname}>=%({{fieldname}})s', '{val}'
        ],
        'between': [
            fields.List(fields.Integer(), validate=validate.Length(equal=2)),
            '{colname} BETWEEN %({{fieldname}}_lower)s AND %({{fieldname}}_upper)s',
            '{val}'
        ]
    }
}
FIELD_EXTENSIONS[fields.Date] = {
    operation: [
        fields.List(fields.Date(), validate=validate.Length(equal=2)) if operation == 'between' else fields.Date(),
        *arr[1:]
    ] for operation, arr in FIELD_EXTENSIONS[fields.Integer].items()
}
FIELD_EXTENSIONS[fields.DateTime] = {
    operation: [
        fields.List(fields.DateTime(), validate=validate.Length(equal=2)) if operation == 'between' else fields.DateTime(),
        *arr[1:]
    ] for operation, arr in FIELD_EXTENSIONS[fields.Integer].items()
}
FIELD_EXTENSIONS[fields.Time] = {
    operation: [
        fields.List(fields.Time(), validate=validate.Length(equal=2)) if operation == 'between' else fields.Time(),
        *arr[1:]
    ] for operation, arr in FIELD_EXTENSIONS[fields.Integer].items()
}


def extend_schema_for_extra_search(schema_cls):
    extended_fields_values = {}
    for coln, colv in dict(schema_cls._declared_fields).items():
        colname = colv.attribute or coln
        field_ext = {}
        for base in colv.__class__.__mro__:
            field_ext = FIELD_EXTENSIONS.get(base, {})
            if field_ext:
                break
        for op, arr in field_ext.items():
            new_colname = f'{colname}__{op}'
            new_fieldname = f'{coln}__{op}'
            new_field_inst = arr[0]
            formatted_value_template = arr[1].format(colname=colname)
            if len(arr) == 3:
                extended_fields_values[new_fieldname] = [colname, formatted_value_template, arr[2]]
            else:
                extended_fields_values[new_fieldname] = [colname, formatted_value_template]
            yield new_colname, new_field_inst

    schema_cls._extended_fields_values = extended_fields_values


def extend_selectors(schema_cls):
    # append newly created extended fields to their get_by and list_by lists,
    # wherever applicable
    public_cls = getattr(schema_cls, 'Public', None)
    authed_cls = getattr(schema_cls, 'Authed', None)
    private_cls = getattr(schema_cls, 'Private', None)

    for new_fieldname, arr in schema_cls._extended_fields_values.items():
        fieldname, _ = new_fieldname.split('__')
        if public_cls:
            if fieldname in getattr(public_cls, 'get_by', []):
                schema_cls.Public.get_by.append(new_fieldname)
            if fieldname in getattr(public_cls, 'list_by', []):
                schema_cls.Public.list_by.append(new_fieldname)
            if fieldname in getattr(public_cls, 'delete_by', []):
                schema_cls.Public.delete_by.append(new_fieldname)

        if authed_cls:
            if fieldname in getattr(authed_cls, 'get_by', []):
                schema_cls.Authed.get_by.append(new_fieldname)
            if fieldname in getattr(authed_cls, 'list_by', []):
                schema_cls.Authed.list_by.append(new_fieldname)
            if fieldname in getattr(authed_cls, 'delete_by', []):
                schema_cls.Authed.delete_by.append(new_fieldname)

        if private_cls:
            if fieldname in getattr(private_cls, 'get_by', []):
                schema_cls.Private.get_by.append(new_fieldname)
            if fieldname in getattr(private_cls, 'list_by', []):
                schema_cls.Private.list_by.append(new_fieldname)
            if fieldname in getattr(private_cls, 'delete_by', []):
                schema_cls.Private.delete_by.append(new_fieldname)



def build_app(app, registered_schemas):
    app.info_logger.debug("* Building views...")
    router = app.router

    created = dd(int)

    for schema_name, schema_cls in registered_schemas:
        tablename = getattr(schema_cls, '__tablename__', None)
        app.info_logger.debug(f'+ processing {tablename}')

        schema_cls._post_validation_write_cleaners = []
        adjust_fields(schema_cls, registered_schemas)

        # create an SQLAlchemy model
        if tablename is not None:
            schema_cls._model = create_model(schema_cls, app.info_logger)
            created['Models'] += 1

    perm_builder = TopSchemaPermFactory(registered_schemas, app.config.roles)
    aux_perm_builder = AuxSchemaPermFactory(registered_schemas, app.config.roles)

    spec_builder = APISpecBuilder(app, router)

    for schema_name, schema_cls in registered_schemas:
        # extend the schema with extra search criteria fields, if requested
        if getattr(schema_cls.Meta, 'enable_extended_search', False):
            new_fields = dict(extend_schema_for_extra_search(schema_cls))
            schema_cls = retype_schema(schema_cls, new_fields)
            extend_selectors(schema_cls)

        post_view = ViewMaker(schema_cls, router, registered_schemas, app.url_prefix)
        # invoke the relationship processing
        joins = post_view.process_relationships()

        # skip the routes creation, should it be demanded
        if post_view.omit_me:
            continue

        perms = perm_builder(schema_cls)
        cls_view = post_view.create_views(joins)
        cls_view._perm_options = {
            'perms': perms,
            **perm_builder.operation_constraints
        }
        created['Views'] += 1
        aux_routes = dict(post_view.create_aux_views(cls_view, aux_perm_builder))
        if aux_routes:
            schema_cls.__aux_routes__ = aux_routes
            setattr(registered_schemas, schema_name, schema_cls)

        created['Auxiliary views'] += len(aux_routes)
        spec_builder.add_schema_spec(schema_cls, post_view, cls_view, aux_routes.values())

    app.info_logger.info('System ready', created=dict(created))
    return router, spec_builder.build_spec()
