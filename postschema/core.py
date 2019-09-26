import sqlalchemy as sql
import ujson
from aiohttp.hdrs import METH_ALL

from marshmallow import (
    fields,
    missing,
    post_load,
    validate,
    validates
)
from sqlalchemy.ext.declarative import declarative_base

from . import (
    hooks,
    fields as postschema_fields,
    validators as postschema_validators
)
from .schema import PostSchema
from .utils import retype_schema
from .view import AuxViewBase, ViewsBase, ViewsTemplate

Base = declarative_base()

METH_ALL = [meth.lower() for meth in METH_ALL]
ITERABLE_FIELDS = (
    fields.List,
    postschema_fields.Set
)


class DefaultMetaBase:
    excluded_ops = []
    get_by = []
    list_by = []
    exclude_from_updates = []
    create_views = True
    extends_on = None


def popattr(cls, attr):
    try:
        delattr(cls, attr)
    except AttributeError:
        pass


def getattrs(cls):
    return {k: v for k, v in cls.__dict__.items() if not k.startswith('__')}


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

    meta = methods.get('Meta')
    declared_fields = methods['_declared_fields']

    if hasattr(meta, '__table_args__'):
        model_methods['__table_args__'] = meta.__table_args__

    for fieldname, field_attrs in declared_fields.items():
        if isinstance(field_attrs, fields.Field):
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
                translated['server_default'] = default_value

            args = []
            if 'fk' in metadata:
                args.append(metadata['fk'])
            if 'autoincrement' in metadata:
                args.append(metadata.pop('autoincrement'))
            metadict = metadata.copy()
            metadict.pop('fk', None)
            metadict.pop('read_only', None)

            model_methods[fieldname] = sql.Column(field_instance, *args, **metadict, **translated)

    modelname = name + 'Model'
    new_model = type(modelname, (Base,), model_methods)
    print(f"\t   - created model `{modelname}`")
    return new_model


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

    def create_views(self, joins):
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
        cls_view.post_init(joins)

        route_base = self.meta_cls.route_base.replace('/', '').lower()
        self.base_resource_url = f'/{route_base}/'
        self.router.add_route("*", self.base_resource_url, cls_view)

        # some questionable entities may scrape body payload from attempting GET requests
        class cls_view_get_copy(cls_view):
            pass
        for method in METH_ALL:
            if method == 'get':
                continue
            popattr(cls_view_get_copy, method)
        setattr(cls_view_get_copy, 'post', get_proxy)
        self.router.add_post(self.base_resource_url + 'get/', cls_view_get_copy)

        if 'list' not in self.excluded_ops:
            class cls_view_list_copy(cls_view_get_copy):
                pass

            popattr(cls_view_list_copy, 'post')
            setattr(cls_view_list_copy, 'get', list_proxy)
            self.router.add_get(self.base_resource_url + 'list/', cls_view_list_copy)

        return cls_view

    def create_aux_views(self):
        if hasattr(self.schema_cls, '__aux_routes__'):
            for routename, proto_viewcls in self.schema_cls.__aux_routes__.items():
                if routename.startswith('/'):
                    routename = routename[1:]
                if not routename.endswith('/'):
                    routename += '/'
                url = self.base_resource_url + routename
                view_methods = dict(proto_viewcls.__dict__)
                view_cls = type(proto_viewcls.__qualname__, (AuxViewBase, ), view_methods)
                self.router.add_route("*", url, view_cls)

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

    def process_relationships(self, registered_schemas):
        joins = {}

        # REWRITE schema_cls so that it includes after_{post/put/patch},
        # shifting the kids keys from root # to `extends_on` key.
        # Then, Json-ning the `extends_on` nest. That should do the trick with the nested relationship.

        new_schema_methods = {}
        if self.schema_cls.is_kid:
            self.schema_cls._post_validation_write_cleaners.append(
                hooks.clean_before_nested_write(self.schema_cls)
            )

        self.schema_cls._m2m_where_stmts = relation_where_stmts = {}
        # this_table = str(self.schema_cls._model.__table__)
        this_table, this_pk = str(
            self.schema_cls._model.__table__.primary_key.columns_autoinc_first[0]).split('.')

        deletion_cascade = getattr(self.schema_cls, '_deletion_cascade', [])
        m2m_cherrypicks = getattr(self.schema_cls, '_m2m_cherrypicks', [])

        for fieldname, fieldval in self.schema_cls._declared_fields.items():
            if isinstance(fieldval, postschema_fields.Relationship):
                this_target = (fieldname, this_table, this_pk)
                foreign_target = fieldval.target_table
                linked_table = foreign_target['name']
                linked_table_pk = foreign_target['pk']
                linked_target = (linked_table_pk, linked_table, fieldname)
                linked_schema = registered_schemas @ linked_table
                linked_schema._deletion_cascade = getattr(linked_schema, '_deletion_cascade', [])
                linked_schema._m2m_cherrypicks = getattr(linked_schema, '_m2m_cherrypicks', [])

                if isinstance(fieldval, postschema_fields.ForeignResources):
                    # in the deletes departments, we need to faciliate the following scenario:
                    # - one of our parent's ForeignResources' fks gets deleted
                    # - its FK reference in our parent needs to be cleared too
                    # print(self.schema_cls, fieldname, linked_schema)
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

                    joins[fieldname] = [linked_schema, f'{linked_table}.{{subkey}}=%({{fill}})s']

        new_schema_methods['_deletion_cascade'] = deletion_cascade
        new_schema_methods['_m2m_cherrypicks'] = m2m_cherrypicks
        self.schema_cls = retype_schema(self.schema_cls, new_schema_methods)
        return joins


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
        self.extends_on = extends_on

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

        return type('Meta', (kids_meta,), kids_meta_methods)

    def rewrite_inherited(self):
        """Copy appropriate fields from the parent and create the new schema,
        replacing the defining on.
        """
        parent = self.schema_cls.__base__

        extends_on = self.extends_on
        methods = dict(self.schema_cls.__dict__)

        parent_fieldnames = set(parent._declared_fields.keys())
        # for child_fieldname in child_fieldnames:
        #     del self.schema_cls._declared_fields[child_fieldname]
        # child_fields = {k: v for k, v in self if k in child_fieldnames}
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
        self.schema_cls.child_fieldnames = {
            k for k, v in self.schema_cls._declared_fields.items()
        } - parent_fieldnames
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

        # return object()


def adjust_fields(schema_cls):
    declared_fields = dict(schema_cls._declared_fields)
    iterables = []
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
                validator = validate.Length(max=sqlfield.length)
                colv.validators.append(validator)
        elif isinstance(colv, ITERABLE_FIELDS):
            # ensure relation fields are not included
            if not isinstance(colv, postschema_fields.Relationship):
                iterables.append(coln)

    schema_meta = schema_cls.Meta
    omit_me = not getattr(schema_meta, 'create_views', True)
    is_kid = schema_cls.is_kid
    if not is_kid and not omit_me and iterables:
        hook = hooks.escape_iterable(iterables)
        schema_cls._post_validation_write_cleaners.append(hook)
    return schema_cls


def build_app(app, registered_schemas):
    print("* Building views...")
    router = app.router

    for schema_name, schema_cls in registered_schemas:
        tablename = getattr(schema_cls, '__tablename__', None)
        print(f'\t+ processing {tablename}')

        schema_cls._post_validation_write_cleaners = []
        adjust_fields(schema_cls)

        # create an SQLAlchemy model
        if tablename is not None:
            schema_cls._model = create_model(schema_cls)

    for schema_name, schema_cls in registered_schemas:

        # dispatch the view creation handler depending on schema's dependency scheme
        if schema_cls.__base__ is not PostSchema:
            post_view = InheritedViewMaker(schema_cls, router)
            post_view.rewrite_inherited()
        else:
            post_view = ViewMaker(schema_cls, router)

        # invoke the relationship processing
        joins = post_view.process_relationships(registered_schemas)

        # skip the routes creation, should it be demanded
        if post_view.omit_me:
            continue

        post_view.create_views(joins)
        post_view.create_aux_views()
