import asyncio

from marshmallow.schema import ValidationError, BaseSchema as MarshmallowBaseSchema, SchemaMeta
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DefaultMetaBase:
    create_views = True
    excluded_ops = []
    exclude_from_updates = []


class DefaultOperations:
    get_by = []
    list_by = []
    delete_by = []
    disallow_authed = []


class _schemascls:
    _name = 'registered schemas'

    @property
    def full_iter(self):
        'Iterate over all schemas and their aux views'
        for schema_name, schema in self:
            if hasattr(schema, '__aux_routes__'):
                for _, aux_schema in schema.__aux_routes__.items():
                    yield aux_schema.__name__, aux_schema
            yield schema_name, schema

    def __iter__(self):
        for k, v in self.__dict__.copy().items():
            if not k.startswith('_'):
                yield k, v

    def __matmul__(self, other):
        return self._cont(other)

    def __repr__(self):
        attrs = list(self)
        if not attrs:
            return f'<No {self._name}>'
        return f"<{self._name}: {', '.join(k for k, v in self)}>"

    def _cont(self, other):
        for k, v in self:
            if other == v.__tablename__:
                return v


_schemas = _schemascls()


class PostSchemaBase(MarshmallowBaseSchema):

    Base = Base

    def __init__(self, use=None, joins=None, autosession_fields={}, *a, **kwargs):
        super().__init__(*a, **kwargs)
        only = set(kwargs.get('only', []) or [])
        self._use = use
        self._joins = joins
        self._autosession_fields = autosession_fields
        self._joinable_fields = joinable = set(joins or [])
        self._default_joinable_tables = only & joinable
        self._deferred_async_validators = []
        self.parent = self.__class__.__base__

    @property
    def is_read_schema(self):
        return self._use == 'read'

    def _call_and_store(self, getter_func, data, *, field_name, error_store, index=None):
        if asyncio.iscoroutinefunction(getter_func):
            getter_func.__func__.__postschema_hooks__ = {
                'fieldname': field_name,
                'error_store': error_store,
                'index': index
            }
            self._deferred_async_validators.append(getter_func)
            return data
        return MarshmallowBaseSchema._call_and_store(
            getter_func=getter_func,
            data=data,
            field_name=field_name,
            error_store=error_store,
            index=index)

    async def run_async_validators(self, data):
        for async_validator in self._deferred_async_validators:
            hooks = async_validator.__postschema_hooks__
            fieldname = hooks['fieldname']
            try:
                await async_validator(data[fieldname])
                return {}
            except KeyError:
                pass
            except ValidationError as inner_merr:
                inner_emsgs = inner_merr.messages
                index = hooks['index']
                error_store = hooks['error_store']
                error_store.store_error(inner_emsgs, fieldname, index=index)
                return error_store.errors


class PostSchemaMeta(SchemaMeta):

    def __new__(cls, name, bases, methods):
        kls = super(PostSchemaMeta, cls).__new__(cls, name, bases, methods)
        second_base = kls.mro()[1]
        second_base_name = second_base.__name__
        if second_base is not PostSchemaBase and name != "RootSchema" and second_base_name != 'RootSchema':
            if '__tablename__' not in methods and '__tablename__' not in second_base.__dict__:
                raise AttributeError(f'PostSchema `{name}` needs to define a `__tablename__` attribute')
            cls._conflate_meta_classes(kls)
            setattr(_schemas, name, kls)
            return kls
        return kls

    def _conflate_meta_classes(kls):
        '''Not dealing with `Meta` metaclass as it's included on `SchemaMeta`'''
        metaclasses = ['Public', 'Private', 'Authed']
        attrs = kls.__dict__
        new_attrs = {}
        for metaclass_name in metaclasses:
            metacls = getattr(kls, metaclass_name, None)
            # first parent come across to have corresponding metaclass defined will be used
            # as a base for this one

            for ancestor in kls.mro()[1:]:
                parental_metacls = getattr(ancestor, metaclass_name, None)
                if parental_metacls is not None:
                    new_attrs[metaclass_name] = type(
                        metaclass_name,
                        (parental_metacls, ),
                        {} if metacls is None else dict(metacls.__dict__)
                    )

        if 'Public' not in attrs and 'Public' not in new_attrs:
            # it's the only mandatory Meta class
            new_attrs['Public'] = DefaultOperations

        for k, v in new_attrs.items():
            setattr(kls, k, v)


class PostSchema(PostSchemaBase, metaclass=PostSchemaMeta):
    pass


class RootSchema(PostSchema):
    pass
