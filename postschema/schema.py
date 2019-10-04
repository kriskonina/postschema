import asyncio

from marshmallow.schema import ValidationError, BaseSchema as MarshmallowBaseSchema, SchemaMeta
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DefaultMetaBase:
    excluded_ops = []
    get_by = []
    list_by = []
    exclude_from_updates = []
    create_views = True


class _schemascls:
    _name = 'registered schemas'

    def __iter__(self):
        for k, v in self.__dict__.copy().items():
            if not k.startswith('_'):
                yield k, v

    def __matmul__(self, other):
        for k, v in self:
            if other == v.__tablename__:
                return v

    def __repr__(self):
        attrs = list(self)
        if not attrs:
            return f'<No {self._name}>'
        return f"<{self._name}: {', '.join(k for k, v in self)}>"


_schemas = _schemascls()


class PostSchemaMeta(SchemaMeta):
    def __new__(cls, name, bases, methods):
        kls = super(PostSchemaMeta, cls).__new__(cls, name, bases, methods)
        second_base = kls.mro()[1]
        second_base_name = second_base.__name__
        if second_base is not MarshmallowBaseSchema and name != "RootSchema" and second_base_name != 'RootSchema':
            if '__tablename__' not in methods:
                raise AttributeError(f'PostSchema `{name}` needs to define `__tablename__`')
            setattr(_schemas, name, kls)
            for base in kls.mro():
                if base.__name__ == 'RootSchema':
                    return cls._conflate_schemas(kls)
        return kls

    def _conflate_schemas(kls):
        defined_metacls = getattr(kls, 'Meta', None)
        if defined_metacls is None:
            # means 'Meta' isn't defined on neither this schema nor its parent
            attrs = dict(kls.__dict__)
            attrs['Meta'] = defined_metacls
            return type(kls.__name__, kls.__bases__, attrs)
        return kls


class PostSchema(MarshmallowBaseSchema, metaclass=PostSchemaMeta):

    Base = Base

    def __init__(self, use=None, joins=None, *a, **kwargs):
        super().__init__(*a, **kwargs)
        only = set(kwargs.get('only', []) or [])
        self._use = use
        self._joins = joins
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


class RootSchema(PostSchema):
    pass
