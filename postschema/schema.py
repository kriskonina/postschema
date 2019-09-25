import asyncio

from marshmallow.schema import ValidationError, BaseSchema as MarshmallowBaseSchema, SchemaMeta
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


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
        if kls.mro()[1] is not MarshmallowBaseSchema:
            setattr(_schemas, name, kls)
            kls.is_kid = not bases[0] is PostSchema
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

        # self._tables_to_join_from_selects = []

        self._deferred_async_validators = []
        self.parent = self.__class__.__base__

    @property
    def is_read_schema(self):
        return self._use == 'read'

    def _call_and_store(self, getter_func, data, *, field_name, error_store, index=None):
        # data = MarshmallowBaseSchema._call_and_store(
        #     getter_func=getter_func,
        #     data=data,
        #     field_name=field_name,
        #     error_store=error_store,
        #     index=index)
        if asyncio.iscoroutinefunction(getter_func):
            getter_func.__func__.__postschema_hooks__ = {
                'fieldname': field_name,
                'error_store': error_store,
                'index': index
            }
            self._deferred_async_validators.append(getter_func)
            return data
        # else:
        return MarshmallowBaseSchema._call_and_store(
            getter_func=getter_func,
            data=data,
            field_name=field_name,
            error_store=error_store,
            index=index)
        # return data

    #     '''Monkey-patched `marshmallow.schema.Schema.__call_and_store`'''

    #     try:
    #         value = getter_func(data)
    #     except ValidationError as err:
    #         error_store.store_error(err.messages, field_name, index=index)
    #         # When a Nested field fails validation, the marshalled data is stored
    #         # on the ValidationError's valid_data attribute
    #         return err.valid_data or missing
    #     return value

        # try:
        #     if asyncio.iscoroutinefunction(getter_func):
        #         # schedule for deferred execution
        #         getter_func.__func__.__postschema_hooks__ = {
        #             'fieldname': field_name,
        #             'error_store': error_store,
        #             'index': index
        #         }
        #         self._deferred_async_validators.append(getter_func)
        #         value = data
        #     else:
        #         value = getter_func(data) or data
        # except ValidationError as err:
        #     error_store.store_error(err.messages, field_name, index=index)
        #     return err.valid_data or missing
        # return value

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
