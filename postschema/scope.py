from marshmallow.schema import BaseSchema, SchemaMeta

SCOPES = {}


class ScopeBaseMeta(SchemaMeta):

    _scopes = SCOPES

    def __new__(cls, name, bases, methods):
        if name == 'Actor':
            raise NameError(f'`Actor` is a reserved word and can\'t be used as a scope name')
        if name != 'ScopeBase':
            try:
                meta = methods['Meta']
            except KeyError:
                raise AttributeError(
                    'All classes inheriting from `ScopeBase` ought to define `Meta` nested class')
            try:
                set(meta.roles)
            except (AttributeError, TypeError):
                raise AttributeError(
                    f'Missing `roles` attribute on `{name}.Meta` scope class or is of wrong type')

            kls = super(ScopeBaseMeta, cls).__new__(cls, name, bases, methods)
            SCOPES[name] = kls
            return kls

        return super(ScopeBaseMeta, cls).__new__(cls, name, bases, methods)


class ScopeBase(BaseSchema, metaclass=ScopeBaseMeta):
    @classmethod
    def _validate_roles(cls, roles):
        for scope in cls._scopes.values():
            unrecognized_roles = set(scope.Meta.roles) - set(roles)
            if unrecognized_roles:
                raise ValueError(
                    f'{cls}.Meta.roles contains unrecognized roles ({",".join(unrecognized_roles)})')
