from marshmallow.schema import BaseSchema, SchemaMeta

ROLES = {}


class RoleBaseMeta(SchemaMeta):

    _roles = ROLES

    def __new__(cls, name, bases, methods):
        if name == 'Actor':
            raise NameError(f'`Actor` is a reserved word and can\'t be used as a role name')
        if name != 'RoleBase':
            try:
                meta = methods['Meta']
            except KeyError:
                raise AttributeError(
                    'All classes inheriting from `RoleBase` ought to define `Meta` nested class')
            try:
                set(meta.scopes)
            except (AttributeError, TypeError):
                raise AttributeError(f'Missing `scopes` attribute on `{name}.Meta` role class or is of wrong type')

            kls = super(RoleBaseMeta, cls).__new__(cls, name, bases, methods)
            ROLES[name] = kls
            return kls

        return super(RoleBaseMeta, cls).__new__(cls, name, bases, methods)


class RoleBase(BaseSchema, metaclass=RoleBaseMeta):
    @classmethod
    def _validate_scopes(cls, scopes):
        for role in cls._roles.values():
            unrecognized_scopes = set(role.Meta.scopes) - set(scopes)
            if unrecognized_scopes:
                raise ValueError(
                    f'{cls}.Meta.scopes contains unrecognized scopes ({",".join(unrecognized_scopes)})')
