from collections import defaultdict as dd
from dataclasses import dataclass, field
from typing import Iterable

ALL_BASIC_OPERATIONS = ['post', 'get', 'list', 'put', 'patch', 'delete']


@dataclass
class SessionContext:
    actor_id: int = field(metadata={'format': "{}"})
    workspace: int = field(metadata={'format': "{}"})
    phone: str = field(metadata={'format': "'{}'"})
    email: str = field(metadata={'format': "'{}'"})
    status: int = field(metadata={'format': "{}"})
    workspaces: list = field(default_factory=list)


class PublicPrivatePerms:
    post: dict
    read: dict
    get: dict
    list: dict
    update: dict
    put: dict
    patch: dict
    delete: dict


class AuthedPermissions:
    post: Iterable
    read: Iterable
    get: Iterable
    list: Iterable
    update: Iterable
    put: Iterable
    patch: Iterable
    delete: Iterable


COMPOSITE_OPS = {
    'read': ['get', 'list'],
    'update': ['patch', 'put']
}


@dataclass
class SchemaFactoryBase:
    registered_schemas: list
    roles: frozenset

    def __call__(self, schema_cls):
        self.operation_constraints = {}
        self.schema_cls = schema_cls

        private_cls = getattr(schema_cls, 'Private', object)
        authed_cls = getattr(schema_cls, 'Authed', object)
        public_cls = getattr(schema_cls, 'Public', object)

        self.operation_constraints['forced_logout'] = getattr(public_cls, 'forced_logout', False)

        perms = {}

        if hasattr(private_cls, 'permissions'):
            perms['private'] = dict(self.compile_private_perms())
            self._set_operations_routine('verified_email', private_cls, perms['private'])
            self._set_operations_routine('verified_phone', private_cls, perms['private'])

        if hasattr(authed_cls, 'permissions'):
            perms['authed'] = dict(self.compile_secure_perms())
            self._set_operations_routine('verified_email', authed_cls, perms['authed'])
            self._set_operations_routine('verified_phone', authed_cls, perms['authed'])

        if hasattr(public_cls, 'permissions'):
            perms['public'] = dict(self.compile_public_perms())

        return perms

    def compile_perm_type(self, perm_type, perm_name):
        all_annots = perm_type.__annotations__

        try:
            perm_cls = getattr(self.schema_cls, perm_name, object).permissions
        except AttributeError:
            perm_cls = object

        all_perms = {k: v for k, v in perm_cls.__dict__.items() if not k.startswith('__')}

        schema_cls_name = self.schema_cls.__name__
        for operation, details_struct in all_perms.items():
            op_path = f"{schema_cls_name}.{perm_name}.permissions.{operation}"
            if operation not in all_annots:
                raise AttributeError(f"`{op_path}` isn't recognized as a valid resource operation")
            if not isinstance(details_struct, all_annots[operation]):
                raise TypeError(f"`{op_path}` should be of {all_annots[operation]} type")
            if perm_name == 'Private' and isinstance(details_struct, dict):
                # when implemented on AuxSchemaPermFactory, the `Private.permissions` will be a list
                for role, statement in details_struct.items():
                    yield op_path, operation, role, statement
            else:
                yield op_path, operation, details_struct

    def compile_secure_perms(self, perm_cls_name='Authed'):
        '''
        Common method for compiling perms for both Private & Authed permission classes.

        To enable overloading of this method by `AuxSchemaPermFactory`, `perm_cls_name`
        is supplied to trick the `self.compile_perm_type` it's dealing with Auth
        permission class each time.
        '''
        perm_cls = getattr(self.schema_cls, perm_cls_name)
        authed_perms = getattr(perm_cls, 'permissions', object)
        if hasattr(authed_perms, 'allow_all') and authed_perms.allow_all:
            op_path = f"{self.schema_cls.__name__}.{perm_cls_name}.permissions.allow_all"
            roles = set(authed_perms.allow_all)
            invalid_roles = roles - self.roles
            if invalid_roles:
                raise NameError(f'`{op_path}` contains invalid role(s) ({invalid_roles})')
            return {}.fromkeys(ALL_BASIC_OPERATIONS, roles)

        perms = dd(dict)
        perm_template = PublicPrivatePerms if perm_cls_name == 'Private' else AuthedPermissions

        for op_path, operation, roles_list in self.compile_perm_type(perm_template, perm_cls_name):
            operations = COMPOSITE_OPS.get(operation, [operation])
            if not roles_list:
                raise ValueError(f"`{op_path}` can't be empty")
            # ensure each role exists
            roles = set(roles_list)
            invalid_roles = roles - self.roles
            if invalid_roles:
                raise NameError(f'`{op_path}` contains invalid role(s) ({invalid_roles})')
            for oper in operations:
                perms[oper] = roles
        return perms

    def compile_public_perms(self):
        perms = {}
        public_perms = getattr(self.schema_cls.Public, 'permissions', object)

        self._set_operations_routine(
            'disallow_authed',
            self.schema_cls.Public,
            public_perms.__dict__)

        if hasattr(public_perms, 'allow_all') and public_perms.allow_all:
            return {}.fromkeys(ALL_BASIC_OPERATIONS, '*')

        for op_path, operation, details_struct in self.compile_perm_type(PublicPrivatePerms, 'Public'):
            operations = COMPOSITE_OPS.get(operation, [operation])
            for oper in operations:
                perms[oper] = '*'
        return perms

    def _set_operations_routine(self, routinename, perm_cls, allowed):
        declared_ops = getattr(perm_cls, routinename, [])
        unrecognized_ops = [op for op in declared_ops if op not in allowed]
        if unrecognized_ops:
            uo = ', '.join(unrecognized_ops)
            raise ValueError(
                f"{self.schema_cls.__name__}.{perm_cls.__name__}.{routinename} contains undefined operations: ({uo})") # noqa
        self.operation_constraints[routinename] = declared_ops


class TopSchemaPermFactory(SchemaFactoryBase):
    def compile_private_perms(self):
        perms = dd(dict)
        for op_path, operation, role, statement in self.compile_perm_type(PublicPrivatePerms, 'Private'):
            operations = COMPOSITE_OPS.get(operation, [operation])
            op2_path = op_path + '.' + str(role)

            # role can be an asterisk, a single role name or a tuple of more. Validate first.
            roles = set(role) if isinstance(role, tuple) else set([role])
            invalid_roles = roles - self.roles
            if invalid_roles:
                raise NameError(f'`{op_path}` contains invalid role(s) ({invalid_roles})')

            initial_split = statement.split('=')
            if len(initial_split) != 2:
                initial_split = statement.split('->')
                if len(initial_split) != 2:
                    raise TypeError(f'`{op2_path}` contains invalid operator (has to be `=` or `->`)')
                operator = '->'
            else:
                operator = '='
            stmt = self.parse_perm_operation(op2_path, role, initial_split, operator)
            for oper in operations:
                perms[oper][role] = {
                    'type': type(role),
                    **stmt
                }
        return perms

    def parse_perm_operation(self, op_path, role, initial_split, operator):  # noqa
        def _parse_side(side):
            idx, side = side
            side_name = ['left', 'right'][idx]
            side_format = ['<table_name>.<col_name>', 'session.<fieldname>'][idx]
            if not side:
                raise TypeError(f"`{op_path}`'s {side_name}-hand part is empty")
            invalid_format = f"`{op_path}`'s {side_name}-hand path should be of {side_format} format"
            try:
                table, column = [i.strip() for i in side.split('.')]
            except ValueError:
                raise TypeError(invalid_format)

            if not table or not column:
                raise TypeError(invalid_format)
            return table, column

        big = map(_parse_side, enumerate([i.strip() for i in initial_split]))
        tablename, column, authname, authfield_name = [small for item in big for small in item]
        auth_fields = SessionContext.__annotations__

        if authname != 'session':
            raise TypeError(f"`{op_path}`'s right-hand part should start with `session.`")
        if authfield_name not in auth_fields:
            raise TypeError(
                f"`{op_path}`'s right-hand fieldname component should be one of: {list(auth_fields)}")

        auth_field_type = auth_fields[authfield_name]

        orig_tablename = tablename

        if tablename != 'self':
            raise ValueError('No foreign tables can be used at this time')

        if tablename == 'self':
            tablename = self.schema_cls.__tablename__

        schema_cls = self.registered_schemas @ tablename
        if schema_cls is None:
            raise NameError(f'Table `{tablename}` defined on `{op_path}` not found!')
        if column not in schema_cls._declared_fields:
            raise NameError(f'Column `{tablename}.{column}` defined on `{op_path}` not found!')

        if operator == '->':
            if not issubclass(auth_field_type, Iterable):
                raise TypeError(
                    f'Auth field `{op_path}->{orig_tablename}.{authfield_name}` is not of iterable type')
            af = f'{{session.{authfield_name}}}'
            precursor = {
                'stmt': f'''"{tablename}".{column}::text::jsonb <@ '{af}'::jsonb'''
            }
        elif operator == '=':
            if issubclass(auth_field_type, Iterable):
                raise TypeError(f'Auth field `{authfield_name}` is not supposed to be of iterable type')

            authfield_format = SessionContext.__dataclass_fields__[authfield_name].metadata['format']
            af = f'{{session.{authfield_name}}}'
            formatted_authfield = authfield_format.format(af)
            precursor = {
                'stmt': f'''"{tablename}".{column}={formatted_authfield}'''
            }

        return precursor  # column, authname, authfield_name


class AuxSchemaPermFactory(SchemaFactoryBase):
    def compile_private_perms(self):
        return self.compile_secure_perms(perm_cls_name='Private')
