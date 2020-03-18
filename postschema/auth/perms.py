from collections import defaultdict as dd
from dataclasses import dataclass
from typing import Iterable

from .clauses import PermClauseBase, ClauseBus

ALL_BASIC_OPERATIONS = ['post', 'get', 'list', 'put', 'patch', 'delete']


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
        for op_path, operation, role, clause in self.compile_perm_type(PublicPrivatePerms, 'Private'):
            operations = COMPOSITE_OPS.get(operation, [operation])
            op2_path = op_path + '.' + str(role)

            # role can be an asterisk, a single role name or a tuple of more. Validate first.
            roles = set(role) if isinstance(role, tuple) else set([role])
            invalid_roles = roles - self.roles
            if invalid_roles:
                raise NameError(f'`{op_path}` contains invalid role(s) ({invalid_roles})')

            if not isinstance(clause, (PermClauseBase, ClauseBus)):
                raise TypeError(f'`{op_path}` must be of {PermClauseBase} or {ClauseBus} type')

            stmt = clause.digest(op2_path, self.registered_schemas, self.schema_cls.__tablename__)

            for oper in operations:
                perms[oper][role] = {
                    'type': type(role),
                    'stmt': stmt,
                    'has_open_clauses': clause.has_open_clauses
                }
        return perms


class AuxSchemaPermFactory(SchemaFactoryBase):
    def compile_private_perms(self):
        return self.compile_secure_perms(perm_cls_name='Private')
