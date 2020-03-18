from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class SessionContext:
    actor_id: int = field(metadata={'format': "{}"})
    workspace: int = field(metadata={'format': "{}"})
    phone: str = field(metadata={'format': "'{}'"})
    email: str = field(metadata={'format': "'{}'"})
    status: int = field(metadata={'format': "{}"})
    workspaces: list = field(default_factory=list)


class ClauseBus(list):

    @property
    def has_open_clauses(self):
        return any(isinstance(clause_inst, OpenPermClause) for clause_inst in self[::2])

    @property
    def stmt(self):
        return ' '.join(getattr(i, 'clause', i) for i in self[::])

    def __and__(self, other):
        if other in self:
            raise ValueError(f'Clause `{other}` already joined')
        self.append('AND')
        if isinstance(other, list):
            self.extend(other)
        else:
            self.append(other)
        return self

    def __or__(self, other):
        if other in self:
            raise ValueError(f'Clause `{other}` already joined')
        self.append('OR')
        if isinstance(other, list):
            self.extend(other)
        else:
            self.append(other)
        return self

    def __repr__(self):
        return f'<ClauseBus({self.stmt})>'

    def digest(self, *args):
        for clause in self[::2]:
            clause.digest(*args)
        return self.stmt


@dataclass
class PermClauseBase:
    clause: str

    def __and__(self, other):
        return ClauseBus([self]) & other

    def __or__(self, other):
        return ClauseBus([self]) | other

    def __repr__(self):
        return self.clause


class CheckedPermClause(PermClauseBase):

    has_open_clauses = False
    repr_name = 'CheckedClause'

    def digest(self, op_path, registered_schemas, schema_tablename, *args):
        initial_split = self.clause.split('=')
        if len(initial_split) != 2:
            initial_split = self.clause.split('->')
            if len(initial_split) != 2:
                raise TypeError(f'`{op_path}` contains invalid operator (has to be `=` or `->`)')
            operator = '->'
        else:
            operator = '='

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
            tablename = schema_tablename

        schema_cls = registered_schemas[tablename]
        if schema_cls is None:
            raise NameError(f'Table `{tablename}` defined on `{op_path}` not found!')
        if column not in schema_cls._declared_fields:
            raise NameError(f'Column `{tablename}.{column}` defined on `{op_path}` not found!')

        if operator == '->':
            if not issubclass(auth_field_type, Iterable):
                raise TypeError(
                    f'Auth field `{op_path}->{orig_tablename}.{authfield_name}` is not of iterable type')
            af = f'{{session.{authfield_name}}}'
            precursor = f'''"{tablename}".{column}::text::jsonb <@ '{af}'::jsonb'''

        elif operator == '=':
            if issubclass(auth_field_type, Iterable):
                raise TypeError(f'Auth field `{authfield_name}` is not supposed to be of iterable type')

            authfield_format = SessionContext.__dataclass_fields__[authfield_name].metadata['format']
            af = f'{{session.{authfield_name}}}'
            formatted_authfield = authfield_format.format(af)
            precursor = f'''"{tablename}".{column}={formatted_authfield}'''

        self.clause = precursor
        return precursor


class OpenPermClause(PermClauseBase):

    has_open_clauses = True
    repr_name = 'OpenClause'

    def digest(self, *args):
        return self.clause
