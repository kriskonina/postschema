import datetime
from dateutil import tz
from functools import partial

import sqlalchemy as sql
from sqlalchemy_utils import DateTimeRangeType

from marshmallow import fields, ValidationError
from sqlalchemy.dialects.postgresql import JSONB

from . import validators


def len_validator(val):
    if len(val) != 3:
        raise ValidationError('Length must be 2.')


class RangeField(fields.List):
    pass


class RangeDTField(RangeField):
    def __init__(self, **kwargs):
        self.bounds = kwargs.pop('bounds', '(]')
        kwargs.update({
            'sqlfield': DateTimeRangeType,
            'validate': len_validator
        })
        super().__init__(fields.DateTime(), **kwargs)

    def _deserialize(self, *args, **kwargs):
        val = super()._deserialize(*args, **kwargs)
        val.append(self.bounds)
        return val


class Set(fields.List):
    def _deserialize(self, *args, **kwargs):
        return list(set(super()._deserialize(*args, **kwargs)))


class Relationship:
    def process_related_schema(self, related_schema_arg):
        try:
            f_table, f_pk = related_schema_arg.split('.')
        except (ValueError, AttributeError):
            raise TypeError(
                '`related_schema` argument should be of format: <foreign_table_name>.<foreign_table_pk>')
        self.target_table = {
            'name': f_table,
            'pk': f_pk
        }


class ForeignResource(Relationship, fields.Integer):
    def __init__(self, related_schema, *args, **kwargs):
        self.process_related_schema(related_schema)
        kwargs.update({
            'fk': sql.ForeignKey(related_schema),
            'index': True
        })
        super().__init__(*args, **kwargs)


class FRBase(Relationship, fields.List):
    def __init__(self, related_schema, *args, **kwargs):
        self.process_related_schema(related_schema)
        kwargs.update({
            'sqlfield': JSONB,
            'missing': [],
            'default': '[]',
            'validate': validators.must_not_be_empty
        })
        super().__init__(fields.String(), *args, **kwargs)

    def _deserialize(self, *args, **kwargs):
        return list(map(int, set(super()._deserialize(*args, **kwargs))))


class ForeignResources(FRBase):
    pass


class AutoSessionField:
    pass


class AutoImpliedForeignResource(ForeignResource):
    def __init__(self, related_schema, from_column, foreign_column, *args, **kwargs):
        self.from_column = from_column
        self.foreign_column = foreign_column
        kwargs.update({
            'fk': sql.ForeignKey(related_schema),
            'index': True,
            'read_only': True
        })
        super().__init__(related_schema, *args, **kwargs)


class AutoSessionForeignResource(ForeignResource):
    def __init__(self, related_schema, target_column, session_field, **kwargs):
        self.target_column = target_column
        self.session_field = session_field
        kwargs['required'] = True
        super().__init__(related_schema, **kwargs)


class AutoSessionOwner(AutoSessionField, ForeignResource):
    def __init__(self, **kwargs):
        super().__init__('actor.id', **kwargs)
        self.session_key = 'actor_id'


class AutoSessionSelectedWorkspace(AutoSessionField, ForeignResource):
    def __init__(self, **kwargs):
        super().__init__('workspace.id', **kwargs)
        self.session_key = 'workspace'


class AutoSessionPhone(AutoSessionField, fields.String):
    def __init__(self, **kwargs):
        kwargs.update({
            'sqlfield': sql.String(255),
            'required': True,
            'index': True
        })
        super().__init__(**kwargs)
        self.session_key = 'phone'


class AutoSessionEmail(AutoSessionField, fields.Email):
    def __init__(self, **kwargs):
        kwargs.update({
            'sqlfield': sql.String(255),
            'required': True,
            'index': True
        })
        super().__init__(**kwargs)
        self.session_key = 'email'


class AutoSessionStatus(AutoSessionField, fields.Int):
    def __init__(self, **kwargs):
        kwargs.update({
            'sqlfield': sql.Int,
            'required': True,
            'index': True
        })
        super().__init__(**kwargs)
        self.session_key = 'status'


def get_date(aware=False):
    if aware:
        return datetime.datetime.today().replace(tzinfo=tz.tzlocal())
    return datetime.datetime.today().date()


def get_datetime(aware=False):
    if aware:
        return datetime.datetime.now().replace(tzinfo=tz.tzlocal())
    return datetime.datetime.now()


def get_time():
    return datetime.datetime.today().time()


class AutoDateNow(fields.Date):
    # Take heed of Postgres' force-converting time zone
    # to UTC when using time zone awareness.
    def __init__(self, **kwargs):
        is_aware = kwargs.pop('is_aware', False)
        kwargs.update({
            'missing': partial(get_date, aware=is_aware),
            'validate': validators.must_be_empty,
            'sqlfield': sql.DateTime(timezone=True) if is_aware else sql.Date()
        })
        super().__init__(**kwargs)


class AutoDateTimeNow(fields.DateTime):
    def __init__(self, **kwargs):
        is_aware = kwargs.pop('is_aware', False)
        kwargs.update({
            'missing': partial(get_datetime, aware=is_aware),
            'validate': validators.must_be_empty,
            'sqlfield': sql.DateTime(timezone=is_aware)
        })
        super().__init__(**kwargs)


class AutoTimeNow(fields.Time):
    def __init__(self, **kwargs):
        kwargs.update({
            'missing': partial(get_time),
            'validate': validators.must_be_empty,
            'sqlfield': sql.Time()
        })
        super().__init__(**kwargs)
