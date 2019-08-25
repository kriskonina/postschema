import sqlalchemy as sql

# from postschema.bases.schemas import _schemas
from marshmallow import fields, utils
from postschema import validators
from sqlalchemy.dialects.postgresql import JSONB


class Set(fields.List):
    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return None
        if utils.is_collection(value):
            return list({self.inner._serialize(each, attr, obj, **kwargs) for each in value})
        return list({self.inner._serialize(value, attr, obj, **kwargs)})

    def _deserialize(self, *args, **kwargs):
        return list(set(super()._deserialize(*args, **kwargs)))


class Relationship:
    def process_related_schema(self, related_schema_arg):
        f_table, f_pk = related_schema_arg.split('.')
        self.target_table = {
            'name': f_table,
            'pk': f_pk
        }


class OneToMany(Relationship, fields.Field):
    '''We're not interested in either storing or deserializing this field'''
    def __init__(self, related_schema, *args, **kwargs):
        self.process_related_schema(related_schema)
        kwargs.update(dict(
            required=False,
            dump_only=True,
            load_only=True
        ))
        super().__init__(*args, **kwargs)


class ManyToMany(Relationship, Set):
    def __init__(self, related_schema, *args, **kwargs):
        self.process_related_schema(related_schema)
        kwargs.update(dict(
            sqlfield=JSONB,
            missing=[],
            default='[]',
            validate=validators.must_not_be_empty
        ))
        super().__init__(fields.Integer(), *args, **kwargs)


class OneToOne(Relationship, fields.Integer):
    def __init__(self, related_schema, *args, **kwargs):
        self.process_related_schema(related_schema)
        kwargs.update(dict(
            fk=sql.ForeignKey(self.target_table['pk']),
            index=True,
            dump_as=self.target_table['name']
        ))
        super().__init__(*args, **kwargs)
