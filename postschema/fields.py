import sqlalchemy as sql

# from postschema.bases.schemas import _schemas
from marshmallow import fields
from postschema import validators
from sqlalchemy.dialects.postgresql import JSONB


class Set(fields.List):
    def _deserialize(self, *args, **kwargs):
        return list(set(super()._deserialize(*args, **kwargs)))


class Relationship:
    def process_related_schema(self, related_schema_arg):
        f_table, f_pk = related_schema_arg.split('.')
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


class ForeignResources(Relationship, fields.List):
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
        return list(set(super()._deserialize(*args, **kwargs)))
