import sqlalchemy as sql
from marshmallow import fields, Schema
from marshmallow.validate import Range, OneOf
from sqlalchemy.dialects.postgresql import JSONB

from .schema import PostSchema


class Pagination(Schema):
    page = fields.Integer(missing=1, validate=[Range(min=1)])
    limit = fields.Integer(missing=50, validate=[Range(min=1, max=10000)])
    order_by = fields.List(fields.String())
    order_dir = fields.String(missing="asc", validate=[OneOf(['desc', 'asc'])])


class Group(PostSchema):
    __tablename__ = 'group'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('group_id_seq'),
                        primary_key=True)
    scope = fields.String(sqlfield=sql.String(16), required=True, unique=True, default='self')
    name = fields.String(sqlfield=sql.String(16), required=True, unique=True, index=True)

    class Meta:
        get_by = ['id', 'scope']
        pagination_schema = Pagination


class Actor(PostSchema):
    __tablename__ = 'actor'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('actor_id_seq'),
                        read_only=True, primary_key=True)
    status = fields.Integer(sqlfield=sql.Integer, default='0', missing=0)
    name = fields.String(sqlfield=sql.String(16), required=True, index=True)
    email = fields.Email(sqlfield=sql.String(30), required=True, unique=True)
    token = fields.String(sqlfield=sql.String(30), required=True, index=True)
    groups = fields.List(fields.Integer(), sqlfield=JSONB, required=True, default='[]',
                   dump_only=True)
    details = fields.Dict(sqlfield=JSONB, missing='{}')

    async def before_post(self, request, data):
        data['status'] = 0
        data['groups'] = '[]'
        return data

    class Meta:
        create_views = False
        # excluded_ops = ['delete']
        list_by = ['name', 'email', 'id']
        get_by = ['id', 'status', 'name', 'email', 'token']
        exclude_from_updates = ['status', 'token', 'groups']
