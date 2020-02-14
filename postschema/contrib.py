import os

from marshmallow import Schema, fields
from marshmallow.validate import Range, OneOf


APP_MODE = os.environ.get('APP_MODE', 'test')


class Pagination(Schema):
    page = fields.Integer(missing=1, validate=[Range(min=1)])
    limit = fields.Integer(missing=50, validate=[Range(min=1, max=10000)])
    order_by = fields.List(fields.String())
    order_dir = fields.String(missing="asc", validate=[OneOf(['desc', 'asc'])])


class ListMembersFilter(Schema):
    username = fields.String()
    status = fields.Integer(default='0', missing=0)
    phone = fields.String()
    email = fields.String()
    roles = fields.List(fields.String())
    scope = fields.String()
    details = fields.Dict()
