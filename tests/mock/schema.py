import re


import sqlalchemy as sql
from aiohttp import web
from marshmallow import fields, validate, validates, ValidationError
from postschema import PostSchema, validators
from postschema.contrib import ActorRoot
from postschema.fields import ForeignResources, ForeignResource, AutoImpliedForeignResource
from postschema.utils import json_response
from postschema.view import AuxView

from sqlalchemy.dialects.postgresql import JSONB


class PlainResource(PostSchema):
    __tablename__ = 'plainresource'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('plainresource_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(30), index=True)
    unique_field = fields.String(sqlfield=sql.String(16), unique=True)
    required_field = fields.String(sqlfield=sql.String(30), required=True)
    integer = fields.Integer(sqlfield=sql.Integer)
    email = fields.Email(sqlfield=sql.String(60))
    strlen = fields.String(sqlfield=sql.String(10), validate=validate.Length(min=5))
    intrange = fields.Integer(sqlfield=sql.Integer, validate=validate.Range(min=5, max=10))
    choice = fields.String(sqlfield=sql.String(1), validate=validate.OneOf(choices=['a', 'b']))
    date = fields.Date(sqlfield=sql.Date)
    list = fields.List(fields.String, sqlfield=JSONB)

    class Meta:
        exclude_from_updates = ['unique_field', 'integer']
        excluded_ops = ['put']
        get_by = ['id', 'name']
        list_by = ['name', 'email']
        delete_by = ['name']
        __table_args__ = (
            sql.UniqueConstraint('name', 'email', name='_name_email_plain_uq'),
        )


class Product(PostSchema):
    __tablename__ = 'product'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('product_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(50), unique=True)
    # with unique=True it equal a OneToOne relation
    descr = ForeignResource('desc.id', unique=True, required=True)
    producer = ForeignResource('producer.id', required=True)

    class Meta:
        get_by = ['id', 'name', 'descr', 'producer']


class Description(PostSchema):
    __tablename__ = 'desc'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('desc_id_seq'),
                        read_only=True, primary_key=True)
    text = fields.String(sqlfield=sql.String(50))

    class Meta:
        route_base = 'desc'
        get_by = ['id', 'text']


class Store(PostSchema):
    __tablename__ = 'store'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('store_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(50), unique=True)
    distributors = ForeignResources('dist.id')

    class Meta:
        get_by = ['id', 'name', 'distributors']


class Producer(PostSchema):
    __tablename__ = 'producer'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('producer_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(50), unique=True)
    distributors = ForeignResources('dist.id')

    class Meta:
        get_by = ['name', 'id']


class Distributor(PostSchema):
    __tablename__ = 'dist'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('dist_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(50), unique=True)
    meta = fields.String(sqlfield=sql.String(2))

    class Meta:
        route_base = 'dist'

        list_by = ['meta']
        delete_by = ['meta']


class Operator(ActorRoot):
    __tablename__ = 'operator'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('operator_id_seq'),
                        read_only=True, primary_key=True)
    phone = fields.String(sqlfield=sql.String(32), required=True)
    city = fields.String(sqlfield=sql.String(255), required=True, index=True)
    badges = fields.List(fields.String(), sqlfield=JSONB, required=False,
                         validate=validators.must_not_be_empty)

    class Meta:
        get_by = ['name', 'id', 'phone', 'city', 'badges']
        list_by = ['name', 'id', 'email', 'city', 'phone', 'badges']
        excluded_ops = ['delete']
        # exclude_from_updates = ['badges']


class Staff(ActorRoot):
    __tablename__ = 'staff'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('staff_id_seq'),
                        read_only=True, primary_key=True)
    role = fields.String(sqlfield=sql.String(32), required=True)

    class Meta:
        get_by = ['role', 'email']
        excluded_ops = ['delete']


class DrawNumberView(AuxView):
    id = fields.Integer(location='path')
    minint = fields.Integer(location='body')
    maxint = fields.Integer(location='body')
    query_param1 = fields.Integer(location='query')
    query_param2 = fields.List(fields.Int(), location='query')
    header_param1 = fields.Int(location='header')
    header_param2 = fields.List(fields.Int(), location='header')

    @validates('maxint')
    def maxintval(self, item):
        if item < 10:
            raise ValidationError('Lesser than 10')

    @validates('maxint')
    def maxintval2(self, item):
        if item > 50:
            raise ValidationError('Greater than 50')

    @validates('id')
    def idval(self, item):
        if item > 100:
            raise ValidationError('Id value too large')

    async def patch(self):
        payload = await self.validate_payload()
        if not payload:
            return json_response('empty')
        q_payload = await self.validate_query()
        h_payload = await self.validate_header()
        if q_payload:
            return json_response(q_payload['query_param2'])
        if h_payload:
            return json_response(h_payload['header_param2'])
        return json_response('ok')


class SimpleAuxView(AuxView):
    async def get(self):
        return json_response('ok')


class CustomOpsResource(PostSchema):
    __tablename__ = 'customop'
    __aux_routes__ = {
        '/simpleaux_path/': SimpleAuxView,
        '{id}/insert_random_number/': DrawNumberView
    }
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('customop_id_seq'),
                        read_only=True, primary_key=True)
    address = fields.String(sqlfield=sql.String(255), required=True)
    read_only_field = fields.String(sqlfield=sql.String(40), read_only=True)
    custom_getter = fields.String(sqlfield=sql.String(100), missing="default_value")
    state = fields.String(sqlfield=sql.String(20), read_only=True)

    @validates('address')
    def val(self, item):
        if not re.search(r'\d+', item):
            raise ValidationError("This field needs to contain numbers")

    async def before_post(self, request, data):
        data['read_only_field'] = 'initial_val'
        addr = data.get('address', '')
        if 'Washington' in addr:
            data['state'] = 'DC'
        elif 'Seattle' in addr:
            data['state'] = 'Oregon'
        elif addr:
            data['state'] = 'N/A'
        return data

    async def after_post(self, request, payload, res):
        payload = 'sth_else_modified'
        async with request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"UPDATE customop SET read_only_field='{payload}' WHERE id={res}")

    async def before_get(self, request, cleaned_payload):
        '''This is used to modify the search params'''
        if cleaned_payload.get('read_only_field', '') == 'secret_requested':
            raise web.HTTPForbidden(reason='No access')
        return cleaned_payload

    async def before_list(self, *args):
        return await self.before_get(*args)

    async def before_update(self, *args):
        '''Ensure no read-only fields are updated'''
        return await self.before_post(*args)

    async def after_put(self, request, select_payload, update_payload, res):
        values = {'address': update_payload.pop('address'), 'newval': '_put'}
        query = f"UPDATE customop SET custom_getter=custom_getter || %(newval)s WHERE address=%(address)s"
        async with request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, values)

    async def after_patch(self, request, select_payload, update_payload, res):
        values = {'address': update_payload.pop('address'), 'newval': '_patch'}
        query = f"UPDATE customop SET custom_getter=custom_getter || %(newval)s WHERE address=%(address)s"
        async with request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, values)

    class Meta:
        route_base = 'customop'
        get_by = ['id', 'address', 'read_only_field', 'state', 'custom_getter']


class Barn(PostSchema):
    __tablename__ = 'barn'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('barn_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(50), unique=True)

    class Meta:
        get_by = ['id', 'name']


class Fodder(PostSchema):
    __tablename__ = 'fodder'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('fodder_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(50), unique=True)


class Box(PostSchema):
    __tablename__ = 'box'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('box_id_seq'),
                        read_only=True, primary_key=True)
    barn = ForeignResource('barn.id')
    fodder = ForeignResource('fodder.id')

    class Meta:
        get_by = ['id', 'barn', 'fodder']


class Requirements(PostSchema):
    __tablename__ = 'req'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('req_id_seq'),
                        read_only=True, primary_key=True)
    diet = fields.String(sqlfield=sql.Text)
    hygiene = fields.String(sqlfield=sql.Text)

    class Meta:
        route_base = 'req'


class Species(PostSchema):
    __tablename__ = 'species'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('species_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(50), unique=True)
    reqs = ForeignResource('req.id')

    class Meta:
        get_by = ['reqs']


class Animal(PostSchema):
    __tablename__ = 'animal'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('animal_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(50), unique=True)
    box = ForeignResource('box.id')
    species = ForeignResource('species.id')
    barn = AutoImpliedForeignResource('barn.id', from_column='box', foreign_column='barn')
    fodder = AutoImpliedForeignResource('fodder.id', from_column='box', foreign_column='fodder')
    reqs = AutoImpliedForeignResource('req.id', from_column='species', foreign_column='reqs')

    class Meta:
        get_by = ['id', 'name', 'box', 'species', 'barn', 'fodder', 'reqs']
