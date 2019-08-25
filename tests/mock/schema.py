import sqlalchemy as sql
from marshmallow import fields
from postschema import PostSchema, validators
from postschema.contrib import Actor
from postschema.fields import OneToMany, ManyToMany, OneToOne

from sqlalchemy.dialects.postgresql import JSONB


class Clinic(PostSchema):
    __tablename__ = 'clinic'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('account_id_seq'),
                        primary_key=True)
    name = fields.String(sqlfield=sql.String(16), required=True, index=True)
    units = OneToMany('unit.clinic_id', many=True, required=False, only=('id', 'name'))
    city = fields.String(sqlfield=sql.String(255), required=True, index=True)
    street = fields.String(sqlfield=sql.String(255), required=True, index=True)
    street_no = fields.String(sqlfield=sql.String(255), required=True, index=True)

    class Meta:
        get_by = ['id', 'name']
        list_by = ['city']
        __table_args__ = (
            sql.UniqueConstraint('name', 'city', name='_name_city_clinic_uq'),
        )


class Operator(Actor):
    phone = fields.String(sqlfield=sql.String(32), required=True)
    city = fields.String(sqlfield=sql.String(255), required=True, index=True)
    badges = fields.List(fields.String(), sqlfield=JSONB, required=False,
                         validate=validators.must_not_be_empty)

    class Meta:
        extends_on = 'details'
        get_by = ['phone', 'city', 'badges']
        list_by = ['city', 'phone']
        excluded_ops = ['delete']
        # exclude_from_updates = ['badges']


class Unit(PostSchema):
    __tablename__ = 'unit'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('unit_id_seq'),
                        primary_key=True)
    name = fields.String(sqlfield=sql.String(16), required=True, unique=True, index=True)
    clinic_id = OneToOne('clinic.id')
    operators = ManyToMany('actor.id')

    # @validates('clinic_id')
    # def idd(self, item):
    #     raise ValidationError('cliniccc')

    # @validates('name')
    # def namee(self, item):
    #     raise ValidationError('nameee')

    class Meta:
        get_by = ['id', 'name', 'clinic_id', 'operators']
        list_by = ['clinic_id', 'name']

        # __table_args__: sql.UniqueConstraint = (
        #     sql.UniqueConstraint('name', 'venue_id', name='_room_name_venue_uq'),
        # )
