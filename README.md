[![Build Status](https://travis-ci.org/kriskavalieri/postschema.svg?branch=master)](https://travis-ci.org/kriskavalieri/postschema)

# API
_class_ postschema.**PostSchema**(*, marshmallow.Schema...)
--
Base class used to define postgres joint schemas. 

It will first create the appropriate sqlalchemy's BaseModel, allowing for a seamless DB model propagation, then create the respective views.

Each definition of a field needs to include a reference to the corresponding sqlalchemy model column. The remaining arguments to sqlalchemy's _Columns_ class should be passed normally, i.e.:

    
    from postschema import PostSchema

    class Actor(PostSchema):
        __tablename__ = 'actor'
        id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('account_id_seq'), primary_key=True)
        name = fields.String(sqlfield=sql.String(16), required=True, index=True)

    


_class_ __Meta__

Options object for both _marshmallow.Schema_ and _postschema.PostSchema_

Example usage:

    class Meta:
        get_by = ["id", "name"]
        list_by = ["city"]
        fields = ("id", "email", "date_created")
        exclude = ("password", "secret_attribute")
        route_base = "myview"

Refer to [marshmallow documentation](https://marshmallow.readthedocs.io/en/3.0/api_reference.html#marshmallow.Schema.Meta) for more on _Meta_'s available options.

Postschema allows for the following attributes to be defined on top of it:
- `route_base`: URL base for the resource
- `create_views`: Boolean to indicate whether to create views from the schema definition.
- `order_by`: List of fields by which to order the results, unless otherwise specified (i.e. by pagination query object)
- `get_by`: List of fields to allow the HTTP requests to query, while `GET`-ting the schema resource. If found empty, the schema's primary key will be used as the only allowed query field.
- `list_by`: List of fields to allow the HTTP requests to query, while `GET`-ting the schema's **multiple** resources. If this field is left undefined, the default will take `get_by`'s value.
- `delete_by`: List of fields to allow the HTTP requests to query, while `DELETE`-ting the schema's **single/multiple** resources. If this field is left undefined, the default will be set to schema's primary key.
- `exclude_from_updates`: List of fields disallowed in update payload (`PUT`/`PATCH`)
- `excluded_ops`: List of 'operations' not available for the view wizard. These 'operations' include:
    - post
    - get
    - list
    - patch
    - put
    - delete
- `pagination_schema`: **`marshmallow.Schema`**-inheriting class used to deserialize the query pagination payload. Needs to define the following fields:
    * page (must be of `fields.Integer` type)
    * limit (must be of `fields.Integer` type)
    * order_dir (must be of `fields.String` type, values either `ASC` or `DESC`)
    * order_by (must be of `Array` type)

- `__table_args__`: Passed to SQLAlchemy model's Meta class


## Known issues
- Generally, there's no support for recursive nesting of resources. Although it's still possible to define them, the functionality of system is guaranteed to fail.