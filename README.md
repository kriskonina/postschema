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

    

---
_class_ __Meta__

Options object for both _marshmallow.Schema_ and _postschema.PostSchema_

Example usage:

    class Meta:
        create_views = True
        excluded_ops = ['get', 'post']
        exclude_from_updates = ['read_only_field']
        route_base = "myview"
        enable_extended_search = True

        def default_get_critera(request):
            return {'owner': request.session.actor_id}

Refer to [marshmallow documentation](https://marshmallow.readthedocs.io/en/3.0/api_reference.html#marshmallow.Schema.Meta) for more on _Meta_'s available options.

Postschema allows for the following attributes to be defined on top of it:
- `route_base`: URL base for the resource
- `create_views`: Boolean indicating whether to create views from the schema definition.
- `order_by`: List of fields by which to order the results, unless otherwise specified (i.e. by pagination query object)
- `exclude_from_updates`: List of fields disallowed in update payload (`PUT`/`PATCH`)
- `excluded_ops`: List of 'operations' not available for the view wizard. These 'operations' include:
    - post
    - get
    - list
    - patch
    - put
    - delete
- `enable_extended_search`: Boolean to flag the current schema as subject to extended search. This allows the preprocessor to prepare query parts for later injection as needed.
- `pagination_schema`: **`marshmallow.Schema`**-inheriting class used to deserialize the query pagination payload. Needs to define the following fields:
    * page (must be of `fields.Integer` type)
    * limit (must be of `fields.Integer` type)
    * order_dir (must be of `fields.String` type, values either `ASC` or `DESC`)
    * order_by (must be of `Array` type)
- `default_get_critera`: A callable taking one positional argument - an aiohttp Request object. Expected to return a dictionary including query criteria
for the GET operation if no query payload is provided.

- `__table_args__`: Passed to SQLAlchemy model's Meta class

---
_class_ __Public__, _class_ __Authed__, _class_ __Private__

A group of auth mini-framework classes, used to describe the allowed operation and the resources they return. 

The three classes leverage the notion of a _role_. 5 roles are defined as default:
- (All roles)
- Admin
- Owner
- Manager
- Staff

In addition, custom roles can be added by supplying an iterable to `setup_postchema`'s `roles` keyword argument. 

roles live under `app.config.roles`.


All three classes can describe the following attributes:
- `get_by`: List of fields to allow the HTTP requests to query, while `GET`-ting the schema resource. If found empty, the schema's primary key will be used as the only allowed query field.
- `list_by`: List of fields to allow the HTTP requests to query, while `GET`-ting the schema's **multiple** resources. If this field is left undefined, the default will take `get_by`'s value.
- `delete_by`: List of fields to allow the HTTP requests to query, while `DELETE`-ting the schema's **single/multiple** resources. If this field is left undefined, the default will be set to schema's primary key.

In addition, __Private__ and __Authed__ classes accept the following attributes:
- `verified_email` List of operations requiring that the requesting account's email address is verified
- `verified_phone` List of operations requiring that the requesting account's phone number is verified

---
_class_ __Public__

Class describing access rules to resources listed as public. One extra attribute can be defined on this class:
- `disallow_authed` List of operations defined under Public.permissions to be marked inaccessible should the request be authenticated
- `forced_logout` Boolean denoting whether or not each authed request should be stripped off its session token upon requesting resource operation
listed under `Public.disallow_authed`.

To control which operations are allowed for such resource, define a `permissions` subclass, on which the following attributes are allowed:
- `allow_all` Boolean denoting whether to open all operations to public access
- `<operation_name>` An empty dictionary with valid operation name as a key. E.g.

      class Public:
         ...
         class permissions:
            get: {}
            post: {}
---
_class_ __Authed__

Class describing access rules to resources with whom only the authenticated actors can interact. Similarly to `Public`, define `permissions` subclass to define the following allowed attributes:
- `allow_all` A list of valid roles to apply to all operations
- `<operation_name>` A list or iterable containing valid role names. Requesting actors with these roles will be allowed to perform requested actions. Example:

---
_class_ __Private__

Class describe private access rules to private resources, i.e. the ones with a clearly designated owner/administrator. Use a `permissions` subclass desired behaviour for each operation:
- `<operation_name>` A dictionary mapping, rougly, roles to private resource condition statement. The key can be of string type, its value found in the `app.config.roles`, or a tuple of such. Example:

      class Private:
         ...
         class permissions:
            list: {
                ('Admin', 'Owner'): 'self.id = auth.actor_id',
                'Staff': 'foreign_table.staffer = auth.actor_id'
            },
            get: {
                '*': 'foreign_table.workspace -> auth.workspaces'
            }


## TODO:
- !!move workspace membership into actor table
- adopt/refine security measures
- request rate throttling as a default middleware + filtering
- configurable request rate throttling per endpoint
- full support for nested schemas
- list/get ops should accept arrays as filter arguments (inclusion checkup)
- auto insert/update date(time) field
- extend session context with arbitrary fields