import os
from collections import defaultdict as dd
from contextlib import suppress

from apispec import APISpec
from apispec.exceptions import DuplicateComponentNameError
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec.ext.marshmallow.common import resolve_schema_cls
from apispec.ext.marshmallow.openapi import OpenAPIConverter
from marshmallow import Schema

from .auth.perms import ALL_BASIC_OPERATIONS
from .scope import SCOPES

ALL_OPS = ALL_BASIC_OPERATIONS[:]
ALL_OPS.extend(["update", "read"])
LOCATIONS = ['body', 'header', 'query', 'form', 'path']

BODY_LOCATIONS = ['body', 'form']
PARAM_LOCATIONS = ['header', 'query', 'cookie']
CONTENT_TYPES = {
    'body': 'application/json',
    'form': 'application/x-www-form-urlencoded'
}


def schema_name_resolver(schema):
    schema_cls = resolve_schema_cls(schema)
    return schema_cls.__name__


converter = OpenAPIConverter('3.0.2', schema_name_resolver, {})


class AuxSpecBuilder:

    def _build_generic_op(self, method, summary_template, auxurl_root, permissions=None): # noqa
        name = self._auxview.__name__
        parent_tag = self._auxview.schema_cls.__name__

        if parent_tag == 'PrincipalActor':
            parent_tag = 'Actor'

        try:
            summary = getattr(self._auxview, method).summary
        except AttributeError:
            summary = summary_template

        extras = dd(lambda: dd(dict), {
            'summary': summary,
            'tags': [parent_tag],
            'parameters': []
        })

        if permissions:
            perm_ops = getattr(permissions, method)
            try:
                roles = list(perm_ops.keys())[0]
            except AttributeError:
                roles = perm_ops
            if isinstance(roles, str):
                roles = [roles]
            extras['security'] = [{'authed': roles}]

        op_doc = getattr(self._auxview, method).__doc__
        if op_doc:
            extras['description'] = op_doc

        for obj in self._auxschemas:
            location = obj['location']
            auxschema = obj['schema']

            if location in BODY_LOCATIONS:
                content_type = CONTENT_TYPES[location]
                extras['requestBody']['content'][content_type] = {
                    'schema': {
                        '$ref': f'#/components/schemas/{name}{location.title()}'
                    }
                }
                with suppress(DuplicateComponentNameError):
                    self.spec.components.schema(f'{name}{location.title()}', schema=auxschema)
            elif location in PARAM_LOCATIONS:
                schema_obj = converter.schema2parameters(auxschema, default_in=location)
                extras['parameters'].extend(schema_obj)
            else:
                for fieldname, fieldval in auxschema._declared_fields.items():
                    if fieldval.metadata['location'] == 'path':
                        path_obj = converter.field2parameter(fieldval, name=fieldname, default_in='path')
                        path_obj['required'] = True
                        extras['parameters'].append(path_obj)

        if not extras['parameters']:
            del extras['parameters']

        return {
            **extras,
            'responses': {
                '200': {
                    'description': 'Success',
                    'content': {'application/json': {}}
                }
            }
        }

    def _build_aux_post(self, auxurl_root, **kwargs):
        return self._build_generic_op('post', f'Create {self._auxview.__name__}', auxurl_root, **kwargs)

    def _build_aux_get(self, auxurl_root, **kwargs):
        return self._build_generic_op('get', f'Get {self._auxview.__name__}', auxurl_root, **kwargs)

    def _build_aux_delete(self, auxurl_root, **kwargs):
        return self._build_generic_op('delete', f'Delete {self._auxview.__name__}', auxurl_root, **kwargs)

    def _build_aux_put(self, auxurl_root, **kwargs):
        return self._build_generic_op('put', f'Update {self._auxview.__name__}', auxurl_root, **kwargs)

    def _build_aux_patch(self, auxurl_root, **kwargs):
        return self._build_generic_op('patch', f'Update {self._auxview.__name__}', auxurl_root, **kwargs)

    def _parse_ops(self, perms, auxurl_root, authed=False):
        if not perms:
            return
        ops = [op for op in ALL_OPS if hasattr(perms, op)]

        if 'update' in ops:
            ops.remove('update')
            ops.extend(['put', 'patch'])

        if 'read' in ops:
            ops.remove('read')
            ops.extend(['list', 'get'])

        permissions = perms if authed else None

        swagger_ops_raw = {op: getattr(self, f'_build_aux_{op}')(auxurl_root, permissions=permissions)
                           for op in ops}
        swagger_ops = {k: v for k, v in swagger_ops_raw.items() if v is not None}
        self.spec.path(
            auxurl_root,
            operations=swagger_ops
        )

    def _parse_authed_ops(self, public_perms, auxurl_root):
        pass

    def _build_aux_spec(self, auxview, auxurl_root):
        self.auxview = auxview
        public_perms = authed_perms = private_perms = None
        with suppress(AttributeError):
            public_perms = auxview.Public.permissions
        with suppress(AttributeError):
            authed_perms = auxview.Authed.permissions
        with suppress(AttributeError):
            private_perms = auxview.Private.permissions
        self._parse_ops(public_perms, auxurl_root)
        self._parse_ops(authed_perms, auxurl_root, authed=True)
        self._parse_ops(private_perms, auxurl_root, authed=True)
        del self._auxview

    @property
    def auxview(self):
        return self._auxview

    @auxview.setter
    def auxview(self, val):
        self._auxview = val
        self._auxschemas = [
            {
                'location': loc,
                'schema': getattr(val, f'{loc}_schema')
            }
            for loc in LOCATIONS if hasattr(val, f'{loc}_schema')
            and not isinstance(getattr(val, f'{loc}_schema'), property)
        ]


class APISpecBuilder(AuxSpecBuilder):
    def __init__(self, app, router):
        app.info_logger.debug("Building API specification")
        self.desc = app.app_description
        self.spec = APISpec(
            title=app.app_name,
            version=app.version,
            openapi_version="3.0.2",
            plugins=[MarshmallowPlugin()]
        )
        self.spec.components.security_scheme('authed', {
            'type': 'apiKey', 'name': 'postsession', 'in': 'cookie'
        })
        self.router = router
        self._auxview = None
        self._auxschemas = []

    def _build_schema_spec(self, schema_cls, view_maker, cls_view, aux_views, root_url=None):
        self._schema_cls = schema_cls
        meta = schema_cls.Meta
        declared_fields = schema_cls._declared_fields

        self.root_url = root_url or view_maker.base_resource_url
        self.pk_column_name = cls_view.pk_column_name

        public_cls = schema_cls.Public

        schema_name = schema_cls.__name__
        if schema_name == 'PrincipalActor':
            schema_name = 'Actor'

        schema_doc = schema_cls.__doc__
        if schema_doc:
            self.spec.tag({
                'name': schema_name,
                'description': schema_doc
            })

        excluded_ops = getattr(meta, 'excluded_ops', [])
        self.build_route_spec(schema_name, public_cls, declared_fields, excluded_ops)

        authed_cls = getattr(schema_cls, 'Authed', None)
        if authed_cls:
            self.build_route_spec(schema_name, authed_cls, declared_fields, excluded_ops, authed=True)

        private_ops = getattr(schema_cls, 'Private', None)
        if private_ops:
            self.build_route_spec(schema_name, private_ops, declared_fields, excluded_ops, authed=True)

        if aux_views:
            auxview_to_route = {
                v.__name__: k for k, v in schema_cls.__aux_routes__.items()
            }
            for auxview in aux_views:
                cls_view_name = auxview.__name__
                aux_route = auxview_to_route[cls_view_name]
                auxurl_root = os.path.join(self.root_url, aux_route)
                self._build_aux_spec(auxview, auxurl_root)

    def add_schema_spec(self, schema_cls, *args):
        meta = schema_cls.Meta

        if meta.create_views:
            self._build_schema_spec(schema_cls, *args)

    def build_spec(self):
        scopes = []
        for scopename, scope_inst in SCOPES.items():
            scope = f'{scopename}Scope'
            scopes.append(scope)
            self.spec.components.schema(scope, schema=scope_inst)

        raw_spec = dict(self.spec.to_dict())
        details = {'oneOf': [{'$ref': f'#/components/schemas/{scope}'} for scope in scopes]}
        raw_spec['components']['schemas']['ActorPost']['properties']['details'] = details
        raw_spec['components']['schemas']['ActorGet']['properties']['details'] = details
        raw_spec['components']['schemas']['ActorPut']['properties']['details'] = details
        raw_spec['components']['schemas']['ActorPatch']['properties']['details'] = details
        raw_spec['info']['description'] = self.desc
        return raw_spec

    def build_route_spec(self, name, perm_cls, declared_fields, excluded_ops, authed=False):
        try:
            perms = perm_cls.permissions.__dict__
        except AttributeError:
            return

        if hasattr(perm_cls.permissions, 'allow_all'):
            perms = ['get', 'list', 'put', 'patch', 'delete', 'post']

        listed_ops = [op for op in ALL_OPS if op in perms and op not in excluded_ops]

        if 'read' in listed_ops:
            listed_ops.remove('read')
            listed_ops.extend(['list', 'get'])

        if 'update' in listed_ops:
            listed_ops.remove('update')
            listed_ops.extend(['put', 'patch'])

        if 'list' in listed_ops:
            listed_ops.remove('list')
            list_swagger_op = self.build_list_route(name, declared_fields, perm_cls, authed=authed)
            if list_swagger_op:
                self.spec.path(
                    self.root_url, operations={
                        'options': list_swagger_op
                    }
                )

        swagger_ops_raw = {op: getattr(self, f'build_{op}_route')
                           (name, declared_fields, perm_cls, authed=authed)
                           for op in listed_ops}
        swagger_ops = {k: v for k, v in swagger_ops_raw.items() if v is not None}
        if swagger_ops:
            self.spec.path(
                self.root_url,
                operations=swagger_ops
            )

    def build_common_route(self, tagname, schema_name, schema, summary, perm_cls, op, authed=False):
        extras = {
            'summary': summary,
            'tags': [tagname]
        }

        perm_ops = getattr(perm_cls.permissions, op, None)
        if authed and perm_ops:
            try:
                roles = list(perm_ops.keys())[0]
            except AttributeError:
                roles = perm_ops
            if isinstance(roles, str):
                roles = [roles]
            extras['security'] = [{'authed': roles}]

        self.spec.components.schema(schema_name, schema=schema)

        return {
            **extras,
            'responses': {
                '200': {
                    'description': 'Success',
                    'content': {'application/json': {}}
                }
            },
            'requestBody': {
                'content': {
                    'application/json': {
                        'schema': {
                            '$ref': f'#/components/schemas/{schema_name}'
                        }
                    }
                }
            }
        }

    def build_get_route(self, name, declared_fields, perm_cls, authed=False):
        try:
            get_by = perm_cls.get_by
        except AttributeError:
            get_by = [self.pk_column_name]
        schema_name = f'{name}Get'

        schema = type(schema_name, (Schema, ), {k: v for k, v in declared_fields.items() if k in get_by})
        return self.build_common_route(name, schema_name, schema, f'Return {name}', perm_cls, 'get', authed)

    def build_list_route(self, origname, declared_fields, perm_cls, authed=False):
        schema_name = f'{origname}List'
        try:
            list_by = perm_cls.list_by
        except AttributeError:
            try:
                list_by = perm_cls.get_by
            except AttributeError:
                list_by = [self.pk_column_name]

        schema = type(schema_name, (Schema, ), {k: v for k, v in declared_fields.items() if k in list_by})
        return self.build_common_route(origname, schema_name, schema, f'Lists {origname}',
                                       perm_cls, 'list', authed)

    def build_post_route(self, origname, declared_fields, perm_cls, authed=False):
        # remove primary key, if present
        pk_col = list(self._schema_cls._model.__table__.primary_key.columns._data)[0].split('.')[-1]
        if pk_col in declared_fields:
            declared_fields = declared_fields.copy()
            declared_fields.pop(pk_col, None)
        schema_name = f'{origname}Post'
        schema = type(schema_name, (Schema, ), {k: v for k, v in declared_fields.items()})
        return self.build_common_route(origname, schema_name, schema, f'Create new {origname}',
                                       perm_cls, 'post', authed)

    def build_delete_route(self, origname, declared_fields, perm_cls, authed=False):
        schema_name = f'{origname}Delete'
        try:
            delete_by = perm_cls.delete_by
        except AttributeError:
            delete_by = [self.pk_column_name]
        schema = type(schema_name, (Schema, ), {k: v for k, v in declared_fields.items() if k in delete_by})
        return self.build_common_route(origname, schema_name, schema, f'Delete {origname}',
                                       perm_cls, 'delete', authed)

    def build_put_route(self, origname, declared_fields, perm_cls, authed=False):
        schema_name = f'{origname}Put'
        try:
            update_by = perm_cls.get_by
        except AttributeError:
            update_by = [self.pk_column_name]

        schema = type(schema_name, (Schema, ), {k: v for k, v in declared_fields.items() if k in update_by})
        return self.build_common_route(origname, schema_name, schema, f'Replace {origname} details',
                                       perm_cls, 'update', authed)

    def build_patch_route(self, origname, declared_fields, perm_cls, authed=False):
        schema_name = f'{origname}Patch'
        try:
            update_by = perm_cls.get_by
        except AttributeError:
            update_by = [self.pk_column_name]

        schema = type(schema_name, (Schema, ), {k: v for k, v in declared_fields.items() if k in update_by})
        return self.build_common_route(origname, schema_name, schema, f'Update {origname} details',
                                       perm_cls, 'update', authed)
