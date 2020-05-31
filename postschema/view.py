import weakref

from contextlib import suppress

from aiohttp import web
from aiojobs.aiohttp import spawn
from marshmallow import ValidationError

from . import exceptions as post_exceptions
from .utils import json_response
from .view_bases import AuxViewMeta


class ViewsTemplate:
    async def get(self):
        # validate the query payload
        empty_payload_exc = None
        try:
            cleaned_payload = await self._validate_singular_payload()
        except web.HTTPError as exc:
            empty_payload_exc = exc
            cleaned_payload = {}

        if not cleaned_payload:
            if self.request.session.is_authed and hasattr(self.schema.Meta, 'default_get_critera'):
                # This is the only condition under which we allow
                # there to be either no payload or for it to be empty
                cleaned_payload = self.schema.Meta.default_get_critera(self.request)
            else:
                if empty_payload_exc is not None:
                    raise empty_payload_exc
                raise post_exceptions.ValidationError({'payload': ['Empty payload is not accepted']})

        self.cleaned_payload_keys = list(cleaned_payload) or []
        get_query = dict(self.request.query)
        base_stmt = await self._parse_select_fields(
            get_query, self._prepare_get_query) or self.get_query_stmt

        if hasattr(self.schema, 'get'):
            return await self.schema.get(self.request, cleaned_payload)

        if hasattr(self.schema, 'before_get'):
            cleaned_payload = await self.schema.before_get(self.request, cleaned_payload) or cleaned_payload

        return await self._fetch(cleaned_payload, base_stmt)

    async def list(self):
        # validate the query payload
        try:
            cleaned_payload = await self._validate_singular_payload()
        except ValidationError as vexc:
            # Take out only those keys from the error bag that belong to the nested schema
            # (aka the kid) and rerun the validation only on them using that nested schema
            payload = vexc.valid_data
            nested_fieldnames = self.schema.child_fieldnames
            for fieldname in vexc.messages:
                if fieldname in nested_fieldnames:
                    payload[fieldname] = vexc.data[fieldname]
            if not payload:
                raise post_exceptions.ValidationError(vexc.messages)
            # parent_nested_schema = self.schema.parent._declared_fields[self.schema.extends_on].nested
            # cleaned_payload = await self._validate_singular_payload(
            #     payload=payload, schema=parent_nested_schema)

        self.cleaned_payload_keys = list(cleaned_payload) or []

        # validate the GET payload, if present
        get_query_raw = self.request.query
        get_query = dict(get_query_raw)

        if 'order_by' in get_query:
            unified_order_field = get_query_raw.getall('order_by')
            if ',' in unified_order_field[0]:
                unified_order_field = unified_order_field[0].split(',')
            get_query['order_by'] = unified_order_field

        base_stmt = await self._parse_select_fields(
            get_query, self._prepare_list_query) or self.list_query_stmt

        pagination_data = await self._validate_singular_payload(
            get_query or {}, self.pagination_schema, 'query')
        limit = pagination_data['limit']
        page = pagination_data['page'] - 1
        offset = page * limit
        orderby = ','.join(f'"{self.tablename}".{field}' for field in pagination_data['order_by'])
        orderhow = pagination_data['order_dir'].upper()

        if hasattr(self.schema, 'before_list'):
            cleaned_payload = await self.schema.before_list(self.request, cleaned_payload) or cleaned_payload

        query = base_stmt.format(
            limit=limit,
            offset=offset,
            orderby=orderby,
            orderhow=orderhow)

        return await self._fetch(cleaned_payload, query)

    async def post(self):
        # get the payload
        payload = await self.payload
        with suppress(AttributeError):
            # allow custom schema method to modify the payload before validation
            payload = await self.schema.procure_payload(self.request, payload)

        cleaned_payload = await self._validate_singular_payload(payload=payload)
        cleaned_payload = self._clean_write_payload(cleaned_payload)

        if hasattr(self.schema, 'before_post'):
            cleaned_payload = await self.schema.before_post(
                weakref.proxy(self), self.request, cleaned_payload) or cleaned_payload

        insert_query = self._render_insert_query(cleaned_payload)

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await self.request.app.commons.execute(cur, insert_query, cleaned_payload)
                res = await cur.fetchone()
                if res is None:
                    if self.request.session:
                        # Most likely a cross workspace insert or non-existent FK
                        raise web.HTTPConflict(reason='Illegal cross workspace insert or non-existent FK supplied')

                    self.request.app.access_logger = self.request.app.access_logger.bind(msg_context={
                        'query': cur.query.decode()
                    })
                    raise post_exceptions.CreateFailed()

        if hasattr(self.schema, 'after_post'):
            await spawn(self.request, self.schema.after_post(self.request, cleaned_payload, res[0]))

        return json_response({self.pk_column_name: res[0]})

    async def put(self):
        cleaned_select, cleaned_payload = await self._clean_update_payload()
        cleaned_payload = self._clean_write_payload(cleaned_payload)

        if hasattr(self.schema, 'before_update'):
            cleaned_payload = await self.schema.before_update(weakref.proxy(self), self.request, cleaned_payload, cleaned_select) \
                or cleaned_payload

        query_raw = self.update_query_stmt
        try:
            extended_fields = self.schema._extended_fields_values
        except AttributeError:
            extended_fields = {}
        query_with_where, query_values = self._whereize_query(cleaned_select, query_raw, extended_fields)
        updates = []

        for payload_k, payload_v in cleaned_payload.items():
            updates.append(f"{payload_k}=%({payload_k})s")
            query_values[payload_k] = payload_v

        query = query_with_where.format(updates=','.join(updates))

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await self.request.app.commons.execute(cur, query, query_values, envelope='payload')
                res = await cur.fetchone()
                if not res or not res[0]:
                    self.request.app.access_logger = self.request.app.access_logger.bind(msg_context={
                        'query': cur.query.decode()
                    })
                    raise post_exceptions.UpdateFailed()

        if hasattr(self.schema, 'after_put'):
            await spawn(self.request,
                        self.schema.after_put(self.request, cleaned_select, cleaned_payload, res))

        return json_response({'updated': res[0]})

    async def patch(self): # noqa
        cleaned_select, cleaned_payload = await self._clean_update_payload()
        cleaned_payload = self._clean_write_payload(cleaned_payload)

        if hasattr(self.schema, 'patch'):
            return await self.schema.patch(self.request, cleaned_select, cleaned_payload)

        if hasattr(self.schema, 'before_update'):
            cleaned_payload = await self.schema.before_update(
                weakref.proxy(self), self.request, cleaned_payload, cleaned_select) or cleaned_payload

        query_raw = self.update_query_stmt
        try:
            extended_fields = self.schema._extended_fields_values
        except AttributeError:
            extended_fields = {}
        query_with_where, query_values = self._whereize_query(cleaned_select, query_raw, extended_fields)
        updates = []

        for payload_k, payload_v in cleaned_payload.items():
            if payload_k in self.mergeable_fields:
                updates.append(f"{payload_k}=jsonb_merge_deep({payload_k}, %({payload_k})s)")
            else:
                updates.append(f"{payload_k}=%({payload_k})s")
            query_values[payload_k] = payload_v

        query = query_with_where.format(updates=','.join(updates))

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await self.request.app.commons.execute(cur, query, query_values, envelope='payload')
                res = await cur.fetchone()
                if not res or not res[0]:
                    self.request.app.access_logger = self.request.app.access_logger.bind(msg_context={
                        'query': cur.query.decode()
                    })
                    raise post_exceptions.UpdateFailed()

        if hasattr(self.schema, 'after_patch'):
            await spawn(self.request,
                        self.schema.after_patch(self.request, cleaned_select, cleaned_payload, res))

        return json_response({'updated': res[0]})

    async def delete(self): # noqa
        cleaned_payload = await self._validate_singular_payload()

        # validate the GET payload, if present
        get_query_raw = self.request.query
        get_query = dict(get_query_raw)
        # for now, we only support 'deep' param, which denotes that only M2M references of the given model
        # are to be deleted together with the parent.
        # TODO: specify which chidren
        deep_delete = get_query.get('deep', False) or False

        if not cleaned_payload:
            raise post_exceptions.ValidationError({
                '_schema': [
                    "Payload cannot be empty"
                ]
            })

        if hasattr(self.schema, 'delete'):
            return await self.schema.delete(self.request, cleaned_payload)

        if hasattr(self.schema, 'before_delete'):
            cleaned_payload = await self.schema.before_delete(self.request, cleaned_payload) \
                or cleaned_payload

        delete_query = self.delete_query_stmt
        if deep_delete or self.schema._m2m_cherrypicks:
            delete_query = self.delete_deep_query_stmt

        try:
            extended_fields = self.schema._extended_fields_values
        except AttributeError:
            extended_fields = {}

        query, query_values = self._whereize_query(cleaned_payload, delete_query, extended_fields, in_delete=True)

        deleted_resource_instances = 0
        deleted_m2m_refs = 0

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                async with cur.begin():
                    try:
                        await self.request.app.commons.execute(cur, query, query_values)
                    except Exception as exc:
                        self.request.app.error_logger.exception('Failed to execute a delete query',
                                                                query=cur.query.decode())
                        raise exc

                    # fetch the query result under the same transaction, before commiting
                    res = await cur.fetchone()
                    if not res or not res[0]:
                        self.request.app.access_logger = self.request.app.access_logger.bind(msg_context={
                            'query': cur.query.decode()
                        })
                        raise post_exceptions.DeleteFailed()
                    deleted_ids = res[0]

                    try:
                        deleted_resource_instances = len(deleted_ids)
                    except TypeError:
                        deleted_resource_instances = deleted_ids

                    # post-delete hooks, only for the m2m relations
                    if self.schema._m2m_cherrypicks:
                        m2m_query = self.cherrypick_m2m_stmts.format(deleted_pks=f'array{deleted_ids}')
                        try:
                            await self.request.app.commons.execute(cur, m2m_query)
                        except Exception as exc:
                            self.request.app.error_logger.exception(
                                'Failed to execute the deletion of M2M dependencies', query=m2m_query)
                            await cur.execute('rollback;')
                            raise exc

                        res = await cur.fetchone()
                        if not res or not res[0]:
                            await cur.execute('rollback;')
                            self.request.app.error_logger.exception(
                                'Failed to delete the resource\'s M2M dependencies', query=m2m_query)
                            raise post_exceptions.DeleteFailed(body="Failed to delete the M2M dependencies")
                        deleted_m2m_refs = res[0]

        if hasattr(self.schema, 'after_delete'):
            await spawn(self.request, self.schema.after_delete(self.request, cleaned_payload, res))

        return json_response({
            'deleted_resource_records': deleted_resource_instances,
            'deleted_m2m_refs': deleted_m2m_refs
        })


class AuxView(metaclass=AuxViewMeta):
    pass
