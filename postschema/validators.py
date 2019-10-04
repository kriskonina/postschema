from marshmallow import ValidationError
from psycopg2.extras import Json


def must_not_be_empty(val):
    if not val:
        raise ValidationError('Data not provided')


def adjust_children_field(fieldname):
    def make_children_post_load(self, data, **k):
        if self.partial or self._use is None:
            # in case of validating the `select` part of the total payload
            return data
        data[fieldname] = Json(data[fieldname])
        return data

    async def validator_template(self, value):
        if self.is_read_schema or not value or not value[0]:
            return
        target_table = self.declared_fields[fieldname].target_table
        table_name = target_table['name']
        pk = target_table['pk']
        ids = ','.join(value)
        query = f"SELECT COALESCE(json_agg(id::text), '[]'::json) FROM {table_name} WHERE {pk}=ANY('{{{ids}}}')"
        async with self.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query)
                except Exception as exc:
                    print(cur.query.decode())
                    # TODO: sentry
                    raise exc
                res = (await cur.fetchone())[0]
                invalid_pks = set(value) - set(res)
                if invalid_pks:
                    raise ValidationError(f'Foreign keys not found: {", ".join(map(str, invalid_pks))}')
    return validator_template, make_children_post_load
