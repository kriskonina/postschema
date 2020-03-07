from marshmallow import ValidationError
from .utils import Json


def must_not_be_empty(val):
    if not val:
        raise ValidationError('Data not provided')


def must_be_empty(val):
    if val:
        raise ValidationError('Unknown field')


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
        target_col = target_table['target_col']
        ids = ','.join(value)
        query = f"SELECT COALESCE(json_agg(id::text), '[]'::json) FROM \"{table_name}\" WHERE {target_col}=ANY('{{{ids}}}')"
        async with self.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query)
                except Exception:
                    self.app.error_logger.exception('Failed to execute FKs checking query',
                                                    query=cur.query.decode())
                    raise
                res = (await cur.fetchone())[0]
                invalid_pks = set(value) - set(res)
                if invalid_pks:
                    raise ValidationError(f'Foreign keys not found: {", ".join(map(str, invalid_pks))}')
    return validator_template, make_children_post_load


def autosession_field_validator(fieldname):
    async def _autosession_field_validator(self, val):
        if not val:
            return

        fieldval = self.declared_fields[fieldname]
        tablename = fieldval.target_table['name']
        target_col = fieldval.target_table['target_col']
        colname = fieldval.target_column
        sessval = self.session[fieldval.session_field]
        query = f'SELECT 1 FROM "{tablename}" WHERE {colname}=%s AND {target_col}=%s'

        async with self.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, [sessval, val])
                except Exception:
                    self.app.error_logger.exception('Failed to execute an FK checking query',
                                                    query=cur.query.decode())
                    raise
                if not await cur.fetchone():
                    raise ValidationError(f'Foreign key doesn\'t exist or no sufficient permissions held.')
    return _autosession_field_validator
