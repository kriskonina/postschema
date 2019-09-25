from psycopg2.extras import Json


def clean_before_nested_write(schema_cls):
    def wrapped(payload, view_instance):
        view_instance._orig_cleaned_payload = payload.copy()
        iterable_fields = view_instance.iterable_fields
        payload[schema_cls.extends_on] = Json({
            fieldname: payload.pop(fieldname) for fieldname in payload.copy()
            if fieldname in schema_cls.child_fieldnames
        })
        return {
            key: Json(value) if key in iterable_fields else value
            for key, value in payload.items()
        }
    return wrapped


def translate_naive_nested(schema_cls, extraction_field):
    def wrapped(self, payload, **kwargs):
        try:
            nested_map = schema_cls._nested_select_stmts
        except AttributeError:
            # for non-inheriting resources nothing is done here
            return payload
        for payload_field in payload.get(extraction_field, []):
            if payload_field in nested_map:
                payload[extraction_field].remove(payload_field)
                payload[extraction_field].append(nested_map[payload_field])
        return payload
    return wrapped


def translate_naive_nested_to_dict(nested_map, extraction_field):
    def wrapped(self, payload, **kwargs):
        return {
            payload_field: nested_map.get(payload_field, payload_field)
            for payload_field in payload.get(extraction_field, [])
        }
    return wrapped


def escape_iterable(fieldnames):
    def wrapped(payload, view_instance, **kwargs):
        for fieldname in fieldnames:
            try:
                payload[fieldname] = Json(payload[fieldname])
            except KeyError:
                pass
        return payload
    return wrapped


# def translate_naive_fields(schema_cls):
#     def wrapped(self, payload, **kwargs):
#         nested_map = schema_cls._nested_select_stmts
#         for k, v in payload.items():
