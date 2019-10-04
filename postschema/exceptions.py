import ujson
from aiohttp import web

failed_create_json = ujson.dumps({
    "error": "Resource create not complete"
})
failed_update_json = ujson.dumps({
    "error": "Resource update not complete"
})
failed_del_json = ujson.dumps({
    "error": "Resource deletion not complete"
})


class ValidationError(web.HTTPError):
    status_code = 422

    def __init__(self, json, *args, **kwargs):
        super().__init__(
            body=ujson.dumps(json),
            reason='Request payload invalid',
            content_type='application/json',
            *args, **kwargs)


class CreateFailed(web.HTTPError):
    status_code = 422

    def __init__(self, *args, **kwargs):
        super().__init__(
            body=failed_create_json,
            reason='Create failed',
            content_type='application/json',
            *args, **kwargs)


class UpdateFailed(web.HTTPError):
    status_code = 422

    def __init__(self, *args, **kwargs):
        super().__init__(
            body=failed_update_json,
            reason='Update failed',
            content_type='application/json',
            *args, **kwargs)


class DeleteFailed(web.HTTPError):
    status_code = 422

    def __init__(self, *args, **kwargs):
        body = kwargs.pop('body', None)
        payload = ujson.dumps({
            'error': body
        }) if body else failed_del_json
        super().__init__(
            body=payload,
            reason='Delete failed',
            content_type='application/json',
            *args, **kwargs)
