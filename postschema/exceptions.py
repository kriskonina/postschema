import ujson
from aiohttp import web

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
        super().__init__(
            body=failed_del_json,
            reason='Delete failed',
            content_type='application/json',
            *args, **kwargs)

