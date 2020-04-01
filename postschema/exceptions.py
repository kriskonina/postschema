from aiohttp import web
from .utils import dumps

failed_create_json = dumps({
    "error": "Resource create not complete"
})
failed_update_json = dumps({
    "error": "Resource update not complete"
})
failed_del_json = dumps({
    "error": "Resource deletion not complete"
})


class HTTPShieldedResource(web.HTTPClientError):
    status_code = 423

    def __init__(self, json, *args, **kwargs):
        super().__init__(
            body=dumps(json),
            reason='Resource locked',
            content_type='application/json',
            *args, **kwargs)


class ValidationError(web.HTTPError):
    status_code = 422

    def __init__(self, json, *args, **kwargs):
        super().__init__(
            body=dumps(json),
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
        payload = dumps({
            'error': body
        }) if body else failed_del_json
        super().__init__(
            body=payload,
            reason='Delete failed',
            content_type='application/json',
            *args, **kwargs)


class WorkspaceAdditionFailed(web.HTTPError):
    status_code = 400

    def __init__(self, *args, **kwargs):
        super().__init__(
            reason='Failed to add new actor to workspace table',
            content_type='application/json')


class HandledInternalError(web.HTTPInternalServerError):
    def __init__(self, *args, **kwargs):
        kwargs['reason'] = 'A known error occured and it\'s being handled'
        super().__init__(*args, **kwargs)


class WrongType(Exception):
    pass
