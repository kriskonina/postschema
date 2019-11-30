from aiohttp import web

from . import ALLOWED_OPERATIONS
from .auth.context import AuthContext


def set_init_logging_context(request):
    try:
        IP = request.transport.get_extra_info('peername')[0]
    except (IndexError, AttributeError):
        IP = '0.0.0.0'
    request.app.info_logger = request.app.info_logger.renew(ip=IP)
    request.app.error_logger = request.app.error_logger.renew(ip=IP)


def set_logging_context(app, **context):
    app.info_logger = app.info_logger.bind(**context)
    app.error_logger = app.error_logger.bind(**context)


@web.middleware
async def postschema_middleware(request, handler):

    set_init_logging_context(request)

    if '/actor/logout/' in request.path:
        request.operation = request.method.lower()
        return await handler(request)
    try:
        auth_ctxt = AuthContext(request, **handler._perm_options)
    except AttributeError:
        # e.g 404
        request.operation = request.method.lower()
        auth_ctxt = AuthContext(request)
        auth_ctxt.request_type = 'public'
        request.session = auth_ctxt
        await auth_ctxt.set_session_context()
        return await handler(request)
    except TypeError:
        if 'roles' in handler._perm_options:
            request.operation = request.method.lower()
            auth_ctxt = AuthContext(request)
            auth_ctxt.request_type = 'authed'
            await auth_ctxt.set_session_context()
            request.session = auth_ctxt
            set_logging_context(request.app,
                                op=auth_ctxt.operation,
                                actor_id=auth_ctxt['actor_id'],
                                workspace=auth_ctxt['workspace'])
            auth_ctxt.authorize_standalone(**handler._perm_options)
            resp = await handler(request)
            resp.headers['ETag'] = request.app.spec_hash
            return resp
        raise

    if request.method != 'POST':
        raise web.HTTPMethodNotAllowed(request.method, allowed_methods=['POST'])

    try:
        op = request.headers['Range']
    except KeyError:
        raise web.HTTPBadRequest(reason='`Range` header is required to specify the operation name')

    if op not in ALLOWED_OPERATIONS:
        raise web.HTTPRequestRangeNotSatisfiable(reason=f'`{op}` is not a recognized operation name')

    request.operation = op.lower()

    auth_ctxt.set_level_permissions()
    await auth_ctxt.set_session_context()
    extra_ctxt = {
        'actor_id': auth_ctxt['actor_id'],
        'workspace': auth_ctxt['workspace']
    } if auth_ctxt.session_ctxt else {}
    set_logging_context(request.app, op=auth_ctxt.operation, **extra_ctxt)
    request.session = auth_ctxt
    request.auth_conditions = auth_ctxt.authorize()
    resp = await handler(request)
    resp.headers['ETag'] = request.app.spec_hash
    if request.session.delete_session_cookie:
        request.app.info_logger.info('Deleting session cookie')
        resp.del_cookie('postsession')
    return resp
