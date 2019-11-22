from aiohttp import web

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
async def auth_middleware(request, handler):
    set_init_logging_context(request)
    try:
        auth_ctxt = AuthContext(request, **handler._perm_options)
    except AttributeError:
        # .e.g 404
        return await handler(request)
    except TypeError:
        if 'scopes' in handler._perm_options:
            auth_ctxt = AuthContext(request)
            auth_ctxt.request_type = 'authed'
            await auth_ctxt.set_session_context()
            set_logging_context(request.app,
                                op=auth_ctxt.operation,
                                actor_id=auth_ctxt['actor_id'],
                                workspace=auth_ctxt['workspace'])
            auth_ctxt.authorize_standalone(**handler._perm_options)
            return await handler(request)
        raise

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
    if request.session.delete_session_cookie:
        request.app.info_logger.info('Deleting session cookie')
        resp.del_cookie('postsession')
    return resp
