import os

import pyotp
from contextlib import asynccontextmanager, suppress

from aiohttp import web
from aiojobs.aiohttp import spawn
from cryptography.fernet import InvalidToken

with suppress(ImportError):
    from sentry_sdk import configure_scope

from . import ALLOWED_OPERATIONS
from .auth.context import AuthContext
from .exceptions import HTTPShieldedResource
from .utils import generate_num_sequence
from .view_bases import AuxViewBase

APP_MODE = os.environ.get('APP_MODE', 'test')


def set_init_logging_context(request):
    try:
        IP = request.transport.get_extra_info('peername')[0]
    except (IndexError, AttributeError):
        IP = '0.0.0.0'
    request.IP = IP


def set_logging_context(app, **context):
    if 'sentry' in app.installed_plugins:
        with configure_scope() as scope:
            scope.user = context
    if 'id' in context:
        context['actor_id'] = context.pop('id')
    app.info_logger = app.info_logger.renew(**context)
    app.error_logger = app.error_logger.renew(**context)
    app.access_logger = app.access_logger.renew(**context)


@asynccontextmanager
async def switch_workspace(request):
    overwrite_to = request.headers.get('Overwrite', '')
    request.is_overwritten = False
    all_workspaces = [overwrite_to]

    try:
        calling_workspace = None
        if overwrite_to:
            with suppress(AttributeError):
                all_workspaces = request.session.workspaces

            if overwrite_to not in all_workspaces:
                raise web.HTTPPreconditionFailed(reason='Workspace not found or outside of scope')

            with suppress(AttributeError):
                calling_workspace = request.session.workspace
                request.is_overwritten = True
                request.session._session_ctxt['workspace'] = overwrite_to

        for before_handler in request.app.config.before_request_hooks:
            await before_handler(request)
        yield request

    finally:
        if calling_workspace:
            # session exists AND Overwrite header has been used
            request.session._session_ctxt['workspace'] = calling_workspace
        for after_handler in request.app.config.after_request_hooks:
            await after_handler(request)


async def prepare_shielded_response(request, handler):
    if 'shield' not in request.app.installed_plugins:
        return await handler(request)

    async def make_shielded_response(cause, shield_method):
        # TODO rate limiting
        code = generate_num_sequence(6)
        payload = f'{request.path}:{request.session.actor_id}:{code}'
        shield_token = request.app.commons.encrypt(payload)
        context = {'method': shield_method, 'cause': cause}

        if shield_method == 'sms':
            if APP_MODE == 'test':
                context['code'] = code
            else:
                msg = request.app.config.sms_shield_msg.format(code=code)
                await request.app.send_sms(request, request.session.phone, msg)
        elif shield_method == 'otp' and APP_MODE == 'test':
            totp = pyotp.TOTP(request.session.otp_secret)
            context['code'] = totp.now()

        resp = HTTPShieldedResource(context)
        resp.set_cookie(request.app.config.shield_cookie,
                        shield_token,
                        httponly=True,
                        max_age=180)
        return resp

    try:
        schema_name = handler.schema_cls.__name__
    except AttributeError:
        schema_name = handler.__name__

    shields = request.app.shields.get(schema_name) or request.app.shields.get(handler.__name__)

    if request.session and request.session.is_authed and shields:
        shieldcode = request.query.get('shieldcode')
        shield_context = None
        shield_method = None
        with suppress(AttributeError, KeyError):
            shield_context = shields[request.operation]
            cookie_name = request.app.config.shield_cookie
            shield_cookie = request.cookies.get(cookie_name, None)

        if shield_context:
            held_roles = set(request.session.roles)
            for ishielded_roles, ishield_method in shield_context:
                if ishielded_roles & held_roles:
                    # the first matching is enough
                    shield_method = ishield_method
                    break

            if not shield_method:
                # The role on this op isn't shielded, skip
                return await handler(request)

            if shield_cookie and shieldcode:
                try:
                    decrypted_payload = request.app.commons.decrypt(shield_cookie, ttl=200)
                except InvalidToken:
                    raise await make_shielded_response('Shield token invalid or expired', shield_method)
                try:
                    path, actor_id, submitted_code = decrypted_payload.split(":")
                except ValueError:
                    raise web.HTTPForbidden(reason='Shield token misconstructed')

                if path == request.path and actor_id == request.session.actor_id:
                    if shield_method == 'otp':
                        totp = pyotp.TOTP(request.session.otp_secret)
                        if totp.now() == shieldcode:
                            resp = await handler(request)
                            resp.del_cookie(cookie_name)
                            return resp

                    if shield_method == 'sms' and submitted_code == shieldcode:
                        resp = await handler(request)
                        resp.del_cookie(cookie_name)
                        return resp

                raise await make_shielded_response('Shield token invalid', shield_method)

            raise await make_shielded_response('Shield token not supplied or invalid', shield_method)
    return await handler(request)


@web.middleware
async def postschema_middleware(request, handler):
    set_init_logging_context(request)

    if '/actor/logout/' in request.path:
        request.middleware_mode = 'public'
        request.operation = request.method.lower()
        auth_ctxt = AuthContext(request)
        auth_ctxt.request_type = 'public'
        auth_ctxt.ip_address = request.IP
        request.session = auth_ctxt
        try:
            await auth_ctxt.set_session_context()
        except web.HTTPUnauthorized as unauth_exc:
            request.session = {}
            request.session['actor_id'] = getattr(unauth_exc, 'actor_id', 'Unrecognized')
        return await handler(request)

    try:
        auth_ctxt = AuthContext(request, **handler._perm_options)
    except AttributeError:
        # e.g 404
        request.operation = request.method.lower()
        auth_ctxt = AuthContext(request)
        auth_ctxt.request_type = 'public'
        auth_ctxt.ip_address = request.IP
        request.session = auth_ctxt
        await auth_ctxt.set_session_context()
        return await handler(request)
    except TypeError:
        if 'roles' in handler._perm_options:
            request.operation = request.method.lower()
            auth_ctxt = AuthContext(request)
            auth_ctxt.request_type = 'authed'
            auth_ctxt.ip_address = request.IP
            await auth_ctxt.set_session_context()
            if str(auth_ctxt.status) != '1':
                raise web.HTTPForbidden(reason='Account inactive')
            request.session = auth_ctxt
            set_logging_context(request.app,
                                id=auth_ctxt['actor_id'],
                                email=auth_ctxt['email'],
                                workspace=auth_ctxt['workspace'])
            auth_ctxt.authorize_standalone(**handler._perm_options)

            async with switch_workspace(request):
                resp = await prepare_shielded_response(request, handler)
                with suppress(AttributeError):
                    await spawn(request, handler.log_request(request, resp))

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
    auth_ctxt.ip_address = request.IP
    try:
        await auth_ctxt.set_session_context()
    except web.HTTPException as err_resp:
        resp = err_resp
        with suppress(AttributeError):
            await spawn(request, handler.log_request(request, resp))
        raise resp

    if auth_ctxt and str(auth_ctxt.status) != '1':
        resp = web.HTTPForbidden(reason='Account inactive')
        await spawn(request, handler.log_request(request, resp))
        raise resp

    extra_ctxt = {
        'id': auth_ctxt['actor_id'],
        'email': auth_ctxt['email'],
        'workspace': auth_ctxt['workspace']
    } if auth_ctxt.session_ctxt else {}

    set_logging_context(request.app, **extra_ctxt)
    request.session = auth_ctxt

    async with switch_workspace(request):
        request.auth_conditions = auth_ctxt.authorize()
        resp = None
        try:
            resp = await prepare_shielded_response(request, handler)
        except web.HTTPException as err_resp:
            resp = err_resp
        with suppress(AttributeError):
            if request.path not in ['/actor/logout/', '/actor/login/']:
                await spawn(request, handler.log_request(request, resp))

    resp.headers['ETag'] = request.app.spec_hash
    if request.session.delete_session_cookie:
        request.app.info_logger.info('Deleting session cookie')
        resp.del_cookie('postsession')
    return resp
