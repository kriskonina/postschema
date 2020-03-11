from types import MappingProxyType

from aiohttp import web
from cryptography.fernet import InvalidToken


ILLEGAL_XROLE = 'Illegal cross-role request'


class AccessBase:
    def __bool__(self):
        return bool(self.session_ctxt)

    def __contains__(self, key):
        return key in self.session_ctxt

    def __getitem__(self, key):
        'Allow dictionary access to session context without accessing `self.session_ctxt`'
        return self.session_ctxt[key]

    def __getattribute__(self, key):
        'Allow property access to session context without accessing `self.session_ctxt`'
        try:
            return super().__getattribute__(key)
        except AttributeError:
            return self.session_ctxt[key]

    def check_verification_status(self):
        if self.operation in self.verified_email and not self.session_ctxt['email_confirmed']:
            raise web.HTTPForbidden(reason='Email address not verified')
        if self.operation in self.verified_phone and not self.session_ctxt['phone_confirmed']:
            raise web.HTTPForbidden(reason='Phone number not verified')


class BracketedFrozenset(frozenset):
    def __repr__(self):
        try:
            return f'{[int(i) for i in self]}'
        except (TypeError, ValueError):
            return f'{[i for i in self]}'


class StandaloneAuthedView:
    def authorize_standalone(self, roles, phone_verified=False, email_verified=False):
        if '*' not in roles and not set(roles) & self.session_ctxt['roles']:
            raise web.HTTPForbidden(reason='Actor is short of required roles')
        self.verified_email = email_verified and [self.operation] or []
        self.verified_phone = phone_verified and [self.operation] or []
        self.check_verification_status()


class AuthContext(AccessBase, StandaloneAuthedView):
    perms = {}

    def __init__(self, request, forced_logout=False,
                 perms={}, disallow_authed=[],
                 verified_email=[], verified_phone=[]):

        self.delete_session_cookie = False
        self.request = request
        self.error_logger = request.app.error_logger
        self.perms = perms
        self.forced_logout = forced_logout
        self.disallow_authed = disallow_authed
        self.verified_email = verified_email
        self.verified_phone = verified_phone
        self.is_authed = False
        self._session_ctxt = {}

    def __bool__(self):
        return bool(self._session_ctxt)

    def _authorize_private(self):
        held_roles = self.session_ctxt['roles']
        selected_role = None

        if '*' in self.level_permissions:
            selected_role = '*'
        else:
            for allowed_role, role_details in self.level_permissions.items():
                role_type = role_details['type']
                if role_type == tuple:
                    for arole in allowed_role:
                        if arole in held_roles:
                            selected_role = allowed_role
                            break
                elif allowed_role in held_roles:
                    selected_role = allowed_role
                    break

        if selected_role is None:
            raise web.HTTPForbidden(reason=ILLEGAL_XROLE)

        auth_condition = self.level_permissions[selected_role].copy()
        auth_condition['stmt'] = auth_condition['stmt'].format(session=self)

        return auth_condition

    def authorize(self):
        if not self.needs_session:
            return {}

        if 'Admin' in self.session_ctxt['roles']:
            return {}

        try:
            if not self.level_permissions & self.session_ctxt['roles']:
                # already certain it's an Authed type request
                self.check_verification_status()
                if '*' in self.level_permissions:
                    return {}
                raise web.HTTPForbidden(reason=ILLEGAL_XROLE)
        except (KeyError, TypeError):
            # private request_type
            self.check_verification_status()
            return self._authorize_private()

    def set_level_permissions(self):
        for level in ['public', 'private', 'authed']:
            try:
                level_perms = self.perms[level][self.operation]
                break
            except KeyError:
                level_perms = None

        if level_perms is None:
            # If it's private or authed request, and public access is possible, let it run
            raise web.HTTPNotImplemented(reason='Requested resource has no implemented access policy')

        self.request_type = level
        self.level_permissions = level_perms

    async def set_session_context(self): # noqa
        session_token_name = self.request.app.config.session_key
        session_token = self.request.cookies.get(session_token_name, None)
        session_ttl = self.request.app.config.session_ttl

        if not session_token and self.needs_session:
            raise web.HTTPUnauthorized(reason='No session token found')

        if session_token and not self.needs_session and self.operation in self.disallow_authed:
            raise web.HTTPConflict(reason='Public access resource only')

        if session_token:
            if self.request.path == '/actor/login/':
                self.session_ctxt = MappingProxyType({})
                return

            try:
                actor_id = self.request.app.commons.decrypt(session_token, ttl=session_ttl)
            except InvalidToken:
                resp = web.HTTPForbidden(reason='Session token invalid or expired')
                resp.del_cookie(session_token_name)
                raise resp

            self.is_authed = True
            account_details_key = self.request.app.config.account_details_key.format(actor_id)
            workspaces_key = self.request.app.config.workspaces_key.format(actor_id)
            roles_key = self.request.app.config.roles_key.format(actor_id)

            if not self.needs_session and self.forced_logout:
                # erase the session aka forced logout if this is an authed request
                await self.request.app.redis_cli.delete(account_details_key, roles_key)
                self.session_ctxt = MappingProxyType({})
                self.delete_session_cookie = True
                return

            pipe = self.request.app.redis_cli.pipeline()
            pipe.hgetall(account_details_key)
            pipe.smembers(workspaces_key)
            pipe.smembers(roles_key)
            session_ctxt, workspaces, roles = await pipe.execute()
            if not session_ctxt:
                # session cookie is valid, but not pointing to any active account
                resp = web.HTTPUnauthorized(reason='Unknown actor')
                resp.actor_id = actor_id
                raise resp

            if session_ctxt['workspace'] == '-1' and 'Admin' not in roles:
                raise web.HTTPUnauthorized(reason='Actor not assigned to any workspace')

            if not session_ctxt:
                # most likely session invalidated by removing session context from cache
                raise web.HTTPUnauthorized(reason='Session has been shut down')

            session_ctxt['phone_confirmed'] = int(session_ctxt['phone_confirmed'])
            session_ctxt['email_confirmed'] = int(session_ctxt['email_confirmed'])
            session_ctxt['workspaces'] = BracketedFrozenset(workspaces)
            session_ctxt['roles'] = BracketedFrozenset(roles)

        else:
            session_ctxt = {}

        self._session_ctxt = session_ctxt
        self.session_ctxt = MappingProxyType(self._session_ctxt)

    @property
    def needs_session(self):
        return self.request_type in ['private', 'authed']

    @property
    def operation(self):
        return self.request.operation
