import os
import orjson
import urllib.parse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import aiosmtplib
import bcrypt
import pyotp
import sqlalchemy as sql
from aiojobs.aiohttp import spawn
from aiohttp import web
from cryptography.fernet import InvalidToken
from marshmallow import fields, validate, validates, ValidationError
from psycopg2 import errors as postgres_errors
from sqlalchemy.dialects.postgresql import JSONB

from . import (
    fields as postschema_field,
    exceptions as post_exceptions,
    validators
)
from .auth.clauses import CheckedPermClause
from .contrib import Pagination, ListMembersFilter
from .decorators import summary
from .schema import RootSchema
from .utils import (
    generate_random_word,
    generate_num_sequence,
    json_response,
    parse_postgres_err,
    seconds_to_human,
    Json,
    dumps
)
from .scope import ScopeBase
from .view import AuxView


APP_MODE = os.environ.get('APP_MODE', 'test')
INALIENABLE_ROLES = ['*', 'Admin', 'Owner']
# we don't allow any actor to have a wildcard and Admin role
ROLES = sorted(
    role for role in orjson.loads(os.environ.get('ROLES', '[]'))
    if role not in INALIENABLE_ROLES)

pagination_fields = Pagination._declared_fields.copy()
for k, v in pagination_fields.items():
    pagination_fields[k].metadata['location'] = 'body'


def clean_phone_number(phoneno):
    return phoneno.replace('(', '').replace(')', '').replace(' ', '')


async def send_email_user_invitation(request, by, link, to):
    if APP_MODE == 'test':
        return link

    ttl_seconds = request.app.config.invitation_link_ttl
    ttl = seconds_to_human(ttl_seconds)

    html_template = request.app.config.invitation_email_html.render(
        by=by,
        registration_link=link,
        ttl=ttl
    )
    text_template = request.app.config.invitation_email_text.format(
        by=by,
        registration_link=link
    )

    message = MIMEMultipart("alternative")
    message["From"] = os.environ.get('EMAIL_FROM')
    message["To"] = to
    message["Subject"] = request.app.config.invitation_email_subject

    plain_part = MIMEText(text_template, "plain")
    message.attach(plain_part)

    if html_template:
        html_part = MIMEText(html_template, "html")
        message.attach(html_part)

    await aiosmtplib.send(
        message,
        hostname=os.environ.get('EMAIL_HOSTNAME'),
        use_tls=True,
        username=os.environ.get('EMAIL_USERNAME'),
        password=os.environ.get('EMAIL_PASSWORD')
    )
    request.app.info_logger.info("Invitation email sent", invited=to)


async def send_email_reset_link(request, checkcode, to):
    reset_form_url = request.app.config.password_reset_form_link

    if reset_form_url.startswith('/'):
        reset_form_url = reset_form_url[1:]
    if not reset_form_url.endswith('/'):
        reset_form_url += '/'

    reset_form_url = reset_form_url.format(
        scheme=f'{request.scheme}://{request.host}/',
        checkcode=checkcode)

    if APP_MODE == 'test':
        return reset_form_url

    ttl_seconds = request.app.config.reset_link_ttl
    ttl = seconds_to_human(ttl_seconds)

    html_template = request.app.config.reset_pass_email_html.render(
        reset_link=reset_form_url,
        ttl=ttl)
    text_template = request.app.config.reset_pass_email_text.format(reset_link=reset_form_url)

    message = MIMEMultipart("alternative")
    message["From"] = os.environ.get('EMAIL_FROM')
    message["To"] = to
    message["Subject"] = request.app.config.reset_pass_email_subject

    plain_part = MIMEText(text_template, "plain")
    message.attach(plain_part)

    if html_template:
        html_part = MIMEText(html_template, "html")
        message.attach(html_part)

    await aiosmtplib.send(
        message,
        hostname=os.environ.get('EMAIL_HOSTNAME'),
        use_tls=True,
        username=os.environ.get('EMAIL_USERNAME'),
        password=os.environ.get('EMAIL_PASSWORD')
    )
    request.app.info_logger.info("Sent password reset link", email=to)


async def send_email_verification_link(request, to):
    verif_token = generate_random_word(20)
    ttl_seconds = request.app.config.activation_link_ttl
    email_verification_link = request.app.config.email_verification_link
    verif_link = email_verification_link.format(scheme=f'{request.scheme}://{request.host}',
                                                verif_token=verif_token)
    actor_id = request.session.actor_id

    key = f'postschema:verify:email:{verif_token}'
    await request.app.redis_cli.set(key, actor_id)
    await request.app.redis_cli.expire(key, ttl_seconds)

    if APP_MODE == 'test':
        return verif_link

    ttl = seconds_to_human(ttl_seconds)

    html_template = request.app.config.verification_email_html.render(verif_link=verif_link, ttl=ttl)
    text_template = request.app.config.verification_email_text.format(verif_link=verif_link)

    message = MIMEMultipart("alternative")
    message["From"] = os.environ.get('EMAIL_FROM')
    message["To"] = to
    message["Subject"] = request.app.config.verification_email_subject

    plain_part = MIMEText(text_template, "plain")
    message.attach(plain_part)

    if html_template:
        html_part = MIMEText(html_template, "html")
        message.attach(html_part)

    await aiosmtplib.send(
        message,
        hostname=os.environ.get('EMAIL_HOSTNAME'),
        use_tls=True,
        username=os.environ.get('EMAIL_USERNAME'),
        password=os.environ.get('EMAIL_PASSWORD')
    )
    request.app.info_logger.info("Sent email verification link", email=to)


async def send_email_activation_link(request, data, link_path_base, ttl_seconds):
    reg_token = generate_random_word(20)
    activation_link = link_path_base.format(reg_token=reg_token, scheme=f'{request.scheme}://{request.host}')
    data['details'] = dumps(data.get('details', {}))
    data['status'] = 0
    data['email_confirmed'] = 0
    data['phone_confirmed'] = 0
    data['roles'] = ','.join(data['roles'])
    data['workspaces'] = data.get('workspaces', '') or ''

    key = f'postschema:activate:email:{reg_token}'

    await request.app.redis_cli.hmset_dict(key, **data)
    await request.app.redis_cli.expire(key, ttl_seconds)

    if APP_MODE == 'test':
        return activation_link

    ttl = seconds_to_human(ttl_seconds)

    html_template = request.app.config.activation_email_html.render(activation_link=activation_link, ttl=ttl)
    text_template = request.app.config.activation_email_text.format(activation_link=activation_link)

    message = MIMEMultipart("alternative")
    message["From"] = os.environ.get('EMAIL_FROM')
    message["To"] = data['email']
    message["Subject"] = request.app.config.activation_email_subject

    plain_part = MIMEText(text_template, "plain")
    message.attach(plain_part)

    if html_template:
        html_part = MIMEText(html_template, "html")
        message.attach(html_part)

    await aiosmtplib.send(
        message,
        hostname=os.environ.get('EMAIL_HOSTNAME'),
        use_tls=True,
        username=os.environ.get('EMAIL_USERNAME'),
        password=os.environ.get('EMAIL_PASSWORD')
    )
    request.app.info_logger.info("Activation email sent", email=data['email'], username=data['username'])


async def send_phone_verification_code(request, phone_num, actor_id):
    verification_code = generate_num_sequence()
    key = f'postschema:activate:phone:{verification_code}'
    await request.app.redis_cli.set(key, actor_id, expire=request.app.config.sms_verification_ttl)
    msg = request.app.config.sms_verification_cta.format(verification_code=verification_code)
    if APP_MODE == 'test':
        return verification_code
    await request.app.send_sms(request, phone_num, msg)


async def login(request, payload, is_trusted=False):
    get_actor_query = ('SELECT json_build_object('
            "'actor_id',actor.id,"
            "'phone',COALESCE(phone, ''),"
            "'email',email,"
            "'username',COALESCE(username, split_part(email, '@', 1)),"
            "'email_confirmed',COALESCE(email_confirmed, False)::int,"
            "'phone_confirmed',COALESCE(phone_confirmed, False)::int,"
            "'scope',scope,"
            "'roles',roles,"
            "'status',status,"
            "'otp_secret',otp_secret,"
            "'password',password,"
            "'workspaces', COALESCE(jsonb_object_agg(workspace.id, workspace.name) FILTER (WHERE workspace.id IS NOT NULL),'{}'::jsonb)) " # noqa
        "FROM actor "
        'LEFT JOIN workspace ON actor.id=workspace.owner OR format(\'%%s\', actor.id)::jsonb <@ workspace.members ' # noqa
        "WHERE email=%s "
        "GROUP BY actor.id"
        ) # noqa

    async with request.app.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(get_actor_query, [payload['email']])
            try:
                data = (await cur.fetchone())[0]
            except TypeError:
                raise web.HTTPForbidden(reason='Invalid login or password')

    if not is_trusted:
        try:
            if not bcrypt.checkpw(payload['password'].encode(), data['password'].encode()):
                raise web.HTTPForbidden(reason='Invalid login or password')
        except ValueError:
            # invalid salt
            raise web.HTTPForbidden(reason='Invalid salt')

    workspace_ids = [int(key) for key in data['workspaces']]

    if data['workspaces'] and 'workspace' not in payload:
        # User logging in hasn't selected a workspace, and has only one, select it for him.
        if len(data['workspaces']) == 1:
            payload['workspace'] = workspace_ids[0]
        else:
            # User needs to select explicitly which workspace to log in to
            raise web.HTTPConflict(
                content_type='application/json',
                body=dumps({'workspaces': data['workspaces']}),
                reason='This account has more than one workspace. You need to select one.')

    workspace = payload.get('workspace')
    if workspace and workspace not in workspace_ids:
        raise post_exceptions.ValidationError({
            'workspace': ['Workspace doesn\'t exist or doesn\'t belong to you']
        })

    # put selected workspace on future session context
    data['workspace'] = workspace or -1
    data['scope'] = data['scope'] or 'Generic'

    payload.pop('password', None)
    actor_id = data.get('actor_id')
    account_key = request.app.config.account_details_key.format(actor_id)
    roles_key = request.app.config.roles_key.format(actor_id)
    workspaces_key = request.app.config.workspaces_key.format(actor_id)
    roles = data.pop('roles', [])

    # cache actor details
    workspaces = data.pop('workspaces')
    pipe = request.app.redis_cli.pipeline()
    pipe.hmset_dict(account_key, data)
    if roles:
        pipe.sadd(roles_key, *roles)
    if workspace_ids:
        pipe.sadd(workspaces_key, *workspace_ids)
    await pipe.execute()

    session_token = request.app.commons.encrypt(actor_id)
    response = json_response({
        'id': data['actor_id'],
        'username': data['username'],
        'email': data['email'],
        'email_confirmed': data['email_confirmed'],
        'phone_confirmed': data['phone_confirmed'],
        'phone': data['phone'],
        'roles': roles,
        'scope': data['scope'],
        'active_workspace': data['workspace'],
        'workspaces': workspaces
    })
    session_cookie = request.app.config.session_key
    response.set_cookie(session_cookie,
                        session_token,
                        httponly=True,
                        max_age=request.app.config.session_ttl)
    request.app.info_logger.info("User logged in", actor_id=actor_id)
    return response


class LoginView(AuxView):
    email = fields.Email(required=True, location='body')
    password = fields.String(required=True, location='body')
    workspace = fields.Int(location='body')

    @summary('Log in a user') # noqa
    async def post(self):
        '''Create a session entry in Redis for the authenticated user,
        set a session cookie on the response object.
        Return logged-in user details.
        '''
        payload = await self.validate_payload()
        return await login(self.request, payload)

    class Public:
        class permissions:
            post = {}


class LogoutView(AuxView):
    @summary('Log out a user')
    async def get(self):
        session_cookie_name = self.request.app.config.session_key
        session_cookie = self.request.cookies.get(session_cookie_name, None)
        if not session_cookie:
            raise web.HTTPNoContent(reason='No session found')

        if hasattr(self.request, 'session'):
            try:
                actor_id = self.request.session['actor_id']
                account_key = self.request.app.config.account_details_key.format(actor_id)
                roles_key = self.request.app.config.roles_key.format(actor_id)
                await self.request.app.redis_cli.delete(account_key, roles_key)
            except (AttributeError, KeyError):
                # session cookie doesn't point to any actor Ids nor session caches
                actor_id = 'unrecognized'
        else:
            try:
                actor_id = self.request.app.commons.decrypt(session_cookie)
            except InvalidToken:
                actor_id = 'unrecognized'

        response = web.HTTPOk()
        response.del_cookie('postsession')
        self.request.app.info_logger.info("User logged out", actor_id=actor_id)
        return response

    class Authed:
        class permissions:
            get = ['*']


class PhoneActivationView(AuxView):
    verification_code = fields.String(location='path')

    @summary('Verify phone number')
    async def get(self):
        verification_code = self.path_payload['verification_code']
        key = f'postschema:activate:phone:{verification_code}'
        actor_id = await self.request.app.redis_cli.get(key)
        if not actor_id:
            raise post_exceptions.ValidationError(
                {'verification_code': ["Entered code invalid or expired"]})

        await self.request.app.redis_cli.delete(key)

        query = 'UPDATE actor SET phone_confirmed = true, status = 1 WHERE id = %s RETURNING phone'
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [actor_id])
                try:
                    phone = (await cur.fetchone())[0]
                except TypeError:
                    raise post_exceptions.ValidationError({'verification_code': ["Actor doesn't exist"]})

        resp = json_response({'phone': [f'Verified number {phone}']})

        # in case this is an authed request, also update the cache entry
        try:
            actor_id = self.request.session.actor_id
        except KeyError:
            return resp
        account_key = self.request.app.config.account_details_key.format(actor_id)
        self.request.app.redis_cli.hset(account_key, 'phone_confirmed', 1)
        await self.request.session.set_session_context()
        self.request.app.info_logger.info("Session context updated",
                                          actor_id=actor_id, changes={'phone_confirmed': 1})
        return resp

    class Public:
        class permissions:
            get = {}


class VerfiyEmailAddress(AuxView):
    verif_code = fields.String(location='path')

    @summary('Verify email address')
    async def get(self):
        verif_code = self.path_payload['verif_code']
        key = f'postschema:verify:email:{verif_code}'
        actor_id = await self.request.app.redis_cli.get(key)

        if not actor_id:
            raise post_exceptions.ValidationError(
                {'verif_code': ["Verification code invalid or expired"]})

        await self.request.app.redis_cli.delete(key)

        query = f'UPDATE actor SET email_confirmed=true WHERE id=%s RETURNING email'
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [actor_id])
                try:
                    email = (await cur.fetchone())[0]
                except TypeError:
                    raise post_exceptions.ValidationError({'verif_code': ["Actor doesn't exist"]})

        resp = json_response({'email': [f'Verified email address {email}']})

        # in case this is an authed request, also update the cache entry
        try:
            actor_id = self.request.session.actor_id
        except KeyError:
            return resp
        account_key = self.request.app.config.account_details_key.format(actor_id)
        self.request.app.redis_cli.hset(account_key, 'email_confirmed', 1)
        await self.request.session.set_session_context()
        self.request.app.info_logger.info("Session context updated",
                                          actor_id=actor_id, changes={'email_confirmed': 1})
        return resp

    class Public:
        class permissions:
            get = {}
            post = {}

    class Authed:
        class permissions:
            get = ['*']
            post = ['*']


class CreatedUserActivationView(AuxView):
    reg_token = fields.String(location='path')
    workspace_name = fields.String(location='body', required=True)

    @summary('Verify standalone user')
    async def post(self):
        reg_token = self.path_payload['reg_token']
        workspace_payload = await self.validate_payload()

        account_key = f'postschema:activate:email:{reg_token}'
        account_data = await self.request.app.redis_cli.hgetall(account_key)
        if not account_data:
            raise web.HTTPNotFound(reason='Link expired or invalid')
        await self.request.app.redis_cli.delete(account_key)

        account_data['email_confirmed'] = True
        account_data['status'] = 1
        account_data['roles'] = Json(account_data['roles'].split(','))
        # account_data['workspaces'] = Json(account_data['workspaces'].split(','))
        account_data['details'] = Json(orjson.loads(account_data['details']))
        account_data.pop('workspaces', None)

        on_conflict = 'ON CONFLICT (email) DO UPDATE SET email_confirmed=true'
        insert_query = self._render_insert_query(account_data, on_conflict=on_conflict)

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                async with cur.begin():

                    await cur.execute(insert_query, account_data)
                    actor_id = (await cur.fetchone())[0]

                    name = workspace_payload.get('workspace_name')
                    try:
                        await cur.execute((
                            "INSERT INTO workspace (id, name, owner) VALUES "
                            "(NEXTVAL('workspace_id_seq'), %s, %s) "
                            "RETURNING id"
                        ), [name, actor_id])
                    except postgres_errors.IntegrityError as ierr:
                        raise post_exceptions.ValidationError(parse_postgres_err(ierr))
                    except Exception:
                        self.request.app.error_logger.exception(
                            'Failed adding to workspace resource',
                            query=cur.query.decode())
                        raise

                    workspace_id = (await cur.fetchone())[0]

        return await login(
            self.request,
            {'email': account_data['email'],
             'workspace': workspace_id,
             'password': ''},
            is_trusted=True
        )
        # return json_response({
        #     'actor_id': actor_id,
        #     'workspace_id': workspace_id,
        #     'workspace_name': name
        # })

    class Public:
        disallow_authed = ['post']
        forced_logout = True

        class permissions:
            post = {}


class SendPhoneLink(AuxView):
    phone = fields.String(location='body', required=True)

    @summary('Send phone number verification link')
    async def post(self):
        payload = await self.validate_payload()
        number = payload['phone']

        # first check if this phone number exists in our database
        query = 'SELECT id FROM actor WHERE phone=%s AND phone_confirmed=False OR status=0'
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [number])
                try:
                    actor_id = (await cur.fetchone())[0]
                except TypeError:
                    raise post_exceptions.ValidationError(
                        {'phone': ["Phone number doesn't exist or already confirmed"]})
        verification_code = await send_phone_verification_code(self.request, number, actor_id)
        if verification_code:
            return web.HTTPOk(text=verification_code)
        return web.HTTPNoContent()

    class Public:
        class permissions:
            post = {}


class SendEmailLink(AuxView):
    email = fields.Email(required=True, location='body')

    @summary('Send email with verification link')
    async def post(self):
        payload = await self.validate_payload()
        # first check if this phone number exists in our database
        query = ("SELECT json_build_object("
                 "'id',id,'phone',phone,'phone_confirmed',phone_confirmed,"
                 "'email',email,'email_confirmed',email_confirmed,'password',password) FROM actor "
                 "WHERE email = %s AND email_confirmed = False")
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [payload['email']])
                try:
                    data = (await cur.fetchone())[0]
                    data.pop('id')
                except TypeError:
                    raise post_exceptions.ValidationError(
                        {'email': ["Email doesn't exist or already confirmed"]})

        if self.request.session.ia_authed:
            data['workspace'] = self.request.session.workspace

        link_path_base = self.request.app.created_email_confirmation_link
        ttl = self.request.app.config.activation_link_ttl

        if APP_MODE == 'test':
            activation_link = await send_email_activation_link(self.request, data, link_path_base, ttl)
            return web.HTTPNoContent(body=activation_link)

        await spawn(self.request, send_email_activation_link(self.request, data, link_path_base, ttl))
        return web.HTTPNoContent(reason='Activation link has been resent')

    class Public:
        class permissions:
            post = {}


class ChangePassword(AuxView):
    checkcode = fields.String(location='path')
    password1 = fields.String(sqlfield=sql.String(555),
                              location='form', required=True,
                              validate=validate.Length(min=6))
    password2 = fields.String(sqlfield=sql.String(555),
                              location='form', required=True,
                              validate=validate.Length(min=6))

    @summary('Change user\'s password')
    async def post(self):
        checkcode = self.path_payload['checkcode']
        payload = await self.validate_form()

        if payload['password1'] != payload['password2']:
            raise post_exceptions.ValidationError({
                'password': ["Passwords don't match"]
            })

        key = f'postschema:pass:verify:{checkcode}'
        actor_id = await self.request.app.redis_cli.get(key)
        await self.request.app.redis_cli.delete(key)

        password = bcrypt.hashpw(payload['password1'].encode(), bcrypt.gensalt()).decode()
        query = 'UPDATE actor SET password=%s WHERE id=%s RETURNING id'

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [password, actor_id])
                if not await cur.fetchone():
                    raise web.HTTPBadRequest(reason="Couldn't change password")

        return web.HTTPOk()

    class Public:
        class permissions:
            post = {}


class ResetPassword(AuxView):
    email = fields.Email(required=True, location='body')

    @summary('Reset user\'s password')
    async def post(self):
        payload = await self.validate_payload()
        query = 'SELECT id, email FROM actor WHERE email=%s'
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [payload['email']])
                res = await cur.fetchone()
                if not res:
                    raise post_exceptions.ValidationError({
                        'email': ['Email address does not point to any active account']
                    })
        id, email = res

        # set a check code
        checkcode = generate_random_word(30)
        key = f'postschema:pass:reset:{checkcode}'
        expire = self.request.app.config.reset_link_ttl
        payload = {
            'id': id,
            'swapcode': generate_random_word(60)
        }
        await self.request.app.redis_cli.hmset_dict(key, payload)
        await self.request.app.redis_cli.expire(key, expire)

        # send email with reset link
        if APP_MODE == 'test':
            reset_link = await send_email_reset_link(self.request, checkcode, email)
            raise web.HTTPOk(reason='Reset link sent', text=reset_link)

        await spawn(self.request, send_email_reset_link(self.request, checkcode, email))
        raise web.HTTPNoContent(reason='Reset link sent')

    class Public:
        class permissions:
            post = {}


class InviteUser(AuxView):
    email = fields.Email(required=True, location='body')
    scope = fields.String(sqlfield=sql.String(60),
                          validate=[validate.OneOf(ScopeBase._scopes)],
                          location='body', required=True)
    workspaces = fields.List(
        fields.String(),
        location='body',
        validate=[validators.must_not_be_empty],
        sqlfield=JSONB,
        missing=[]
    )

    @summary('Invite a user')
    async def post(self):
        inviter = self.request.session['email']
        owned_workspaces = set(self.request.session['workspaces'])
        if not owned_workspaces:
            raise web.HTTPForbidden(reason='Requesting actor has no assigned workspace(s)')

        payload = await self.validate_payload()
        query = 'SELECT 1 FROM actor WHERE email=%s'
        email = payload['email']
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [email])
                if await cur.fetchone():
                    raise post_exceptions.ValidationError({
                        'email': ['Email address already assigned to another account']
                    })

        workspaces = payload['workspaces']
        not_owned_workspaces = set(workspaces) - owned_workspaces

        if not_owned_workspaces:
            sorted_not_owned = sorted(not_owned_workspaces)
            raise post_exceptions.ValidationError({
                'workspaces': [f"Workspaces {', '.join(sorted_not_owned)} don't belong to the requesting actor"] # noqa
            })

        if not workspaces:
            # add owner's workspace as a default
            workspaces = [self.request.session.workspace]

        scope = payload['scope'].title()
        roles = self.request.app.config.scopes[scope].Meta.roles

        workspace = self.request.session.workspace

        payload = f"{','.join(roles)}:{','.join(workspaces)}:{scope}:{email}:{workspace}"
        encrypted_payload = self.request.app.commons.encrypt(payload)
        escaped_payload = urllib.parse.quote(encrypted_payload)
        invitation_link = self.request.app.invitation_link.format(
            scheme=f'{self.request.scheme}://{self.request.host}/',
            payload=escaped_payload
        )
        # path = f'actor/?inv={escaped_payload}'
        # invitation_link = f'{self.request.scheme}://{self.request.host}/{path}/'

        # send email with invitation link
        if APP_MODE == 'test':
            reset_link = await send_email_user_invitation(self.request, inviter, invitation_link, email)
            return web.HTTPOk(reason='Invitation link sent', text=reset_link)

        await spawn(self.request, send_email_user_invitation(self.request, inviter, invitation_link, email))
        raise web.HTTPNoContent(reason='Invitation link sent')

    class Authed:
        class permissions:
            post = ['Owner']


class GrantRole(AuxView):
    actor_id = fields.Int(location='path')  # grantee
    roles = postschema_field.Set(
        fields.String(),
        location='body',
        sqlfield=JSONB,
        required=True
    )

    @validates('roles')
    def roles_validator(self, val):
        delta = set(val) - self.app.allowed_roles
        if delta:
            bad_roles = ', '.join(delta)
            raise ValidationError(f'Invalid roles: {bad_roles}')

    @summary('Update user\'s roles')
    async def patch(self):
        actor_id = self.path_payload['actor_id']
        payload = await self.validate_payload()
        roles = payload['roles']

        query = ('UPDATE actor set roles=('
                 'SELECT json_agg(DISTINCT t.j_els) '
                 '''FROM (SELECT jsonb_array_elements(roles || %s) AS j_els) t '''
                 ') WHERE id=%s RETURNING roles')

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [Json(roles), actor_id])
                ret = await cur.fetchone()
                if not ret:
                    raise post_exceptions.ValidationError({
                        'path': {
                            'actor_id': ['Actor ID doesn\'t not exist']
                        }
                    })
                new_roles = ret[0]

        roles_key = self.request.app.config.roles_key.format(actor_id)
        self.request.session._session_ctxt['roles'] = set(new_roles)
        if await self.request.app.redis_cli.exists(roles_key):
            await self.request.app.redis_cli.delete(roles_key)
            await self.request.app.redis_cli.sadd(roles_key, *new_roles)

        return web.HTTPOk()

    @summary('Grant roles on a user')
    async def post(self):
        '''
        Override actor's roles, subject to same workspace membership policy.
        Only Owners and Admins roles can perform this.
        If the requested actor already holds Admin/Owner role(s), they will be preserved.
        '''
        actor_id = self.path_payload['actor_id']
        payload = await self.validate_payload()
        roles = payload['roles']

        if 'Owner' in self.request.session.roles:
            # ensure that actor_id requested for is the member of the requester's workspaces
            workspaces = f"{{{','.join(self.request.session.workspaces)}}}"
            async with self.request.app.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f'''WITH workspace_cte AS (
                        SELECT jsonb_agg(t.mems) AS mems FROM (
                            SELECT distinct jsonb_array_elements(members) AS mems
                            FROM workspace
                            WHERE id=ANY('{workspaces}')
                            ) t
                        )
                        UPDATE actor set roles=(
                            SELECT COALESCE(jsonb_agg(y.droles), '[]'::jsonb) FROM (
                                SELECT distinct t.j_els AS droles FROM (
                                    SELECT jsonb_array_elements(roles) AS j_els
                                ) t WHERE t.j_els <@ '["Admin", "Owner"]'::jsonb
                            ) y
                        ) || %s
                        FROM workspace_cte
                        WHERE actor.id=%s AND '%s' <@ workspace_cte.mems
                        RETURNING roles''',
                        [
                            Json(roles), actor_id, actor_id
                        ]
                    )
                    ret = await cur.fetchone()
                    if not ret:
                        raise web.HTTPForbidden(reason=("Requested actor ID doesn't exist or you don't "
                                                        "have permission to execute this operation"))
                    new_roles = ret[0]

        elif 'Admin' in self.request.session.roles:
            query = '''UPDATE actor
            SET roles=(
                SELECT COALESCE(jsonb_agg(y.droles), '[]'::jsonb) FROM (
                    SELECT distinct t.j_els AS droles FROM (
                        SELECT jsonb_array_elements(roles) AS j_els
                    ) t WHERE t.j_els <@ '["Admin", "Owner"]'::jsonb
                ) y
            ) || %s
            WHERE id=%s RETURNING roles'''
            async with self.request.app.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, [Json(roles), actor_id])
                    ret = await cur.fetchone()
                    if not ret:
                        raise post_exceptions.ValidationError({
                            'path': {
                                'actor_id': ['Actor ID doesn\'t not exist']
                            }
                        })
                    new_roles = ret[0]

        roles_key = self.request.app.config.roles_key.format(actor_id)
        self.request.session._session_ctxt['roles'] = set(new_roles)
        if await self.request.app.redis_cli.exists(roles_key):
            await self.request.app.redis_cli.delete(roles_key)
            if new_roles:
                await self.request.app.redis_cli.sadd(roles_key, *new_roles)

        return web.HTTPOk()

    @summary('Revoke user\'s roles')
    async def delete(self):
        actor_id = self.path_payload['actor_id']
        payload = await self.validate_payload()
        roles = payload['roles']

        roles_joined = '-'.join(f"'{i}'" for i in roles)

        query = f'UPDATE actor SET roles = roles-{roles_joined} WHERE id=%s RETURNING roles'
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [actor_id])
                ret = await cur.fetchone()
                if not ret:
                    raise post_exceptions.ValidationError({
                        'path': {
                            'actor_id': ['Actor ID doesn\'t not exist']
                        }
                    })
                new_roles = ret[0]

        roles_key = self.request.app.config.roles_key.format(actor_id)
        self.request.session._session_ctxt['roles'] = set(new_roles)
        if await self.request.app.redis_cli.exists(roles_key):
            await self.request.app.redis_cli.delete(roles_key)
            if new_roles:
                await self.request.app.redis_cli.sadd(roles_key, *new_roles)

        return web.HTTPOk()

    class Authed:
        class permissions:
            post = ['Admin', 'Owner']
            patch = ['Admin']
            delete = ['Admin']


class GrantWorkspace(AuxView):
    actor_id = fields.Int(location='path')  # grantee
    workspaces = postschema_field.Set(
        fields.String(),
        location='body',
        validate=[validators.must_not_be_empty],
        sqlfield=JSONB,
        required=True
    )

    @summary('Grant workspaces on a user')
    async def post(self):
        actor_id = self.path_payload['actor_id']
        owner_id = self.request.session['actor_id']
        payload = await self.validate_payload()
        workspaces = payload['workspaces']

        owned_workspaces = set(self.request.session['workspaces'])

        if not owned_workspaces:
            raise web.HTTPForbidden(
                reason=f'Requesting actor ({owner_id}) has no assigned workspace(s) to invite to.')

        not_owned_workspaces = set(workspaces) - owned_workspaces
        if not_owned_workspaces:
            raise post_exceptions.ValidationError({
                'workspaces': [f"Workspaces ({', '.join(not_owned_workspaces)}) don't belong to the requesting actor"] # noqa
            })

        workspaces_joined = ','.join(workspaces)

        member = f''''[{actor_id}]'::jsonb'''

        query = (
            'WITH update_workspace_cte as ('
            f'UPDATE workspace SET members = members || {member} '
            f"WHERE owner=%s AND id=ANY('{{{workspaces_joined}}}') "
            f'AND NOT(members @> {member}) RETURNING id) '
            'SELECT json_agg(id) FROM update_workspace_cte'
        )
        owner_id = self.request.session.actor_id
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT 1 FROM actor WHERE id=%s', [actor_id])
                res = await cur.fetchone()
                if not res:
                    raise post_exceptions.ValidationError({
                        'path': {
                            'actor_id': ['Actor ID doesn\'t not exist']
                        }
                    })
                await cur.execute(query, [owner_id])
                ret = await cur.fetchone()
                if not ret or not ret[0]:
                    raise web.HTTPOk(text='Workspace(s) already assigned to this actor')
                new_workspaces = ret[0]

        workspaces_key = self.request.app.config.workspaces_key.format(actor_id)
        account_key = self.request.app.config.account_details_key.format(actor_id)
        workspace_held = await self.request.app.redis_cli.hget(account_key, 'workspace')

        if workspace_held == '-1':
            await self.request.app.redis_cli.hset(account_key, 'workspace', new_workspaces[0])
            self.request.session._session_ctxt['workspace'] = new_workspaces[0]

        self.request.session._session_ctxt['workspaces'] = set(new_workspaces)
        if await self.request.app.redis_cli.exists(workspaces_key):
            await self.request.app.redis_cli.delete(workspaces_key)
            await self.request.app.redis_cli.sadd(workspaces_key, *new_workspaces)

        return web.HTTPOk()

    @summary('Deregister user\'s workspaces')
    async def delete(self):
        actor_id = self.path_payload['actor_id']
        owner_id = self.request.session['actor_id']
        payload = await self.validate_payload()
        workspaces = payload['workspaces']

        owned_workspaces = set(self.request.session['workspaces'])

        if not owned_workspaces:
            raise web.HTTPForbidden(
                reason=f'Requesting actor ({owner_id}) has no assigned workspace(s).')

        not_owned_workspaces = set(workspaces) - owned_workspaces
        if not_owned_workspaces:
            raise post_exceptions.ValidationError({
                'workspaces': [f"Workspaces ({', '.join(not_owned_workspaces)}) don't belong to the requesting actor."] # noqa
            })

        workspaces_joined = ','.join(workspaces)

        query = (
            'WITH delete_from_workspace_cte as ('
            f"UPDATE workspace SET members = ("
            "SELECT COALESCE(jsonb_agg(j.i), '[]'::jsonb) FROM "
            "(SELECT jsonb_array_elements(members) AS i) j WHERE j.i <> '%s') "
            f"WHERE owner=%s AND id=ANY('{{{workspaces_joined}}}')  RETURNING id)"
            'SELECT json_agg(id) FROM delete_from_workspace_cte'
        )

        owner_id = self.request.session.actor_id
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [actor_id, owner_id])
                ret = await cur.fetchone()
                if not ret or not ret[0]:
                    raise web.HTTPOk(
                        text='Actor ID doesn\'t exist or never belonged to the workspaces in request.')
                new_workspaces = ret[0]

        workspaces_key = self.request.app.config.workspaces_key.format(actor_id)
        self.request.session._session_ctxt['workspaces'] = set(new_workspaces)
        if await self.request.app.redis_cli.exists(workspaces_key):
            await self.request.app.redis_cli.delete(workspaces_key)
            await self.request.app.redis_cli.sadd(workspaces_key, *new_workspaces)

        return web.HTTPOk()

    class Authed:
        class permissions:
            post = ['Owner']
            delete = ['Owner']


class ListMembers(AuxView):
    page = pagination_fields['page']
    limit = pagination_fields['limit']
    order_by = pagination_fields['order_by']
    order_dir = pagination_fields['order_dir']
    select = fields.List(fields.String(), location='body')
    filter = fields.Dict(location='body')
    workspace = fields.Int(location='path')

    @summary('List all members belonging the requester\'s workspace')
    async def get(self):
        payload = await self.validate_payload()

        workspace = str(self.path_payload['workspace'])

        if workspace not in self.request.session.workspaces and self.request.session.username != 'admin':
            raise post_exceptions.ValidationError({
                'path': {'workspace': ['Workspace does not exist or you do not have rights to access it']}
            })

        # validate filter, if present
        filter_payload = payload.get('filter', {})
        filter_cleaned = await self._validate_singular_payload(
            payload=filter_payload,
            schema=ListMembersFilter(partial=True),
            envelope_key='filter')

        allowed_order_by = set(self.schema_cls.Meta.order_by)
        allowed_select_by = set(self.schema_cls.Private.get_by)

        errors = {}
        invalid_order_by = set(payload.get('order_by', [])) - allowed_order_by
        invalid_selects = set(payload.get('select', [])) - allowed_select_by

        if invalid_order_by:
            errors['order_by'] = [f'The following fields are invalid: {", ".join(invalid_order_by)}']
        if invalid_selects:
            errors['select'] = [f'The following fields are invalid: {", ".join(invalid_selects)}']
        if errors:
            raise post_exceptions.ValidationError(errors)

        limit = payload['limit']
        page = payload['page'] - 1
        offset = page * limit

        selects_list = payload.get('select', self.schema_cls.Private.list_by)
        selects = ','.join(f"'{sel}',{sel}" for sel in selects_list)
        orderby = ','.join(payload.get('order_by', ['id']))
        orderhow = payload['order_dir'].upper()

        where = ['1=1']

        for fieldname, val in filter_cleaned.items():
            if isinstance(val, list):
                where.append(f'{fieldname}::jsonb ?| %({fieldname})s')
                filter_cleaned[fieldname] = val
            else:
                where.append(f'{fieldname} ILIKE %({fieldname})s')
                filter_cleaned[fieldname] = "%" + val + "%"

        get_actors_ids_query = self.request.app.queries['list_members'].format(
            workspace=workspace,
            where=' AND '.join(where),
            selects=selects,
            orderby=orderby,
            orderhow=orderhow,
            limit=limit,
            offset=offset
        )

        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(get_actors_ids_query, filter_cleaned)
                res = await cur.fetchone()

        return json_response(res and res[0] or {})

    class Authed:
        class permissions:
            get = ['Owner']


class GetOtpSecret(AuxView):
    @summary('Get the logged-in\'s actor otp secret')
    async def get(self):
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT otp_secret FROM actor WHERE id=%s', [self.request.session.actor_id])
                res = await cur.fetchone()
        return json_response({
            'otp_secret': res[0]
        })

    class Authed:
        class permissions:
            get = ['*']

    class Shield:
        get = {
            '*': 'sms'
        }


class PrincipalActorBase(RootSchema):
    __tablename__ = 'actor'
    __aux_routes__ = {
        '/activate/email/send/': SendEmailLink,
        '/created/activate/email/{reg_token}/': CreatedUserActivationView,
        '/activate/phone/send/': SendPhoneLink,
        '/activate/phone/{verification_code}/': PhoneActivationView,
        '/verify/email/{verif_code}/': VerfiyEmailAddress,
        '/login/': LoginView,
        '/logout/': LogoutView,
        '/pass/reset/': ResetPassword,
        '/pass/change/{checkcode}/': ChangePassword,
        '/invite/': InviteUser,
        '/grant/{actor_id}/roles/': GrantRole,
        '/grant/{actor_id}/workspaces/': GrantWorkspace,
        '/list/members/{workspace}/': ListMembers,
        '/otp/secret/': GetOtpSecret
    }
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('actor_id_seq'),
                        read_only=True, primary_key=True)
    username = fields.String(sqlfield=sql.String(255), unique=True)
    status = fields.Integer(sqlfield=sql.Integer, default='0', missing=0)
    phone = fields.String(sqlfield=sql.String(255), unique=True)
    phone_confirmed = fields.Boolean(sqlfield=sql.Boolean, read_only=True)
    email = fields.Email(sqlfield=sql.String(255), required=True, unique=True)
    email_confirmed = fields.Boolean(sqlfield=sql.Boolean, read_only=True)
    password = fields.String(sqlfield=sql.String(555), required=True, validate=validate.Length(min=6))
    roles = fields.List(
        fields.String(),
        validate=[validators.must_not_be_empty],
        sqlfield=JSONB
    )
    scope = fields.String(sqlfield=sql.String(255))
    otp_secret = fields.String(sqlfield=sql.String(24))
    details = fields.Dict(sqlfield=JSONB)

    @validates('roles')
    def roles_validator(self, val):
        delta = set(val) - self.app.allowed_roles
        if delta:
            bad_roles = ', '.join(delta)
            raise ValidationError(f'Invalid roles: {bad_roles}')

    async def before_update(self, parent, request, payload, selector):
        if 'phone' in payload:
            payload['phone'] = clean_phone_number(payload['phone'])
        if 'details' in payload:
            payload['details'] = payload['details'].adapted
            scope = request.session.scope
            scope_inst = request.app.config.scopes[scope]
            details = await parent._validate_singular_payload(
                payload['details'],
                schema=scope_inst(),
                envelope_key='details')
            payload['details'] = Json(details)
            return payload

    async def after_update(self, request, select_payload, payload, res):
        actor_id = request.session.actor_id

        names_changed = []
        values_changed = []

        if 'email' in payload:
            names_changed.append('email')
            values_changed.append(payload['email'])
        if 'phone' in payload:
            names_changed.append('phone')
            values_changed.append(payload['phone'])

        if not names_changed:
            return

        set_values = ','.join(f'{name}_confirmed=False' for name in names_changed)
        query = f'UPDATE actor SET {set_values} WHERE id=%s RETURNING 1'

        async with request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(query, [actor_id])
                except postgres_errors.IntegrityError as ierr:
                    raise post_exceptions.ValidationError({"payload": parse_postgres_err(ierr)})
                except Exception:
                    request.app.error_logger.exception(
                        'Failed updating the actor resource', query=cur.query.decode())
                    raise post_exceptions.HandledInternalError()
                res = await cur.fetchone()
                if not res or not res[0]:
                    raise post_exceptions.UpdateFailed()

        if 'email' in payload:
            # send an email to verify the new email address
            await spawn(request, send_email_verification_link(request, payload['email']))

        # cache the new session context
        account_key = request.app.config.account_details_key.format(actor_id)
        pipe = request.app.redis_cli.pipeline()
        zipped_changes = dict(zip(names_changed, values_changed))
        for name, val in zipped_changes.items():
            pipe.hset(account_key, name, val)
            pipe.hset(account_key, f'{name}_confirmed', '0')
            request.session._session_ctxt[f'{name}_confirmed'] = False
        await pipe.execute()
        request.app.info_logger.info("Session context updated", actor_id=actor_id, changes=zipped_changes)
        return web.HTTPNoContent()

    async def after_put(self, *args):
        return await self.after_update(*args)

    async def after_patch(self, *args):
        return await self.after_update(*args)

    async def procure_payload(self, request, payload):
        if request.query.get('inv'):
            payload['email'] = 'dummy@example.com'
        return payload

    async def process_invited_actor(self, invitation_token, request, data, parent):
        ttl = request.app.config.invitation_link_ttl
        try:
            decrypted_payload = request.app.commons.decrypt(invitation_token, ttl=ttl)
        except InvalidToken:
            raise web.HTTPForbidden(reason='Invitation link is invalid or expired')
        try:
            raw_roles, raw_workspaces, scope, email, workspace = decrypted_payload.split(":")
        except ValueError:
            raise post_exceptions.ValidationError({
                'query': {
                    'inv': ['Invalid value']
                }
            })

        data['email'] = email
        data['roles'] = Json(raw_roles.split(','))
        data['scope'] = scope
        workspaces_invited_to = raw_workspaces.split(',')

        scope_inst = ScopeBase._scopes[scope]

        # If the invitee already exists in the actor table, return its workspaces
        query = (
            "WITH user_cte AS (SELECT id FROM actor WHERE email=%s)\n"
            "SELECT json_agg(DISTINCT workspace.id) FROM workspace, user_cte WHERE user_cte.id::text::jsonb <@ members" # noqa
        )

        async with request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [data['email']])
                ret = await cur.fetchone()
                if ret and ret[0]:
                    joined_workspaces = ret[0]
                    raw_workspaces = ','.join(w for w in workspaces_invited_to
                                              if w not in joined_workspaces)
                else:
                    # if invitee doesn't exist yet, let's also validate the details key.
                    # Its schema is represented by `scope_inst`
                    details = data.get('details', {})
                    details_payload = await parent._validate_singular_payload(
                        details, schema=scope_inst(), envelope_key='details')
                    data['details'] = Json(details_payload)

        salt = bcrypt.gensalt()
        data['password'] = bcrypt.hashpw(data['password'].encode(), salt).decode()
        data['email_confirmed'] = True

        if request.app.config.activate_invited_user_with_sms:
            # demand the presence of phone field in the payload
            try:
                payload = await request.json()
            except Exception:
                raise web.HTTPBadRequest(reason='cannot read payload')

            if 'phone' not in payload or not payload['phone']:
                raise post_exceptions.ValidationError({
                    'phone': ['This field is required']
                })
            data['status'] = 0
        else:
            data['status'] = 1

        on_conflict = 'ON CONFLICT (email) DO UPDATE SET email_confirmed=true'
        insert_query_stmt = self.insert_query_stmt
        vals = ',' + ','.join(f"%({colname})s" for colname in data)
        cols = ',' + ','.join(data)
        insert_query = insert_query_stmt.format(cols=cols, vals=vals, on_conflict=on_conflict,
                                                session=request.session)

        async with request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                async with cur.begin():
                    await cur.execute(insert_query, data)
                    actor_id = (await cur.fetchone())[0]

                    workspaces_query = (
                        f'''UPDATE workspace SET members=members || '[{actor_id}]'::jsonb '''
                        f"WHERE id=ANY('{{{raw_workspaces}}}') "
                        "RETURNING id"
                    )
                    async with request.app.db_pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(workspaces_query)
                            if not await cur.fetchone():
                                request.app.error_logger.error(
                                    'Failed to add workspace for invited user',
                                    actor_id=actor_id,
                                    query=cur.query.decode())
                                raise post_exceptions.WorkspaceAdditionFailed()

        raise web.HTTPOk(body=dumps({'actor_id': actor_id}), content_type='application/json')

    async def process_created_actor(self, data, request, parent):
        try:
            data['roles'] = data['roles'].adapted
            data['roles'].append('Owner')
        except KeyError:
            data['roles'] = ['Owner']
        data['roles'] = set(data['roles'])
        data['workspaces'] = []

        scope = data.get('scope', 'Generic').title()

        if scope != 'Generic':
            if scope not in ScopeBase._scopes:
                raise post_exceptions.ValidationError({
                    'scope': ['Scope doesn\'t exist']
                })

            # extend submitted roles with the ones defined on the assigned scope
            scope_roles = request.app.config.scopes[scope].Meta.roles
            data['roles'] |= set(scope_roles)

            # validate `details` key, using the schema instance described by `scope` key
            scope_inst = ScopeBase._scopes[scope]
            details = data.get('details', {})
            details_payload = await parent._validate_singular_payload(
                details, schema=scope_inst(), envelope_key='details')
            data['details'] = details_payload

        async with request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT 1 FROM actor WHERE email=%s', [data['email']])
                ret = await cur.fetchone()
                if ret:
                    raise post_exceptions.ValidationError({
                        'email': ['Email address not available.']
                    })

        salt = bcrypt.gensalt()
        data['password'] = bcrypt.hashpw(data['password'].encode(), salt).decode()
        return request.app.created_email_confirmation_link, request.app.config.activation_link_ttl

    async def before_post(self, parent, request, data):
        if 'details' in data:
            data['details'] = data['details'].adapted
        data['otp_secret'] = pyotp.random_base32(24)
        query = request.query
        invitation_token = query.get('inv')
        if invitation_token is not None and invitation_token.endswith('/'):
            invitation_token = invitation_token[:-1]

        if invitation_token:
            await self.process_invited_actor(invitation_token, request, data, parent)
        else:
            link_path_base, ttl = await self.process_created_actor(data, request, parent)

        if APP_MODE == 'test':
            activation_link = await send_email_activation_link(request, data, link_path_base, ttl)
            raise web.HTTPOk(reason='Account creation pending', text=activation_link)

        if 'phone' in data:
            data['phone'] = clean_phone_number(data['phone'])

        await spawn(request, send_email_activation_link(request, data, link_path_base, ttl))
        raise web.HTTPNoContent(reason='Account creation pending')

    class AccessLogging:
        authed = '*'

    class Public:
        disallow_authed = ['post']

        class permissions:
            post = {}

    class Private:
        get_by = ['id', 'phone', 'username', 'status', 'email', 'scope', 'roles', 'details',
                  'phone_confirmed', 'email_confirmed']
        list_by = ['id', 'phone', 'username', 'status', 'email', 'scope', 'roles', 'details',
                   'phone_confirmed', 'email_confirmed']
        order_by = ['id', 'username', 'email']

        class permissions:
            get = {
                '*': CheckedPermClause('self.id = session.actor_id')
            }
            list = {
                'Admin': CheckedPermClause('self.status = session.status')
            }
            update = {
                '*': CheckedPermClause('self.id = session.actor_id')
            }

    class Meta:
        exclude_from_updates = ['id', 'status', 'password', 'scope',
                                'roles', 'email_confirmed', 'phone_confirmed']
        route_base = 'actor'
        order_by = ['id', 'username', 'email', 'scope']

        def default_get_critera(request):
            return {'id': request.session.actor_id}
        # excluded_ops = ['delete']


class PrincipalActor(PrincipalActorBase):
    '''Handles all operations on Actor'''
