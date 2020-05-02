import inspect
import os

from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from functools import lru_cache
from glob import glob
from hashlib import md5
from importlib import import_module
from pathlib import Path
from typing import Callable, Optional, List

import aiohttp
import aiohttp_jinja2
import aiopg
import aioredis
import jinja2
import pytz

from aiojobs.aiohttp import setup as aiojobs_setup
from aiohttp.web_urldispatcher import UrlDispatcher
from cryptography.fernet import Fernet

DEFAULT_TZ = os.environ.get("DEFAULT_TZ", 'UTC')
local_tz = pytz.timezone(DEFAULT_TZ)

from .commons import Commons
from .core import build_app
from .decorators import auth
from .logging import setup_logging
from .schema import PostSchema, _schemas as registered_schemas # noqa
from .utils import generate_random_word, json_response, dumps

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR  # / "postschema"
Q_PATTERN = BASE_DIR / "sql" / "queries" / "*.sql"

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')
REDIS_DB = int(os.environ.get('REDIS_DB', '3'))
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
DEFAULT_ROLES = {'*', 'Admin', 'Owner', 'Manager', 'Staff'}
THIS_DIR = Path(__file__).parent
AUTH_TEMPLATES_DIR = THIS_DIR / 'auth' / 'templates'
ROLES = []

ALLOWED_OPERATIONS = ['post', 'patch', 'put', 'delete', 'get', 'list']


async def default_send_sms(*args):
    pass


async def cleanup(app):
    app.redis_cli.close()
    await app.redis_cli.wait_closed()
    app.db_pool.terminate()


async def on_connect_postgres(conn):
    async with conn.cursor() as cur:
        await cur.execute("SET session TIME ZONE %s", [local_tz.zone])


async def init_resources(app):
    dsn = f'dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST} port={POSTGRES_PORT}' # noqa
    pool = await aiopg.create_pool(dsn, echo=False, pool_recycle=3600, on_connect=on_connect_postgres)
    app.db_pool = pool
    redis_pool = await aioredis.create_pool(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        db=REDIS_DB,
        encoding="utf8")
    app.redis_cli = aioredis.Redis(redis_pool)
    app.info_logger.debug("Resources set up OK")


async def startup(app):
    app.commons = Commons(app)


async def reset_form_context(request):
    checkcode = request.match_info.get('checkcode')
    if not checkcode:
        raise aiohttp.web.HTTPNotFound()

    key = f'postschema:pass:reset:{checkcode}'
    data = await request.app.redis_cli.hgetall(key)
    if not data:
        raise aiohttp.web.HTTPUnauthorized(reason='Reset link expired or checkcode invalid')

    swapcode = data.pop('swapcode')
    newkey = f'postschema:pass:verify:{swapcode}'
    expire = request.app.config.reset_link_ttl
    await request.app.redis_cli.delete(key)
    await request.app.redis_cli.set(newkey, data['id'], expire=expire)
    return swapcode


@aiohttp_jinja2.template('set_new_password.html')
async def pass_reset_checkcode_template(request):
    swapcode = await reset_form_context(request)
    return {'checkcode': swapcode}


async def pass_reset_checkcode_raw(request):
    swapcode = await reset_form_context(request)
    return json_response({'checkcode': swapcode})


@dataclass
class PluginConfig:
    # shield
    shield_cookie: str = 'postshield'
    sms_shield_msg: str = 'Your postschema confirmation number: {code}'

    # sentry
    sentry_dsn: str = ''


@dataclass
class AppConfig:
    # general
    alembic_dest = None
    constraint_to_error_map: dict = field(default_factory=dict)
    description: str = ''
    node_id: str = generate_random_word(10)
    session_key: str = 'postsession'
    template_dirs: List[str] = field(default_factory=list)
    url_prefix: str = ''
    version: str = 'unreleased'

    # auth
    activate_invited_user_with_sms: bool = False
    fernet: Fernet = Fernet(os.environ.get('FERNET_KEY').encode())
    redirect_reset_password_to: str = ''
    roles: List[str] = field(default_factory=list)
    password_reset_form_link: str = ''

    # links
    created_email_confirmation_link: str = '{{scheme}}{url_prefix}/actor/created/activate/email/{{reg_token}}/'
    email_verification_link: str = '{{scheme}}{url_prefix}/actor/verify/email/{{verif_token}}/'
    invitation_link: str = '{scheme}actor/?inv={payload}'
    invited_email_confirmation_link: str = '{{scheme}}{url_prefix}/actor/invitee/activate/email/{{reg_token}}/'

    # logging
    initial_logging_context: dict = field(default_factory=dict)
    info_logger_processors: Optional[list] = None
    error_logger_processors: Optional[list] = None
    default_logging_level: Optional[int] = None

    # TTLs
    session_ttl: int = 3600 * 24 * 30  # a month
    invitation_link_ttl: int = 3600 * 24 * 7  # a week
    activation_link_ttl: int = 3600 * 6  # 6 hours
    sms_verification_ttl: int = 60  # 1 minute
    reset_link_ttl: int = 60 * 10  # 10 minutes

    # sms
    send_sms: Optional[Callable] = None
    sms_sender: str = os.environ.get('DEFAULT_SMS_SENDER')
    sms_verification_cta: str = 'Enter code to confirm number: {verification_code}'

    # email templating
    activation_email_subject: str = 'Activate your account'
    invitation_email_subject: str = 'Create your new account'
    reset_pass_email_subject: str = 'Reset your password'
    verification_email_subject: str = 'Verify your new email address'
    activation_email_text: str = 'Follow this link to activate the account -> {activation_link}'
    invitation_email_text: str = ("You were invited to join the application by {by}.\n"
                                  "Click the link below to create your account\n{registration_link}")
    reset_pass_email_text: str = 'Follow this link to reset your password -> {reset_link}'
    verification_email_text: str = 'Follow this link to verify your new email address -> {verif_link}'
    activation_email_html: str = ''
    reset_pass_email_html: str = ''
    invitation_email_html: str = ''
    verification_email_html: str = ''

    plugins: List[str] = field(default_factory=list)
    before_request_hooks: List[Callable] = field(default_factory=list)
    after_request_hooks: List[Callable] = field(default_factory=list)
    metainfo_extender: Callable = None

    def _update(self, cls):
        for k, v in cls.__dict__.items():
            setattr(self, k, v)


@dataclass(frozen=True)
class ImmutableConfig:
    account_details_key: str = 'postschema:account:{}'
    workspaces_key: str = 'postschema:workspaces:{}'
    roles_key: str = 'postschema:roles:{}'
    scopes: dict = field(default_factory=dict)


def exception_handler(logger):
    def wrapped(scheduler, context):
        exc = context['exception']
        job = context['job']
        coroname = job._coro.__name__
        logger.error(f'Aiojob exception in {coroname}', exception=exc)
    return wrapped


class ConfigBearer(dict):
    def __getattribute__(self, key):
        'Allow property access to session context without accessing `self.session_ctxt`'
        try:
            return super().__getattribute__(key)
        except AttributeError:
            return self[key]

    def __setattr__(self, key, val):
        self[key] = val

    def __delattr__(self, key):
        del self[key]


@dataclass
class PathReturner:
    json_spec: dict
    router: UrlDispatcher
    roles: tuple = ()

    def __hash__(self):
        return hash(tuple(self.roles))

    @property
    @lru_cache()
    def paths_by_roles(self):
        spec = deepcopy(self.json_spec)
        for path, pathobj in self.json_spec['paths'].items():
            for op, op_obj in pathobj.copy().items():
                with suppress(KeyError, TypeError):
                    authed = set(op_obj['security'][0]['authed'])
                    if '*' in authed or 'Admin' in self.roles:
                        continue
                    if not (authed & self.roles):
                        del spec['paths'][path][op]

        out = {}
        for resource in self.router.resources():
            try:
                route = resource._routes[0]
            except AttributeError:
                # skip subapp
                continue

            if route._method in ['OPTIONS', 'POST']:
                continue

            try:
                url = resource._path
            except AttributeError:
                url = resource._formatter

            viewname = resource._routes[0].handler.__name__.replace('View', '')
            viewname = viewname[0].lower() + viewname[1:]

            view_spec = spec['paths'].get(url)
            if not view_spec:
                continue

            for method, obj in view_spec.items():
                if method == 'options':
                    method = 'list'

                try:
                    schema_key = obj['requestBody']['content']['application/json']['schema']['$ref'].rsplit('/', 1)[1]
                    schema = spec['components']['schemas'][schema_key]
                except KeyError:
                    schema = {}

                out[f'{viewname}:{method}'] = {
                    'url': url,
                    'authed': 'security' in obj,
                    'schema': schema
                }
        return out


async def apispec_metainfo(request):
    '''Return current hashsum for the OpenAPI spec + authentication status'''
    is_authed = request.session.is_authed

    base_ctxt = {
        'spec_hashsum': request.app.spec_hash,
        'authed': is_authed
    }

    if not is_authed:
        return json_response(base_ctxt)

    with suppress(AttributeError, ValueError, TypeError):
        ext = await request.app.config.metainfo_extender(request) or {}
        base_ctxt.update(ext)

    return json_response({
        'scopes': request.app.scopes,
        'roles': request.app.allowed_roles,
        **base_ctxt
    })


def setup_postschema(app, appname: str, *,
                     plugin_config={},
                     extra_config={},
                     **app_config):

    roles = app_config.get('roles', [])
    ROLES = frozenset(role.title() for role in DEFAULT_ROLES | set(roles))
    os.environ['ROLES'] = dumps(ROLES)

    app_config = AppConfig(**app_config)
    app_config.initial_logging_context['version'] = app_config.version
    app_config.initial_logging_context['app_mode'] = app_config.app_mode = os.environ.get('APP_MODE')

    plugin_config = PluginConfig(**plugin_config)

    url_prefix = app_config.url_prefix

    if url_prefix and not url_prefix.startswith('/'):
        url_prefix = '/' + url_prefix
    if url_prefix.endswith('/'):
        url_prefix = url_prefix[:-1]

    app.app_name = appname
    app.url_prefix = url_prefix
    app.app_mode = app_config.app_mode
    app.app_description = app_config.description
    app.version = app_config.version
    app.queries = {
        os.path.split(filename)[1].rsplit('.', 1)[0]: open(filename).read().strip()
        for filename in glob(str(Q_PATTERN))
    }

    # create loggers
    info_logger, error_logger, access_logger = setup_logging(
        app_config.info_logger_processors,
        app_config.error_logger_processors,
        app_config.default_logging_level)

    from . import middlewares
    from .actor import PrincipalActor
    from .core import Base
    from .provision_db import setup_db
    from .scope import ScopeBase
    from .workspace import Workspace  # noqa

    ScopeBase._validate_roles(ROLES)

    # setup middlewares
    app.middlewares.append(middlewares.postschema_middleware)

    app.info_logger = info_logger.new(**app_config.initial_logging_context)
    app.error_logger = error_logger.new(**app_config.initial_logging_context)
    app.access_logger = access_logger.new(**app_config.initial_logging_context)

    aiojobs_setup(app, exception_handler=exception_handler(app.error_logger))

    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(
        [AUTH_TEMPLATES_DIR, *app_config.template_dirs]
    ))

    if not app_config.redirect_reset_password_to:
        app_config.redirect_reset_password_to = redirect_reset_password_to = '/passform/{checkcode}/'
        app.add_routes(
            [aiohttp.web.get(redirect_reset_password_to, pass_reset_checkcode_template)]
        )
    else:
        app.add_routes(
            [aiohttp.web.get(app_config.redirect_reset_password_to, pass_reset_checkcode_raw)]
        )

    if not app_config.password_reset_form_link:
        app_config.password_reset_form_link = '{scheme}passform/{checkcode}/'

    app.on_startup.extend([startup, init_resources])
    app.on_cleanup.append(cleanup)

    if app_config.alembic_dest is None:
        stack = inspect.stack()
        stack_frame = stack[1]
        calling_module_path = Path(inspect.getmodule(stack_frame[0]).__file__).parent
        os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', str(calling_module_path))
    else:
        alembic_destination = str(app_config.alembic_dest)
        assert os.path.exists(alembic_destination),\
            "`alembic_dest` argument doesn't point to an existing directory"
        os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', alembic_destination)

    app_config.activation_email_html = jinja2.Template(app_config.activation_email_html)
    app_config.invitation_email_html = jinja2.Template(app_config.invitation_email_html)
    app_config.reset_pass_email_html = jinja2.Template(app_config.reset_pass_email_html)
    app_config.verification_email_html = jinja2.Template(app_config.verification_email_html)

    config = ConfigBearer(**extra_config, **plugin_config.__dict__)

    # extend with immutable config opts
    app_config._update(ImmutableConfig(scopes=ScopeBase._scopes))
    config.update(app_config.__dict__)
    app.config = config
    app.scopes = frozenset(ScopeBase._scopes)
    app.allowed_roles = frozenset([role for role in ROLES if role not in ['Admin', '*', 'Owner']])

    app.principal_actor_schema = PrincipalActor
    app.schemas = registered_schemas
    app.config.roles = ROLES
    app.send_sms = app_config.send_sms or default_send_sms
    app.invitation_link = app_config.invitation_link
    app.created_email_confirmation_link = app_config.created_email_confirmation_link.format(
        url_prefix=url_prefix)
    app.invited_email_confirmation_link = app_config.invited_email_confirmation_link.format(
        url_prefix=url_prefix)

    # build the views
    router, openapi_spec = build_app(app, registered_schemas)

    # hash the spec
    app.spec_hash = md5(dumps(openapi_spec).encode()).hexdigest()

    # map paths to roles
    paths_by_roles = PathReturner(openapi_spec, router)

    # parse plugins
    installed_plugins = {}
    for plugin in app_config.plugins:
        print(f"* Installing plugin {plugin}...")
        assert plugin in ['shield', 'sentry'], f'Plugin `{plugin}` is not recognized'
        installed_plugins[plugin] = plugin_module = import_module(f'.{plugin}', 'postschema')
        with suppress(AttributeError, TypeError):
            plugin_module.install(app)

    app.installed_plugins = installed_plugins.keys()

    @auth(roles=['Admin'])
    @aiohttp_jinja2.template('redoc.html')
    async def apidoc(request):
        return {'appname': request.app.app_name}

    @auth(roles=['Admin'])
    async def apispec_context(request):
        return json_response(openapi_spec)

    @auth(roles=['*'], email_verified=False)
    async def actor_apispec(request):
        '''OpenAPI JSON spec filtered to include only the public
        and requester-specific routes.
        '''
        paths_by_roles.roles = set(request.session.roles)
        return json_response(paths_by_roles.paths_by_roles)

    try:
        app.info_logger.debug("Provisioning DB...")
        setup_db(Base)
        app.info_logger.debug("DB provisioning done")
    except Exception:
        app.error_logger.exception("Provisioning failed", exc_info=True)
        raise

    router.add_get(f'{url_prefix}/doc/', apidoc)
    router.add_get(f'{url_prefix}/doc/openapi.yaml', apispec_context)
    router.add_get(f'{url_prefix}/doc/spec.json', actor_apispec)
    router.add_get(f'{url_prefix}/doc/meta/', apispec_metainfo)
