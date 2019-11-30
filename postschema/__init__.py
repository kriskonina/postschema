import inspect
import os
import traceback

from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from functools import lru_cache, partial
from hashlib import md5
from pathlib import Path
from typing import Callable, Optional, List

import aiohttp
import aiohttp_jinja2
import aiopg
import aioredis
import jinja2
import ujson
from aiojobs.aiohttp import setup as aiojobs_setup
from aiohttp.web_urldispatcher import UrlDispatcher
from cryptography.fernet import Fernet

from .commons import Commons
from .core import build_app
from .decorators import auth
from .logging import setup_logging
from .schema import PostSchema, _schemas as registered_schemas # noqa
from .utils import generate_random_word, json_response

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


async def init_resources(app):
    dsn = f'dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST} port={POSTGRES_PORT}' # noqa
    pool = await aiopg.create_pool(dsn, echo=False, pool_recycle=3600)
    app.db_pool = pool
    redis_pool = await aioredis.create_pool(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        db=REDIS_DB,
        encoding="utf8")
    app.redis_cli = aioredis.Redis(redis_pool)
    app.info_logger.debug("Resources set up OK")


async def startup(app):
    app.commons = Commons(app)


@aiohttp_jinja2.template('set_new_password.html')
async def pass_reset_form(request):
    checkcode = request.match_info.get('checkcode')
    if not checkcode:
        raise aiohttp.web.HTTPNotFound()

    key = f'postschema:pass:reset:{checkcode}'
    data = await request.app.redis_cli.hgetall(key)
    if not data:
        raise aiohttp.web.HTTPUnauthorized(reason='Invalid checkcode')

    swapcode = data.pop('swapcode')
    newkey = f'postschema:pass:verify:{swapcode}'
    expire = request.app.config.reset_link_ttl
    await request.app.redis_cli.delete(key)
    await request.app.redis_cli.set(newkey, data['id'], expire=expire)

    return {'checkcode': swapcode}


@dataclass
class AppConfig:
    roles: List[str] = field(default_factory=list)
    session_key: str = 'postsession'
    session_ttl: int = 3600 * 24 * 30  # a month
    invitation_link_ttl: int = 3600 * 24 * 7  # a week
    reset_link_ttl: int = 60 * 10  # 10 minutes
    redirect_reset_password_to: str = ''
    node_id: str = generate_random_word(10)
    fernet: Fernet = Fernet(os.environ.get('FERNET_KEY').encode())
    sms_sender: str = os.environ.get('DEFAULT_SMS_SENDER')

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
        logger.error('Aiojob exception', exception=traceback.print_tb(exc.__traceback__))
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
class PathsReturner:
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
                    if not authed & self.roles:
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

            with suppress(KeyError):
                view_spec = spec['paths'][url]
                for method, obj in view_spec.items():
                    if method == 'options':
                        method = 'list'
                    schema_key = obj['requestBody']['content']['application/json']['schema']['$ref'].rsplit('/', 1)[1]
                    schema = spec['components']['schemas'][schema_key]

                    out[f'{viewname}:{method}'] = {
                        'url': url,
                        'authed': 'security' in obj,
                        'schema': schema
                    }
        return out


async def apispec_metainfo(request):
    '''Return current hashsum for the OpenAPI spec + authentication status'''
    return json_response({
        'spec_hashsum': request.app.spec_hash,
        'authed': request.session.is_authed
    })


def setup_postschema(app, appname: str, *,
                     url_prefix: str = '',
                     version: str = 'unreleased',
                     template_dirs: list = [],
                     description: str = '',
                     send_sms: Optional[Callable] = None,
                     info_logger_processors: Optional[list] = None,
                     error_logger_processors: Optional[list] = None,
                     initial_logging_context: Optional[dict] = {},
                     default_logging_level: Optional[int] = None,
                     alembic_dest=None, extra_config={}, **app_config):

    roles = app_config.get('roles', [])
    ROLES = frozenset(role.title() for role in DEFAULT_ROLES | set(roles))
    os.environ['ROLES'] = ujson.dumps(ROLES)

    initial_logging_context['version'] = version
    initial_logging_context['app_mode'] = app_mode = os.environ.get('APP_MODE')
    if url_prefix and not url_prefix.startswith('/'):
        url_prefix = '/' + url_prefix
    if url_prefix.endswith('/'):
        url_prefix = url_prefix[:-1]
    app.url_prefix = url_prefix
    app.app_mode = app_mode
    app.app_name = appname
    app.app_description = description
    app.version = version

    # create loggers
    info_logger, error_logger = setup_logging(info_logger_processors, error_logger_processors,
                                              default_logging_level)

    from .actor import PrincipalActor
    from .core import Base
    from .provision_db import setup_db
    from .scope import ScopeBase
    from .workspace import Workspace  # noqa

    ScopeBase._validate_roles(ROLES)

    app.info_logger = info_logger.new(**initial_logging_context)
    app.error_logger = error_logger.new(**initial_logging_context)

    aiojobs_setup(app, exception_handler=exception_handler(app.error_logger))

    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(
        [AUTH_TEMPLATES_DIR, *template_dirs]
    ))

    if 'redirect_reset_password_to' not in app_config:
        app_config['redirect_reset_password_to'] = redirect_reset_password_to = '/passform/{checkcode}/'
        app.add_routes(
            [aiohttp.web.get(redirect_reset_password_to, pass_reset_form)]
        )

    app.on_startup.extend([startup, init_resources])
    app.on_cleanup.append(cleanup)

    if alembic_dest is None:
        stack = inspect.stack()
        stack_frame = stack[1]
        calling_module_path = Path(inspect.getmodule(stack_frame[0]).__file__).parent
        os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', str(calling_module_path))
    else:
        alembic_destination = str(alembic_dest)
        assert os.path.exists(alembic_destination),\
            "`alembic_dest` argument doesn't point to an existing directory"
        os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', alembic_destination)

    config = ConfigBearer(extra_config)
    app_config = AppConfig(**app_config)

    # extend with immutable config opts
    app_config._update(ImmutableConfig(scopes=ScopeBase._scopes))
    config.update(app_config.__dict__)
    app.config = config

    app.principal_actor_schema = PrincipalActor
    app.schemas = registered_schemas
    app.config.roles = ROLES
    app.send_sms = partial(send_sms or default_send_sms, app)

    # build the views
    router, openapi_spec = build_app(app, registered_schemas)

    # hash the spec
    app.spec_hash = md5(ujson.dumps(openapi_spec).encode()).hexdigest()

    paths_by_roles = PathsReturner(openapi_spec, router)

    paths_by_roles.paths_by_roles

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
