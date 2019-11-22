import inspect
import os
import traceback

from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Callable, Optional, List

import aiohttp
import aiohttp_jinja2
import jinja2
import ujson
from aiojobs.aiohttp import setup as aiojobs_setup
from cryptography.fernet import Fernet

from .commons import Commons
from .core import build_app
from .logging import setup_logging
from .schema import PostSchema, _schemas as registered_schemas # noqa
from .utils import generate_random_word

DEFAULT_SCOPES = {'*', 'Admin', 'Owner', 'Manager', 'Staff'}
THIS_DIR = Path(__file__).parent
AUTH_TEMPLATES_DIR = THIS_DIR / 'auth' / 'templates'
SCOPES = []


async def default_send_sms(*args):
    pass


async def cleanup(app):
    await app.cli_session.close()


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
    scopes: List[str] = field(default_factory=list)
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
    scopes_key: str = 'postschema:scopes:{}'
    roles: dict = field(default_factory=dict)


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


def setup_postschema(app, appname: str, *,
                     version: str = 'unreleased',
                     template_dirs: list = [],
                     description: str = '',
                     send_sms: Optional[Callable] = None,
                     info_logger_processors: Optional[list] = None,
                     error_logger_processors: Optional[list] = None,
                     initial_logging_context: Optional[dict] = {},
                     default_logging_level: Optional[int] = None,
                     alembic_dest=None, extra_config={}, **app_config):

    scopes = app_config.get('scopes', [])
    SCOPES = frozenset(scope.title() for scope in DEFAULT_SCOPES | set(scopes))
    os.environ['SCOPES'] = ujson.dumps(SCOPES)

    initial_logging_context['version'] = version
    initial_logging_context['app_mode'] = app_mode = os.environ.get('APP_MODE')
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
    from .role import RoleBase
    from .workspace import Workspace  # noqa

    RoleBase._validate_scopes(SCOPES)

    app.info_logger = info_logger.new(**initial_logging_context)
    app.error_logger = error_logger.new(**initial_logging_context)

    aiojobs_setup(app, exception_handler=exception_handler(app.error_logger))

    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(
        [AUTH_TEMPLATES_DIR, *template_dirs]
    ))

    app.cli_session = aiohttp.ClientSession
    if 'redirect_reset_password_to' not in app_config:
        app_config['redirect_reset_password_to'] = redirect_reset_password_to = '/passform/{checkcode}/'
        app.add_routes(
            [aiohttp.web.get(redirect_reset_password_to, pass_reset_form)]
        )

    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)

    if alembic_dest is None:
        stack = inspect.stack()
        stack_frame = stack[1]
        calling_module_path = Path(inspect.getmodule(stack_frame[0]).__file__).parent
        os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', str(calling_module_path))
    else:
        alembic_destination = str(alembic_dest)
        assert os.path.exists(alembic_destination), "`alembic_dest` argument doesn't point to an existing directory"
        os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', alembic_destination)

    config = ConfigBearer(extra_config)
    app_config = AppConfig(**app_config)
    # extend with immutable config opts
    app_config._update(ImmutableConfig(roles=RoleBase._roles))
    config.update(app_config.__dict__)
    app.config = config

    app.principal_actor_schema = PrincipalActor
    app.schemas = registered_schemas
    app.config.scopes = SCOPES
    app.send_sms = partial(send_sms or default_send_sms, app)
    build_app(app, registered_schemas)

    try:
        app.info_logger.debug("Provisioning DB...")
        setup_db(Base)
        app.info_logger.debug("DB provisioning done")
    except Exception:
        app.error_logger.exception("Provisioning failed", exc_info=True)
        raise
