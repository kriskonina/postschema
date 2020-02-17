import os

try:
    import sentry_sdk
    from sentry_sdk.integrations.aiohttp import AioHttpIntegration
except ImportError:
    raise ImportError('You need to install sentry_sdk to use this plugin!')


def install(app):
    if not app.config.sentry_dsn:
        raise ValueError('You need to set `sentry_dsn` plugin option to use `sentry` plugin')
    sentry_sdk.init(
        app.config.sentry_dsn,
        environment=os.environ.get('APP_MODE', 'dev'),
        integrations=[AioHttpIntegration()],
        release=app.config.version
    )
    app.sentry = sentry_sdk
