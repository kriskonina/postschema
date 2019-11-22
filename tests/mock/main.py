import os
from pathlib import Path

from aiohttp import web
from postschema import setup_postschema

import schema # noqa

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent
POSTSCHEMA_PORT = os.environ.get('POSTSCHEMA_PORT')

scopes = ['patient', 'doctor', 'operator']


def create_app():
    from postschema.middlewares import auth_middleware
    app = web.Application(middlewares=[auth_middleware])
    config = {
        'scopes': scopes
    }
    setup_postschema(app, 'test_app', version='0.5.0', description='My New API Server', **config)
    return app


if __name__ == '__main__':
    app = create_app()
    web.run_app(app, host='localhost', port=POSTSCHEMA_PORT)
