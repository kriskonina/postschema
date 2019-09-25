import os
from pathlib import Path

import aiopg
# import aioredis
from aiohttp import web
from aiojobs.aiohttp import setup as aiojobs_setup
from postschema import setup_postschema

import schema # noqa

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent

POSTSCHEMA_PORT = os.environ.get('POSTSCHEMA_PORT')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')
REDIS_DB = int(os.environ.get('REDIS_DB', '3'))
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')


async def cleanup(_app):
    _app.db_pool.terminate()
    # _app.redis_cli.close()
    # await _app.redis_cli.wait_closed()


async def init_resources(_app):
    dsn = f'dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST} port={POSTGRES_PORT}' # noqa
    pool = await aiopg.create_pool(dsn, echo=False, pool_recycle=3600)
    _app.db_pool = pool
    # redis_pool = await aioredis.create_pool(
    #     f"redis://{REDIS_HOST}:{REDIS_PORT}",
    #     db=REDIS_DB,
    #     encoding="utf8")
    # _app.redis_cli = aioredis.Redis(redis_pool)
    print("* Resources set up OK.")


def create_app():
    app = web.Application()
    aiojobs_setup(app)
    setup_postschema(app)
    app.on_startup.append(init_resources)
    app.on_cleanup.append(cleanup)
    return app


if __name__ == '__main__':
    app = create_app()
    web.run_app(app, host='localhost', port=POSTSCHEMA_PORT)