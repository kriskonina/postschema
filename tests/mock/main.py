import os
from datetime import datetime
from pathlib import Path
from random import choice

from aiohttp import web
from faker import Faker
from postschema import setup_postschema
from postschema.utils import json_response

import schema # noqa

APP_MODE = os.environ.get('APP_MODE', 'test')
THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent
POSTSCHEMA_PORT = os.environ.get('POSTSCHEMA_PORT')

fake = Faker()
roles = ['patient', 'doctor', 'operator']


async def generate_test_resources(request):
    query = '''INSERT INTO extrasearch (id, str1, number, date, autodatenow, autodatetimenow, time) VALUES (
        NEXTVAL('extrasearch_id_seq'),%s,%s,%s,%s,%s,%s
    )'''
    async with request.app.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            for i in range(1000):
                await cur.execute(query, [
                    fake.name(),
                    choice(range(10, 100)),
                    fake.date_between_dates(
                        datetime(year=2100, month=1, day=1),
                        datetime(year=2100, month=3, day=30)
                    ),
                    fake.date_between_dates(
                        datetime(year=2200, month=1, day=1),
                        datetime(year=2200, month=3, day=30)
                    ),
                    fake.date_time_between_dates(
                        datetime(year=2200, month=1, day=1),
                        datetime(year=2200, month=3, day=30)
                    ),
                    fake.time()
                ])
    return json_response({})


def create_app():
    app = web.Application()
    config = {
        'roles': roles,
        'email_verification_link': '{{scheme}}/actor/verify/email/{{verif_token}}/',
        'plugins': [
            'shield'
        ]
    }
    plugin_config = {
        'sentry_dsn': '***'
    }
    setup_postschema(
        app, 'test_app',
        version='0.5.0',
        description='My New API Server',
        plugin_config=plugin_config,
        **config)
    if APP_MODE == 'test':
        # manually add endpoints
        app.router.add_get('/genfake/', generate_test_resources)

    return app


if __name__ == '__main__':
    app = create_app()
    web.run_app(app, host='localhost', port=POSTSCHEMA_PORT)
