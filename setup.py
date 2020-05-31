from setuptools import setup

setup(
    name='postschema',
    version='0.17.1',
    description='Async python ORM for postgres',
    url='https://github.com/kriskavalieri/postschema',
    author='Kris Kavalieri',
    author_email='kris.kavalieri@gmail.com',
    license='AGPL-3.0-only',
    packages=['postschema', 'postschema.alembic', 'postschema.auth'],
    zip_safe=False,
    python_requires='>=3.7',
    include_package_data=True,
    install_requires=[
        'aiohttp>=3.6.1',
        'aiojobs==0.2.2',
        'aiopg>=1.0.0',
        'aioredis>=1.3.0',
        'alembic>=1.2.1',
        'aiohttp_jinja2>=1.1.1',
        'async-property==0.2.1',
        'marshmallow>=3.2.0',
        'psycopg2-binary>=2.8.3',
        'SQLAlchemy>=1.3.8',
        'aiosmtplib>=1.1.0',
        'bcrypt>=3.1.7',
        'aiohttp_jinja2>=1.1.1',
        'cryptography>=2.7',
        'colorama==0.4.1',
        'structlog>=19.2.0',
        'apispec>=3.1.0',
        'cached-property>=1.5.1',
        'pyotp>=2.3.0',
        'orjson>=2.2.0',
        'sqlalchemy_utils>=0.36.1'
    ],
    extras_required={
        'sentry': ['sentry==0.14.1']
    }
)
