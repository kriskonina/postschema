from setuptools import setup

setup(
    name='postschema',
    version='0.5.6',
    description='Async python ORM for postgres',
    url='https://github.com/kriskavalieri/postschema',
    author='Kris Kavalieri',
    author_email='kris.kavalieri@gmail.com',
    license='MIT',
    packages=['postschema', 'postschema.alembic'],
    zip_safe=False,
    python_requires='>=3.7',
    include_package_data=True,
    install_requires=[
        'aiohttp==3.6.1',
        'aiojobs==0.2.2',
        'aiopg==1.0.0',
        'aioredis==1.3.0',
        'alembic==1.2.1',
        'async-property==0.2.1',
        'marshmallow==3.2.0',
        'psycopg2-binary==2.8.3',
        'SQLAlchemy==1.3.8',
        'ujson==1.35'
    ]
)
