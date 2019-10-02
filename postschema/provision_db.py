import os
import shutil
from glob import glob
from pathlib import Path
from time import sleep

# from aiohttp import web
from alembic.config import Config
from alembic import command
from sqlalchemy import create_engine

# from postschema import setup_postschema

APP_MODE = os.environ.get("APP_MODE", 'dev')
THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR  # / "postschema"
FNS_PATTERN = BASE_DIR / "sql" / "functions" / "*.sql"
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')


def get_url():
    return "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}".format(**os.environ) 


def make_alembic_dir():
    postschema_instance_path = os.environ.get('POSTCHEMA_INSTANCE_PATH')
    alembic_destination = os.path.join(postschema_instance_path, 'alembic')
    versions_dest = os.path.join(alembic_destination, 'versions')
    alembic_ini_destination = os.path.join(postschema_instance_path, 'alembic.ini')

    if not os.path.exists(alembic_destination):
        src = THIS_DIR / 'alembic'
        shutil.copytree(src, alembic_destination)
        os.mkdir(versions_dest)
    if not os.path.exists(alembic_ini_destination):
        src = THIS_DIR / 'alembic.ini'
        shutil.copy2(src, alembic_ini_destination)

    return alembic_ini_destination, postschema_instance_path


def setup_db(Base):
    alembic_ini_destination, postschema_instance_path = make_alembic_dir()
    uri = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/'
    engine = create_engine(uri + "postgres")
    time_wait = 1
    retries = 10
    while retries >= 0:
        try:
            conn = engine.connect()
            conn.execute("COMMIT")
            print("\t* Connected!")
            break
        except Exception:
            print(f"\t ! Can't connect to DB. Waiting {time_wait}s...")
            sleep(time_wait)
            time_wait *= 2
            retries -= 1
    if retries == 0:
        raise RuntimeError("Couldn't establish a DB connection. Terminating")

    try:
        conn.execute("CREATE DATABASE %s" % POSTGRES_DB)
    except Exception as perr:
        if "already exists" not in str(perr):
            raise
    finally:
        conn.close()
        engine.dispose()

    uri += POSTGRES_DB
    engine = create_engine(uri, pool_recycle=3600)
    conn = engine.connect()
    conn.execute("COMMIT")

    print("\t* Adding Postgres functions...")
    # try:
    #     for fn_sql in glob(str(FNS_PATTERN)):
    #         print(f'\t  - adding `{os.path.split(fn_sql)[1]}`')
    #         conn.execute(open(fn_sql).read(), [])
    # finally:
    #     conn.close()

    from sqlalchemy import event
    from sqlalchemy.schema import DDL

    for fn_sql in glob(str(FNS_PATTERN)):
        event.listen(
            Base.metadata,
            'before_create',
            DDL(open(fn_sql).read())
        )

    Base.metadata.create_all(engine)
    if not os.environ.get('TRAVIS', False):
        alembic_dist = os.path.join(postschema_instance_path, 'alembic')
        alembic_cfg = Config(alembic_ini_destination)
        alembic_cfg.set_main_option("sqlalchemy.url", get_url())
        alembic_cfg.set_main_option("script_location", alembic_dist)
        command.stamp(alembic_cfg, "head")

    conn.close()
    return engine


def provision_db(engine):
    conn = engine.connect()
    print("\t* Adding Postgres functions...")
    