import os
from glob import glob
from pathlib import Path
from time import sleep

from aiohttp import web
from sqlalchemy import create_engine

from postschema import setup_postschema

APP_MODE = os.environ.get("APP_MODE", 'dev')
THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR / ".." / ".." / "postschema"
FNS_PATTERN = BASE_DIR / "sql" / "functions" / "*.sql"

POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')


def setup_db(Base):
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
    Base.metadata.create_all(engine)
    conn.close()
    return engine


def provision_db(engine):
    conn = engine.connect()
    print("\t* Adding Postgres functions...")
    try:
        for fn_sql in glob(str(FNS_PATTERN)):
            print(f'\t  - adding `{os.path.split(fn_sql)[1]}`')
            conn.execute(open(fn_sql).read())
    finally:
        conn.close()


if __name__ == "__main__":
    print("** Provisioning DB...")
    engine = None
    app = web.Application()
    app.print = print
    setup_postschema(app)
    from postschema.core import Base
    try:
        engine = setup_db(Base)
        provision_db(engine)
        print("** Provisioning done")
    except Exception as exc:
        print("!! Provisioning failed")
        raise exc
    finally:
        del app
