import inspect
import os

from pathlib import Path

from .schema import PostSchema, _schemas as registered_schemas # noqa
from .core import build_app


def setup_postschema(app):
    from .provision_db import setup_db, provision_db
    from .core import Base
    stack = inspect.stack()
    stack_frame = stack[1]
    calling_module_path = Path(inspect.getmodule(stack_frame[0]).__file__).parent
    os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', str(calling_module_path))

    app.schemas = registered_schemas
    build_app(app, registered_schemas)

    try:
        print("* Provisioning DB...")
        engine = setup_db(Base)
        provision_db(engine)
        print("* Provisioning done")
    except Exception:
        print("* Provisioning failed")
        raise
