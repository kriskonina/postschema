import inspect
import os

from pathlib import Path

from .schema import PostSchema, _schemas as registered_schemas # noqa
from .core import build_app


def setup_postschema(app, alembic_dest=None):
    from .provision_db import setup_db, provision_db
    from .core import Base

    if alembic_dest is None:
        stack = inspect.stack()
        stack_frame = stack[1]
        calling_module_path = Path(inspect.getmodule(stack_frame[0]).__file__).parent
        os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', str(calling_module_path))
    else:
        alembic_destination = str(alembic_dest)
        assert os.path.exists(alembic_destination), "`alembic_dest` argument doesn't point to an existing directory"
        os.environ.setdefault('POSTCHEMA_INSTANCE_PATH', alembic_destination)

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
