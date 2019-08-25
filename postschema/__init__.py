from postschema.schema import PostSchema, _schemas as registered_schemas # noqa
from postschema.core import build_app


def setup_postschema(app):
    app.schemas = registered_schemas
    build_app(app, registered_schemas)
