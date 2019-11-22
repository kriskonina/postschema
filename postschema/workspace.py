import sqlalchemy as sql
from marshmallow import fields

from .fields import ForeignResources, AutoSessionOwner
from .schema import PostSchema


class Workspace(PostSchema):
    '''Manages workspace operations'''
    __tablename__ = 'workspace'
    id = fields.Integer(sqlfield=sql.Integer, autoincrement=sql.Sequence('workspace_id_seq'),
                        read_only=True, primary_key=True)
    name = fields.String(sqlfield=sql.String(255), required=True, unique=True)
    owner = AutoSessionOwner()
    members = ForeignResources('actor.id')

    async def after_post(self, request, _, workspace_id, actor_id=None):
        "Cache the new workspace on the requester's session object"
        actor_id = actor_id or request.session.actor_id
        workspaces_key = request.app.config.workspaces_key.format(actor_id)
        request.session._session_ctxt['workspaces'] = {workspace_id}
        if await request.app.redis_cli.exists(workspaces_key):
            await request.app.redis_cli.delete(workspaces_key)
            await request.app.redis_cli.sadd(workspaces_key, workspace_id)

    class Meta:
        # changing members redelegated for readibility to auxiliary route defined under Actor
        exclude_from_updates = ['members', 'owner']

        def default_get_critera(request):
            return {'owner': request.session.actor_id}

    class Authed:
        # get_by = ['id']
        class permissions:
            post = ['Owner']

    class Private:
        get_by = ['id', 'name', 'members']

        class permissions:
            get = {
                'Owner': 'self.id = session.workspace'
            }
            update = {
                'Owner': 'self.owner = session.actor_id'
            }

