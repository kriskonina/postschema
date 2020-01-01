import sqlalchemy as sql
from marshmallow import fields

from .decorators import summary
from .fields import ForeignResources, AutoSessionOwner
from .schema import PostSchema
from .utils import json_response
from .view import AuxView


class GetMembersCount(AuxView):

    @summary('Returns member count for each owned workspace')
    async def get(self):
        query = ('SELECT json_agg(t.t) FROM ('
                 "SELECT json_build_object('id', id, 'count', jsonb_array_length(members)) "
                 "AS t FROM workspace WHERE owner=%s GROUP BY id"
                 ') t')
        async with self.request.app.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [self.request.session.actor_id])
                ret = await cur.fetchone()

        return json_response(ret[0] or {})

    class Authed:
        class permissions:
            get = ['Owner']


class Workspace(PostSchema):
    '''Manages workspace operations'''
    __tablename__ = 'workspace'
    __aux_routes__ = {
        '/membcount/': GetMembersCount
    }
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
        get_by = ['id', 'name']
        list_by = ['id', 'name']

        class permissions:
            get = {
                'Owner': 'self.id = session.workspace'
            }
            list = {
                'Owner': 'self.owner = session.actor_id'
            }
            update = {
                'Owner': 'self.owner = session.actor_id'
            }
