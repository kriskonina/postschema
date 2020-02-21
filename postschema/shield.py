from collections import defaultdict as dd


def install(app):
    system_roles = app.config.roles
    schemas = app.schemas
    shields = {}
    for schema_name, schema_inst in schemas.full_iter:
        shield_cls = getattr(schema_inst, 'Shield', None)
        if shield_cls:
            local_shields = dd(list)
            authed_cls = getattr(schema_inst, 'Authed', None)
            private_cls = getattr(schema_inst, 'Private', None)
            present_ops_all = set(getattr(authed_cls, 'permissions', object).__dict__.keys()) | \
                set(getattr(private_cls, 'permissions', object).__dict__.keys())
            present_ops = [op for op in present_ops_all if not op.startswith('_')]

            if 'update' in present_ops:
                present_ops.remove('update')
                present_ops.extend(['patch', 'put'])
            if 'read' in present_ops:
                present_ops.remove('read')
                present_ops.extend(['get', 'list'])

            for op in present_ops:
                shield_op_dict = getattr(shield_cls, op, {})
                for roles, shield_op in shield_op_dict.items():
                    if type(roles) == tuple:
                        for role in roles:
                            if role not in system_roles:
                                raise ValueError(
                                    f'Shield at `{schema_name}.Shield.{op}` contains invalid role(s)')
                        allowed_roles = set(roles)
                    elif roles == '*':
                        allowed_roles = set(system_roles)
                    elif roles not in system_roles:
                        raise ValueError(
                            f'Shield at `{schema_name}.Shield.{op}` contains invalid role')
                    else:
                        allowed_roles = set([roles])
                    if shield_op not in ('otp', 'sms'):
                        raise ValueError(
                            f'{schema_inst.__module__}.{schema_inst.__name__}.Shield.{op} defines an invalid shield method (can be "otp" or "sms")')
                    local_shields[op].append([allowed_roles, shield_op])
            shields[schema_name] = dict(local_shields)
    app.shields = shields
