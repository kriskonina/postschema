def parse_schemas(schemas):
    for schema_name, schema_inst in schemas.full_iter:
        shield_cls = getattr(schema_inst, 'Shield', None)
        schema_inst.shields = {}
        if shield_cls:
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
                shield_op = getattr(shield_cls, op, None)
                if shield_op:
                    if shield_op not in ('otp', 'sms'):
                        raise ValueError(
                            f'{schema_inst.__module__}.{schema_inst.__name__}.Shield.{op} defines an invalid shield method (can be "otp" or "sms")')
                    schema_inst.shields[op] = shield_op

