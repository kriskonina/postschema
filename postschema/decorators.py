import os
import orjson

DEFINED_ROLES_CACHE = []


def summary(msg):
    def wrapped(fn):
        fn.summary = msg
        return fn
    return wrapped


def auth(roles: list, phone_verified=False, email_verified=True):
    """Provide auth control to custom, standalone views.
    It's possible to require that email/phone is verified
    for the requesting actor, or/and that the requesting
    actor is a holder of certain roles.
    """
    global DEFINED_ROLES_CACHE
    DEFINED_ROLES = DEFINED_ROLES_CACHE or set(orjson.loads(os.environ.get('ROLES')))
    if not DEFINED_ROLES_CACHE:
        DEFINED_ROLES_CACHE = DEFINED_ROLES

    def wrapped(fn):
        diff = set(roles) - DEFINED_ROLES
        if diff:
            raise ValueError(f'Invalid roles ({diff}) defined for view coroutine `{fn}`')
        perm_options = {
            'roles': roles,
            'phone_verified': phone_verified,
            'email_verified': email_verified
        }
        fn._perm_options = perm_options
        return fn
    return wrapped
