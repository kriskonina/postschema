import os
import ujson

DEFINED_SCOPES_CACHE = []


def summary(msg):
    def wrapped(fn):
        fn.summary = msg
        return fn
    return wrapped


def auth(scopes: list, phone_verified=False, email_verified=True):
    """Provide auth control to custom, standalone views.
    It's possible to require that email/phone is verified
    for the requesting actor, or/and that the requesting
    actor is a holder of certain scopes.
    """
    global DEFINED_SCOPES_CACHE
    DEFINED_SCOPES = DEFINED_SCOPES_CACHE or set(ujson.loads(os.environ.get('SCOPES')))
    if not DEFINED_SCOPES_CACHE:
        DEFINED_SCOPES_CACHE = DEFINED_SCOPES

    def wrapped(fn):
        diff = set(scopes) - DEFINED_SCOPES
        if diff:
            raise ValueError(f'Invalid scopes ({diff}) defined for view coroutine `{fn}`')
        perm_options = {
            'scopes': scopes,
            'phone_verified': phone_verified,
            'email_verified': email_verified
        }
        fn._perm_options = perm_options
        return fn
    return wrapped
