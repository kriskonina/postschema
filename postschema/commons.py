from psycopg2 import errors as postgres_errors

from . import exceptions as post_exceptions
from .utils import parse_postgres_err, parse_postgres_constraint_err


class Commons:
    def __init__(self, app):
        self.app = app

    def encrypt(self, string):
        encoded_payload = str(string).encode()
        return self.app.config.fernet.encrypt(encoded_payload).decode()

    def decrypt(self, encrypted, **opts):
        encoded_payload = encrypted.encode()
        return self.app.config.fernet.decrypt(encoded_payload, **opts).decode()

    async def execute(self, cur, query, params=[], envelope=None):

        try:
            await cur.execute(query, params)
        except postgres_errors.IntegrityError as ierr:
            parsed_err = None
            constraint_key = parse_postgres_constraint_err(ierr)
            if constraint_key:
                parsed_err = self.app.config.constraint_to_error_map.get(constraint_key)
            parsed_err = parsed_err or parse_postgres_err(ierr)
            errors = {
                envelope: parsed_err
            } if envelope else parsed_err

            self.app.access_logger = self.app.access_logger.bind(
                msg_context=dict(
                    errors=errors,
                    query=cur.query.decode()
                )
            )
            raise post_exceptions.ValidationError(errors)

        except postgres_errors.DataException as derr:
            if 'range lower bound must be less than or equal' in str(derr):
                errors = {
                    'error': 'One of the Range fields\' lower bound is lower than its upper bound value'
                }
            else:
                base = {'error': derr.args[0].split('\n', 1)[0].capitalize()}
                errors = {
                    envelope: base
                } if envelope else base

            self.app.access_logger = self.app.access_logger.bind(
                msg_context=dict(
                    errors=errors,
                    query=cur.query.decode()
                )
            )
            raise post_exceptions.ValidationError(errors)

        except Exception:
            self.app.error_logger.exception('Failed to execute query',
                                            query=cur.query.decode())
            raise
