class Commons:
    def __init__(self, app):
        self.app = app

    def encrypt(self, string):
        encoded_payload = str(string).encode()
        return self.app.config.fernet.encrypt(encoded_payload).decode()

    def decrypt(self, encrypted, **opts):
        encoded_payload = encrypted.encode()
        return self.app.config.fernet.decrypt(encoded_payload, **opts).decode()
