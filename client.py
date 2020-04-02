import socket

from utils.exceptions import CommandError, Error
from utils.protocol_handler import ProtocolHandler


def gen_id():
    i = 0
    while True:
        i += 1
        yield i


client_id_factory = gen_id()


class Client:
    def __init__(self, host="127.0.0.1", port=31337):
        self._protocol = ProtocolHandler()
        self._connection = socket.create_connection((host, port))
        self._f = self._connection.makefile("rwb")

        self.id = next(client_id_factory)

    def stop(self):
        self._connection.close()

    def _execute(self, *args):
        self._protocol.write_response(self._f, args)
        resp = self._protocol.handle_request(self._f)

        if isinstance(resp, Error):
            raise CommandError(resp.message)

        return resp

    def get(self, key):
        return self._execute("GET", key)

    def set(self, key, value):
        return self._execute("SET", key, value)

    def delete(self, key):
        return self._execute("DELETE", key)

    def flush(self):
        return self._execute("FLUSH")

    def mget(self, *keys):
        return self._execute("MGET", *keys)

    def mset(self, *items):
        return self._execute("MSET", *items)

    def getall(self):
        return self._execute("GETALL")
