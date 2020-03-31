import asyncio

from utils.exceptions import CommandError, Disconnect, Error
from utils.protocol_handler import ProtocolHandler


class Server:
    def __init__(self):
        self._server = None
        self._protocol = ProtocolHandler()
        self._kv = {}

    @property
    def commands(self):
        return self._get_commands().keys()

    async def connection_handler(self, reader, writer):
        # process client requests until client disconnects
        while True:
            try:
                data = await self._protocol.handle_request(reader)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

            await self._protocol.write_response(writer, resp)

    def _get_commands(self):
        return {
            b"GET": self._get,
            b"SET": self._set,
            b"DELETE": self._delete,
            b"FLUSH": self._flush,
            b"MGET": self._mget,
            b"MSET": self._mset,
            b"GETALL": lambda: self._kv,
        }

    def _get(self, key):
        return self._kv.get(key)

    def _set(self, key, value):
        self._kv[key] = value
        return 1

    def _delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def _flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def _mget(self, *keys):
        return [self._kv.get(k) for k in keys]

    def _mset(self, *items):
        data = zip(items[::2], items[1::2])
        for k, v in data:
            self._kv[k] = v
        return len(items)//2

    def get_response(self, data):
        # TODO: unpack data sent by client, execute command, and return value
        if not isinstance(data, list):
            try:
                data = data.split()
            except Exception:
                raise CommandError("request must be a list or a simple string")

        if not data:
            raise CommandError("missing command")

        command = data[0].upper()
        if command not in self.commands:
            raise CommandError(f"unrecognized command: {command}")
        return self._get_commands()[command](*data[1:])

    async def run(self, host="127.0.0.1", port=31337):
        self._server = await asyncio.start_server(self.connection_handler, host=host, port=port)
        async with self._server as server:
            await server.serve_forever()