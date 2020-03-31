import asyncio

from utils.exceptions import CommandError, Error
from utils.protocol_handler import ProtocolHandler


class Client:
    def __init__(self):
        self._protocol = ProtocolHandler()

    async def _execute(self, *args):
        await self._protocol.write_response(self.writer, args)
        resp = await self._protocol.handle_request(self.reader)

        if isinstance(resp, Error):
            raise CommandError(resp.message)

        return resp

    async def get(self, key):
        return await self._execute("GET", key)

    async def set(self, key, value):
        return await self._execute("SET", key, value)

    async def delete(self, key):
        return await self._execute("DELETE", key)

    async def flush(self):
        return await self._execute("FLUSH")

    async def mget(self, *keys):
        return await self._execute("MGET", *keys)

    async def mset(self, *items):
        return await self._execute("MSET", *items)

    async def getall(self):
        return await self._execute("GETALL")

    async def start(self, host="127.0.0.1", port=31337):
        self.reader, self.writer = await asyncio.open_connection(host, port)
