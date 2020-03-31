from asyncio import StreamReader, StreamWriter
from asyncio.exceptions import IncompleteReadError
from io import BytesIO

from .exceptions import CommandError, Disconnect, Error


class ProtocolHandler:
    def __init__(self):
        self._handlers = {
            b"+": self._handle_simple_string,
            b"-": self._handle_error,
            b":": self._handle_integer,
            b"$": self._handle_string,
            b"*": self._handle_array,
            b"%": self._handle_dict,
        }

    async def handle_request(self, stream_reader: StreamReader):
        # parse a request from a client
        try:
            first_byte = await stream_reader.readexactly(1)

        except IncompleteReadError:
            raise Disconnect()

        try:
            return await self._handlers[first_byte](stream_reader)
        except KeyError:
            raise CommandError("bad request")

    async def _handle_simple_string(self, stream_reader: StreamReader):
        n = await stream_reader.readline()
        return n.rstrip(b'\r\n')

    async def _handle_error(self, stream_reader: StreamReader):
        n = await stream_reader.readline()
        return Error(n.rstrip(b'\r\n'))

    async def _handle_integer(self, stream_reader: StreamReader):
        return int(await stream_reader.readline())

    async def _handle_string(self, stream_reader: StreamReader):
        length = int(await stream_reader.readline())
        if length == -1:  # protocol's special case for NULL
            return None
        length += 2  # include the trailing \r\n in count
        resp = await stream_reader.readexactly(length)
        return resp[:-2]

    async def _handle_array(self, stream_reader: StreamReader):
        num_elements = int(await stream_reader.readline())
        return [await self.handle_request(stream_reader) for _ in range(num_elements)]

    async def _handle_dict(self, stream_reader: StreamReader):
        num_items = int(await stream_reader.readline())
        elements = [await self.handle_request(stream_reader) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    async def write_response(self, stream_writer: StreamWriter, data: dict):
        # serialize the response data and send to client
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        stream_writer.write(buf.getvalue())
        await stream_writer.drain()

    def _write(self, buf: BytesIO, data):
        if isinstance(data, str):
            data = data.encode("utf-8")

        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, int):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, Error):
            buf.write(b'-%s\r\n' % data.message.encode("utf-8"))
        elif isinstance(data, (list, tuple)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b'$-1\r\n')
        else:
            raise CommandError('unrecognized type: %s' % type(data))
