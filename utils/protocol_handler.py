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

    def handle_request(self, reader):
        # parse a request from a client
        first_byte = reader.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            return self._handlers[first_byte](reader)
        except KeyError:
            raise CommandError("bad request")

    def _handle_simple_string(self, reader):
        n = reader.readline()
        return n.rstrip(b"\r\n")

    def _handle_error(self, reader):
        n = reader.readline()
        return Error(n.rstrip(b"\r\n"))

    def _handle_integer(self, reader):
        return int(reader.readline())

    def _handle_string(self, reader):
        length = int(reader.readline())
        if length == -1:  # protocol's special case for NULL
            return None
        length += 2  # include the trailing \r\n in count
        resp = reader.read(length)
        return resp[:-2]

    def _handle_array(self, reader):
        num_elements = int(reader.readline())
        return [self.handle_request(reader) for _ in range(num_elements)]

    def _handle_dict(self, reader):
        num_items = int(reader.readline())
        elements = [
            self.handle_request(reader) for _ in range(num_items * 2)
        ]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, writer, data: dict):
        # serialize the response data and send to client
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        writer.write(buf.getvalue())
        writer.flush()

    def _write(self, buf: BytesIO, data):
        if isinstance(data, str):
            data = data.encode("utf-8")

        if isinstance(data, bytes):
            buf.write(b"$%d\r\n%s\r\n" % (len(data), data))
        elif isinstance(data, int):
            buf.write(b":%d\r\n" % data)
        elif isinstance(data, Error):
            buf.write(b"-%s\r\n" % data.message.encode("utf-8"))
        elif isinstance(data, (list, tuple)):
            buf.write(b"*%d\r\n" % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b"%%%d\r\n" % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b"$-1\r\n")
        else:
            raise CommandError("unrecognized type: %s" % type(data))
