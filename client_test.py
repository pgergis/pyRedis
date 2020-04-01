"""
Integration test will attempt to hit running PygRedis server, so make sure that's running
"""

import asyncio

from client import Client


client = Client()

async def tests():
    await client.start()

    try:
        assert await client.mset('k1', 'v1', 'k2', ['v2-0', 1, 'v2-2'], 'k3', 'v3') == 3

        assert await client.get('k2') == [b'v2-0', 1, b'v2-2']

        assert await client.mget('k3', 'k1') == [b'v3', b'v1']

        assert await client.delete('k1') == 1

        assert await client.get('k1') is None

        assert await client.delete('k1') == 0

        assert await client.set('kx', {'vx': {'vy': 0, 'vz': [1,2,3]}}) == 1

        assert await client.get('kx') == {b'vx': {b'vy': 0, b'vz': [1,2,3]}}

        await client.delete('kx')

        assert await client.flush() == 2

        print("tests pass")

    except Exception as exc:
        print("tests failed with exception:", str(exc))
        raise


# TODO: test concurrency with multiple connected clients
# TODO: use lower-level sockets/threading to allow fn calls in repl
asyncio.run(tests(), debug=True)
