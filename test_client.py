"""
Integration test will attempt to hit running PygRedis server, so make sure that's running
"""

import asyncio

from client import Client


async def run_test(client):
    await client.start()
    await client.flush()

    assert await client.mset("k1", "v1", "k2", ["v2-0", 1, "v2-2"], "k3", "v3") == 3

    assert await client.get("k2") == [b"v2-0", 1, b"v2-2"]

    assert await client.mget("k3", "k1") == [b"v3", b"v1"]

    assert await client.delete("k1") == 1

    assert await client.get("k1") is None

    assert await client.delete("k1") == 0

    assert await client.set("kx", {"vx": {"vy": 0, "vz": [1, 2, 3]}}) == 1

    assert await client.get("kx") == {b"vx": {b"vy": 0, b"vz": [1, 2, 3]}}

    await client.set("extra", ["a", "b", "c"])  # should pop 'k2'
    await client.delete("kx")

    remaining_cache = await client.getall()
    assert list(remaining_cache.keys()) == [b"k3", b"extra"]
    assert await client.flush() == 2

    return True


async def concurrent_tests(clients):
    tasks = [asyncio.ensure_future(run_test(client)) for client in clients]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    if all(results):
        print("tests pass")
    else:
        print("at least some tests failed", results)


# TODO: use lower-level sockets/threading to allow fn calls in repl
test_clients = [Client() for _ in range(5)]
asyncio.run(concurrent_tests(test_clients), debug=True)
