"""
Integration test will attempt to hit running PygRedis server, so make sure that's running
"""
import concurrent.futures

from client import Client


def run_test(client):
    print("running client #", client.id)

    client.mset(f"{client.id}1", "v1", f"{client.id}2", "v2")
    print(client.getall())
    client.stop()

    return True


def concurrent_tests(clients):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(run_test, clients)

    if all([r is True for r in results]):
        print("tests pass")
    else:
        print("at least some tests failed", results)


test_clients = [Client() for _ in range(10)]
concurrent_tests(test_clients)
