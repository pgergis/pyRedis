import asyncio

from server import Server

serv = Server(max_store_size=3)

asyncio.run(serv.run(), debug=True)
