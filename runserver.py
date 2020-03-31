import asyncio

from server import Server

serv = Server()

asyncio.run(serv.run(), debug=True)
